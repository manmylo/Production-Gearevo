"""
shopify_sync.py
---------------
Fetches recent Shopify orders, filters for Layo services
(Servis Asahan, Gearevo Kydex, Laser Engraving), maps them
to app service labels, and upserts to Firestore.

New/updated behaviour:
  - NEW orders    → inserted as before
  - EDITED orders → mutable fields (name, phone, service, note,
                    fulfilmentType) are patched in Firestore
  - CANCELLED orders → status set to "cancelled", shopifyCancelReason
                       added, storeId requirement bypassed
  - Delivery method  → fulfilmentType: "Shipping" | "In-Store Pickup"
"""

import os
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIG — all values come from GitHub Secrets
# ──────────────────────────────────────────────
SHOPIFY_STORE_URL    = os.environ["SHOPIFY_STORE_URL"]
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
FIREBASE_PROJECT_ID  = os.environ["FIREBASE_PROJECT_ID"]
SA_JSON              = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]

# ──────────────────────────────────────────────
# SERVICE MAPPING  (Shopify line item → app label)
# ──────────────────────────────────────────────
SERVICE_KEYWORDS = {
    "servis asah pisau": "Sharpening",
    "gearevo kydex":     "Kydex Sheath",
    "laser engraving":   "Engraving",
}


def map_services(line_items: list[dict]) -> str | None:
    found = []
    for item in line_items:
        title = (item.get("title") or "").lower()
        for keyword, label in SERVICE_KEYWORDS.items():
            if keyword in title and label not in found:
                found.append(label)

    if not found:
        return None

    has_sharp   = "Sharpening"   in found
    has_kydex   = "Kydex Sheath" in found
    has_engrave = "Engraving"    in found

    if has_sharp and has_kydex and has_engrave:
        return "Sharpening + Kydex + Engraving"
    if has_sharp and has_kydex:
        return "Sharpening + Kydex"
    if has_sharp and has_engrave:
        return "Sharpening + Engraving"
    if has_kydex and has_engrave:
        return "Kydex + Engraving"
    if has_sharp:
        return "Sharpening"
    if has_kydex:
        return "Kydex Sheath"
    if has_engrave:
        return "Engraving"

    return " + ".join(found)


# ──────────────────────────────────────────────
# DELIVERY METHOD
# ──────────────────────────────────────────────
def map_fulfilment_type(order: dict) -> tuple[str, str]:
    """
    Returns (fulfilmentType, carrierName).

    Shipping carriers (→ "Shipping"):
      - J&T Express  (j&t, jnt, j and t)
      - PosLaju      (poslaju, pos laju, pos malaysia)
      - TikTok Shop  (tiktok, tik tok)
      - DHL, GDEX, Ninja, SkyNet, CityLink
      - Any unrecognised shipping line

    In-Store Pickup (→ "In-Store Pickup"):
      - Title/code contains pickup/in-store/collect keywords
      - Shopify's standard local pickup codes/titles
      - GEAREVO (Adiat Resources) — this store's Shopify pickup method name
      - No shipping lines at all (walk-in / POS order)
    """
    shipping_lines = order.get("shipping_lines") or []
    if not shipping_lines:
        return "In-Store Pickup", ""

    PICKUP_KEYWORDS = [
        # Generic pickup words
        "pickup", "pick up", "pick-up", "in store", "in-store",
        "collect", "walk in", "walkin", "layan diri",
        # Shopify standard local pickup identifiers
        "local pickup", "local_pickup", "shopify-local-pickup",
        # This store's specific pickup shipping line title
        "gearevo", "adiat",
    ]

    # Map of keyword → display name
    CARRIER_MAP = [
        (["j&t", "jnt", "j and t"],                "J&T Express"),
        (["poslaju", "pos laju", "pos malaysia"],   "PosLaju"),
        (["tiktok", "tik tok", "tiktok shop"],      "TikTok"),
        (["dhl"],                                   "DHL"),
        (["gdex"],                                  "GDEX"),
        (["ninja"],                                 "Ninja Van"),
        (["skynet"],                                "SkyNet"),
        (["citylink", "city-link"],                 "CityLink"),
    ]

    for line in shipping_lines:
        title    = (line.get("title") or "").lower()
        code     = (line.get("code")  or "").lower()
        combined = title + " " + code

        # Pickup keyword takes priority — check before carrier map
        if any(kw in combined for kw in PICKUP_KEYWORDS):
            return "In-Store Pickup", ""

        # Match known shipping carrier
        for keywords, display_name in CARRIER_MAP:
            if any(kw in combined for kw in keywords):
                return "Shipping", display_name

    # Has shipping lines but no keyword matched — use raw title as carrier name
    raw_title = (shipping_lines[0].get("title") or "").strip()
    return "Shipping", raw_title


# ──────────────────────────────────────────────
# SHOPIFY — fetch orders updated in last 10 min
# ──────────────────────────────────────────────
def fetch_shopify_orders() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url   = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/orders.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    params = {
        "status":         "any",
        "updated_at_min": since,
        "limit":          250,
        "fields": (
            "id,order_number,name,customer,line_items,"
            "created_at,financial_status,"
            "cancelled_at,cancel_reason,shipping_lines"
        ),
    }

    all_orders = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        all_orders.extend(data.get("orders", []))

        link   = resp.headers.get("Link", "")
        url    = None
        params = {}
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break

    log.info("Fetched %d orders from Shopify (updated since %s)", len(all_orders), since)
    return all_orders


# ──────────────────────────────────────────────
# FIRESTORE
# ──────────────────────────────────────────────
def get_firestore_client() -> firestore.Client:
    sa_info     = json.loads(SA_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return firestore.Client(project=FIREBASE_PROJECT_ID, credentials=credentials)


def get_current_due_days(db: firestore.Client) -> int:
    """
    Read the most recent config/{YYYY-MM-DD} doc to get today's due days.
    Falls back to 3 if no config exists.
    """
    try:
        docs = (
            db.collection("config")
            .order_by("setAt", direction=firestore.Query.DESCENDING)
            .limit(1)
            .stream()
        )
        for d in docs:
            return int(d.to_dict().get("days", 3))
    except Exception as e:
        log.warning("Could not read due days config: %s", e)
    return 3


def get_relevant_shopify_docs(
    db: firestore.Client,
    batch_ids: list[str],
) -> dict[str, tuple[str, dict]]:
    """
    Returns { shopifyOrderId: (firestoreDocId, docData) }
    using two cheap targeted queries instead of reading the entire collection:

    Query A — today's Shopify orders (covers new-order dedup for same-day inserts)
    Query B — specific shopifyOrderIds returned in this batch (covers edits /
              cancellations on older orders that predate today)

    Both results are merged into one dict; duplicates (same doc appearing in
    both queries) are deduplicated by Firestore doc ID.
    """
    result: dict[str, tuple[str, dict]] = {}

    # ── Query A: today's orders ────────────────────────────────────
    start_of_day = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    today_docs = (
        db.collection("orders")
        .where("source",    "==", "shopify")
        .where("createdAt", ">=", start_of_day)
        .stream()
    )
    for d in today_docs:
        data = d.to_dict()
        sid  = data.get("shopifyOrderId")
        if sid:
            result[sid] = (d.id, data)

    log.info("Query A (today's orders): %d docs", len(result))

    # ── Query B: specific IDs from this batch (older order edits/cancels) ──
    # Only query IDs not already loaded by Query A
    missing_ids = [bid for bid in batch_ids if bid not in result]

    if missing_ids:
        # Firestore "in" operator supports max 30 values — chunk if needed
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i:i + n]

        batch_hits = 0
        for chunk in chunks(missing_ids, 30):
            batch_docs = (
                db.collection("orders")
                .where("shopifyOrderId", "in", chunk)
                .stream()
            )
            for d in batch_docs:
                data = d.to_dict()
                sid  = data.get("shopifyOrderId")
                if sid and sid not in result:
                    result[sid] = (d.id, data)
                    batch_hits += 1

        log.info("Query B (batch older orders): %d extra docs", batch_hits)

    return result


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    db       = get_firestore_client()
    due_days = get_current_due_days(db)

    raw_orders = fetch_shopify_orders()

    # Extract IDs for orders that match our services — these are the only ones
    # we'll ever read or write, so no point querying Firestore for others
    batch_ids = [
        str(o["id"])
        for o in raw_orders
        if map_services(o.get("line_items", []))
    ]

    existing_docs = get_relevant_shopify_docs(db, batch_ids)
    log.info(
        "Batch: %d relevant Shopify order(s) | Firestore loaded: %d doc(s) | due days: %d",
        len(batch_ids), len(existing_docs), due_days,
    )
    added = updated = cancelled_count = 0

    for order in raw_orders:
        shopify_id = str(order["id"])
        line_items = order.get("line_items", [])

        service = map_services(line_items)
        if not service:
            continue  # not one of our services — ignore

        # ── Customer info ──────────────────────────────────────────
        customer   = order.get("customer") or {}
        first      = (customer.get("first_name") or "").strip()
        last       = (customer.get("last_name")  or "").strip()
        # Filter out "None" string that some integrations produce
        name_parts = [p for p in [first, last] if p and p.lower() != "none"]
        name       = " ".join(name_parts) or "Unknown"
        phone      = (customer.get("phone") or "").strip()
        order_name = order.get("name", "")

        # ── Flags ──────────────────────────────────────────────────
        is_express = any(
            "express" in (item.get("title") or "").lower()
            for item in line_items
        )

        fulfilment_type, carrier_name = map_fulfilment_type(order)

        is_cancelled      = bool(order.get("cancelled_at"))
        cancel_reason_raw = order.get("cancel_reason") or ""
        cancel_reason_map = {
            "customer":  "Cancelled by customer",
            "fraud":     "Cancelled — suspected fraud",
            "inventory": "Cancelled — out of stock",
            "declined":  "Cancelled — payment declined",
            "other":     "Cancelled",
        }
        cancel_reason = cancel_reason_map.get(cancel_reason_raw, "Cancelled")

        # Note field: Express and/or cancel reason
        note_parts = []
        if is_express:
            note_parts.append("Express")
        if is_cancelled:
            note_parts.append(cancel_reason)
        note = ", ".join(note_parts)

        # ─────────────────────────────────────────────────────────
        # UPSERT
        # ─────────────────────────────────────────────────────────
        if shopify_id in existing_docs:
            # ── UPDATE ─────────────────────────────────────────────
            fs_doc_id, fs_data = existing_docs[shopify_id]
            ref     = db.collection("orders").document(fs_doc_id)
            updates = {}

            # Sync all Shopify-owned mutable fields
            if fs_data.get("name")             != name:            updates["name"]             = name
            if fs_data.get("phone")            != phone:           updates["phone"]            = phone
            if fs_data.get("service")          != service:         updates["service"]          = service
            if fs_data.get("shopifyOrderName") != order_name:      updates["shopifyOrderName"] = order_name
            if fs_data.get("fulfilmentType")   != fulfilment_type: updates["fulfilmentType"]   = fulfilment_type
            if fs_data.get("carrierName")      != carrier_name:    updates["carrierName"]      = carrier_name
            if fs_data.get("note")             != note:            updates["note"]             = note

            # Cancellation — only transition once
            if is_cancelled and fs_data.get("status") != "cancelled":
                updates["status"]              = "cancelled"
                updates["shopifyCancelReason"] = cancel_reason
                updates["cancelledAt"]         = firestore.SERVER_TIMESTAMP
                cancelled_count += 1
                log.info("Cancelled order %s (%s) — reason: %s", shopify_id, order_name, cancel_reason)

            if updates:
                ref.update(updates)
                updated += 1
                log.info("Updated order %s (%s) — %s", shopify_id, order_name, list(updates.keys()))
            else:
                log.debug("No changes for order %s", shopify_id)

        else:
            # ── INSERT ─────────────────────────────────────────────
            doc = {
                "shopifyOrderId":   shopify_id,
                "shopifyOrderName": order_name,
                "name":             name,
                "phone":            phone,
                "service":          service,
                "fulfilmentType":   fulfilment_type,
                "carrierName":      carrier_name,
                "storeId":          "",
                "status":           "cancelled" if is_cancelled else "pending",
                "source":           "shopify",
                "note":             note,
                "dueDays":          due_days,
                "createdAt":        firestore.SERVER_TIMESTAMP,
                "notifiedAt":       None,
                "readyAt":          None,
                "collectedAt":      None,
            }
            if is_cancelled:
                doc["shopifyCancelReason"] = cancel_reason
                doc["cancelledAt"]         = firestore.SERVER_TIMESTAMP

            db.collection("orders").add(doc)
            existing_docs[shopify_id] = ("", doc)
            added += 1
            log.info(
                "Added order %s (%s) — service: %s | fulfilment: %s%s%s",
                shopify_id, order_name, service, fulfilment_type,
                f" [{carrier_name}]" if carrier_name else "",
                " [CANCELLED]" if is_cancelled else "",
            )

    log.info(
        "Sync complete. %d added, %d updated (%d cancellations).",
        added, updated, cancelled_count,
    )


if __name__ == "__main__":
    main()
