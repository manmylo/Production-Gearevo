"""
shopify_sync.py
---------------
Fetches recent Shopify orders, filters for items whose SKU is in our
service mapping (sku_services.csv), maps them to app service labels,
and upserts to Firestore.

What changed vs. the keyword-based version:
  - SKU-based matching: an order is relevant if any line item's SKU
    appears in sku_services.csv. The CSV is the single source of truth
    for what counts as a service order. Edit + push to update.
  - Store ID auto-fill: a line item with SKU 'GE-OID-N' (1 <= N <= 100)
    sets `storeId` to N on the Firestore doc. The webapp uses this to
    decide between prompting WhatsApp send-out (storeId set) vs.
    prompting staff to fill it in (storeId blank). If multiple distinct
    OIDs appear in one order, the first wins and a warning is logged.
  - Existing manually-entered storeIds are NEVER overwritten with
    blank — anything filled in via the webapp survives later syncs.

Other behaviour (unchanged):
  - NEW orders        -> inserted
  - EDITED orders     -> mutable fields patched (name, phone, service,
                          note, fulfilmentType, storeId-from-OID)
  - CANCELLED orders  -> status 'cancelled', shopifyCancelReason added,
                          storeId requirement bypassed
  - Delivery method   -> fulfilmentType: 'Shipping' | 'In-Store Pickup'
  - EXPRESS / pure-Engraving orders -> auto-collected on insert

Lookback window:
  - Default: 10 minutes (regular cron sync)
  - Set SYNC_LOOKBACK_HOURS env var for backfill (e.g. '24' = last 24h)
"""

import os
import re
import csv
import json
import logging
import requests
from pathlib import Path
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
# LOOKBACK WINDOW
# ──────────────────────────────────────────────
def get_lookback_minutes() -> int:
    """
    Returns the lookback window in minutes.
    - If SYNC_LOOKBACK_HOURS is set and non-empty -> use that (converted to minutes)
    - Otherwise default to 10 minutes (normal cron interval)
    """
    hours_str = os.environ.get("SYNC_LOOKBACK_HOURS", "").strip()
    if hours_str:
        try:
            hours = float(hours_str)
            minutes = int(hours * 60)
            log.info("Backfill mode: looking back %s hours (%d minutes)", hours_str, minutes)
            return max(minutes, 1)
        except ValueError:
            log.warning("Invalid SYNC_LOOKBACK_HOURS value '%s', using default 10 min", hours_str)
    return 10


# ──────────────────────────────────────────────
# SKU → SERVICE MAPPING (loaded from sku_services.csv)
# ──────────────────────────────────────────────
# CSV format (in repo root, alongside this script):
#   product_name,sku,service
#   Knife Sharpening - Standard,SK-001,Sharpening
#   Custom Kydex Sheath,KX-001,Kydex Sheath
#   Laser Engraving - 1 Line,EN-001,Engraving
#
# `service` is normalized to one of: Sharpening, Kydex Sheath, Engraving
# (the combo-label logic below depends on these exact strings). The CSV
# can use any of the tolerated aliases below — e.g. the Excel sheet uses
# "Kydex" which gets normalized to the canonical "Kydex Sheath".
#
# `product_name` is informational only — kept so the CSV mirrors the
# Excel sheet 1:1 and is easy for the team to maintain.
#
# Do NOT add GE-OID-N entries here — those are store-ID markers, not
# services. They're handled separately by extract_store_id().
SKU_MAP_PATH = Path(__file__).parent / "sku_services.csv"

# Map any tolerated CSV value (lowercased) -> canonical service label.
SERVICE_NORMALIZE = {
    "sharpening":      "Sharpening",
    "sharpen":         "Sharpening",
    "kydex":           "Kydex Sheath",
    "kydex sheath":    "Kydex Sheath",
    "engraving":       "Engraving",
    "laser engraving": "Engraving",
}


def load_sku_services() -> dict[str, str]:
    """Load SKU -> service map from CSV. Keys lowercased for case-insensitive lookup."""
    mapping: dict[str, str] = {}
    if not SKU_MAP_PATH.exists():
        log.error("SKU mapping file not found at %s — no orders will match!", SKU_MAP_PATH)
        return mapping
    with open(SKU_MAP_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get("sku") or "").strip()
            raw_service = (row.get("service") or "").strip()
            if not sku or not raw_service:
                continue
            canonical = SERVICE_NORMALIZE.get(raw_service.lower())
            if not canonical:
                log.warning(
                    "SKU '%s' has unknown service '%s' — skipping (accepted: %s)",
                    sku, raw_service, sorted(set(SERVICE_NORMALIZE.values())),
                )
                continue
            mapping[sku.lower()] = canonical
    log.info("Loaded %d SKU -> service mappings from %s", len(mapping), SKU_MAP_PATH.name)
    return mapping


SKU_TO_SERVICE = load_sku_services()


# ──────────────────────────────────────────────
# STORE ID DETECTION
# ──────────────────────────────────────────────
# A line item with SKU 'GE-OID-15' indicates the order belongs to store
# (outlet) 15. This lets the webapp skip the manual store-ID prompt and
# go straight to the WhatsApp send-out step.
STORE_ID_PATTERN = re.compile(r"^GE-OID-(\d+)$", re.IGNORECASE)


def extract_store_id(line_items: list[dict]) -> str:
    """
    Returns the store ID as a string ('1'..'100') if any line item's SKU
    matches GE-OID-N. Returns '' when no match. Logs a warning if more
    than one distinct OID appears in the same order (uses the first).
    """
    found: list[str] = []
    for item in line_items:
        sku = (item.get("sku") or "").strip()
        m = STORE_ID_PATTERN.match(sku)
        if not m:
            continue
        n = int(m.group(1))
        if 1 <= n <= 100:
            found.append(str(n))
    if not found:
        return ""
    if len(set(found)) > 1:
        log.warning(
            "Multiple distinct store IDs in one order: %s — using first ('%s')",
            found, found[0],
        )
    return found[0]


# ──────────────────────────────────────────────
# SERVICE MAPPING (Shopify line item SKU → app label)
# ──────────────────────────────────────────────
def map_services(line_items: list[dict]) -> str | None:
    """Combine all matched services into one label (e.g. 'Sharpening + Kydex')."""
    found: list[str] = []
    for item in line_items:
        sku = (item.get("sku") or "").strip().lower()
        if not sku:
            continue
        label = SKU_TO_SERVICE.get(sku)
        if label and label not in found:
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


def extract_service_line_items(line_items: list[dict]) -> tuple[list[dict], float, int]:
    """
    Returns (matched_items, total_sales, total_quantity) for line items
    whose SKU is in our service mapping. GE-OID-N store-marker SKUs are
    excluded (they're not in the mapping CSV) and so don't count toward
    sales/qty totals.

    Each matched_item is:
        {
          "title":    str,    # original Shopify product title
          "sku":      str,    # the matched SKU (preserves original case)
          "service":  str,    # mapped app label
          "quantity": int,
          "price":    float,  # per-unit price
          "subtotal": float,  # price * quantity (before discounts)
        }
    """
    matched: list[dict] = []
    total_sales = 0.0
    total_qty   = 0

    for item in line_items:
        sku = (item.get("sku") or "").strip()
        if not sku:
            continue
        label = SKU_TO_SERVICE.get(sku.lower())
        if not label:
            continue

        try:
            qty = int(item.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0

        try:
            price = float(item.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0

        subtotal = round(price * qty, 2)

        matched.append({
            "title":    item.get("title") or "",
            "sku":      sku,
            "service":  label,
            "quantity": qty,
            "price":    price,
            "subtotal": subtotal,
        })
        total_sales += subtotal
        total_qty   += qty

    return matched, round(total_sales, 2), total_qty


# ──────────────────────────────────────────────
# DELIVERY METHOD
# ──────────────────────────────────────────────
def map_fulfilment_type(order: dict) -> tuple[str, str]:
    """
    Returns (fulfilmentType, carrierName).

    The store's configured delivery methods in Shopify are:
      1. "In store"              -> In-Store Pickup
      2. "Shipping"              -> Shipping (generic)
      3. "TikTok"                -> Shipping (TikTok)
      4. "PosLaju"               -> Shipping (PosLaju)
      5. "J&T (Peninsular only)" -> Shipping (J&T Express)

    Additional detection:
      - TikTok Shop orders sometimes arrive WITHOUT shipping_lines because
        TikTok handles fulfilment on their side. We detect these via
        order.source_name containing "tiktok" (or similar marketplace
        indicators) and classify them as Shipping/TikTok.
      - Orders with no shipping_lines AND no marketplace source -> In-Store
        Pickup (genuine walk-in / POS orders).
    """
    shipping_lines = order.get("shipping_lines") or []
    source_name    = (order.get("source_name") or "").lower()

    # ── Step 1: If shipping_lines is present, use it (authoritative) ──
    if shipping_lines:
        PICKUP_KEYWORDS = [
            # Explicit in-store / pickup wording
            "in store", "in-store", "instore",
            "pickup", "pick up", "pick-up",
            "collect", "walk in", "walkin", "layan diri",
            # Shopify standard local pickup identifiers
            "local pickup", "local_pickup", "shopify-local-pickup",
            # This store's specific pickup shipping line title
            "gearevo", "adiat",
        ]

        # Ordered carrier keywords — first match wins.
        # J&T comes first so "J&T (Peninsular only)" matches cleanly.
        CARRIER_MAP = [
            (["j&t", "jnt", "j and t"],                 "J&T Express"),
            (["poslaju", "pos laju", "pos malaysia"],    "PosLaju"),
            (["tiktok", "tik tok", "tiktok shop"],       "TikTok"),
            (["dhl"],                                    "DHL"),
            (["gdex"],                                   "GDEX"),
            (["ninja"],                                  "Ninja Van"),
            (["skynet"],                                 "SkyNet"),
            (["citylink", "city-link"],                  "CityLink"),
        ]

        for line in shipping_lines:
            title    = (line.get("title")  or "").lower()
            code     = (line.get("code")   or "").lower()
            source   = (line.get("source") or "").lower()
            combined = f"{title} {code} {source}"

            # Pickup keyword takes priority — check before carrier map
            if any(kw in combined for kw in PICKUP_KEYWORDS):
                return "In-Store Pickup", ""

            # Match known shipping carrier
            for keywords, display_name in CARRIER_MAP:
                if any(kw in combined for kw in keywords):
                    return "Shipping", display_name

        # Has shipping lines but no keyword matched — fall through to raw title.
        raw_title = (shipping_lines[0].get("title") or "").strip() or "Shipping"
        return "Shipping", raw_title

    # ── Step 2: No shipping_lines. Check marketplace/source signals first. ──
    MARKETPLACE_SOURCES = [
        (["tiktok", "tik_tok"], "TikTok"),
        (["shopee"],            "Shopee"),
        (["lazada"],            "Lazada"),
    ]
    for keywords, display_name in MARKETPLACE_SOURCES:
        if any(kw in source_name for kw in keywords):
            return "Shipping", display_name

    # ── Step 3: Draft orders or any other order with no shipping line.
    # The decisive signal is whether the customer has a shipping address.
    shipping_address = order.get("shipping_address")
    if shipping_address and isinstance(shipping_address, dict):
        return "Shipping", "Shipping"

    # ── Step 4: No shipping lines, no marketplace, no shipping address ──
    return "In-Store Pickup", ""


# ──────────────────────────────────────────────
# AUTO-COLLECT SHORTCUT
# ──────────────────────────────────────────────
def is_auto_collect(service: str, note: str) -> bool:
    """
    Express orders and pure Engraving-only orders are done on-the-spot —
    no workflow tracking needed. Mark them collected immediately.
    Combo services (Sharpening + Engraving, Kydex + Engraving, etc.)
    still need full workflow tracking.
    """
    is_express = "express" in (note or "").lower()
    is_engrave = (service or "").strip().lower() == "engraving"
    return is_express or is_engrave


# ──────────────────────────────────────────────
# SHOPIFY — fetch orders updated in lookback window
# ──────────────────────────────────────────────
def fetch_shopify_orders(lookback_minutes: int) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url   = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/orders.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    params = {
        "status":         "any",
        "updated_at_min": since,
        "limit":          250,
        # source_name, tags, and shipping_address help classify orders that
        # arrive without shipping_lines (draft orders, TikTok/marketplace, etc.)
        # line_items already includes 'sku' by default, no need to request it.
        "fields": (
            "id,order_number,name,customer,line_items,"
            "created_at,financial_status,"
            "cancelled_at,cancel_reason,shipping_lines,"
            "source_name,tags,shipping_address"
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

    log.info("Fetched %d orders from Shopify (updated since %s, lookback %d min)", len(all_orders), since, lookback_minutes)
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

    Queries only the specific shopifyOrderIds returned in this batch.
    """
    result: dict[str, tuple[str, dict]] = {}

    if not batch_ids:
        return result

    # Firestore "in" operator supports max 30 values — chunk if needed
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for chunk in chunks(batch_ids, 30):
        docs = (
            db.collection("orders")
            .where("shopifyOrderId", "in", chunk)
            .stream()
        )
        for d in docs:
            data = d.to_dict()
            sid  = data.get("shopifyOrderId")
            if sid:
                result[sid] = (d.id, data)

    log.info("Firestore lookup: %d/%d batch IDs already exist", len(result), len(batch_ids))
    return result


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    lookback_minutes = get_lookback_minutes()
    db       = get_firestore_client()
    due_days = get_current_due_days(db)

    raw_orders = fetch_shopify_orders(lookback_minutes)

    # ── Diagnostic accumulators (reported at end of run) ──
    # These help answer "why didn't order #41679 sync?" without manually
    # poking around — the summary at the bottom of the log shows exactly
    # which SKUs got dropped and which line items had no SKU at all.
    diag_skipped_orders     = 0
    diag_unmatched_sku_count: dict[str, int] = {}   # unknown SKU -> times seen
    diag_unmatched_examples: dict[str, str]   = {}  # unknown SKU -> sample title
    diag_no_sku_items: list[tuple[str, str, str]] = []  # (order_name, source, title)

    # Hint keywords — used ONLY for the diagnostic warning about line
    # items that have no SKU but look like services. They do not affect
    # matching itself; that's still pure SKU.
    DIAG_TITLE_HINTS = (
        "servis asah", "asah pisau", "kydex", "engraving", "engrav",
        "sandwich", "belt loop", "perbaiki sumbing",
    )

    # Extract IDs for orders that match a service SKU — these are the only
    # ones we'll ever read or write, so no point querying Firestore for others
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
            # ── Diagnostic: this order didn't match any service SKU ──
            # Record what was in it so the run summary can pinpoint
            # missing entries in sku_services.csv or POS items with
            # blank SKUs.
            diag_skipped_orders += 1
            order_name = order.get("name", "")
            source     = order.get("source_name", "") or "—"
            for item in line_items:
                sku   = (item.get("sku") or "").strip()
                title = (item.get("title") or "").strip()
                if sku:
                    diag_unmatched_sku_count[sku] = diag_unmatched_sku_count.get(sku, 0) + 1
                    diag_unmatched_examples.setdefault(sku, title)
                else:
                    title_l = title.lower()
                    if any(kw in title_l for kw in DIAG_TITLE_HINTS):
                        diag_no_sku_items.append((order_name, source, title))
            continue  # not one of our services — ignore

        # ── Extract line item details (for reporting: qty, sales) ──
        matched_items, total_sales, total_qty = extract_service_line_items(line_items)

        # ── Store ID from GE-OID-N marker SKU (if present) ──
        extracted_store_id = extract_store_id(line_items)

        # ── Customer info ──────────────────────────────────────────
        customer   = order.get("customer") or {}
        first      = (customer.get("first_name") or "").strip()
        last       = (customer.get("last_name")  or "").strip()
        # Filter out "None" string that some integrations produce
        name_parts = [p for p in [first, last] if p and p.lower() != "none"]
        name       = " ".join(name_parts) or "Unknown"
        phone      = (customer.get("phone") or "").strip()
        order_name = order.get("name", "")

        # ── Shopify order creation date (actual order date, not sync time) ──
        shopify_created_at_str = order.get("created_at", "")
        if shopify_created_at_str:
            shopify_created_at = datetime.fromisoformat(shopify_created_at_str.replace("Z", "+00:00"))
        else:
            shopify_created_at = datetime.now(timezone.utc)

        # ── Flags ──────────────────────────────────────────────────
        is_express = any(
            "express" in (item.get("title") or "").lower()
            for item in line_items
        )

        fulfilment_type, carrier_name = map_fulfilment_type(order)

        # Diagnostic log — helps debug future misclassifications
        shipping_titles = [
            (sl.get("title") or sl.get("code") or "").strip()
            for sl in (order.get("shipping_lines") or [])
        ]
        has_ship_addr = bool(order.get("shipping_address"))
        log.info(
            "Fulfilment for %s (%s): type=%s carrier=%s | shipping_lines=%s | source_name=%s | has_shipping_address=%s | storeId=%s",
            shopify_id, order_name, fulfilment_type, carrier_name or "—",
            shipping_titles or "[]", order.get("source_name") or "—",
            has_ship_addr, extracted_store_id or "—",
        )

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

            # Sync reporting fields — always refresh so edits on Shopify propagate
            if fs_data.get("lineItems")   != matched_items: updates["lineItems"]   = matched_items
            if fs_data.get("totalSales")  != total_sales:   updates["totalSales"]  = total_sales
            if fs_data.get("totalQty")    != total_qty:     updates["totalQty"]    = total_qty

            # Store ID: only patch when Shopify gave us one and it differs.
            # Never blank out an existing manually-entered storeId.
            existing_store_id = (fs_data.get("storeId") or "").strip()
            if extracted_store_id and extracted_store_id != existing_store_id:
                updates["storeId"] = extracted_store_id

            # Sync createdAt to Shopify's actual order creation date
            existing_created = fs_data.get("createdAt")
            if existing_created and hasattr(existing_created, 'timestamp'):
                if abs(existing_created.timestamp() - shopify_created_at.timestamp()) > 60:
                    updates["createdAt"] = shopify_created_at
            elif not existing_created:
                updates["createdAt"] = shopify_created_at

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

            # Auto-collect if the order became Express/Engraving and isn't terminal
            current_status = fs_data.get("status", "pending")
            if (
                is_auto_collect(service, note)
                and current_status not in ("collected", "cancelled")
            ):
                ref.update({
                    "status":      "collected",
                    "collectedAt": firestore.SERVER_TIMESTAMP,
                })
                log.info(
                    "Auto-collected order %s (%s) — Express/Engraving shortcut",
                    shopify_id, order_name,
                )

        else:
            # ── INSERT ─────────────────────────────────────────────
            auto_collect = is_auto_collect(service, note)

            doc = {
                "shopifyOrderId":   shopify_id,
                "shopifyOrderName": order_name,
                "name":             name,
                "phone":            phone,
                "service":          service,
                "lineItems":        matched_items,
                "totalSales":       total_sales,
                "totalQty":         total_qty,
                "fulfilmentType":   fulfilment_type,
                "carrierName":      carrier_name,
                # Auto-filled from GE-OID-N when present, else "" (webapp prompts to fill)
                "storeId":          extracted_store_id,
                "status":           "cancelled" if is_cancelled else ("collected" if auto_collect else "pending"),
                "source":           "shopify",
                "note":             note,
                "dueDays":          due_days,
                "createdAt":        shopify_created_at,
                "notifiedAt":       None,
                "readyAt":          None,
                "collectedAt":      firestore.SERVER_TIMESTAMP if auto_collect else None,
            }
            if is_cancelled:
                doc["shopifyCancelReason"] = cancel_reason
                doc["cancelledAt"]         = firestore.SERVER_TIMESTAMP

            db.collection("orders").add(doc)
            existing_docs[shopify_id] = ("", doc)
            added += 1
            log.info(
                "Added order %s (%s) — service: %s | fulfilment: %s%s | storeId: %s%s%s",
                shopify_id, order_name, service, fulfilment_type,
                f" [{carrier_name}]" if carrier_name else "",
                extracted_store_id or "—",
                " [CANCELLED]"      if is_cancelled  else "",
                " [AUTO-COLLECTED]" if auto_collect  else "",
            )

    # ──────────────────────────────────────────────
    # DIAGNOSTIC SUMMARY  (added to help debug missing orders)
    # ──────────────────────────────────────────────
    if diag_skipped_orders or diag_unmatched_sku_count or diag_no_sku_items:
        log.info("=" * 64)
        log.info("DIAGNOSTIC SUMMARY")
        log.info("Total Shopify orders fetched:      %d", len(raw_orders))
        log.info("Matched (one+ SKU in mapping):     %d", len(batch_ids))
        log.info("Skipped (no SKU matched):          %d", diag_skipped_orders)
        if diag_unmatched_sku_count:
            log.info("")
            log.info("Top unmatched SKUs (not in sku_services.csv) — add these to the CSV if they are services:")
            top = sorted(diag_unmatched_sku_count.items(), key=lambda x: (-x[1], x[0]))[:25]
            for sku, count in top:
                example = diag_unmatched_examples.get(sku, "")
                log.info("  %-30s  seen %3d time(s)  | example title: %s",
                         sku, count, example[:60])
        if diag_no_sku_items:
            log.info("")
            log.warning(
                "Line items with NO SKU but title looks like a service "
                "(%d total) — fix the SKU on these products in Shopify, or "
                "they will keep being skipped:",
                len(diag_no_sku_items),
            )
            seen: set[tuple[str, str]] = set()
            shown = 0
            for order_name, source, title in diag_no_sku_items:
                key = (order_name, title)
                if key in seen:
                    continue
                seen.add(key)
                log.warning("  %-10s  source=%-15s  title=%s",
                            order_name, source[:15], title[:60])
                shown += 1
                if shown >= 25:
                    log.warning("  ... and %d more", len(diag_no_sku_items) - shown)
                    break
        log.info("=" * 64)

    log.info(
        "Sync complete. %d added, %d updated (%d cancellations).",
        added, updated, cancelled_count,
    )


if __name__ == "__main__":
    main()
