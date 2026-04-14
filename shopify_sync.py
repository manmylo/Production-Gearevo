"""
shopify_sync.py
---------------
Fetches recent Shopify orders, filters for Layo services
(Servis Asahan, Gearevo Kydex, Laser Engraving), maps them
to app service labels, and pushes new orders to Firestore.
Skips any order whose shopifyOrderId already exists in Firestore.
Runs every 5 minutes via GitHub Actions cron.
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
SHOPIFY_STORE_URL    = os.environ["SHOPIFY_STORE_URL"]       # e.g. yourstore.myshopify.com
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]    # Admin API access token
FIREBASE_PROJECT_ID  = os.environ["FIREBASE_PROJECT_ID"]    # e.g. production-tracker-3a3b1
SA_JSON              = os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"]  # full JSON string

# ──────────────────────────────────────────────
# SERVICE MAPPING  (Shopify line item → app label)
# ──────────────────────────────────────────────
# Keys are lowercase fragments to match against line item titles
SERVICE_KEYWORDS = {
    "servis asah pisau":      "Sharpening",
    "gearevo kydex":          "Kydex Sheath",
    "laser engraving":        "Engraving",
}

# Combined label lookup (sorted longest-match first so combos win)
def map_services(line_items: list[dict]) -> str | None:
    """
    Inspect all line items, collect every matched service,
    return the combined label string or None if no match.
    """
    found = []
    for item in line_items:
        title = (item.get("title") or "").lower()
        for keyword, label in SERVICE_KEYWORDS.items():
            if keyword in title and label not in found:
                found.append(label)

    if not found:
        return None

    # Canonical combination labels
    has_sharp   = "Sharpening"  in found
    has_kydex   = "Kydex Sheath" in found
    has_engrave = "Engraving"   in found

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

    return " + ".join(found)   # fallback


# ──────────────────────────────────────────────
# SHOPIFY  — fetch orders updated in last 10 min
# (overlap to survive any clock drift / late webhooks)
# ──────────────────────────────────────────────
def fetch_shopify_orders() -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url   = f"https://{SHOPIFY_STORE_URL}/admin/api/2024-01/orders.json"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json",
    }
    params = {
        "status":        "any",
        "updated_at_min": since,
        "limit":         250,
        "fields":        "id,order_number,name,customer,line_items,created_at,financial_status",
    }

    all_orders = []
    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        all_orders.extend(data.get("orders", []))

        # Handle Shopify cursor pagination via Link header
        link = resp.headers.get("Link", "")
        url  = None
        params = {}   # clear params for subsequent pages (URL is already complete)
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
                break

    log.info("Fetched %d orders from Shopify (updated since %s)", len(all_orders), since)
    return all_orders


# ──────────────────────────────────────────────
# FIRESTORE  — init client from service-account JSON
# ──────────────────────────────────────────────
def get_firestore_client() -> firestore.Client:
    sa_info     = json.loads(SA_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return firestore.Client(project=FIREBASE_PROJECT_ID, credentials=credentials)


def get_existing_shopify_ids(db: firestore.Client) -> set[str]:
    """Return the set of shopifyOrderId values already in Firestore."""
    docs = db.collection("orders").where("shopifyOrderId", "!=", "").stream()
    return {d.get("shopifyOrderId") for d in docs if d.get("shopifyOrderId")}


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    db = get_firestore_client()
    existing_ids = get_existing_shopify_ids(db)
    log.info("Firestore already has %d Shopify-sourced orders", len(existing_ids))

    raw_orders = fetch_shopify_orders()
    added = 0

    for order in raw_orders:
        shopify_id = str(order["id"])

        # Skip if already synced
        if shopify_id in existing_ids:
            continue

        service = map_services(order.get("line_items", []))
        if not service:
            # Order has none of our target services — ignore
            continue

        # Build customer info
        customer   = order.get("customer") or {}
        first      = customer.get("first_name", "")
        last       = customer.get("last_name", "")
        name       = f"{first} {last}".strip() or "Unknown"
        phone      = (customer.get("phone") or "").strip()
        order_name = order.get("name", "")   # e.g. "#1234" — Shopify display name

        # Detect express order
        is_express = any(
            "express" in (item.get("title") or "").lower()
            for item in order.get("line_items", [])
        )

        doc = {
            "shopifyOrderId": shopify_id,
            "shopifyOrderName": order_name,   # "#1234" — shown in app
            "name":    name,
            "phone":   phone,
            "service": service,
            "storeId": "",           # staff fills this in before first WA is sent
            "status":  "pending",
            "source":  "shopify",
            "note":    "Express" if is_express else "",
            "createdAt":   firestore.SERVER_TIMESTAMP,
            "notifiedAt":  None,
            "readyAt":     None,
            "collectedAt": None,
        }

        db.collection("orders").add(doc)
        existing_ids.add(shopify_id)
        added += 1
        log.info("Added order %s (%s) — service: %s", shopify_id, order_name, service)

    log.info("Sync complete. %d new order(s) added to Firestore.", added)


if __name__ == "__main__":
    main()
