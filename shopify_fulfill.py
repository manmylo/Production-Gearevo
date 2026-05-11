"""
shopify_fulfill.py
------------------
Mark a Shopify order's open fulfillment_orders as fulfilled.

Triggered by the webapp via GitHub Actions (workflow_dispatch) when staff
clicks "Collected" on a Shopify-sourced order in the dashboard.

Spec:
  - Fulfills only fulfillment_orders in 'open' state (idempotent — if the
    order is already fully fulfilled, this script exits 0 with a log line).
  - No tracking info attached.
  - notify_customer = false (we already WhatsApp the customer ourselves).
  - Uses Shopify Admin API 2024-01 — the modern fulfillment_orders +
    /fulfillments.json flow. The legacy POST /orders/{id}/fulfillments.json
    endpoint was deprecated in 2022-07 and we don't use it.

Required env vars:
  SHOPIFY_STORE_URL    — e.g. mystore.myshopify.com
  SHOPIFY_ACCESS_TOKEN — admin API token with read_orders + write_orders
  ORDER_ID             — Shopify numeric order ID (NOT the #41524 order name)

Exit codes:
  0 — fulfilled successfully OR nothing left to fulfill (already done)
  1 — error (bad token, missing order, network, etc.)
"""

import os
import sys
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
SHOPIFY_STORE_URL    = os.environ["SHOPIFY_STORE_URL"]
SHOPIFY_ACCESS_TOKEN = os.environ["SHOPIFY_ACCESS_TOKEN"]
ORDER_ID             = os.environ["ORDER_ID"].strip()

API_VERSION = "2024-01"
BASE_URL    = f"https://{SHOPIFY_STORE_URL}/admin/api/{API_VERSION}"
HEADERS     = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type":           "application/json",
}


def fetch_fulfillment_orders(order_id: str) -> list[dict]:
    """Returns the list of fulfillment_orders for the order."""
    url  = f"{BASE_URL}/orders/{order_id}/fulfillment_orders.json"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    if resp.status_code == 404:
        log.error("Order %s not found in Shopify (404)", order_id)
        sys.exit(1)
    resp.raise_for_status()
    return resp.json().get("fulfillment_orders", [])


def fulfill_order(order_id: str) -> int:
    """
    Returns the number of fulfillments created. 0 means the order was
    already fully fulfilled (or has no fulfillable items).
    """
    fos = fetch_fulfillment_orders(order_id)
    if not fos:
        log.info("Order %s has no fulfillment_orders — nothing to do.", order_id)
        return 0

    # Filter to those still open (not closed, cancelled, or already fulfilled)
    open_fos = [fo for fo in fos if fo.get("status") == "open"]
    log.info(
        "Order %s: %d fulfillment_order(s) total, %d open",
        order_id, len(fos), len(open_fos),
    )
    for fo in fos:
        log.info("  fo=%s status=%s items=%d", fo.get("id"), fo.get("status"), len(fo.get("line_items") or []))

    if not open_fos:
        log.info("Order %s is already fully fulfilled — no action needed.", order_id)
        return 0

    # Build the fulfillments payload. One fulfillment per open
    # fulfillment_order, fulfilling all line items in it (no partial).
    # No tracking, no customer notification.
    created = 0
    for fo in open_fos:
        line_items = fo.get("line_items") or []
        if not line_items:
            log.info("  fo=%s has no line items — skipping", fo.get("id"))
            continue

        payload = {
            "fulfillment": {
                "notify_customer": False,
                "line_items_by_fulfillment_order": [
                    {
                        "fulfillment_order_id": fo["id"],
                        # omit fulfillment_order_line_items → fulfills all
                    }
                ],
            }
        }
        url  = f"{BASE_URL}/fulfillments.json"
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=20)

        if resp.status_code == 201:
            data = resp.json().get("fulfillment", {})
            log.info("  ✓ Created fulfillment %s for fo=%s (status=%s)",
                     data.get("id"), fo["id"], data.get("status"))
            created += 1
        else:
            log.error(
                "  ✗ Failed to fulfill fo=%s — HTTP %d: %s",
                fo["id"], resp.status_code, resp.text[:500],
            )
            # Don't sys.exit — try the rest of the FOs first so a partial
            # success isn't lost. We exit with error code only if NOTHING
            # got fulfilled.

    if created == 0 and open_fos:
        log.error("Order %s: had %d open fulfillment_order(s) but none could be fulfilled.", order_id, len(open_fos))
        sys.exit(1)

    return created


def main():
    log.info("Fulfilling Shopify order_id=%s", ORDER_ID)
    n = fulfill_order(ORDER_ID)
    if n == 0:
        log.info("Done — no new fulfillments created (order already fulfilled).")
    else:
        log.info("Done — created %d fulfillment(s).", n)


if __name__ == "__main__":
    main()
