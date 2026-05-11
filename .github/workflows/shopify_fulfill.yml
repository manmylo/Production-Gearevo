name: Shopify → Mark Fulfilled

on:
  workflow_dispatch:
    inputs:
      order_id:
        description: 'Shopify numeric order ID (NOT the #41524 order name)'
        required: true
        type: string

  repository_dispatch:
    types: [shopify-fulfill]

jobs:
  fulfill:
    runs-on: ubuntu-latest
    # 5 min (was 3). Fulfill itself is ~2s, but actions/checkout sometimes
    # retries on transient GitHub 5xx — give those retries room before being
    # killed by the timeout.
    timeout-minutes: 5

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Cache pip dependencies
        uses: actions/cache@v4
        with:
          path: ~/.cache/pip
          key: ${{ runner.os }}-pip-${{ hashFiles('requirements.txt') }}
          restore-keys: |
            ${{ runner.os }}-pip-

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run Shopify fulfill
        env:
          SHOPIFY_STORE_URL: ${{ secrets.SHOPIFY_STORE_URL }}
          SHOPIFY_ACCESS_TOKEN: ${{ secrets.SHOPIFY_ACCESS_TOKEN }}
          ORDER_ID: ${{ github.event.inputs.order_id || github.event.client_payload.order_id }}
        run: python shopify_fulfill.py
