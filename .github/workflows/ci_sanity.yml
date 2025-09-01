name: CI • Sanity

on:
  push:
    branches: [ main, master, develop, feature/** ]
  pull_request:

jobs:
  sanity:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install deps
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

      # minimal env so config loads in dev mode
      - name: Set defaults
        run: |
          echo "ENV=dev" >> $GITHUB_ENV
          echo "SQLITE_DB=./data/db/central.db" >> $GITHUB_ENV
          echo "PORTALS_CONFIG=./config/portals.json" >> $GITHUB_ENV

      - name: Run sanity checks
        run: python scripts/ci_sanity.py
