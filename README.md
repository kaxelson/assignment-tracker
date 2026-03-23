# Academic Command Center

Academic Command Center is a Python 3.11+ project for aggregating coursework data from D2L and related learning platforms into a single planning workflow.

This first milestone provides:

- a minimal Python application scaffold
- async SQLAlchemy models for core academic data
- a FastAPI dashboard backed by the synced SQLite data
- a Playwright-based D2L login flow with session persistence
- a D2L snapshot command for courses, tool links, syllabus topics, external learning tools, upcoming due items, and grades
- a D2L normalization command for internal course and assignment records
- an external platform snapshot command for Pearson and Cengage assignment scraping
- database sync commands for persisting normalized D2L and external platform records into SQLAlchemy models

## Prerequisites

- Python 3.11+
- Playwright browser dependencies
- SQLite for local development

## Quick Start

1. Install `uv`.
2. Let `uv` provision Python 3.11 and the local virtual environment.
3. Sync dependencies and install Playwright browsers.
4. Copy `.env.example` to `.env` and fill in the D2L settings.

```bash
uv python install 3.11
uv venv --python 3.11
source .venv/bin/activate
uv sync --extra dev
PLAYWRIGHT_BROWSERS_PATH=.playwright uv run playwright install chromium
cp .env.example .env
```

## D2L Login

The D2L auth flow is designed for real-world SSO and MFA:

- if saved session state exists, it reuses it first
- if credentials are configured, it attempts to autofill known login forms
- if Oakton's identity provider or MFA needs manual steps, it waits for you to finish them in the browser
- once authenticated, it saves storage state to `.state/d2l-storage.json`

Run the login flow:

```bash
uv run acc d2l-login
```

Useful flags:

```bash
uv run acc d2l-login --force
uv run acc d2l-login --headless
uv run acc d2l-check
uv run acc d2l-snapshot --limit 2
uv run acc d2l-normalize
uv run acc d2l-sync-db
uv run acc external-snapshot
uv run acc external-sync-db
uv run acc agenda-generate --days 7 --daily-minutes 120
uv run acc agenda-show --days 7
uv run acc serve --reload
```

The snapshot command writes JSON to `.state/d2l-snapshot.json`.
The normalization command writes JSON to `.state/d2l-normalized.json`.
The external snapshot command writes JSON to `.state/external-snapshot.json`.
The database sync commands create missing tables and upsert both normalized D2L data and external platform assignments into the configured database, including extracted syllabus text and detected primary external platform metadata on each course.
Dashboard counts, upcoming-work views, and the saved agenda panel reconcile overlapping D2L and external assignments into a single canonical task entry per course.
The agenda generation command plans saved `agenda_entries` from those canonical assignments, and `agenda-show` prints the current saved agenda window as JSON.
With the default setup, `ACC_DATABASE_URL` points at a local SQLite file in `.state/acc.db`, so no separate database service is required.
After `uv run acc d2l-sync-db` and `uv run acc external-sync-db`, `uv run acc serve` exposes a browser dashboard at `/` and a JSON summary at `/api/overview`.

## Environment

All configuration is loaded from environment variables prefixed with `ACC_`.

Important values:

- `ACC_D2L_BASE_URL`
- `ACC_D2L_USERNAME`
- `ACC_D2L_PASSWORD`
- `ACC_TIMEZONE`
- `ACC_BROWSER_HEADLESS`
- `ACC_PLAYWRIGHT_BROWSERS_PATH`
- `ACC_RUNTIME_TMP_DIR`
- `ACC_D2L_STORAGE_STATE_PATH`
- `ACC_D2L_SNAPSHOT_PATH`
- `ACC_D2L_NORMALIZED_PATH`
- `ACC_EXTERNAL_SNAPSHOT_PATH`
- `ACC_DATABASE_URL`

## Project Layout

```text
src/acc/
  config.py
  main.py
  dashboard/
  db/
  scrapers/
tests/
```

## Validation

The current scaffold includes a small config test suite:

```bash
uv run pytest
```
