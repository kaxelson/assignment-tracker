# Academic Command Center

Academic Command Center is a Python 3.11+ project for aggregating coursework data from D2L and related learning platforms into a single planning workflow.

This first milestone provides:

- a minimal Python application scaffold
- async SQLAlchemy models for core academic data
- a FastAPI dashboard backed by the synced SQLite data
- a Playwright-based D2L login flow with session persistence
- a D2L snapshot command for courses, tool links, announcements, syllabus topics, external learning tools, upcoming due items, and grades
- a D2L normalization command for internal course and assignment records
- an external platform snapshot command for Pearson and Cengage assignment scraping
- a crawl snapshot command that persists HTML, visible text, screenshots, and row-level artifacts for D2L, Cengage, and Pearson course surfaces (by default D2L targets come from the D2L snapshot with coursework-focused heuristics; optional OpenAI-assisted navigation keeps those seeds and, on each visited page, uses visible text plus on-page links to decide which extra same-course URLs to enqueue; Pearson rows click through See score so fragments include the resolved score text)
- a crawl extraction command that uses OpenAI to turn those artifacts into structured assignment facts and course grading rules
- an AI syllabus parsing command that structures grading policies and late rules
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
uv run acc crawl-snapshot
uv run acc crawl-extract
uv run acc crawl-sync-db
uv run acc crawl-sync-db --mode additive
uv run acc syllabus-parse
uv run acc agenda-generate --days 7 --daily-minutes 120
uv run acc agenda-show --days 7
uv run acc serve --reload
```

The snapshot command writes JSON to `.state/d2l-snapshot.json`.
The normalization command writes JSON to `.state/d2l-normalized.json`.
Normalization merges **every** syllabus-module content extract for each course (not only the first topic) and appends grading- or policy-like topics from other Content modules when titles match, into one `syllabus_raw_text` blob for `syllabus-parse`.
The dashboard exposes `GET /api/debug/provenance?course_id=&assignment_id=&limit=` for the SQL **provenance** audit trail (URLs, artifact paths, JSON details). Populate it by calling `Repository.record_provenance_event` from pipelines as they gain traceability hooks.
The external snapshot command writes JSON to `.state/external-snapshot.json`.
The crawl snapshot command writes a manifest to `.state/crawl-snapshot.json` and stores the captured HTML, text, and optional screenshots under `.state/crawl-artifacts/<timestamp>/`.
For D2L, the default crawl seeds a standard set of course surfaces: `home`, `Content`, `Assignments`, `Quizzes/Exams`, `Grades`, `Calendar`, `Announcements`, plus each announcement detail URL from the snapshot; during calendar capture it also switches to `Full Schedule` and saves that view as a separate artifact so assignment due dates are preserved. With `ACC_CRAWL_AI_NAVIGATION=true` (and `ACC_OPENAI_API_KEY` set), D2L crawling starts from those same seed pages and then asks the model which additional same-course links on each visited page are worth following (bounded by `ACC_CRAWL_AI_MAX_D2L_PAGES` and `ACC_CRAWL_AI_MAX_LINKS_PER_PAGE`); Cengage and Pearson extra nav pages use the same model when that mode is on. You can also pass `--ai-navigation` to `acc crawl-snapshot` for a one-off run.
For Pearson and Cengage, it also follows in-course links and left-nav items that look like assignments or grades (skipping eText, video, and obvious support or marketing URLs), up to `ACC_CRAWL_MAX_EXTERNAL_NAV_PAGES` extra destinations per course (default 40).
Use `uv run acc crawl-snapshot --course-id <course-id>` when you want to debug or refresh one course without recrawling everything.
The crawl extraction command reads `.state/crawl-snapshot.json`, sends chunked crawl artifacts to OpenAI, supplements that with deterministic D2L content due-date parsing, and writes structured per-course results to `.state/crawl-extracted.json`; extracted assignment facts now include `rationale` plus `evidence_spans` (artifact id + supporting quote), and chunk-level provenance events store those traces for audit. Progress lines go to stderr; **sending** / structlog **openai.request** lines appear when a request actually acquires a concurrency slot, not when work is only queued — cap is `ACC_CRAWL_EXTRACT_CONCURRENCY`. A **response received** line follows each completed call. The JSON summary on stdout is unchanged.
You can rerun `uv run acc crawl-extract --course-id <course-id>` incrementally; it replaces that course in the saved extraction snapshot instead of discarding previously extracted courses.
The `crawl-sync-db` command reads `.state/crawl-extracted.json` and upserts courses and assignments from the AI extraction (progress lines go to stderr; the JSON summary on stdout is unchanged) (grading categories, late-policy text, calculated current grade, and assignment rows). In `--mode full` (default), for each course in that file it removes other assignment rows for the same course that are not in the extraction, so the database reflects the crawl analysis; in `--mode additive`, it only upserts and does not prune missing rows. Optional `d2l-sync-db` / `external-sync-db` remain available if you want to populate the database from the older JSON snapshot pipelines instead.
The syllabus parsing command reads saved syllabus text from synced courses, calls OpenAI, and stores structured grading, late-policy, and review-flag data back onto each course row. Each attempted parse records a row in SQL **provenance** (`llm_syllabus_parse` on success, `llm_syllabus_parse_error` on failure), including a text preview and JSON detail (model id, counts, review flags, or error metadata).
Dashboard counts, upcoming-work views, and the saved agenda panel reconcile overlapping D2L and external assignments into a single canonical task entry per course.
For courses like Python that assign work through weekly D2L announcements but host the actual activities in Cengage, the dashboard now cross-references announcement links, D2L grades, and Cengage status so unassigned MindTap inventory does not inflate the live coursework counts.
Course Health also uses those canonical assignments to merge visible D2L and external grades into one effective current grade, preferring fresher external assignment scores when they duplicate D2L rows, keeping D2L due dates when D2L provides them, and avoiding double counting across systems.
Whenever the course has **syllabus-derived grade categories** with weights (from syllabus parse or crawl extraction), the effective course grade is blended by **category** (Exams vs Homework, etc.): each category’s share of the final comes from the syllabus, and items inside the category are averaged by points. D2L per-row “weight” values are not used as percent-of-final in that case. If no weighted categories are stored yet, the estimate falls back to LMS row weights until the syllabus is captured.
The agenda generation command plans saved `agenda_entries` from those canonical assignments, and `agenda-show` prints the current saved agenda window as JSON.
If syllabus parsing has populated grade categories, grading scale cutoffs, or late-policy details, agenda priority scoring uses those signals to rank work before allocating each day's minutes.
With the default setup, `ACC_DATABASE_URL` points at a local SQLite file in `.state/acc.db`, so no separate database service is required.
After at least one successful `crawl-sync-db` (or the legacy D2L/external sync pair), `uv run acc serve` exposes a browser dashboard at `/` and a JSON summary at `/api/overview`.
The HTML dashboard is organized around `Today's Focus`, `Due Soon`, `Next 7 Days`, and `Course Health`, with planner rationale plus a `Go to assignment` button on assignment cards and a `Syllabus` link on each course card when a D2L course URL is available.
Incomplete work that is already past its due calendar day is still planned on **today** (and appears under today's card in the 7-day agenda) until it is submitted or completed.
The dashboard `Refresh now` button runs D2L login, `d2l-snapshot`, full `crawl-snapshot` (including full-page screenshots), `crawl-extract`, and `crawl-sync-db`, then regenerates the agenda. It requires `ACC_OPENAI_API_KEY` because extraction is part of refresh.

## Environment

All configuration is loaded from environment variables prefixed with `ACC_`.

Important values:

- `ACC_D2L_BASE_URL`
- `ACC_D2L_USERNAME`
- `ACC_D2L_PASSWORD`
- `ACC_OPENAI_API_KEY`
- `ACC_OPENAI_MODEL` (GPT-5-family chat models are supported; unsupported optional sampling fields like explicit `temperature` are omitted automatically)
- `ACC_OPENAI_TIMEOUT_SECONDS` (per-request socket timeout, default `90`; models whose name includes `nano` use at least **240s** so slower tiers still have time on large crawl prompts)
- `ACC_OPENAI_RETRY_MAX_ATTEMPTS` (total tries per chat completion, including the first; default `5`; retries on timeouts and HTTP 429)
- `ACC_OPENAI_RETRY_BASE_DELAY_SECONDS` (exponential backoff base for retries, default `1`; capped at 120s; 429 responses honor `Retry-After` when present)
- `ACC_OPENAI_MAX_CONCURRENT_REQUESTS` (global cap on simultaneous OpenAI chat calls across link picking, crawl extract, syllabus parse; default `8`)
- `ACC_FETCH_MAX_CONCURRENT_GLOBAL` (max concurrent Playwright navigations across all hosts; default `12`)
- `ACC_FETCH_MAX_CONCURRENT_PER_HOST` (per-host navigation cap; default `4`)
- `ACC_TIMEZONE` (assignment due instants are stored in UTC; the dashboard and agenda planner use this zone when no crawl metadata exists; when `raw_scraped_data` includes `due_on` / offset `due_at` from crawl, those drive calendar labels and day buckets so fixed-offset deadlines are not shifted by CST vs `America/Chicago` DST)
- `ACC_BROWSER_HEADLESS`
- `ACC_BROWSER_COURSE_CONCURRENCY` (parallel D2L / crawl / external course scrapes, default `4`)
- `ACC_CENGAGE_ACTIVITY_TIMEOUT_MS` (MindTap activity list wait budget; default `90000`; effective budget is the greater of this and `ACC_BROWSER_TIMEOUT_MS`)
- `ACC_PLAYWRIGHT_BROWSERS_PATH`
- `ACC_RUNTIME_TMP_DIR`
- `ACC_D2L_STORAGE_STATE_PATH`
- `ACC_D2L_SNAPSHOT_PATH`
- `ACC_D2L_NORMALIZED_PATH`
- `ACC_EXTERNAL_SNAPSHOT_PATH`
- `ACC_CRAWL_SNAPSHOT_PATH`
- `ACC_CRAWL_EXTRACTED_PATH`
- `ACC_CRAWL_ARTIFACTS_DIR`
- `ACC_CRAWL_EXTRACT_CONCURRENCY` (max parallel OpenAI requests during `crawl-extract`, default `5`)
- `ACC_CRAWL_PAGE_CONCURRENCY` (max parallel in-course browser tabs per publisher surface during crawl, default `1`; the old name `ACC_CRAWL_D2L_PAGE_CONCURRENCY` still works if the new variable is unset)
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
