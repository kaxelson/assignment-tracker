import argparse
import asyncio
from datetime import UTC, datetime, timedelta
import json
import os
import sys

import structlog
import uvicorn
from sqlalchemy.exc import SQLAlchemyError

from acc.ai.syllabus_parser import parse_saved_syllabi
from acc.ai.crawl_extractor import CrawlExtractionSnapshot, CrawlExtractor
from acc.config import Settings, get_settings
from acc.engine.normalizer import normalize_d2l_snapshot
from acc.engine.normalizer import NormalizedSnapshot
from acc.db.engine import SessionLocal, init_models
from acc.db.repository import Repository
from acc.progress import ProgressCallback
from acc.scrapers.d2l import D2LScraper
from acc.scrapers.external import ExternalScraper
from acc.scrapers.crawl import CrawlScraper
from acc.scrapers.snapshots import CrawlSnapshot, D2LDashboardSnapshot, ExternalScrapeSnapshot
from acc.scheduler import generate_agenda_plan

RefreshProgressFn = ProgressCallback


def cli_sync_progress(headline: str, detail: str | None = None, *, fraction: float | None = None) -> None:
    """Print crawl-sync-db steps on stderr so stdout stays the final JSON summary."""
    suffix = ""
    if fraction is not None:
        suffix = f" [{int(round(fraction * 100))}%]"
    if detail:
        print(f"{headline}: {detail}{suffix}", file=sys.stderr, flush=True)
    else:
        print(f"{headline}{suffix}", file=sys.stderr, flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Academic Command Center CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    login_parser = subparsers.add_parser("d2l-login", help="Authenticate with D2L")
    login_parser.add_argument("--force", action="store_true", help="Ignore saved session state")
    login_parser.add_argument("--headless", action="store_true", help="Run browser headlessly")

    subparsers.add_parser("d2l-check", help="Verify saved D2L session state")
    snapshot_parser = subparsers.add_parser(
        "d2l-snapshot",
        help="Scrape D2L courses and upcoming events into a JSON snapshot",
    )
    snapshot_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit how many course home pages to scrape",
    )
    subparsers.add_parser(
        "d2l-normalize",
        help="Normalize the saved D2L snapshot into course and assignment records",
    )
    subparsers.add_parser(
        "d2l-sync-db",
        help="Persist the normalized D2L snapshot into the configured database",
    )
    subparsers.add_parser(
        "external-snapshot",
        help="Scrape Pearson/Cengage assignments using the saved D2L snapshot and session",
    )
    subparsers.add_parser(
        "external-sync-db",
        help="Persist the external platform snapshot into the configured database",
    )
    crawl_sync_parser = subparsers.add_parser(
        "crawl-sync-db",
        help="Persist crawl AI extraction results into the database (replaces D2L/external rows for crawled courses)",
    )
    crawl_sync_parser.add_argument(
        "--mode",
        choices=("full", "additive"),
        default="full",
        help="`full` prunes missing assignments for crawled courses; `additive` only upserts.",
    )
    crawl_snapshot_parser = subparsers.add_parser(
        "crawl-snapshot",
        help="Persist crawl artifacts for D2L and external course pages",
    )
    crawl_snapshot_parser.add_argument(
        "--course-id",
        default=None,
        help="Only crawl one course by its internal course id",
    )
    crawl_snapshot_parser.add_argument(
        "--limit-courses",
        type=int,
        default=None,
        help="Limit how many courses to crawl",
    )
    crawl_snapshot_parser.add_argument(
        "--max-external-details",
        type=int,
        default=None,
        help="Limit how many external assignment detail pages to crawl per course",
    )
    crawl_snapshot_parser.add_argument(
        "--no-screenshots",
        action="store_true",
        help="Skip full-page screenshots and only persist HTML plus visible text",
    )
    crawl_snapshot_parser.add_argument(
        "--ai-navigation",
        action="store_true",
        help=(
            "Use OpenAI on each crawled page to choose which links to follow next "
            "(requires ACC_OPENAI_API_KEY; same as ACC_CRAWL_AI_NAVIGATION=true)"
        ),
    )
    crawl_extract_parser = subparsers.add_parser(
        "crawl-extract",
        help="Use OpenAI to extract assignment facts and grading rules from crawl artifacts",
    )
    crawl_extract_parser.add_argument(
        "--course-id",
        default=None,
        help="Only extract one course by its internal course id",
    )
    syllabus_parse_parser = subparsers.add_parser(
        "syllabus-parse",
        help="Parse saved syllabus text into structured course data using OpenAI",
    )
    syllabus_parse_parser.add_argument(
        "--course-id",
        default=None,
        help="Only parse one course by its internal course id",
    )
    syllabus_parse_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-parse courses even if they already have parsed syllabus data",
    )
    agenda_generate_parser = subparsers.add_parser(
        "agenda-generate",
        help="Print an agenda plan from canonical assignments (computed at runtime; not saved)",
    )
    agenda_generate_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="How many days ahead to plan",
    )
    agenda_generate_parser.add_argument(
        "--daily-minutes",
        type=int,
        default=120,
        help="Soft daily planning budget in minutes",
    )
    agenda_show_parser = subparsers.add_parser(
        "agenda-show",
        help="Print the runtime agenda plan as JSON (same planner as the dashboard)",
    )
    agenda_show_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="How many days ahead to display",
    )
    agenda_show_parser.add_argument(
        "--daily-minutes",
        type=int,
        default=120,
        help="Soft daily planning budget in minutes",
    )

    serve_parser = subparsers.add_parser("serve", help="Run the FastAPI dashboard")
    serve_parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    return parser


def configure_logging() -> None:
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(20),
        logger_factory=structlog.PrintLoggerFactory(),
    )


def apply_cli_overrides(settings: Settings, args: argparse.Namespace) -> Settings:
    overrides: dict[str, object] = {}
    if getattr(args, "headless", False):
        overrides["browser_headless"] = True
    return settings.model_copy(update=overrides) if overrides else settings


def prepare_runtime_environment(settings: Settings) -> None:
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(settings.playwright_browsers_path.resolve())
    os.environ["TMPDIR"] = f"{settings.runtime_tmp_dir.resolve()}/"


async def run_d2l_login(
    settings: Settings,
    force: bool,
    on_progress: RefreshProgressFn | None = None,
) -> int:
    result = await D2LScraper(settings).login(force=force, on_progress=on_progress)
    return 0 if result.authenticated else 1


async def run_d2l_check(settings: Settings) -> int:
    authenticated = await D2LScraper(settings).check_saved_session()
    return 0 if authenticated else 1


async def run_d2l_snapshot(
    settings: Settings,
    limit: int | None,
    on_progress: RefreshProgressFn | None = None,
) -> int:
    await D2LScraper(settings).save_snapshot(limit_courses=limit, on_progress=on_progress)
    return 0


async def run_d2l_normalize(settings: Settings) -> int:
    if not settings.d2l_snapshot_path.exists():
        raise FileNotFoundError(
            f"No D2L snapshot found at {settings.d2l_snapshot_path}. Run `acc d2l-snapshot` first."
        )
    snapshot = D2LDashboardSnapshot.model_validate_json(
        settings.d2l_snapshot_path.read_text(encoding="utf-8")
    )
    normalized = normalize_d2l_snapshot(snapshot, timezone=settings.timezone)
    settings.d2l_normalized_path.write_text(
        json.dumps(normalized.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return 0


async def run_d2l_sync_db(settings: Settings) -> int:
    if not settings.d2l_normalized_path.exists():
        raise FileNotFoundError(
            f"No normalized D2L data found at {settings.d2l_normalized_path}. Run `acc d2l-normalize` first."
        )

    normalized = NormalizedSnapshot.model_validate_json(
        settings.d2l_normalized_path.read_text(encoding="utf-8")
    )
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            summary = await repository.sync_normalized_snapshot(normalized)
            await session.commit()
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            {
                "courses_upserted": summary.courses_upserted,
                "assignments_upserted": summary.assignments_upserted,
                "assignments_deleted": summary.assignments_deleted,
            },
            indent=2,
        )
    )
    return 0


async def run_external_snapshot(settings: Settings) -> int:
    await ExternalScraper(settings).save_snapshot()
    return 0


async def run_crawl_snapshot(
    settings: Settings,
    *,
    course_id: str | None,
    limit_courses: int | None,
    max_external_details: int | None,
    capture_screenshots: bool,
    on_progress: RefreshProgressFn | None = None,
) -> int:
    await CrawlScraper(settings).save_snapshot(
        course_id=course_id,
        limit_courses=limit_courses,
        max_external_details=max_external_details,
        capture_screenshots=capture_screenshots,
        on_progress=on_progress,
    )
    return 0


async def run_crawl_extract(
    settings: Settings,
    *,
    course_id: str | None,
    on_progress: RefreshProgressFn | None = None,
) -> int:
    if settings.openai_api_key is None:
        raise RuntimeError("ACC_OPENAI_API_KEY is required for crawl extraction.")

    snapshot = await CrawlExtractor(settings).save_snapshot(
        course_id=course_id,
        on_progress=on_progress,
    )
    print(
        json.dumps(
            {
                "courses_extracted": len(snapshot.courses),
                "path": str(settings.crawl_extracted_path),
            },
            indent=2,
        )
    )
    return 0


async def run_crawl_sync_db(
    settings: Settings,
    on_progress: RefreshProgressFn | None = None,
    *,
    mode: str = "full",
) -> int:
    if not settings.crawl_extracted_path.exists():
        raise FileNotFoundError(
            f"No crawl extraction found at {settings.crawl_extracted_path}. "
            "Run `acc crawl-extract` first."
        )

    snapshot = CrawlExtractionSnapshot.model_validate_json(
        settings.crawl_extracted_path.read_text(encoding="utf-8")
    )
    manifest: CrawlSnapshot | None = None
    if settings.crawl_snapshot_path.exists():
        manifest = CrawlSnapshot.model_validate_json(
            settings.crawl_snapshot_path.read_text(encoding="utf-8")
        )

    courses = list(snapshot.courses)
    total_courses = len(courses)

    def report_db_course(index: int, total: int, code: str) -> None:
        if on_progress is not None and total > 0:
            on_progress(
                "Database",
                f"Upserting {code} ({index}/{total})...",
                fraction=min(1.0, index / total),
            )

    if on_progress is not None:
        on_progress(
            "Database",
            f"Writing {total_courses} course(s) and their assignments...",
            fraction=0.0 if total_courses else 1.0,
        )

    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            summary = await repository.sync_crawl_extraction_snapshot(
                snapshot,
                crawl_manifest=manifest,
                on_course_progress=report_db_course if on_progress is not None else None,
                prune_missing_assignments=(mode != "additive"),
            )
            await session.commit()
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    if on_progress is not None:
        on_progress(
            "Database",
            "Cleaning up stale crawl-sourced assignments...",
            fraction=1.0,
        )

    print(
        json.dumps(
            {
                "courses_upserted": summary.courses_upserted,
                "assignments_upserted": summary.assignments_upserted,
                "assignments_deleted": summary.assignments_deleted,
                "mode": mode,
            },
            indent=2,
        )
    )
    return 0


async def run_external_sync_db(settings: Settings) -> int:
    if not settings.external_snapshot_path.exists():
        raise FileNotFoundError(
            "No external snapshot found at "
            f"{settings.external_snapshot_path}. Run `acc external-snapshot` first."
        )

    snapshot = ExternalScrapeSnapshot.model_validate_json(
        settings.external_snapshot_path.read_text(encoding="utf-8")
    )
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            summary = await repository.sync_external_snapshot(snapshot)
            await session.commit()
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            {
                "courses_upserted": summary.courses_upserted,
                "assignments_upserted": summary.assignments_upserted,
                "assignments_deleted": summary.assignments_deleted,
            },
            indent=2,
        )
    )
    return 0


async def run_syllabus_parse(
    settings: Settings,
    *,
    course_id: str | None,
    force: bool,
) -> int:
    if settings.openai_api_key is None:
        raise RuntimeError(
            "ACC_OPENAI_API_KEY is required for syllabus parsing."
        )

    try:
        await init_models()

        async with SessionLocal() as session:
            summary = await parse_saved_syllabi(
                session,
                settings,
                force=force,
                course_id=course_id,
            )
            await session.commit()
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            {
                "courses_parsed": summary.courses_parsed,
                "courses_skipped": summary.courses_skipped,
                "courses_failed": summary.courses_failed,
            },
            indent=2,
        )
    )
    return 0


async def run_agenda_generate(
    settings: Settings,
    *,
    days: int,
    daily_minutes: int,
    on_progress: RefreshProgressFn | None = None,
) -> int:
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            canonical_assignments = list(await repository.list_canonical_assignments())
            if on_progress is not None:
                on_progress(
                    "Agenda",
                    f"Planning from {len(canonical_assignments)} canonical assignment(s)...",
                    fraction=0.2,
                )
            now = datetime.now(UTC)
            plan = generate_agenda_plan(
                canonical_assignments,
                now=now,
                horizon_days=days,
                daily_minutes=daily_minutes,
            )
            if on_progress is not None:
                on_progress(
                    "Agenda",
                    f"Built {len(plan)} plan row(s) (computed at runtime; not persisted).",
                    fraction=1.0,
                )
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            {
                "agenda_plan_rows": len(plan),
                "assignments_planned": len({entry.assignment_id for entry in plan}),
                "days": days,
                "daily_minutes": daily_minutes,
            },
            indent=2,
        )
    )
    return 0


async def run_agenda_show(settings: Settings, *, days: int, daily_minutes: int = 120) -> int:
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            now = datetime.now(UTC)
            canonical_assignments = {
                assignment.id: assignment
                for assignment in await repository.list_canonical_assignments()
            }
            plan = generate_agenda_plan(
                list(canonical_assignments.values()),
                now=now,
                horizon_days=days,
                daily_minutes=daily_minutes,
            )
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            [
                {
                    "agenda_date": entry.agenda_date.isoformat(),
                    "planned_minutes": entry.planned_minutes,
                    "priority_score": entry.priority_score,
                    "assignment_id": entry.assignment_id,
                    "assignment_title": canonical_assignments[entry.assignment_id].title
                    if entry.assignment_id in canonical_assignments
                    else entry.assignment_id,
                    "course_code": canonical_assignments[entry.assignment_id].course.code
                    if entry.assignment_id in canonical_assignments
                    and canonical_assignments[entry.assignment_id].course
                    else None,
                    "notes": entry.notes,
                }
                for entry in plan
            ],
            indent=2,
        )
    )
    return 0


async def run_refresh_pipeline(
    settings: Settings,
    *,
    include_external: bool = True,
    include_syllabus_parse: bool = False,
    agenda_days: int = 7,
    daily_minutes: int = 120,
    refresh_mode: str = "full",
    on_progress: RefreshProgressFn | None = None,
) -> dict[str, object]:
    """Refresh DB from crawl: D2L snapshot -> crawl-snapshot -> crawl-extract -> crawl-sync-db.

    ``include_external`` is kept for call-site compatibility and is ignored.
    ``on_progress`` is called with headline, optional detail, and optional ``fraction`` (0..1).
    """
    def phase(
        headline: str,
        detail: str | None = None,
        *,
        fraction: float | None = None,
    ) -> None:
        if on_progress is not None:
            on_progress(headline, detail, fraction=fraction)

    phase("Environment", "Preparing browser paths and temp directories...", fraction=0.0)
    prepare_runtime_environment(settings)
    phase("D2L session", "Verifying login in the browser...", fraction=0.05)
    await run_d2l_login(settings, force=False, on_progress=phase)
    phase("D2L snapshot", "Course list, grades, announcements, and content modules...", fraction=0.1)
    await run_d2l_snapshot(settings, limit=None, on_progress=phase)

    if settings.openai_api_key is None:
        raise RuntimeError(
            "ACC_OPENAI_API_KEY is required for dashboard refresh. "
            "Refresh uses crawl snapshot, OpenAI extraction, and crawl-sync-db."
        )

    phase("Crawl", "Capturing D2L pages and linked publisher surfaces...", fraction=0.2)
    await run_crawl_snapshot(
        settings,
        course_id=None,
        limit_courses=None,
        max_external_details=None,
        capture_screenshots=True,
        on_progress=phase,
    )
    phase("AI extraction", "Reading saved pages and extracting assignments...", fraction=0.55)
    await run_crawl_extract(settings, course_id=None, on_progress=phase)
    phase("Database", f"Writing courses and assignments ({refresh_mode} mode)...", fraction=0.85)
    try:
        await run_crawl_sync_db(settings, on_progress=phase, mode=refresh_mode)
    except TypeError:
        # Backward-compatible for monkeypatched tests/helpers with older signature.
        await run_crawl_sync_db(settings, on_progress=phase)

    syllabus_parsed = False
    if include_syllabus_parse and settings.openai_api_key is not None:
        phase("Syllabus", "Parsing saved syllabus text with AI...")
        await run_syllabus_parse(
            settings,
            course_id=None,
            force=False,
        )
        syllabus_parsed = True

    return {
        "d2l_snapshot_refreshed": True,
        "crawl_synced": True,
        "syllabus_parsed": syllabus_parsed,
        "agenda_days": agenda_days,
        "daily_minutes": daily_minutes,
        "refresh_mode": refresh_mode,
    }


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    settings = apply_cli_overrides(get_settings(), args)
    prepare_runtime_environment(settings)

    if args.command == "d2l-login":
        raise SystemExit(asyncio.run(run_d2l_login(settings, args.force)))

    if args.command == "d2l-check":
        raise SystemExit(asyncio.run(run_d2l_check(settings)))

    if args.command == "d2l-snapshot":
        raise SystemExit(asyncio.run(run_d2l_snapshot(settings, args.limit)))

    if args.command == "d2l-normalize":
        raise SystemExit(asyncio.run(run_d2l_normalize(settings)))

    if args.command == "d2l-sync-db":
        raise SystemExit(asyncio.run(run_d2l_sync_db(settings)))

    if args.command == "external-snapshot":
        raise SystemExit(asyncio.run(run_external_snapshot(settings)))

    if args.command == "external-sync-db":
        raise SystemExit(asyncio.run(run_external_sync_db(settings)))

    if args.command == "crawl-snapshot":
        crawl_settings = (
            settings.model_copy(update={"crawl_ai_navigation": True})
            if args.ai_navigation
            else settings
        )
        raise SystemExit(
            asyncio.run(
                run_crawl_snapshot(
                    crawl_settings,
                    course_id=args.course_id,
                    limit_courses=args.limit_courses,
                    max_external_details=args.max_external_details,
                    capture_screenshots=not args.no_screenshots,
                )
            )
        )

    if args.command == "crawl-extract":
        raise SystemExit(
            asyncio.run(
                run_crawl_extract(
                    settings,
                    course_id=args.course_id,
                    on_progress=cli_sync_progress,
                )
            )
        )

    if args.command == "crawl-sync-db":
        raise SystemExit(
            asyncio.run(
                run_crawl_sync_db(
                    settings,
                    on_progress=cli_sync_progress,
                    mode=args.mode,
                )
            )
        )

    if args.command == "syllabus-parse":
        raise SystemExit(
            asyncio.run(
                run_syllabus_parse(
                    settings,
                    course_id=args.course_id,
                    force=args.force,
                )
            )
        )

    if args.command == "agenda-generate":
        raise SystemExit(
            asyncio.run(
                run_agenda_generate(
                    settings,
                    days=args.days,
                    daily_minutes=args.daily_minutes,
                )
            )
        )

    if args.command == "agenda-show":
        raise SystemExit(
            asyncio.run(
                run_agenda_show(
                    settings,
                    days=args.days,
                    daily_minutes=args.daily_minutes,
                )
            )
        )

    if args.command == "serve":
        uvicorn.run(
            "acc.dashboard.app:app",
            host=settings.dashboard_host,
            port=settings.dashboard_port,
            reload=args.reload,
        )
        return

    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
