import argparse
import asyncio
from datetime import UTC, datetime, timedelta
import json
import os

import structlog
import uvicorn
from sqlalchemy.exc import SQLAlchemyError

from acc.config import Settings, get_settings
from acc.engine.normalizer import normalize_d2l_snapshot
from acc.engine.normalizer import NormalizedSnapshot
from acc.db.engine import SessionLocal, init_models
from acc.db.models import AgendaEntry
from acc.db.repository import Repository
from acc.scrapers.d2l import D2LScraper
from acc.scrapers.external import ExternalScraper
from acc.scrapers.snapshots import D2LDashboardSnapshot, ExternalScrapeSnapshot
from acc.scheduler import generate_agenda_plan


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
    agenda_generate_parser = subparsers.add_parser(
        "agenda-generate",
        help="Generate agenda entries from canonical upcoming assignments",
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
        help="Show saved agenda entries from the database",
    )
    agenda_show_parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="How many days ahead to display",
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


async def run_d2l_login(settings: Settings, force: bool) -> int:
    result = await D2LScraper(settings).login(force=force)
    return 0 if result.authenticated else 1


async def run_d2l_check(settings: Settings) -> int:
    authenticated = await D2LScraper(settings).check_saved_session()
    return 0 if authenticated else 1


async def run_d2l_snapshot(settings: Settings, limit: int | None) -> int:
    await D2LScraper(settings).save_snapshot(limit_courses=limit)
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


async def run_agenda_generate(
    settings: Settings,
    *,
    days: int,
    daily_minutes: int,
) -> int:
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            canonical_assignments = list(await repository.list_canonical_assignments())
            now = datetime.now(UTC)
            plan = generate_agenda_plan(
                canonical_assignments,
                now=now,
                horizon_days=days,
                daily_minutes=daily_minutes,
            )
            agenda_entries = [
                AgendaEntry(
                    assignment_id=entry.assignment_id,
                    agenda_date=entry.agenda_date,
                    planned_minutes=entry.planned_minutes,
                    priority_score=entry.priority_score,
                    notes=entry.notes,
                )
                for entry in plan
            ]
            horizon_end = now + timedelta(days=max(0, days - 1))
            candidate_assignment_ids = sorted(
                {
                    assignment.id
                    for assignment in canonical_assignments
                    if assignment.due_date is not None
                    and now.date() <= assignment.due_date.date() <= horizon_end.date()
                }
            )
            saved_count = await repository.replace_agenda_entries(agenda_entries, candidate_assignment_ids)
            await session.commit()
    except (OSError, SQLAlchemyError) as error:
        raise RuntimeError(
            "Could not connect to the configured database. "
            "Ensure ACC_DATABASE_URL is correct and any required database file or service is available."
        ) from error

    print(
        json.dumps(
            {
                "agenda_entries_saved": saved_count,
                "assignments_planned": len({entry.assignment_id for entry in plan}),
                "days": days,
                "daily_minutes": daily_minutes,
            },
            indent=2,
        )
    )
    return 0


async def run_agenda_show(settings: Settings, *, days: int) -> int:
    try:
        await init_models()

        async with SessionLocal() as session:
            repository = Repository(session)
            now = datetime.now(UTC)
            canonical_assignments = {
                assignment.id: assignment
                for assignment in await repository.list_canonical_assignments()
            }
            entries = await repository.list_agenda_entries(
                date_from=now,
                date_to=now + timedelta(days=max(0, days - 1)),
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
                    else entry.assignment.title
                    if entry.assignment
                    else entry.assignment_id,
                    "course_code": entry.assignment.course.code
                    if entry.assignment and entry.assignment.course
                    else None,
                    "notes": entry.notes,
                }
                for entry in entries
            ],
            indent=2,
        )
    )
    return 0


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    settings = apply_cli_overrides(get_settings(), args)
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(settings.playwright_browsers_path.resolve())
    os.environ["TMPDIR"] = f"{settings.runtime_tmp_dir.resolve()}/"

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
        raise SystemExit(asyncio.run(run_agenda_show(settings, days=args.days)))

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
