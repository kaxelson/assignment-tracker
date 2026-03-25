import asyncio
from dataclasses import dataclass
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from html import escape
import json
import re
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from acc.config import Settings, get_settings
from acc.db.engine import get_session
from acc.db.models import Assignment
from acc.db.repository import (
    CanonicalAssignment,
    Repository,
    assignment_grade_pct,
    canonical_due_calendar_date,
    canonical_due_instant_utc,
    explain_effective_course_grade,
)
from acc.main import run_refresh_pipeline
from acc.scheduler.planner import PlannedAgendaEntry, explain_priority, generate_agenda_plan, priority_score

settings = get_settings()

app = FastAPI(title="Academic Command Center")


@dataclass(slots=True)
class DashboardRefreshState:
    running: bool = False
    current_phase: str | None = None
    current_detail: str | None = None
    progress_fraction: float | None = None
    last_started_at: datetime | None = None
    last_completed_at: datetime | None = None
    last_error: str | None = None
    last_result: dict[str, object] | None = None
    requested_mode: str = "full"
    task: asyncio.Task | None = None


refresh_state = DashboardRefreshState()


def api_due_at_iso(assignment: CanonicalAssignment) -> str | None:
    raw = assignment.raw_scraped_data if isinstance(assignment.raw_scraped_data, dict) else None
    if raw and isinstance(raw.get("due_at"), str):
        text = raw["due_at"].strip()
        if text:
            return text
    instant = canonical_due_instant_utc(assignment)
    if instant is None:
        return None
    return instant.isoformat().replace("+00:00", "Z")


def due_calendar_iso(assignment: CanonicalAssignment) -> str | None:
    cal = canonical_due_calendar_date(assignment)
    return cal.isoformat() if cal is not None else None


def due_calendar_from_item(item: dict[str, object]) -> date | None:
    raw = item.get("due_calendar_date")
    if isinstance(raw, str) and raw.strip():
        try:
            return date.fromisoformat(raw.strip())
        except ValueError:
            return None
    return None


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "env": settings.env}


def build_empty_overview(settings: Settings, error: str | None = None) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_ready": error is None,
        "error": error,
        "d2l_storage_state": settings.d2l_storage_state_path.exists(),
        "refresh_status": serialize_refresh_status(),
        "summary": {
            "course_count": 0,
            "assignment_count": 0,
            "upcoming_count": 0,
            "urgent_count": 0,
        },
        "courses": [],
        "upcoming_assignments": [],
        "agenda_entries": [],
        "agenda_days": [],
    }


def format_due_label(assignment: Assignment) -> str | None:
    if assignment.due_date is None:
        return None

    return format_local_due_label(assignment.due_date)


def aggregate_planned_agenda(
    plan: list[PlannedAgendaEntry],
    canonical_assignments: dict[str, CanonicalAssignment],
    *,
    today: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    aggregated_by_key: dict[tuple[str, str], dict[str, object]] = {}

    for entry in plan:
        agenda_date = entry.agenda_date.isoformat()
        assignment_id = entry.assignment_id
        canonical = canonical_assignments.get(assignment_id)
        if is_dashboard_hidden_assignment(canonical.title if canonical is not None else None):
            continue
        assignment_title = canonical.title if canonical is not None else assignment_id
        course_code = canonical.course.code if canonical is not None and canonical.course else None
        key = (agenda_date, assignment_id)
        external_url = canonical.external_url if canonical is not None else None
        due_at = api_due_at_iso(canonical) if canonical is not None else None
        due_calendar_iso_str = due_calendar_iso(canonical) if canonical is not None else None
        due_cal = canonical_due_calendar_date(canonical) if canonical is not None else None
        due_label = format_due_label_for_canonical(canonical) if canonical is not None else None
        priority_reasons = (
            explain_priority(canonical, today=localize_datetime(today).date())
            if canonical is not None
            else []
        )
        priority_reasons = normalize_due_reason_labels(
            priority_reasons,
            due_at=due_at,
            now=today,
            due_calendar_date=due_cal,
        )
        status = canonical.status if canonical is not None else "upcoming"

        existing = aggregated_by_key.get(key)
        if existing is None:
            aggregated_by_key[key] = {
                "agenda_date": agenda_date,
                "planned_minutes": entry.planned_minutes,
                "priority_score": entry.priority_score,
                "assignment_id": assignment_id,
                "assignment_title": assignment_title,
                "course_code": course_code,
                "external_url": external_url,
                "due_at": due_at,
                "due_calendar_date": due_calendar_iso_str,
                "due_label": due_label,
                "status": status,
                "priority_reasons": priority_reasons,
                "notes": entry.notes,
            }
            continue

        existing["planned_minutes"] += entry.planned_minutes
        existing["priority_score"] = max(existing["priority_score"], entry.priority_score)
        if not existing["external_url"] and external_url:
            existing["external_url"] = external_url
        if not existing["due_at"] and due_at:
            existing["due_at"] = due_at
        if not existing.get("due_calendar_date") and due_calendar_iso_str:
            existing["due_calendar_date"] = due_calendar_iso_str
        if not existing.get("due_label") and due_label:
            existing["due_label"] = due_label
        if not existing["priority_reasons"] and priority_reasons:
            existing["priority_reasons"] = priority_reasons
        if not existing["notes"] and entry.notes:
            existing["notes"] = entry.notes

    agenda_entries = sorted(
        aggregated_by_key.values(),
        key=lambda item: (
            str(item["agenda_date"]),
            due_sort_value(item.get("due_at")),
            str(item.get("course_code") or "").lower(),
            str(item["assignment_title"]).lower(),
        ),
    )

    agenda_days: list[dict[str, object]] = []
    grouped_by_day: dict[str, list[dict[str, object]]] = {}
    for item in agenda_entries:
        grouped_by_day.setdefault(str(item["agenda_date"]), []).append(item)

    for agenda_date in sorted(grouped_by_day):
        day_items = grouped_by_day[agenda_date]
        agenda_days.append(
            {
                "agenda_date": agenda_date,
                "total_minutes": sum(int(item["planned_minutes"]) for item in day_items),
                "entry_count": len(day_items),
                "items": day_items,
            }
        )

    return agenda_entries, agenda_days


async def load_dashboard_overview(
    session: AsyncSession,
    settings: Settings,
) -> dict[str, object]:
    repository = Repository(session)
    now = datetime.now(UTC)
    canonical_assignment_list = [
        assignment
        for assignment in await repository.list_canonical_assignments()
        if is_dashboard_relevant_assignment(assignment)
    ]
    canonical_assignments = {assignment.id: assignment for assignment in canonical_assignment_list}
    course_rows = await repository.list_course_overview()
    assignment_counts = Counter(assignment.course_id for assignment in canonical_assignment_list)
    actionable_due_assignments = sorted(
        [
            assignment
            for assignment in canonical_assignment_list
            if assignment.due_date is not None and assignment.status not in {"completed", "graded", "submitted"}
        ],
        key=due_priority_sort_key,
    )
    upcoming_assignments = actionable_due_assignments[:12]
    agenda_plan = generate_agenda_plan(
        canonical_assignment_list,
        now=now,
        horizon_days=7,
        daily_minutes=120,
    )
    upcoming_counts = Counter(assignment.course_id for assignment in actionable_due_assignments)
    urgent_count = sum(
        1
        for assignment in actionable_due_assignments
        if counts_for_urgent_work(assignment) and is_urgent_due_date(assignment, now=now)
    )

    assignments_by_course: dict[str, list[CanonicalAssignment]] = {}
    for assignment in canonical_assignment_list:
        assignments_by_course.setdefault(assignment.course_id, []).append(assignment)

    courses = [
        {
            "id": row.course.id,
            "code": row.course.code,
            "name": row.course.name,
            "semester": row.course.semester,
            "current_grade_pct": row.course.current_grade_pct,
            "assignment_count": assignment_counts.get(row.course.id, 0),
            "upcoming_count": upcoming_counts.get(row.course.id, 0),
            "external_platform": row.course.external_platform,
            "d2l_url": row.course.d2l_url,
            "syllabus_url": infer_course_syllabus_url(row.course),
            "grade_detail": explain_effective_course_grade(
                assignments_by_course.get(row.course.id, []),
                course=row.course,
                now=now,
            ),
        }
        for row in course_rows
    ]
    upcoming = [
        {
            "id": assignment.id,
            "title": assignment.title,
            "course_code": assignment.course.code if assignment.course else assignment.course_id,
            "course_name": assignment.course.name if assignment.course else assignment.course_id,
            "due_at": api_due_at_iso(assignment),
            "due_calendar_date": due_calendar_iso(assignment),
            "due_label": format_due_label_for_canonical(assignment),
            "status": assignment.status,
            "type": assignment.type,
            "grade_pct": assignment_grade_pct(assignment),
            "external_url": assignment.external_url,
            "priority_score": priority_score(assignment, today=localize_datetime(now).date()),
            "priority_reasons": explain_priority(assignment, today=localize_datetime(now).date()),
        }
        for assignment in upcoming_assignments
    ]
    for item in upcoming:
        item["priority_reasons"] = normalize_due_reason_labels(
            item["priority_reasons"],
            due_at=item["due_at"],
            now=now,
            due_calendar_date=due_calendar_from_item(item),
        )
    agenda, agenda_days = aggregate_planned_agenda(
        agenda_plan,
        canonical_assignments,
        today=now,
    )

    return {
        "generated_at": now.isoformat(),
        "database_ready": True,
        "error": None,
        "d2l_storage_state": settings.d2l_storage_state_path.exists(),
        "refresh_status": serialize_refresh_status(),
        "summary": {
            "course_count": len(courses),
            "assignment_count": len(canonical_assignment_list),
            "upcoming_count": len(actionable_due_assignments),
            "urgent_count": urgent_count,
        },
        "courses": courses,
        "upcoming_assignments": upcoming,
        "agenda_entries": agenda,
        "agenda_days": agenda_days,
    }


async def get_dashboard_overview(session: AsyncSession) -> dict[str, object]:
    try:
        return await load_dashboard_overview(session, settings)
    except SQLAlchemyError:
        return build_empty_overview(
            settings,
            error="Database not ready. Run `uv run acc d2l-sync-db` after creating a snapshot.",
        )


def render_dashboard_html(overview: dict[str, object]) -> str:
    summary = overview["summary"]
    courses = overview["courses"]
    upcoming_assignments = overview["upcoming_assignments"]
    agenda_entries = overview["agenda_entries"]
    agenda_days = overview["agenda_days"]
    error = overview["error"]
    generated_at = parse_generated_at(overview)
    local_generated_at = localize_datetime(generated_at)
    refresh_status = overview.get("refresh_status")
    today_label = local_generated_at.strftime("%A, %B %-d")
    today_iso = local_generated_at.date().isoformat()
    today_agenda = [item for item in agenda_entries if str(item["agenda_date"]) == today_iso]
    due_soon_items = upcoming_assignments
    today_minutes = sum(int(item["planned_minutes"]) for item in today_agenda)
    urgent_count = int(summary.get("urgent_count", 0))
    refresh_status_primary = format_refresh_primary_label(refresh_status)
    refresh_status_detail = format_refresh_secondary_label(refresh_status)
    refresh_status_classes = "refresh-status"
    refresh_status_title = ""
    if isinstance(refresh_status, dict):
        if refresh_status.get("running"):
            refresh_status_classes += " refresh-status--running"
        elif refresh_status.get("last_error"):
            refresh_status_classes += " refresh-status--failed"
            refresh_status_title = format_refresh_error_title_attr(refresh_status.get("last_error"))

    focus_items = "".join(render_focus_item(item, generated_at) for item in today_agenda)
    due_soon_cards = "".join(render_due_card(item, generated_at) for item in due_soon_items)
    day_cards = "".join(render_day_card(day, generated_at) for day in agenda_days)
    course_cards = "".join(render_course_card(course) for course in courses)

    if not focus_items:
        focus_items = '<article class="empty-state">No work is scheduled for today yet.</article>'
    if not due_soon_cards:
        due_soon_cards = '<article class="empty-state">No upcoming assignments found.</article>'
    if not day_cards:
        day_cards = '<article class="empty-state">No saved agenda entries yet.</article>'
    if not course_cards:
        course_cards = '<article class="empty-state">No synced course data yet.</article>'

    error_banner = ""
    if error:
        error_banner = f'<section class="alert">{escape(str(error))}</section>'

    page = f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Academic Command Center</title>
    <style>
      :root {{
        --paper: #f6f0e8;
        --canvas: #efe4d5;
        --ink: #1f2630;
        --muted: #677280;
        --line: rgba(31, 38, 48, 0.12);
        --panel: rgba(255, 251, 247, 0.84);
        --panel-strong: rgba(255, 255, 255, 0.94);
        --navy: #20344a;
        --navy-soft: #dbe7f2;
        --sand: #e8d4bd;
        --amber: #a65d2a;
        --amber-soft: #f5dfcf;
        --green: #275948;
        --green-soft: #ddefe4;
        --rose: #7c3642;
        --rose-soft: #f2d6dc;
        --shadow: 0 20px 45px rgba(31, 38, 48, 0.08);
      }}

      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        color: var(--ink);
        font-family: "Avenir Next", "Helvetica Neue", "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at top right, rgba(32, 52, 74, 0.16), transparent 22rem),
          radial-gradient(circle at bottom left, rgba(166, 93, 42, 0.14), transparent 24rem),
          linear-gradient(180deg, var(--paper) 0%, #fbf8f2 52%, var(--canvas) 100%);
      }}

      main {{
        width: min(1200px, calc(100vw - 2rem));
        margin: 0 auto;
        padding: 2rem 0 4rem;
      }}

      h1,
      h2,
      h3,
      p,
      dl,
      ul {{
        margin: 0;
      }}

      .hero {{
        display: grid;
        grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.95fr);
        gap: 1rem;
        padding: 1rem 0;
        margin-bottom: 1rem;
      }}

      .hero-card,
      .panel,
      .metric-card,
      .alert,
      .empty-state {{
        border: 1px solid var(--line);
        border-radius: 1.5rem;
        background: var(--panel);
        backdrop-filter: blur(10px);
        box-shadow: var(--shadow);
      }}

      .hero-card {{
        position: relative;
        overflow: hidden;
        padding: 1.75rem;
        background:
          radial-gradient(circle at top left, rgba(32, 52, 74, 0.18), transparent 22rem),
          linear-gradient(140deg, rgba(255, 255, 255, 0.92), rgba(244, 233, 220, 0.88));
      }}

      .hero-card::after {{
        content: "";
        position: absolute;
        inset: auto -4rem -4rem auto;
        width: 14rem;
        height: 14rem;
        border-radius: 999px;
        background: rgba(166, 93, 42, 0.11);
        filter: blur(1px);
      }}

      .hero-copy {{
        position: relative;
        z-index: 1;
      }}

      .hero-copy h1 {{
        font-family: "Iowan Old Style", "Baskerville", "Times New Roman", serif;
        font-size: clamp(2.5rem, 5vw, 4.4rem);
        line-height: 0.92;
        letter-spacing: -0.05em;
        max-width: 12ch;
      }}

      .hero-copy p {{
        margin-top: 0.9rem;
        color: var(--muted);
        max-width: 38rem;
        font-size: 1rem;
        line-height: 1.55;
      }}

      .eyebrow {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        margin-bottom: 0.75rem;
        color: var(--navy);
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
      }}

      .eyebrow::before {{
        content: "";
        width: 0.75rem;
        height: 0.75rem;
        border-radius: 999px;
        background: linear-gradient(135deg, var(--amber), #d89b66);
      }}

      .hero-note {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        margin-top: 1.1rem;
        padding: 0.5rem 0.8rem;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.66);
        color: var(--navy);
        font-size: 0.82rem;
        font-weight: 600;
      }}

      .hero-actions {{
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.75rem 1rem;
        margin-top: 1rem;
      }}

      .refresh-button {{
        border: none;
        border-radius: 999px;
        padding: 0.85rem 1.15rem;
        background: var(--navy);
        color: white;
        font-size: 0.92rem;
        font-weight: 700;
        cursor: pointer;
      }}

      .refresh-button:hover {{
        background: #182a3d;
      }}

      .refresh-button:disabled {{
        cursor: wait;
        opacity: 0.78;
      }}

      .refresh-status {{
        color: var(--muted);
        font-size: 0.88rem;
        line-height: 1.35;
        max-width: min(28rem, 100%);
        cursor: default;
      }}

      .refresh-status.refresh-status--failed {{
        color: #9a3412;
        cursor: help;
        text-decoration: underline;
        text-decoration-style: dotted;
        text-underline-offset: 0.15em;
      }}

      .refresh-status.refresh-status--running {{
        color: var(--navy);
        font-weight: 600;
      }}

      .refresh-status-detail {{
        font-size: 0.82rem;
        font-weight: 500;
        opacity: 0.9;
        margin-top: 0.2rem;
        line-height: 1.4;
      }}

      .refresh-progress {{
        display: none;
        margin-top: 0.45rem;
        height: 0.35rem;
        border-radius: 999px;
        background: rgba(24, 42, 61, 0.12);
        overflow: hidden;
        max-width: min(22rem, 100%);
      }}

      .refresh-progress.refresh-progress--visible {{
        display: block;
      }}

      .refresh-progress-fill {{
        height: 100%;
        width: 0%;
        border-radius: 999px;
        background: linear-gradient(90deg, #1e3a5f, #2d5a87);
        transition: width 0.35s ease-out;
      }}

      .hero-metrics {{
        display: grid;
        gap: 0.9rem;
      }}

      .metric-card {{
        padding: 1.15rem 1.25rem;
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(249, 242, 233, 0.82));
      }}

      .metric-card .label {{
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
      }}

      .metric-card .value {{
        display: block;
        margin-top: 0.35rem;
        color: var(--navy);
        font-family: "Iowan Old Style", "Baskerville", serif;
        font-size: 2rem;
        line-height: 1;
      }}

      .metric-card .subtext {{
        margin-top: 0.4rem;
        color: var(--muted);
        font-size: 0.9rem;
      }}

      .top-grid,
      .course-grid,
      .day-grid {{
        display: grid;
        gap: 1rem;
      }}

      .top-grid {{
        grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr);
        align-items: start;
      }}

      .course-grid {{
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      }}

      .day-grid {{
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        align-items: start;
      }}

      .panel {{
        padding: 1.3rem;
      }}

      .panel-header {{
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
        margin-bottom: 1rem;
      }}

      .panel-header h2 {{
        font-family: "Iowan Old Style", "Baskerville", serif;
        font-size: 1.55rem;
        letter-spacing: -0.03em;
      }}

      .panel-header p,
      .panel-kicker,
      .course-meta,
      .meta {{
        color: var(--muted);
        font-size: 0.92rem;
      }}

      .panel-kicker {{
        font-size: 0.8rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}

      .focus-list {{
        display: grid;
        gap: 0.85rem;
      }}

      .due-list {{
        display: grid;
        gap: 0.85rem;
        max-height: min(65vh, 38rem);
        overflow-y: auto;
        overflow-x: hidden;
        padding-right: 0.35rem;
        margin-right: -0.15rem;
        overscroll-behavior: contain;
        scrollbar-gutter: stable;
      }}

      .focus-item,
      .due-card,
      .day-card,
      .course-card {{
        border: 1px solid rgba(31, 38, 48, 0.08);
        border-radius: 1.2rem;
        background: var(--panel-strong);
        padding: 1rem;
      }}

      .focus-item {{
        display: grid;
        gap: 0.7rem;
      }}

      .item-header,
      .course-card-header {{
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
      }}

      .item-course {{
        color: var(--navy);
        font-size: 0.8rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}

      .item-title,
      .course-title {{
        color: var(--ink);
        font-size: 1.04rem;
        font-weight: 700;
        line-height: 1.2;
        text-decoration: none;
      }}

      .item-title:hover,
      .course-title:hover {{
        color: var(--amber);
      }}

      .item-meta,
      .day-meta {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        align-items: center;
      }}

      .badge {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        min-height: 1.9rem;
        padding: 0.15rem 0.7rem;
        border-radius: 999px;
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        white-space: nowrap;
      }}

      .badge-neutral {{
        background: rgba(32, 52, 74, 0.08);
        color: var(--navy);
      }}

      .badge-critical {{
        background: var(--rose-soft);
        color: var(--rose);
      }}

      .badge-warning {{
        background: var(--amber-soft);
        color: var(--amber);
      }}

      .badge-safe {{
        background: var(--green-soft);
        color: var(--green);
      }}

      .minutes-pill {{
        color: var(--navy);
        background: rgba(32, 52, 74, 0.08);
      }}

      .reason-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.45rem;
      }}

      .reason-chip {{
        display: inline-flex;
        align-items: center;
        padding: 0.35rem 0.6rem;
        border-radius: 999px;
        background: rgba(32, 52, 74, 0.06);
        color: var(--muted);
        font-size: 0.78rem;
        line-height: 1.2;
      }}

      .day-card {{
        display: grid;
        gap: 0.85rem;
        align-content: start;
        width: 100%;
      }}

      .day-card h3 {{
        font-family: "Iowan Old Style", "Baskerville", serif;
        font-size: 1.1rem;
      }}

      .day-items {{
        list-style: none;
        padding: 0;
        display: grid;
        gap: 0.85rem;
        align-content: start;
      }}

      .day-item {{
        display: grid;
        gap: 0.35rem;
        align-content: start;
        padding-top: 0.75rem;
        border-top: 1px solid rgba(31, 38, 48, 0.08);
      }}

      .day-item:first-child {{
        padding-top: 0;
        border-top: none;
      }}

      .course-card {{
        display: grid;
        gap: 1rem;
      }}

      .course-grade {{
        font-family: "Iowan Old Style", "Baskerville", serif;
        font-size: 2rem;
        line-height: 1;
        color: var(--navy);
      }}

      .course-grade-row {{
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 0.65rem 1rem;
      }}

      .text-button.grade-detail-trigger {{
        border: none;
        background: none;
        padding: 0;
        margin: 0;
        cursor: pointer;
        color: var(--amber);
        font-size: 0.82rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-decoration: underline;
        text-underline-offset: 0.15em;
      }}

      .text-button.grade-detail-trigger:hover {{
        color: var(--navy);
      }}

      .grade-detail-dialog {{
        max-width: min(560px, 94vw);
        width: 100%;
        border: 1px solid var(--line);
        border-radius: 1.25rem;
        padding: 0;
        background: var(--panel-strong);
        box-shadow: var(--shadow);
      }}

      .grade-detail-dialog::backdrop {{
        background: rgba(31, 38, 48, 0.45);
      }}

      .grade-detail-shell {{
        display: grid;
        gap: 0;
        max-height: min(78vh, 640px);
      }}

      .grade-detail-header {{
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
        padding: 1.1rem 1.25rem;
        border-bottom: 1px solid var(--line);
      }}

      .grade-detail-header h2 {{
        font-family: "Iowan Old Style", "Baskerville", serif;
        font-size: 1.15rem;
        margin: 0;
        color: var(--ink);
      }}

      .grade-detail-close {{
        border: none;
        background: rgba(32, 52, 74, 0.08);
        color: var(--navy);
        width: 2.25rem;
        height: 2.25rem;
        border-radius: 999px;
        font-size: 1.35rem;
        line-height: 1;
        cursor: pointer;
      }}

      .grade-detail-body {{
        padding: 1rem 1.25rem 1.25rem;
        overflow: auto;
        font-size: 0.88rem;
        line-height: 1.45;
        color: var(--ink);
      }}

      .grade-detail-lead {{
        margin: 0 0 0.75rem;
        font-size: 1rem;
      }}

      .grade-detail-notes {{
        margin: 0 0 1rem;
        padding-left: 1.2rem;
        color: var(--muted);
      }}

      .grade-detail-table {{
        width: 100%;
        border-collapse: collapse;
        margin: 0 0 1rem;
        font-size: 0.82rem;
      }}

      .grade-detail-table th,
      .grade-detail-table td {{
        text-align: left;
        padding: 0.45rem 0.5rem;
        border-bottom: 1px solid rgba(31, 38, 48, 0.08);
        vertical-align: top;
      }}

      .grade-detail-table th {{
        color: var(--muted);
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }}

      .grade-detail-subhead {{
        margin: 0.5rem 0 0.35rem;
        font-size: 0.85rem;
      }}

      .grade-detail-excluded {{
        margin: 0 0 0.75rem;
        padding-left: 1.2rem;
        color: var(--muted);
        font-size: 0.82rem;
      }}

      .grade-detail-excluded .ex-title {{
        color: var(--ink);
        font-weight: 600;
      }}

      .grade-detail-table tr.grade-detail-member td {{
        padding-left: 1.35rem;
        font-size: 0.78rem;
        color: var(--muted);
        border-bottom: 1px solid rgba(31, 38, 48, 0.05);
      }}

      .grade-detail-muted {{
        margin: 0;
        font-size: 0.8rem;
        color: var(--muted);
      }}

      .course-stats {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.8rem;
      }}

      .course-stat-label {{
        color: var(--muted);
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }}

      .course-stat-value {{
        margin-top: 0.2rem;
        font-size: 1rem;
        font-weight: 700;
      }}

      .course-link {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: fit-content;
        padding: 0.7rem 1rem;
        border-radius: 999px;
        background: var(--navy);
        color: white;
        font-size: 0.88rem;
        font-weight: 700;
        text-decoration: none;
      }}

      .course-link:hover {{
        background: #182a3d;
      }}

      .alert {{
        padding: 1rem 1.1rem;
        margin-bottom: 1rem;
        color: var(--rose);
        background: rgba(124, 54, 66, 0.08);
      }}

      .empty-state {{
        padding: 1rem 1.1rem;
        color: var(--muted);
      }}

      .section-stack {{
        display: grid;
        gap: 1rem;
        margin-top: 1rem;
      }}

      @media (max-width: 980px) {{
        .hero,
        .top-grid {{
          grid-template-columns: 1fr;
        }}
      }}

      @media (max-width: 720px) {{
        main {{
          width: min(100%, calc(100vw - 1rem));
          padding: 1rem 0 3rem;
        }}

        .hero-card,
        .panel,
        .metric-card {{
          padding: 1rem;
        }}

        .course-stats {{
          grid-template-columns: 1fr 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <header class="hero">
        <section class="hero-card">
          <div class="hero-copy">
            <p class="eyebrow">Academic Command Center</p>
            <h1>Today is organized.</h1>
            <p>See what to work on now, what is coming over the next seven days, and how each course is trending without digging through D2L.</p>
            <div class="hero-note">{escape(today_label)} • updated {escape(local_generated_at.strftime("%-I:%M %p %Z"))}</div>
            <div class="hero-actions">
              <button class="refresh-button" id="refresh-button" type="button">Refresh now</button>
              <div
                class="{refresh_status_classes}"
                id="refresh-status"
                role="status"
                aria-live="polite"
                {f'title="{refresh_status_title}"' if refresh_status_title else ""}
              >
                <span id="refresh-status-label">{escape(refresh_status_primary)}</span>
                <div class="refresh-status-detail" id="refresh-status-detail">{escape(refresh_status_detail)}</div>
                <div class="refresh-progress" id="refresh-progress" aria-hidden="true">
                  <div class="refresh-progress-fill" id="refresh-progress-fill"></div>
                </div>
              </div>
            </div>
          </div>
        </section>
        <section class="hero-metrics">
          <article class="metric-card">
            <span class="label">Today's Plan</span>
            <span class="value">{today_minutes}</span>
            <p class="subtext">minutes scheduled across {len(today_agenda)} item{'s' if len(today_agenda) != 1 else ''}</p>
          </article>
          <article class="metric-card">
            <span class="label">Urgent Work</span>
            <span class="value">{urgent_count}</span>
            <p class="subtext">overdue, due today, or due tomorrow</p>
          </article>
          <article class="metric-card">
            <span class="label">Connected</span>
            <span class="value">{'Yes' if overview["d2l_storage_state"] else 'No'}</span>
            <p class="subtext">{summary["course_count"]} courses and {summary["assignment_count"]} canonical assignments tracked</p>
          </article>
        </section>
      </header>

      {error_banner}

      <section class="top-grid">
        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Today</p>
              <h2>Today's Focus</h2>
            </div>
            <div class="badge minutes-pill">{today_minutes} min planned</div>
          </div>
          <div class="focus-list">{focus_items}</div>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Upcoming</p>
              <h2>Due Soon</h2>
            </div>
            <p>{summary["upcoming_count"]} upcoming item{'s' if summary["upcoming_count"] != 1 else ''}</p>
          </div>
          <div
            class="due-list"
            role="region"
            aria-label="Assignments due soon"
            tabindex="0"
          >{due_soon_cards}</div>
        </article>
      </section>

      <section class="section-stack">
        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Agenda</p>
              <h2>Next 7 Days</h2>
            </div>
            <p>{len(agenda_days)} day{'s' if len(agenda_days) != 1 else ''} with scheduled work</p>
          </div>
          <div class="day-grid">{day_cards}</div>
        </article>

        <article class="panel">
          <div class="panel-header">
            <div>
              <p class="panel-kicker">Courses</p>
              <h2>Course Health</h2>
            </div>
            <p>Grade snapshot, workload, and direct links back into D2L.</p>
          </div>
          <div class="course-grid">{course_cards}</div>
        </article>
      </section>
    </main>
    <dialog id="grade-detail-dialog" class="grade-detail-dialog" aria-labelledby="grade-detail-title">
      <div class="grade-detail-shell">
        <header class="grade-detail-header">
          <h2 id="grade-detail-title">Course grade calculation</h2>
          <button type="button" class="grade-detail-close" id="grade-detail-close" aria-label="Close">&times;</button>
        </header>
        <div class="grade-detail-body" id="grade-detail-body"></div>
      </div>
    </dialog>
    <script>
      const refreshTimezone = {json.dumps(settings.timezone)};
      {GRADE_DETAIL_HOOK}
      const refreshButton = document.getElementById("refresh-button");
      let refreshPollTimer = null;

      function formatRefreshTime(value) {{
        if (!value) {{
          return null;
        }}
        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) {{
          return null;
        }}
        return new Intl.DateTimeFormat("en-US", {{
          timeZone: refreshTimezone,
          hour: "numeric",
          minute: "2-digit",
          timeZoneName: "short",
        }}).format(parsed);
      }}

      function formatElapsedSince(startIso) {{
        if (!startIso) {{
          return "";
        }}
        const t = new Date(startIso).getTime();
        if (Number.isNaN(t)) {{
          return "";
        }}
        const sec = Math.max(0, Math.floor((Date.now() - t) / 1000));
        if (sec < 60) {{
          return sec + "s";
        }}
        const m = Math.floor(sec / 60);
        const s = sec % 60;
        return m + "m " + s + "s";
      }}

      function renderRefreshStatus(payload) {{
        if (!payload) {{
          return;
        }}
        const running = Boolean(payload.running);
        const statusEl = document.getElementById("refresh-status");
        const labelEl = document.getElementById("refresh-status-label");
        const detailEl = document.getElementById("refresh-status-detail");
        if (refreshButton) {{
          refreshButton.disabled = running;
          refreshButton.textContent = running ? "Refreshing..." : "Refresh now";
        }}
        if (!statusEl || !labelEl) {{
          return;
        }}
        statusEl.classList.remove("refresh-status--running", "refresh-status--failed");
        statusEl.removeAttribute("title");

        if (running) {{
          statusEl.classList.add("refresh-status--running");
          const phase =
            typeof payload.current_phase === "string" && payload.current_phase.trim()
              ? payload.current_phase.trim()
              : "Refreshing…";
          const detailRaw =
            typeof payload.current_detail === "string" ? payload.current_detail.trim() : "";
          const elapsed = formatElapsedSince(payload.last_started_at);
          labelEl.textContent = phase;
          if (detailEl) {{
            let line = detailRaw;
            if (elapsed) {{
              line = line ? line + " · " + elapsed : "Elapsed " + elapsed;
            }}
            detailEl.textContent = line;
          }}
          const trackEl = document.getElementById("refresh-progress");
          const fillEl = document.getElementById("refresh-progress-fill");
          const fracRaw = payload.progress_fraction;
          const frac =
            typeof fracRaw === "number" && Number.isFinite(fracRaw)
              ? Math.min(1, Math.max(0, fracRaw))
              : null;
          if (trackEl && fillEl) {{
            if (frac !== null) {{
              trackEl.classList.add("refresh-progress--visible");
              fillEl.style.width = Math.round(frac * 100) + "%";
            }} else {{
              trackEl.classList.remove("refresh-progress--visible");
              fillEl.style.width = "0%";
            }}
          }}
          return;
        }}

        const trackElDone = document.getElementById("refresh-progress");
        const fillElDone = document.getElementById("refresh-progress-fill");
        if (trackElDone && fillElDone) {{
          trackElDone.classList.remove("refresh-progress--visible");
          fillElDone.style.width = "0%";
        }}
        if (detailEl) {{
          detailEl.textContent = "";
        }}
        const completedAt = formatRefreshTime(payload.last_completed_at);
        if (payload.last_error) {{
          statusEl.classList.add("refresh-status--failed");
          statusEl.setAttribute("title", String(payload.last_error));
          labelEl.textContent = completedAt
            ? "Last refresh failed at " + completedAt + "."
            : "Last refresh failed.";
          return;
        }}
        if (completedAt) {{
          labelEl.textContent = "Last refresh finished " + completedAt + ".";
          return;
        }}
        labelEl.textContent = "Ready to refresh.";
      }}

      async function fetchRefreshStatus() {{
        const response = await fetch("/api/refresh-status");
        return response.json();
      }}

      function beginRefreshPolling(reloadOnComplete) {{
        if (refreshPollTimer !== null) {{
          return;
        }}
        refreshPollTimer = window.setInterval(async () => {{
          try {{
            const payload = await fetchRefreshStatus();
            renderRefreshStatus(payload);
            if (!payload.running) {{
              window.clearInterval(refreshPollTimer);
              refreshPollTimer = null;
              if (reloadOnComplete) {{
                window.location.reload();
              }}
            }}
          }} catch (_error) {{
            window.clearInterval(refreshPollTimer);
            refreshPollTimer = null;
            const statusEl = document.getElementById("refresh-status");
            const labelEl = document.getElementById("refresh-status-label");
            if (statusEl && labelEl) {{
              statusEl.classList.remove("refresh-status--running");
              statusEl.classList.add("refresh-status--failed");
              statusEl.setAttribute("title", String(_error));
              labelEl.textContent = "Could not read refresh status.";
            }}
          }}
        }}, 400);
      }}

      async function triggerRefresh(reloadOnComplete) {{
        try {{
          const response = await fetch("/api/refresh", {{ method: "POST" }});
          const payload = await response.json();
          renderRefreshStatus(payload);
          if (payload.running) {{
            beginRefreshPolling(reloadOnComplete);
          }}
        }} catch (_error) {{
          const statusEl = document.getElementById("refresh-status");
          const labelEl = document.getElementById("refresh-status-label");
          if (statusEl && labelEl) {{
            statusEl.classList.add("refresh-status--failed");
            statusEl.setAttribute("title", String(_error));
            labelEl.textContent = "Refresh failed to start.";
          }}
        }}
      }}

      if (refreshButton) {{
        refreshButton.addEventListener("click", () => {{
          triggerRefresh(true);
        }});
      }}
    </script>
  </body>
</html>"""
    grade_detail_map = {
        str(c["id"]): c["grade_detail"]
        for c in courses
        if isinstance(c, dict)
        and c.get("id") is not None
        and grade_detail_is_populated(c.get("grade_detail"))
    }
    grade_json = re.sub(r"</script", r"<\\/script", json.dumps(grade_detail_map), flags=re.I)
    return page.replace(
        GRADE_DETAIL_HOOK,
        "\n      window.__ACC_GRADE_DETAILS = "
        + grade_json
        + ";\n"
        + GRADE_DETAIL_CLIENT_JS,
    )


def format_grade(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


def parse_generated_at(overview: dict[str, object]) -> datetime:
    generated_at = overview.get("generated_at")
    if isinstance(generated_at, str):
        try:
            parsed = datetime.fromisoformat(generated_at)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
    return datetime.now(UTC)


def render_assignment_due_badge(item: dict[str, object]) -> str:
    label = item.get("due_label")
    if not isinstance(label, str) or not label.strip():
        return ""
    return render_badge(escape(f"Due {label.strip()}"), "neutral", extra_class="due-time-pill")


def render_focus_item(item: dict[str, object], now: datetime) -> str:
    urgency_label, urgency_tone = urgency_variant(
        item.get("due_at"),
        now,
        due_calendar_date=due_calendar_from_item(item),
    )
    return f"""
    <article class="focus-item">
      <div class="item-header">
        <div>
          <div class="item-course">{escape(str(item.get("course_code") or "Unknown"))}</div>
          {render_assignment_title(item.get("assignment_title"), item.get("external_url"))}
        </div>
        {render_badge(urgency_label, urgency_tone)}
      </div>
      <div class="item-meta">
        {render_badge(f"{item.get('planned_minutes', 0)} min", "neutral", extra_class="minutes-pill")}
        {render_assignment_due_badge(item)}
        {render_badge(str(item.get("status", "upcoming")).replace("_", " ").title(), "neutral")}
        {render_assignment_action_button(item.get("external_url"))}
      </div>
      {render_reason_chips(item.get("priority_reasons"))}
    </article>
    """


def render_due_card(item: dict[str, object], now: datetime) -> str:
    urgency_label, urgency_tone = urgency_variant(
        item.get("due_at"),
        now,
        due_calendar_date=due_calendar_from_item(item),
    )
    raw_due = item.get("due_label")
    due_display = (
        f"Due {raw_due.strip()}"
        if isinstance(raw_due, str) and raw_due.strip()
        else "TBD"
    )
    return f"""
    <article class="due-card">
      <div class="item-header">
        <div>
          <div class="item-course">{escape(str(item.get("course_code") or "Unknown"))}</div>
          {render_assignment_title(item.get("title"), item.get("external_url"))}
        </div>
        {render_badge(urgency_label, urgency_tone)}
      </div>
      <div class="item-meta">
        {render_badge(escape(str(due_display)), "neutral", extra_class="due-time-pill")}
        {render_badge(str(item.get("status", "upcoming")).replace("_", " ").title(), "neutral")}
        {render_assignment_action_button(item.get("external_url"))}
      </div>
      {render_reason_chips(item.get("priority_reasons"))}
    </article>
    """


def render_day_card(day: dict[str, object], now: datetime) -> str:
    today = now.date()
    agenda_date = str(day.get("agenda_date", ""))
    day_label = format_day_heading(agenda_date)
    total_minutes = int(day.get("total_minutes", 0))
    entry_count = int(day.get("entry_count", 0))
    items = day.get("items")
    if not isinstance(items, list):
        items = []
    day_tone = "warning" if agenda_date == today.isoformat() else "safe"
    day_items = "".join(
        f"""
        <li class="day-item">
          <div class="item-course">{escape(str(item.get("course_code") or "Unknown"))}</div>
          {render_assignment_title(item.get("assignment_title"), item.get("external_url"))}
          <div class="day-meta">
            {render_badge(f"{item.get('planned_minutes', 0)} min", "neutral", extra_class="minutes-pill")}
            {render_assignment_due_badge(item)}
            {render_assignment_action_button(item.get("external_url"))}
            {render_badge(
                *urgency_variant(
                    item.get("due_at"),
                    now,
                    due_calendar_date=due_calendar_from_item(item),
                )
            )}
          </div>
        </li>
        """
        for item in items
    )
    return f"""
    <article class="day-card">
      <div class="item-header">
        <div>
          <h3>{escape(day_label)}</h3>
          <p class="meta">{entry_count} item{'s' if entry_count != 1 else ''} planned</p>
        </div>
        {render_badge(f"{total_minutes} min", day_tone)}
      </div>
      <ul class="day-items">{day_items}</ul>
    </article>
    """


GRADE_DETAIL_HOOK = "/*__GRADE_DETAIL_HOOK__*/"

GRADE_DETAIL_CLIENT_JS = """
      function accEscapeHtml(value) {
        return String(value)
          .replace(/&/g, "&amp;")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")
          .replace(/"/g, "&quot;");
      }

      function renderGradeDetailHtml(detail) {
        if (!detail) {
          return "<p>No breakdown available.</p>";
        }
        const parts = [];
        if (detail.final_grade_pct != null) {
          parts.push(
            '<p class="grade-detail-lead"><strong>Estimated course grade:</strong> ' +
              accEscapeHtml(detail.final_grade_pct) +
              "%</p>"
          );
        }
        if (Array.isArray(detail.notes) && detail.notes.length) {
          parts.push('<ul class="grade-detail-notes">');
          for (const note of detail.notes) {
            parts.push("<li>" + accEscapeHtml(note) + "</li>");
          }
          parts.push("</ul>");
        }
        const rows = Array.isArray(detail.components) ? detail.components : [];
        if (rows.length) {
          parts.push('<table class="grade-detail-table"><thead><tr>');
          parts.push("<th>Source</th><th>Weight %</th><th>Grade %</th><th>Weighted points</th>");
          parts.push("</tr></thead><tbody>");
          for (const row of rows) {
            if (row.type === "assignment") {
              parts.push(
                "<tr><td>" +
                  accEscapeHtml(row.title || "") +
                  "</td><td>" +
                  accEscapeHtml(row.weight_pct) +
                  "</td><td>" +
                  accEscapeHtml(row.grade_pct) +
                  "</td><td>" +
                  accEscapeHtml(row.weighted_points) +
                  "</td></tr>"
              );
            } else if (row.type === "category") {
              parts.push(
                "<tr><td>Category: " +
                  accEscapeHtml(row.name || "") +
                  "</td><td>" +
                  accEscapeHtml(row.weight_pct) +
                  '</td><td>' +
                  accEscapeHtml(row.grade_pct) +
                  ' <span class="grade-detail-muted">(mean of graded items)</span></td><td>' +
                  accEscapeHtml(row.weighted_points) +
                  "</td></tr>"
              );
              const members = Array.isArray(row.members) ? row.members : [];
              for (const m of members) {
                const gradeStr =
                  m.grade_pct != null && m.grade_pct !== undefined
                    ? accEscapeHtml(m.grade_pct) + "%"
                    : "—";
                let pts = "";
                if (m.points_earned != null || m.points_possible != null) {
                  pts =
                    ' <span class="grade-detail-muted">(' +
                    (m.points_earned != null ? accEscapeHtml(m.points_earned) : "—") +
                    " / " +
                    (m.points_possible != null ? accEscapeHtml(m.points_possible) : "—") +
                    " pts)</span>";
                }
                let share = "";
                if (m.share_of_category_avg_pct != null && m.share_of_category_avg_pct !== undefined) {
                  share =
                    ' <span class="grade-detail-muted">· ' +
                    accEscapeHtml(m.share_of_category_avg_pct) +
                    "% of category average (by points)</span>";
                }
                let wu = "";
                if (
                  m.points_possible == null &&
                  m.category_weight_units != null &&
                  m.category_weight_units !== undefined
                ) {
                  wu =
                    ' <span class="grade-detail-muted">· blend weight ' +
                    accEscapeHtml(m.category_weight_units) +
                    "</span>";
                }
                parts.push(
                  '<tr class="grade-detail-member"><td colspan="4">' +
                    accEscapeHtml(m.title || "") +
                    pts +
                    " — grade " +
                    gradeStr +
                    share +
                    wu +
                    (m.note
                      ? ' <span class="grade-detail-muted">' + accEscapeHtml(m.note) + "</span>"
                      : "") +
                    "</td></tr>"
                );
              }
            }
          }
          parts.push("</tbody></table>");
        }
        const ex = Array.isArray(detail.excluded) ? detail.excluded : [];
        const exCount = detail.excluded_count || 0;
        if (ex.length || exCount > 0) {
          parts.push('<p class="grade-detail-subhead"><strong>Not included in this calculation</strong></p>');
          parts.push('<ul class="grade-detail-excluded">');
          for (const row of ex) {
            parts.push(
              '<li><span class="ex-title">' +
                accEscapeHtml(row.title || "") +
                "</span> — " +
                accEscapeHtml(row.reason || "") +
                "</li>"
            );
          }
          parts.push("</ul>");
          if (detail.excluded_truncated > 0) {
            parts.push(
              '<p class="grade-detail-muted">…and ' +
                accEscapeHtml(detail.excluded_truncated) +
                " more.</p>"
            );
          }
        }
        if (!parts.length) {
          return "<p>No breakdown available.</p>";
        }
        return parts.join("");
      }

      const gradeDetailDialog = document.getElementById("grade-detail-dialog");
      const gradeDetailBody = document.getElementById("grade-detail-body");
      const gradeDetailClose = document.getElementById("grade-detail-close");

      function openGradeDetail(courseId) {
        const map = window.__ACC_GRADE_DETAILS || {};
        const detail = map[courseId];
        if (!gradeDetailDialog || !gradeDetailBody) {
          return;
        }
        gradeDetailBody.innerHTML = renderGradeDetailHtml(detail);
        gradeDetailDialog.showModal();
      }

      if (gradeDetailClose && gradeDetailDialog) {
        gradeDetailClose.addEventListener("click", () => gradeDetailDialog.close());
      }
      if (gradeDetailDialog) {
        gradeDetailDialog.addEventListener("click", (event) => {
          if (event.target === gradeDetailDialog) {
            gradeDetailDialog.close();
          }
        });
      }

      document.querySelectorAll(".grade-detail-trigger").forEach((btn) => {
        btn.addEventListener("click", () => {
          const id = btn.getAttribute("data-course-id");
          if (id) {
            openGradeDetail(id);
          }
        });
      });
"""


def grade_detail_is_populated(detail: object) -> bool:
    if not isinstance(detail, dict):
        return False
    if detail.get("final_grade_pct") is not None:
        return True
    if detail.get("components"):
        return True
    if detail.get("excluded"):
        return True
    return bool(detail.get("notes"))


def render_grade_detail_button(course: dict[str, object]) -> str:
    if not grade_detail_is_populated(course.get("grade_detail")):
        return ""
    cid = course.get("id")
    if cid is None:
        return ""
    return (
        f'<button type="button" class="text-button grade-detail-trigger" '
        f"data-course-id={escape(str(cid), quote=True)}>Show detail</button>"
    )


def render_course_card(course: dict[str, object]) -> str:
    grade = course.get("current_grade_pct")
    grade_label, grade_tone = grade_variant(grade)
    external_platform = course.get("external_platform")
    platform_label = (
        str(external_platform).replace("_", " ").title() if isinstance(external_platform, str) else "D2L only"
    )
    d2l_url = course.get("d2l_url")
    new_tab = ' target="_blank" rel="noopener noreferrer"'
    course_link = ""
    if d2l_url:
        course_link = (
            f'<a class="course-link" href="{escape(str(d2l_url))}"{new_tab}>Open Course</a>'
        )
    syllabus_link = ""
    syllabus_url = course.get("syllabus_url")
    if isinstance(syllabus_url, str) and syllabus_url:
        syllabus_link = (
            f'<a class="course-link" href="{escape(str(syllabus_url))}"{new_tab}>Syllabus</a>'
        )

    title_name = escape(str(course.get("name") or "Untitled Course"))
    if d2l_url:
        title_html = f'<a class="course-title" href="{escape(str(d2l_url))}"{new_tab}>{title_name}</a>'
    else:
        title_html = f'<a class="course-title" href="#">{title_name}</a>'

    return f"""
    <article class="course-card">
      <div class="course-card-header">
        <div>
          <div class="item-course">{escape(str(course.get("code") or "Unknown"))}</div>
          {title_html}
          <p class="course-meta">{escape(str(course.get("semester") or ""))}</p>
        </div>
        {render_badge(grade_label, grade_tone)}
      </div>
      <div class="course-grade-row">
        <span class="course-grade">{escape(format_grade(grade if isinstance(grade, int | float) else None))}</span>
        {render_grade_detail_button(course)}
      </div>
      <div class="course-stats">
        <div>
          <div class="course-stat-label">Assignments</div>
          <div class="course-stat-value">{course.get("assignment_count", 0)}</div>
        </div>
        <div>
          <div class="course-stat-label">Due Soon</div>
          <div class="course-stat-value">{course.get("upcoming_count", 0)}</div>
        </div>
        <div>
          <div class="course-stat-label">Platform</div>
          <div class="course-stat-value">{escape(platform_label)}</div>
        </div>
        <div>
          <div class="course-stat-label">Status</div>
          <div class="course-stat-value">{escape(grade_label)}</div>
        </div>
      </div>
      <div class="item-meta">{course_link}{syllabus_link}</div>
    </article>
    """


def render_assignment_title(title: object, url: object) -> str:
    label = escape(str(title or "Untitled"))
    if isinstance(url, str) and url:
        return f'<a class="item-title" href="{escape(url)}">{label}</a>'
    return f'<div class="item-title">{label}</div>'


def render_badge(label: object, tone: str, *, extra_class: str = "") -> str:
    classes = f"badge badge-{tone}"
    if extra_class:
        classes += f" {extra_class}"
    return f'<span class="{classes}">{escape(str(label))}</span>'


def render_assignment_action_button(url: object) -> str:
    if not isinstance(url, str) or not url.strip():
        return ""
    href = escape(url.strip())
    return (
        f'<a class="course-link" href="{href}" target="_blank" rel="noopener noreferrer">'
        "Go to assignment</a>"
    )


def render_reason_chips(reasons: object) -> str:
    if not isinstance(reasons, list) or not reasons:
        return ""
    chips = "".join(f'<span class="reason-chip">{escape(str(reason))}</span>' for reason in reasons[:3])
    return f'<div class="reason-row">{chips}</div>'


def format_refresh_error_title_attr(error: object, *, max_len: int = 8000) -> str:
    if error is None:
        return ""
    text = " ".join(str(error).split())
    if not text:
        return ""
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return escape(text, quote=True)


def infer_course_syllabus_url(course: object) -> str | None:
    if not hasattr(course, "d2l_url"):
        return None
    base_url = getattr(course, "d2l_url", None)
    d2l_course_id = getattr(course, "d2l_course_id", None)
    if not isinstance(base_url, str) or "/d2l/home/" not in base_url:
        return None
    if not isinstance(d2l_course_id, str) or not d2l_course_id.strip():
        return None
    return base_url.rsplit("/d2l/home/", 1)[0] + f"/d2l/le/content/{d2l_course_id}/Home"


def format_refresh_primary_label(status: object) -> str:
    if not isinstance(status, dict):
        return "Ready to refresh."
    if status.get("running"):
        phase = status.get("current_phase")
        if isinstance(phase, str) and phase.strip():
            return phase.strip()
        return "Refreshing…"
    return format_refresh_status_text(status)


def format_refresh_secondary_label(status: object) -> str:
    if not isinstance(status, dict) or not status.get("running"):
        return ""
    detail = status.get("current_detail")
    if isinstance(detail, str) and detail.strip():
        return detail.strip()
    return ""


def format_refresh_status_text(status: object) -> str:
    if not isinstance(status, dict):
        return "Ready to refresh."
    if status.get("running"):
        phase = status.get("current_phase")
        if isinstance(phase, str) and phase.strip():
            return phase.strip()
        return "Refreshing…"
    last_completed_at = parse_status_datetime(status.get("last_completed_at"))
    if status.get("last_error"):
        if last_completed_at is not None:
            return f"Last refresh failed at {last_completed_at.strftime('%-I:%M %p %Z')}."
        return "Last refresh failed."
    if last_completed_at is not None:
        return f"Last refresh finished {last_completed_at.strftime('%-I:%M %p %Z')}."
    return "Ready to refresh."


def parse_due_instant_utc_from_api(due_at: str) -> datetime:
    parsed = datetime.fromisoformat(due_at.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def urgency_variant(
    due_at: object,
    now: datetime,
    *,
    due_calendar_date: date | None = None,
) -> tuple[str, str]:
    if not isinstance(due_at, str):
        return ("No due date", "neutral")
    try:
        due_instant_utc = parse_due_instant_utc_from_api(due_at)
    except ValueError:
        return ("No due date", "neutral")

    now_utc = now if now.tzinfo else now.replace(tzinfo=UTC)
    now_utc = now_utc.astimezone(UTC)
    if due_instant_utc <= now_utc:
        return ("Overdue", "critical")

    local_now = localize_datetime(now_utc)
    if due_calendar_date is not None:
        due_day = due_calendar_date
    else:
        due_day = localize_datetime(due_instant_utc).date()

    delta_days = (due_day - local_now.date()).days
    if delta_days == 0:
        return ("Due today", "warning")
    if delta_days == 1:
        return ("Due tomorrow", "warning")
    if delta_days <= 3:
        return (f"Due in {delta_days} days", "warning")
    return (f"In {delta_days} days", "safe")


def is_urgent_due_date(assignment: CanonicalAssignment, *, now: datetime) -> bool:
    instant = canonical_due_instant_utc(assignment)
    if instant is None:
        return False
    now_utc = (now if now.tzinfo else now.replace(tzinfo=UTC)).astimezone(UTC)
    if instant <= now_utc:
        return True
    cal = canonical_due_calendar_date(assignment)
    if cal is None:
        return False
    local_now = localize_datetime(now_utc).date()
    return (cal - local_now).days <= 1


def due_priority_sort_key(assignment: CanonicalAssignment) -> tuple[datetime, str, str]:
    due_date = canonical_due_instant_utc(assignment)
    if due_date is None:
        due_date = datetime.max.replace(tzinfo=UTC)
    course_code = assignment.course.code if assignment.course else assignment.course_id
    return (due_date, course_code.lower(), assignment.title.lower())


def normalize_due_reason_labels(
    reasons: object,
    *,
    due_at: object,
    now: datetime,
    due_calendar_date: date | None = None,
) -> list[str]:
    if not isinstance(reasons, list):
        return []
    normalized = [str(reason) for reason in reasons]
    urgency_label, _ = urgency_variant(due_at, now, due_calendar_date=due_calendar_date)
    for index, reason in enumerate(normalized):
        if reason.startswith("Due "):
            normalized[index] = urgency_label
            break
    return normalized


def due_sort_value(value: object) -> datetime:
    if not isinstance(value, str):
        return datetime.max.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.max.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def is_dashboard_hidden_assignment(title: object) -> bool:
    return isinstance(title, str) and "practice test" in title.lower()


def is_dashboard_relevant_assignment(assignment: CanonicalAssignment) -> bool:
    if is_dashboard_hidden_assignment(assignment.title):
        return False
    if assignment.due_date is not None:
        return True
    if assignment.grade_category:
        return True
    if assignment.grade_pct is not None or assignment.points_earned is not None:
        return True
    raw_data = assignment.raw_scraped_data if isinstance(assignment.raw_scraped_data, dict) else {}
    if assignment.points_possible not in (None, 0):
        if assignment.source_platform != "d2l" and not raw_data.get("announcement_title"):
            return False
        return True
    if raw_data.get("announcement_title"):
        return False
    if not raw_data:
        return assignment.source_platform == "d2l"
    return assignment.source_platform == "d2l"


def counts_for_urgent_work(assignment: CanonicalAssignment) -> bool:
    return "practice test" not in assignment.title.lower()


def localize_datetime(value: datetime) -> datetime:
    """Convert stored instants to the configured display timezone.

    Naive datetimes from the database are UTC wall times (see repository normalization).
    """
    timezone = ZoneInfo(settings.timezone)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC).astimezone(timezone)
    return value.astimezone(timezone)


def format_local_due_label(value: datetime) -> str:
    local_due_at = localize_datetime(value)
    return local_due_at.strftime("%m/%d/%y %-I:%M %p")


def format_due_label_for_canonical(assignment: CanonicalAssignment) -> str | None:
    raw = assignment.raw_scraped_data if isinstance(assignment.raw_scraped_data, dict) else None
    if raw:
        due_at_str = raw.get("due_at")
        if isinstance(due_at_str, str):
            text = due_at_str.strip()
            if text:
                try:
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                except ValueError:
                    pass
                else:
                    if parsed.tzinfo is not None:
                        return parsed.strftime("%m/%d/%y %-I:%M %p")
    if assignment.due_date is None:
        return None
    return format_local_due_label(assignment.due_date)


def parse_status_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return localize_datetime(parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC))


def serialize_refresh_status(*, message: str | None = None) -> dict[str, object]:
    return {
        "running": refresh_state.running,
        "current_phase": refresh_state.current_phase,
        "current_detail": refresh_state.current_detail,
        "progress_fraction": refresh_state.progress_fraction,
        "last_started_at": refresh_state.last_started_at.isoformat() if refresh_state.last_started_at else None,
        "last_completed_at": refresh_state.last_completed_at.isoformat()
        if refresh_state.last_completed_at
        else None,
        "last_error": refresh_state.last_error,
        "last_result": refresh_state.last_result,
        "message": message,
    }


async def run_dashboard_refresh() -> None:
    try:
        def report_progress(
            headline: str,
            detail: str | None = None,
            *,
            fraction: float | None = None,
        ) -> None:
            refresh_state.current_phase = headline
            refresh_state.current_detail = detail
            if fraction is not None:
                refresh_state.progress_fraction = min(1.0, max(0.0, fraction))

        refresh_state.last_result = await run_refresh_pipeline(
            settings,
            include_external=True,
            include_syllabus_parse=False,
            agenda_days=7,
            daily_minutes=120,
            refresh_mode=refresh_state.requested_mode,
            on_progress=report_progress,
        )
        refresh_state.last_error = None
    except Exception as error:
        refresh_state.last_error = str(error)
    finally:
        refresh_state.running = False
        refresh_state.current_phase = None
        refresh_state.current_detail = None
        refresh_state.progress_fraction = None
        refresh_state.last_completed_at = datetime.now(UTC)
        refresh_state.task = None


def start_dashboard_refresh(mode: str = "full") -> dict[str, object]:
    if refresh_state.running:
        return serialize_refresh_status(message="Refresh already running.")

    refresh_state.running = True
    refresh_state.current_phase = "Starting refresh"
    refresh_state.current_detail = "Queuing pipeline steps..."
    refresh_state.progress_fraction = 0.0
    refresh_state.last_started_at = datetime.now(UTC)
    refresh_state.last_error = None
    refresh_state.requested_mode = mode
    refresh_state.task = asyncio.create_task(run_dashboard_refresh())
    return serialize_refresh_status(message="Refresh started.")


def grade_variant(value: object) -> tuple[str, str]:
    if not isinstance(value, int | float):
        return ("No grade yet", "neutral")
    if value >= 90:
        return ("On track", "safe")
    if value >= 80:
        return ("Needs attention", "warning")
    return ("At risk", "critical")


def format_day_heading(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return value
    return parsed.strftime("%a %b %-d")


@app.get("/api/overview")
async def overview(session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    return await get_dashboard_overview(session)


@app.get("/api/debug/provenance")
async def debug_provenance(
    course_id: str | None = None,
    assignment_id: str | None = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    """Traceability audit log (URLs, artifact refs, LLM JSON). Query by course and/or assignment."""
    repository = Repository(session)
    events = await repository.list_provenance_events(
        course_id=course_id,
        assignment_id=assignment_id,
        limit=limit,
    )
    return {
        "count": len(events),
        "events": [
            {
                "id": event.id,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "course_id": event.course_id,
                "assignment_id": event.assignment_id,
                "stage": event.stage,
                "source_url": event.source_url,
                "artifact_ref": event.artifact_ref,
                "text_preview": event.text_preview,
                "detail": event.detail,
            }
            for event in events
        ],
    }


@app.get("/api/refresh-status")
async def refresh_status() -> dict[str, object]:
    return serialize_refresh_status()


@app.post("/api/refresh", status_code=202)
async def refresh(mode: str = "full") -> dict[str, object]:
    selected = "additive" if mode == "additive" else "full"
    return start_dashboard_refresh(selected)


@app.get("/", response_class=HTMLResponse)
async def index(session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    overview = await get_dashboard_overview(session)
    return HTMLResponse(render_dashboard_html(overview))
