from collections import Counter
from datetime import UTC, datetime, timedelta
from html import escape

from fastapi import Depends, FastAPI
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from acc.config import Settings, get_settings
from acc.db.engine import get_session
from acc.db.models import AgendaEntry, Assignment
from acc.db.repository import Repository

settings = get_settings()

app = FastAPI(title="Academic Command Center")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "env": settings.env}


def build_empty_overview(settings: Settings, error: str | None = None) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_ready": error is None,
        "error": error,
        "d2l_storage_state": settings.d2l_storage_state_path.exists(),
        "summary": {
            "course_count": 0,
            "assignment_count": 0,
            "upcoming_count": 0,
        },
        "courses": [],
        "upcoming_assignments": [],
        "agenda_entries": [],
        "agenda_days": [],
    }


def format_due_label(assignment: Assignment) -> str | None:
    if assignment.raw_scraped_data:
        due_text = assignment.raw_scraped_data.get("due_text")
        if isinstance(due_text, str) and due_text:
            return due_text

    if assignment.due_date is None:
        return None

    return assignment.due_date.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def format_agenda_day(entry: AgendaEntry) -> str:
    return entry.agenda_date.isoformat()


def aggregate_agenda_entries(
    entries: list[AgendaEntry],
    canonical_assignments: dict[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    aggregated_by_key: dict[tuple[str, str], dict[str, object]] = {}

    for entry in entries:
        agenda_date = format_agenda_day(entry)
        assignment_id = entry.assignment_id
        canonical = canonical_assignments.get(assignment_id)
        assignment_title = (
            canonical.title
            if canonical is not None
            else entry.assignment.title
            if entry.assignment
            else assignment_id
        )
        course_code = entry.assignment.course.code if entry.assignment and entry.assignment.course else None
        key = (agenda_date, assignment_id)

        existing = aggregated_by_key.get(key)
        if existing is None:
            aggregated_by_key[key] = {
                "agenda_date": agenda_date,
                "planned_minutes": entry.planned_minutes,
                "priority_score": entry.priority_score,
                "assignment_id": assignment_id,
                "assignment_title": assignment_title,
                "course_code": course_code,
                "notes": entry.notes,
            }
            continue

        existing["planned_minutes"] += entry.planned_minutes
        existing["priority_score"] = max(existing["priority_score"], entry.priority_score)
        if not existing["notes"] and entry.notes:
            existing["notes"] = entry.notes

    agenda_entries = sorted(
        aggregated_by_key.values(),
        key=lambda item: (
            str(item["agenda_date"]),
            -float(item["priority_score"]),
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
    canonical_assignments = {
        assignment.id: assignment for assignment in await repository.list_canonical_assignments()
    }
    course_rows = await repository.list_course_overview()
    upcoming_assignments = await repository.list_upcoming_assignments(
        limit=12,
        now=now,
    )
    agenda_entries = await repository.list_agenda_entries(
        date_from=now,
        date_to=now + timedelta(days=6),
    )
    upcoming_counts = Counter(assignment.course_id for assignment in upcoming_assignments)

    courses = [
        {
            "id": row.course.id,
            "code": row.course.code,
            "name": row.course.name,
            "semester": row.course.semester,
            "current_grade_pct": row.course.current_grade_pct,
            "assignment_count": row.assignment_count,
            "upcoming_count": upcoming_counts.get(row.course.id, 0),
            "external_platform": row.course.external_platform,
            "d2l_url": row.course.d2l_url,
        }
        for row in course_rows
    ]
    upcoming = [
        {
            "id": assignment.id,
            "title": assignment.title,
            "course_code": assignment.course.code if assignment.course else assignment.course_id,
            "course_name": assignment.course.name if assignment.course else assignment.course_id,
            "due_at": assignment.due_date.isoformat() if assignment.due_date else None,
            "due_label": format_due_label(assignment),
            "status": assignment.status,
            "type": assignment.type,
            "grade_pct": assignment.grade_pct,
            "external_url": assignment.external_url,
        }
        for assignment in upcoming_assignments
    ]
    agenda, agenda_days = aggregate_agenda_entries(list(agenda_entries), canonical_assignments)

    return {
        "generated_at": now.isoformat(),
        "database_ready": True,
        "error": None,
        "d2l_storage_state": settings.d2l_storage_state_path.exists(),
        "summary": {
            "course_count": len(courses),
            "assignment_count": sum(row.assignment_count for row in course_rows),
            "upcoming_count": len(upcoming),
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
    agenda_days = overview["agenda_days"]
    error = overview["error"]

    course_cards = "".join(
        f"""
        <article class="card">
          <p class="eyebrow">{escape(str(course["code"]))}</p>
          <h2>{escape(str(course["name"]))}</h2>
          <p class="meta">{escape(str(course["semester"]))}</p>
          <dl class="stats-grid">
            <div><dt>Grade</dt><dd>{escape(format_grade(course["current_grade_pct"]))}</dd></div>
            <div><dt>Assignments</dt><dd>{course["assignment_count"]}</dd></div>
            <div><dt>Due Soon</dt><dd>{course["upcoming_count"]}</dd></div>
          </dl>
        </article>
        """
        for course in courses
    )
    upcoming_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(item["course_code"]))}</td>
          <td>{escape(str(item["title"]))}</td>
          <td>{escape(str(item["due_label"] or "TBD"))}</td>
          <td>{escape(str(item["status"]))}</td>
        </tr>
        """
        for item in upcoming_assignments
    )
    agenda_rows = "".join(
        "".join(
            [
                f"""
                <tr class="agenda-day-row">
                  <td>{escape(str(day["agenda_date"]))}</td>
                  <td colspan="2">{day["entry_count"]} items planned</td>
                  <td>{day["total_minutes"]} min total</td>
                </tr>
                """
            ]
            + [
                f"""
                <tr>
                  <td></td>
                  <td>{escape(str(item["course_code"] or "Unknown"))}</td>
                  <td>{escape(str(item["assignment_title"]))}</td>
                  <td>{item["planned_minutes"]} min</td>
                </tr>
                """
                for item in day["items"]
            ]
        )
        for day in agenda_days
    )

    if not course_cards:
        course_cards = '<article class="empty">No synced course data yet.</article>'
    if not upcoming_rows:
        upcoming_rows = '<tr><td colspan="4" class="empty-table">No upcoming assignments found.</td></tr>'
    if not agenda_rows:
        agenda_rows = '<tr><td colspan="4" class="empty-table">No saved agenda entries yet.</td></tr>'

    error_banner = ""
    if error:
        error_banner = f'<section class="alert">{escape(str(error))}</section>'

    return f"""<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Academic Command Center</title>
    <style>
      :root {{
        --bg: #f4efe6;
        --bg-accent: #e4d2bf;
        --surface: rgba(255, 252, 247, 0.88);
        --surface-strong: #fffaf2;
        --text: #1f2b3d;
        --muted: #5e6a78;
        --line: rgba(31, 43, 61, 0.12);
        --accent: #9d4d2f;
        --accent-strong: #7e351a;
      }}

      * {{
        box-sizing: border-box;
      }}

      body {{
        margin: 0;
        color: var(--text);
        font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
        background:
          radial-gradient(circle at top right, rgba(157, 77, 47, 0.18), transparent 26rem),
          linear-gradient(180deg, var(--bg) 0%, #fbf8f2 55%, var(--bg-accent) 100%);
      }}

      main {{
        width: min(1100px, calc(100vw - 2rem));
        margin: 0 auto;
        padding: 2.5rem 0 4rem;
      }}

      header {{
        margin-bottom: 1.5rem;
      }}

      h1,
      h2,
      p {{
        margin: 0;
      }}

      .hero {{
        display: grid;
        gap: 0.75rem;
        padding: 1.5rem;
        border: 1px solid var(--line);
        border-radius: 1.5rem;
        background: linear-gradient(135deg, rgba(255, 250, 242, 0.96), rgba(255, 245, 233, 0.8));
        box-shadow: 0 18px 45px rgba(31, 43, 61, 0.08);
      }}

      .hero h1 {{
        font-size: clamp(2rem, 4vw, 3.4rem);
        line-height: 0.95;
        letter-spacing: -0.04em;
      }}

      .hero p {{
        color: var(--muted);
        max-width: 42rem;
      }}

      .summary-grid,
      .course-grid {{
        display: grid;
        gap: 1rem;
      }}

      .summary-grid {{
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        margin: 1.5rem 0;
      }}

      .course-grid {{
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      }}

      .summary-card,
      .card,
      .table-card,
      .alert,
      .empty {{
        border: 1px solid var(--line);
        border-radius: 1.25rem;
        background: var(--surface);
        backdrop-filter: blur(8px);
        box-shadow: 0 12px 30px rgba(31, 43, 61, 0.06);
      }}

      .summary-card,
      .card,
      .table-card,
      .alert,
      .empty {{
        padding: 1.25rem;
      }}

      .summary-card .value {{
        display: block;
        font-size: 2rem;
        color: var(--accent-strong);
      }}

      .summary-card .label,
      .meta,
      .eyebrow {{
        color: var(--muted);
      }}

      .eyebrow {{
        margin-bottom: 0.4rem;
        font-size: 0.8rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }}

      .card h2 {{
        font-size: 1.2rem;
        margin-bottom: 0.4rem;
      }}

      .stats-grid {{
        display: grid;
        gap: 0.85rem;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        margin-top: 1rem;
      }}

      .stats-grid dt {{
        font-size: 0.78rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}

      .stats-grid dd {{
        margin: 0.15rem 0 0;
        font-size: 1.1rem;
      }}

      section {{
        margin-top: 1.5rem;
      }}

      .section-title {{
        margin-bottom: 0.85rem;
        font-size: 1.35rem;
      }}

      table {{
        width: 100%;
        border-collapse: collapse;
      }}

      th,
      td {{
        padding: 0.8rem 0;
        border-bottom: 1px solid var(--line);
        text-align: left;
        vertical-align: top;
      }}

      th {{
        font-size: 0.76rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }}

      tr:last-child td {{
        border-bottom: none;
      }}

      .alert {{
        color: var(--accent-strong);
        background: rgba(157, 77, 47, 0.1);
      }}

      .agenda-day-row td {{
        font-weight: 600;
        color: var(--accent-strong);
        background: rgba(157, 77, 47, 0.06);
      }}

      .empty,
      .empty-table {{
        color: var(--muted);
      }}

      @media (max-width: 640px) {{
        .stats-grid {{
          grid-template-columns: 1fr;
        }}
      }}
    </style>
  </head>
  <body>
    <main>
      <header class="hero">
        <p class="eyebrow">Academic Command Center</p>
        <h1>Coursework, scraped and surfaced.</h1>
        <p>Synced D2L data from the local database, ready for review in one place.</p>
      </header>

      {error_banner}

      <section class="summary-grid">
        <article class="summary-card">
          <span class="value">{summary["course_count"]}</span>
          <span class="label">Courses</span>
        </article>
        <article class="summary-card">
          <span class="value">{summary["assignment_count"]}</span>
          <span class="label">Assignments</span>
        </article>
        <article class="summary-card">
          <span class="value">{summary["upcoming_count"]}</span>
          <span class="label">Upcoming Items</span>
        </article>
        <article class="summary-card">
          <span class="value">{'yes' if overview["d2l_storage_state"] else 'no'}</span>
          <span class="label">Saved D2L Session</span>
        </article>
      </section>

      <section>
        <h2 class="section-title">Courses</h2>
        <div class="course-grid">{course_cards}</div>
      </section>

      <section>
        <h2 class="section-title">Upcoming Assignments</h2>
        <div class="table-card">
          <table>
            <thead>
              <tr>
                <th>Course</th>
                <th>Assignment</th>
                <th>Due</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>{upcoming_rows}</tbody>
          </table>
        </div>
      </section>

      <section>
        <h2 class="section-title">Saved Agenda</h2>
        <div class="table-card">
          <table>
            <thead>
              <tr>
                <th>Day</th>
                <th>Course</th>
                <th>Planned Work</th>
                <th>Minutes</th>
              </tr>
            </thead>
            <tbody>{agenda_rows}</tbody>
          </table>
        </div>
      </section>
    </main>
  </body>
</html>"""


def format_grade(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


@app.get("/api/overview")
async def overview(session: AsyncSession = Depends(get_session)) -> dict[str, object]:
    return await get_dashboard_overview(session)


@app.get("/", response_class=HTMLResponse)
async def index(session: AsyncSession = Depends(get_session)) -> HTMLResponse:
    overview = await get_dashboard_overview(session)
    return HTMLResponse(render_dashboard_html(overview))
