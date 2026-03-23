from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import math

from acc.db.repository import CanonicalAssignment

COMPLETED_STATUSES = {"completed", "graded", "submitted"}
MIN_PLANNED_MINUTES = 15

TYPE_DEFAULT_MINUTES = {
    "exam": 90,
    "project": 90,
    "lab": 60,
    "discussion": 30,
    "reading": 30,
    "homework": 45,
}


@dataclass(slots=True)
class PlannedAgendaEntry:
    assignment_id: str
    agenda_date: date
    planned_minutes: int
    priority_score: float
    notes: str | None = None


def generate_agenda_plan(
    assignments: list[CanonicalAssignment],
    *,
    now: datetime | None = None,
    horizon_days: int = 7,
    daily_minutes: int = 120,
) -> list[PlannedAgendaEntry]:
    if horizon_days <= 0:
        return []

    reference = normalize_datetime(now or datetime.now(UTC))
    start_date = reference.date()
    end_date = start_date + timedelta(days=horizon_days - 1)
    day_capacity = {start_date + timedelta(days=offset): daily_minutes for offset in range(horizon_days)}

    candidates = sorted(
        (
            assignment
            for assignment in assignments
            if should_plan_assignment(assignment, start_date=start_date, end_date=end_date)
        ),
        key=lambda assignment: assignment_sort_key(assignment, today=start_date),
    )

    planned_entries: list[PlannedAgendaEntry] = []
    for assignment in candidates:
        planned_entries.extend(
            plan_assignment(
                assignment,
                start_date=start_date,
                end_date=end_date,
                day_capacity=day_capacity,
                today=start_date,
            )
        )

    return planned_entries


def should_plan_assignment(
    assignment: CanonicalAssignment,
    *,
    start_date: date,
    end_date: date,
) -> bool:
    if assignment.status in COMPLETED_STATUSES:
        return False
    if assignment.due_date is None:
        return False

    due_date = normalize_datetime(assignment.due_date).date()
    return start_date <= due_date <= end_date


def assignment_sort_key(
    assignment: CanonicalAssignment,
    *,
    today: date,
) -> tuple[datetime, float, str]:
    due_date = normalize_datetime(assignment.due_date) or datetime.max.replace(tzinfo=UTC)
    return (due_date, -priority_score(assignment, today=today), assignment.title.lower())


def plan_assignment(
    assignment: CanonicalAssignment,
    *,
    start_date: date,
    end_date: date,
    day_capacity: dict[date, int],
    today: date,
) -> list[PlannedAgendaEntry]:
    due_date = normalize_datetime(assignment.due_date)
    if due_date is None:
        return []

    due_day = due_date.date()
    total_minutes = estimate_assignment_minutes(assignment)
    work_days = max(1, min((due_day - start_date).days + 1, math.ceil(total_minutes / 45)))
    window_start = max(start_date, due_day - timedelta(days=work_days - 1))
    planning_days = [window_start + timedelta(days=offset) for offset in range((due_day - window_start).days + 1)]

    remaining_minutes = total_minutes
    priority = priority_score(assignment, today=today)
    entries: list[PlannedAgendaEntry] = []

    for index, day in enumerate(planning_days):
        days_left = len(planning_days) - index
        if remaining_minutes <= 0:
            break

        planned = choose_daily_minutes(
            remaining_minutes=remaining_minutes,
            days_left=days_left,
            available_capacity=day_capacity.get(day, 0),
        )
        if planned <= 0:
            continue

        day_capacity[day] = day_capacity.get(day, 0) - planned
        remaining_minutes -= planned
        entries.append(
            PlannedAgendaEntry(
                assignment_id=assignment.id,
                agenda_date=day,
                planned_minutes=planned,
                priority_score=priority,
                notes=build_planner_note(assignment),
            )
        )

    if remaining_minutes > 0:
        entries.append(
            PlannedAgendaEntry(
                assignment_id=assignment.id,
                agenda_date=due_day,
                planned_minutes=remaining_minutes,
                priority_score=priority + 0.5,
                notes=f"{build_planner_note(assignment)}; overflow on due date",
            )
        )

    return entries


def choose_daily_minutes(
    *,
    remaining_minutes: int,
    days_left: int,
    available_capacity: int,
) -> int:
    if remaining_minutes <= 0 or days_left <= 0:
        return 0

    target = math.ceil(remaining_minutes / days_left)
    target = round_minutes(target)

    if available_capacity <= 0:
        return 0

    planned = min(target, available_capacity, remaining_minutes)
    if planned < MIN_PLANNED_MINUTES and available_capacity >= MIN_PLANNED_MINUTES:
        planned = min(MIN_PLANNED_MINUTES, remaining_minutes, available_capacity)
    return planned


def estimate_assignment_minutes(assignment: CanonicalAssignment) -> int:
    if assignment.estimated_minutes is not None:
        return max(MIN_PLANNED_MINUTES, assignment.estimated_minutes)

    base_minutes = TYPE_DEFAULT_MINUTES.get(assignment.type, 45)
    if assignment.status == "in_progress":
        base_minutes += 15
    if assignment.type == "exam" and assignment.course and assignment.course.current_grade_pct is not None:
        if assignment.course.current_grade_pct < 80:
            base_minutes += 30
    return base_minutes


def priority_score(
    assignment: CanonicalAssignment,
    *,
    today: date,
) -> float:
    due_date = normalize_datetime(assignment.due_date)
    if due_date is None:
        return 0.0

    days_until_due = max(0, (due_date.date() - today).days)
    urgency = max(1.0, 8 - days_until_due)
    minutes_weight = estimate_assignment_minutes(assignment) / 60
    status_weight = 1.5 if assignment.status == "in_progress" else 1.0
    return round(urgency * status_weight + minutes_weight, 2)


def build_planner_note(assignment: CanonicalAssignment) -> str:
    due_date = normalize_datetime(assignment.due_date)
    if due_date is None:
        return "Auto-planned"
    return f"Auto-planned for due {due_date.date().isoformat()}"


def round_minutes(value: int) -> int:
    rounded = int(math.ceil(value / MIN_PLANNED_MINUTES) * MIN_PLANNED_MINUTES)
    return max(MIN_PLANNED_MINUTES, rounded)


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
