from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import math
from zoneinfo import ZoneInfo

from acc.config import get_settings
from acc.db.repository import CanonicalAssignment, canonical_due_calendar_date, canonical_due_instant_utc


def _user_timezone() -> ZoneInfo:
    return ZoneInfo(get_settings().timezone)

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

TYPE_IMPACT_SCORES = {
    "exam": 80.0,
    "project": 68.0,
    "lab": 52.0,
    "homework": 45.0,
    "discussion": 30.0,
    "reading": 24.0,
}

TYPE_CATEGORY_HINTS = {
    "homework": ("homework", "programming exercise", "mindtap", "mylab", "assignment", "problem"),
    "lab": ("lab", "laboratory"),
    "exam": ("exam", "quiz", "test"),
    "project": ("project", "paper"),
    "discussion": ("discussion",),
    "reading": ("reading", "chapter"),
}


@dataclass(slots=True)
class PlannedAgendaEntry:
    assignment_id: str
    agenda_date: date
    planned_minutes: int
    priority_score: float
    notes: str | None = None


@dataclass(slots=True)
class PriorityReason:
    label: str
    contribution: float


def generate_agenda_plan(
    assignments: list[CanonicalAssignment],
    *,
    now: datetime | None = None,
    horizon_days: int = 7,
    daily_minutes: int = 120,
) -> list[PlannedAgendaEntry]:
    if horizon_days <= 0:
        return []

    tz = _user_timezone()
    reference = normalize_datetime(now or datetime.now(UTC))
    start_date = reference.astimezone(tz).date()
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

    due_day = canonical_due_calendar_date(assignment)
    if due_day is None:
        return False
    if start_date <= due_day <= end_date:
        return True
    # Past-due work still belongs on today's plan until it is completed or submitted.
    return due_day < start_date


def assignment_sort_key(
    assignment: CanonicalAssignment,
    *,
    today: date,
) -> tuple[float, datetime, str]:
    due_date = canonical_due_instant_utc(assignment) or datetime.max.replace(tzinfo=UTC)
    return (-priority_score(assignment, today=today), due_date, assignment.title.lower())


def plan_assignment(
    assignment: CanonicalAssignment,
    *,
    start_date: date,
    end_date: date,
    day_capacity: dict[date, int],
    today: date,
) -> list[PlannedAgendaEntry]:
    due_day = canonical_due_calendar_date(assignment)
    if due_day is None:
        return []
    total_minutes = estimate_assignment_minutes(assignment)
    # Quizzes/exams are usually a single sitting on the due date; spreading them across
    # prior days looks like duplicate rows on the agenda (same title on two day cards).
    if assignment.type in {"exam", "quiz"}:
        work_days = 1
    else:
        work_days = max(1, min((due_day - start_date).days + 1, math.ceil(total_minutes / 45)))
    window_start = max(start_date, due_day - timedelta(days=work_days - 1))
    planning_end = max(due_day, window_start)
    planning_days = [
        window_start + timedelta(days=offset) for offset in range((planning_end - window_start).days + 1)
    ]

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
        catch_up_day = max(due_day, start_date)
        overflow_note = (
            "overflow on due date"
            if due_day >= start_date
            else "overflow scheduled today (overdue)"
        )
        entries.append(
            PlannedAgendaEntry(
                assignment_id=assignment.id,
                agenda_date=catch_up_day,
                planned_minutes=remaining_minutes,
                priority_score=priority + 0.5,
                notes=f"{build_planner_note(assignment)}; {overflow_note}",
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
    due_cal = canonical_due_calendar_date(assignment)
    if due_cal is None:
        return 0.0

    days_until_due = (due_cal - today).days
    urgency = urgency_score(days_until_due)
    impact = impact_score(assignment)
    late_risk = late_risk_score(assignment, days_until_due=days_until_due)
    momentum = 100.0 if assignment.status == "in_progress" else 0.0

    score = (
        urgency * 0.45
        + impact * 0.30
        + late_risk * 0.15
        + momentum * 0.10
    )

    if assignment.type == "exam":
        score += 5.0

    return round(min(100.0, score), 2)


def explain_priority(
    assignment: CanonicalAssignment,
    *,
    today: date,
    max_reasons: int = 3,
) -> list[str]:
    reasons = priority_reasons(assignment, today=today)
    ordered = sorted(reasons, key=lambda reason: (-reason.contribution, reason.label))
    return [reason.label for reason in ordered[:max_reasons]]


def priority_reasons(
    assignment: CanonicalAssignment,
    *,
    today: date,
) -> list[PriorityReason]:
    due_cal = canonical_due_calendar_date(assignment)
    if due_cal is None:
        return []

    days_until_due = (due_cal - today).days
    reasons: list[PriorityReason] = [
        PriorityReason(
            label=urgency_label(days_until_due),
            contribution=urgency_score(days_until_due) * 0.45,
        )
    ]

    weight = infer_assignment_weight(assignment)
    weight_reason = weight_label(assignment, weight=weight)
    if weight_reason is not None:
        reasons.append(weight_reason)

    cutoff_reason = grading_scale_reason(assignment, weight=weight)
    if cutoff_reason is not None:
        reasons.append(cutoff_reason)

    late_reason = late_policy_reason(assignment, days_until_due=days_until_due)
    if late_reason is not None:
        reasons.append(late_reason)

    if assignment.status == "in_progress":
        reasons.append(PriorityReason(label="Already in progress", contribution=10.0))

    return reasons


def urgency_score(days_until_due: int) -> float:
    if days_until_due <= 0:
        return 100.0
    if days_until_due == 1:
        return 92.0
    if days_until_due == 2:
        return 82.0
    if days_until_due == 3:
        return 72.0
    return max(24.0, 72.0 - (days_until_due - 3) * 10.0)


def urgency_label(days_until_due: int) -> str:
    if days_until_due <= 0:
        return "Overdue"
    if days_until_due == 1:
        return "Due within 1 day"
    return f"Due within {days_until_due} days"


def impact_score(assignment: CanonicalAssignment) -> float:
    score = TYPE_IMPACT_SCORES.get(assignment.type, 40.0)
    weight = infer_assignment_weight(assignment)
    if weight is not None:
        score = max(score, min(100.0, 24.0 + weight * 220.0))

    course = assignment.course
    if course and course.current_grade_pct is not None and course.current_grade_pct < 80:
        if assignment.type in {"exam", "project"}:
            score += 8.0
        elif weight is not None and weight >= 0.15:
            score += 6.0

    if assignment.points_possible is not None and assignment.points_possible >= 100:
        score += 3.0
    score += grading_scale_pressure(assignment, weight=weight)

    return min(100.0, score)


def weight_label(
    assignment: CanonicalAssignment,
    *,
    weight: float | None,
) -> PriorityReason | None:
    if weight is None:
        return None

    category = assignment.grade_category or inferred_category_name(assignment)
    category_label = category if category else assignment.type.replace("_", " ").title()
    return PriorityReason(
        label=f"{category_label} worth about {weight * 100:.0f}% of course grade",
        contribution=min(32.0, 8.0 + weight * 80.0),
    )


def infer_assignment_weight(assignment: CanonicalAssignment) -> float | None:
    direct_weight = normalize_fraction(assignment.grade_weight_pct)
    if direct_weight is not None and assignment.grade_category:
        return direct_weight

    course = assignment.course
    if course is None or not course.grade_categories:
        return direct_weight

    matched_weights = [
        weight
        for category in course.grade_categories
        if isinstance(category, Mapping)
        for weight in [normalize_fraction(category.get("weight"))]
        if weight is not None and category_matches_assignment(category, assignment)
    ]
    if matched_weights:
        return max(matched_weights)
    return direct_weight


def category_matches_assignment(category: Mapping[str, object], assignment: CanonicalAssignment) -> bool:
    category_name = normalize_text(category.get("name"))
    if not category_name:
        return False

    grade_category = normalize_text(assignment.grade_category)
    if grade_category and (grade_category in category_name or category_name in grade_category):
        return True

    hints = set(TYPE_CATEGORY_HINTS.get(assignment.type, ()))
    hints.add(assignment.type)
    for hint in hints:
        normalized_hint = normalize_text(hint)
        if normalized_hint and normalized_hint in category_name:
            return True

    title = normalize_text(assignment.title)
    return bool(title and category_name in title and len(category_name) >= 4)


def late_risk_score(
    assignment: CanonicalAssignment,
    *,
    days_until_due: int,
) -> float:
    policy = late_policy_payload(assignment)
    if policy is None:
        score = 55.0
    else:
        score = late_policy_base_score(assignment, policy)

    if days_until_due <= 0:
        score += 10.0
    elif days_until_due <= 1:
        score += 8.0
    elif days_until_due <= 3:
        score += 4.0

    if assignment.status == "overdue":
        score += 10.0

    return min(100.0, max(0.0, score))


def late_policy_reason(
    assignment: CanonicalAssignment,
    *,
    days_until_due: int,
) -> PriorityReason | None:
    policy = late_policy_payload(assignment)
    if policy is None:
        if assignment.status == "overdue":
            return PriorityReason(label="Already overdue", contribution=20.0)
        return None

    exceptions = normalize_text(policy.get("exceptions"))
    if exceptions_block_submission(assignment, exceptions):
        return PriorityReason(label="No late submissions for this item type", contribution=18.0)

    accepts_late = read_bool(policy.get("accepts_late"))
    max_late_days = read_int(policy.get("max_late_days"))
    penalty_per_day = normalize_fraction(policy.get("default_penalty_per_day"))

    if accepts_late is False or max_late_days == 0:
        return PriorityReason(label="No late work accepted", contribution=17.0)
    if penalty_per_day is not None and penalty_per_day > 0:
        return PriorityReason(
            label=f"{penalty_per_day * 100:.0f}% penalty per late day",
            contribution=min(16.0, 5.0 + penalty_per_day * 100.0),
        )
    if days_until_due <= 1 and accepts_late is True and max_late_days is not None:
        return PriorityReason(
            label=f"Only {max_late_days} late day{'s' if max_late_days != 1 else ''} allowed",
            contribution=10.0,
        )
    return None


def late_policy_payload(assignment: CanonicalAssignment) -> Mapping[str, object] | None:
    if isinstance(assignment.late_policy, Mapping):
        return assignment.late_policy

    course = assignment.course
    if course and isinstance(course.syllabus_parsed, Mapping):
        late_policy = course.syllabus_parsed.get("late_policy")
        if isinstance(late_policy, Mapping):
            return late_policy
    return None


def late_policy_base_score(
    assignment: CanonicalAssignment,
    policy: Mapping[str, object],
) -> float:
    accepts_late = read_bool(policy.get("accepts_late"))
    max_late_days = read_int(policy.get("max_late_days"))
    penalty_per_day = normalize_fraction(policy.get("default_penalty_per_day"))
    exceptions = normalize_text(policy.get("exceptions"))

    if exceptions_block_submission(assignment, exceptions):
        return 96.0
    if accepts_late is False or max_late_days == 0:
        return 92.0

    score = 30.0 if accepts_late is True else 55.0

    if penalty_per_day is not None:
        score += min(35.0, penalty_per_day * 1000.0)
    else:
        score += 8.0 if accepts_late is not True else 0.0

    if max_late_days is not None:
        if max_late_days <= 1:
            score += 25.0
        elif max_late_days <= 3:
            score += 15.0
        elif max_late_days <= 5:
            score += 8.0

    if accepts_late is True and (penalty_per_day or 0.0) <= 0.01 and (max_late_days or 999) >= 5:
        score -= 10.0

    return score


def exceptions_block_submission(assignment: CanonicalAssignment, exceptions: str) -> bool:
    if not exceptions:
        return False
    if assignment.type == "exam" and "exam" in exceptions:
        return True
    if assignment.type == "discussion" and "discussion" in exceptions:
        return True
    if assignment.type == "quiz" and "quiz" in exceptions:
        return True
    return False


def build_planner_note(assignment: CanonicalAssignment) -> str:
    due_cal = canonical_due_calendar_date(assignment)
    if due_cal is None:
        return "Auto-planned"
    return f"Auto-planned for due {due_cal.isoformat()}"


def grading_scale_pressure(
    assignment: CanonicalAssignment,
    *,
    weight: float | None,
) -> float:
    gap = next_grade_cutoff_gap(assignment)
    if gap is None:
        return 0.0

    pressure = max(2.0, 12.0 - gap * 2.0)
    if weight is not None:
        pressure *= min(1.5, max(0.75, 1.0 + weight))
    return pressure


def grading_scale_reason(
    assignment: CanonicalAssignment,
    *,
    weight: float | None,
) -> PriorityReason | None:
    gap = next_grade_cutoff_gap(assignment)
    if gap is None:
        return None

    contribution = grading_scale_pressure(assignment, weight=weight)
    if contribution <= 0:
        return None

    return PriorityReason(
        label=f"{gap:.1f} points from the next grade cutoff",
        contribution=contribution,
    )


def next_grade_cutoff_gap(assignment: CanonicalAssignment) -> float | None:
    course = assignment.course
    if course is None or course.current_grade_pct is None:
        return None
    if not isinstance(course.grading_scale, Mapping):
        return None

    current_grade = course.current_grade_pct
    future_cutoffs = []
    for bounds in course.grading_scale.values():
        if not isinstance(bounds, list) or not bounds:
            continue
        lower_bound = normalize_fraction(bounds[0])
        if lower_bound is None:
            continue
        lower_pct = lower_bound * 100.0
        if lower_pct > current_grade:
            future_cutoffs.append(lower_pct)

    if not future_cutoffs:
        return None

    gap = min(future_cutoffs) - current_grade
    if gap <= 0 or gap > 5:
        return None
    return gap


def inferred_category_name(assignment: CanonicalAssignment) -> str | None:
    course = assignment.course
    if course is None or not course.grade_categories:
        return None

    for category in course.grade_categories:
        if isinstance(category, Mapping) and category_matches_assignment(category, assignment):
            name = category.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return None


def round_minutes(value: int) -> int:
    rounded = int(math.ceil(value / MIN_PLANNED_MINUTES) * MIN_PLANNED_MINUTES)
    return max(MIN_PLANNED_MINUTES, rounded)


def normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def normalize_fraction(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        text = str(value).strip().rstrip("%")
        try:
            number = float(text)
        except ValueError:
            return None

    if number < 0:
        return None
    if number > 1:
        if number <= 100:
            return number / 100.0
        return None
    return number


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(str(value).lower().split())


def read_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes"}:
            return True
        if normalized in {"false", "no"}:
            return False
    return None


def read_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
