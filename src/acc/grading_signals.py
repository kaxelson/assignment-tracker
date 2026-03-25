"""Shared interpretation of numeric grades from scraped or model-extracted data."""


def zero_grade_means_not_turned_in(
    *,
    grade_pct: float | None,
    points_earned: float | None,
    points_possible: float | None,
) -> bool:
    """True when an explicit zero should be treated as missing work, not a scored attempt."""
    if grade_pct is not None and grade_pct <= 0:
        return True
    if (
        points_earned is not None
        and points_earned <= 0
        and points_possible is not None
        and points_possible > 0
    ):
        return True
    return False
