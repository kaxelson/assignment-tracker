from datetime import datetime

from pydantic import BaseModel, Field


class D2LToolLink(BaseModel):
    name: str
    url: str


class D2LUpcomingEvent(BaseModel):
    title: str
    due_text: str | None = None
    details_url: str | None = None


class D2LGradeSummary(BaseModel):
    weight_achieved_text: str | None = None
    grade_text: str | None = None


class D2LGradeRow(BaseModel):
    title: str
    is_category: bool
    category_title: str | None = None
    points_text: str | None = None
    weight_achieved_text: str | None = None
    grade_text: str | None = None


class D2LContentTopic(BaseModel):
    title: str
    url: str
    module_title: str | None = None
    content_type: str | None = None
    launch_url: str | None = None
    extracted_text: str | None = None


class D2LCourseSnapshot(BaseModel):
    course_id: str
    code: str
    name: str
    offering_code: str | None = None
    semester: str | None = None
    end_date_text: str | None = None
    home_url: str
    final_calculated_grade: D2LGradeSummary | None = None
    tool_links: list[D2LToolLink] = Field(default_factory=list)
    upcoming_events: list[D2LUpcomingEvent] = Field(default_factory=list)
    grade_rows: list[D2LGradeRow] = Field(default_factory=list)
    syllabus_topics: list[D2LContentTopic] = Field(default_factory=list)
    external_tools: list[D2LContentTopic] = Field(default_factory=list)


class D2LDashboardSnapshot(BaseModel):
    fetched_at: datetime
    source_url: str
    courses: list[D2LCourseSnapshot]


class ExternalCourseSnapshot(BaseModel):
    course_id: str
    source_platform: str
    launch_url: str
    title: str | None = None


class ExternalAssignmentSnapshot(BaseModel):
    id: str
    course_id: str
    source_platform: str
    title: str
    type: str
    status: str
    external_url: str | None = None
    description: str | None = None
    due_at: datetime | None = None
    due_text: str | None = None
    points_earned: float | None = None
    points_possible: float | None = None
    grade_pct: float | None = None
    estimated_minutes: int | None = None
    raw_source: dict[str, str | int | float | None] = Field(default_factory=dict)


class ExternalScrapeSnapshot(BaseModel):
    fetched_at: datetime
    courses: list[ExternalCourseSnapshot]
    assignments: list[ExternalAssignmentSnapshot]
