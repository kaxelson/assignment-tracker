from datetime import date, datetime

from sqlalchemy import JSON, Boolean, Date, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Course(Base):
    __tablename__ = "courses"

    id: Mapped[str] = mapped_column(String(50), primary_key=True)
    code: Mapped[str] = mapped_column(String(20))
    name: Mapped[str] = mapped_column(String(255))
    instructor: Mapped[str | None] = mapped_column(String(255))
    d2l_course_id: Mapped[str] = mapped_column(String(50))
    d2l_url: Mapped[str] = mapped_column(Text)
    semester: Mapped[str] = mapped_column(String(50))
    external_platform: Mapped[str | None] = mapped_column(String(50))
    external_platform_url: Mapped[str | None] = mapped_column(Text)
    textbook: Mapped[str | None] = mapped_column(Text)
    syllabus_raw_text: Mapped[str | None] = mapped_column(Text)
    syllabus_parsed: Mapped[dict | None] = mapped_column(JSON)
    grading_scale: Mapped[dict | None] = mapped_column(JSON)
    grade_categories: Mapped[dict | None] = mapped_column(JSON)
    late_policy_global: Mapped[str | None] = mapped_column(Text)
    current_grade_pct: Mapped[float | None] = mapped_column(Float)
    current_letter_grade: Mapped[str | None] = mapped_column(String(5))
    last_scraped_d2l: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_scraped_external: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_syllabus_parse: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    assignments: Mapped[list["Assignment"]] = relationship(back_populates="course")
    provenance_events: Mapped[list["ProvenanceEvent"]] = relationship(back_populates="course")


class Assignment(Base):
    __tablename__ = "assignments"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    course_id: Mapped[str] = mapped_column(ForeignKey("courses.id"))
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(String(30))
    source_platform: Mapped[str] = mapped_column(String(30))
    external_url: Mapped[str | None] = mapped_column(Text)
    available_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    due_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    close_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    grade_category: Mapped[str | None] = mapped_column(String(100))
    grade_weight_pct: Mapped[float | None] = mapped_column(Float)
    points_possible: Mapped[float | None] = mapped_column(Float)
    points_earned: Mapped[float | None] = mapped_column(Float)
    grade_pct: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(30))
    is_submitted: Mapped[bool] = mapped_column(Boolean, default=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_late: Mapped[bool] = mapped_column(Boolean, default=False)
    days_late: Mapped[int] = mapped_column(Integer, default=0)
    late_policy: Mapped[dict | None] = mapped_column(JSON)
    estimated_minutes: Mapped[int | None] = mapped_column(Integer)
    is_multi_day: Mapped[bool] = mapped_column(Boolean, default=False)
    raw_scraped_data: Mapped[dict | None] = mapped_column(JSON)
    last_scraped: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    course: Mapped["Course"] = relationship(back_populates="assignments")
    agenda_entries: Mapped[list["AgendaEntry"]] = relationship(back_populates="assignment")
    provenance_events: Mapped[list["ProvenanceEvent"]] = relationship(back_populates="assignment")


class ProvenanceEvent(Base):
    """Audit trail: where dashboard values came from (URLs, artifacts, LLM rationale JSON)."""

    __tablename__ = "provenance_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    course_id: Mapped[str | None] = mapped_column(
        String(50), ForeignKey("courses.id", ondelete="SET NULL"), nullable=True
    )
    assignment_id: Mapped[str | None] = mapped_column(
        String(100), ForeignKey("assignments.id", ondelete="SET NULL"), nullable=True
    )
    stage: Mapped[str] = mapped_column(String(64))
    source_url: Mapped[str | None] = mapped_column(Text)
    artifact_ref: Mapped[str | None] = mapped_column(Text)
    text_preview: Mapped[str | None] = mapped_column(Text)
    detail: Mapped[dict | None] = mapped_column(JSON)

    course: Mapped["Course | None"] = relationship(back_populates="provenance_events")
    assignment: Mapped["Assignment | None"] = relationship(back_populates="provenance_events")


class AgendaEntry(Base):
    __tablename__ = "agenda_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    assignment_id: Mapped[str] = mapped_column(ForeignKey("assignments.id"))
    agenda_date: Mapped[date] = mapped_column(Date)
    planned_minutes: Mapped[int] = mapped_column(Integer, default=30)
    priority_score: Mapped[float] = mapped_column(Float, default=0.0)
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    assignment: Mapped["Assignment"] = relationship(back_populates="agenda_entries")

