SYLLABUS_PARSE_PROMPT = """
You are an expert academic advisor. Analyze this course syllabus and extract structured information.

Return a JSON object with exactly this structure:

{
  "course_name": "string or null",
  "course_code": "string or null",
  "instructor": "string or null",
  "semester": "string or null",
  "grade_categories": [
    {
      "name": "Homework",
      "weight": 0.20,
      "description": "Weekly homework assignments via MyLab",
      "drop_lowest": 1,
      "total_count": null
    }
  ],
  "grading_scale": {
    "A": [93, 100],
    "A-": [90, 93]
  },
  "late_policy": {
    "default_penalty_per_day": 0.02,
    "max_late_days": 5,
    "accepts_late": true,
    "exceptions": "Exams and quizzes cannot be submitted late",
    "raw_text": "Exact syllabus late-work text"
  },
  "exams": [
    {
      "name": "Midterm Exam 1",
      "date": "2026-02-15",
      "weight_pct": 15.0,
      "topics": "Chapters 1-6",
      "location": "In-class"
    }
  ],
  "important_dates": [
    {
      "date": "2026-03-09",
      "event": "Spring Break - no classes"
    }
  ],
  "external_tools": [
    {
      "name": "Pearson MyLab Mastering",
      "purpose": "Homework and some quizzes",
      "textbook": "Physics for Scientists & Engineers: A Strategic Approach"
    }
  ],
  "office_hours": "string or null",
  "attendance_policy": "string or null",
  "extra_credit": "string or null"
}

Rules:
- Extract only what the syllabus states. Do not invent information.
- If a field cannot be determined, use null or an empty list/object as appropriate.
- Grade-category weights should be decimals like 0.20, not percentages like 20.
- Dates must be ISO format YYYY-MM-DD when the year can be determined.
- Include the exact late-policy text in late_policy.raw_text when possible.
""".strip()


CRAWL_ASSIGNMENT_EXTRACTION_PROMPT = """
You are extracting coursework facts from authenticated learning-platform crawl artifacts.

Return a JSON object with exactly this structure:

{
  "assignments": [
    {
      "title": "string",
      "assignment_type": "homework | exam | lab | discussion | reading | project | other | null",
      "source_platform": "d2l | cengage_mindtap | pearson_mylab | null",
      "grade_category": "string or null",
      "due_at": "ISO 8601 datetime with timezone or null",
      "due_on": "YYYY-MM-DD or null",
      "due_text": "string or null",
      "weight_pct": 5.0,
      "points_possible": 100.0,
      "points_earned": 95.0,
      "grade_pct": 95.0,
      "submitted": true,
      "graded": true,
      "optional": false,
      "extra_credit": false,
      "counts_toward_grade": true,
      "status": "upcoming | overdue | in_progress | submitted | completed | graded | available | unknown",
      "rationale": "short explanation of why this item is considered coursework",
      "evidence_spans": [
        {
          "artifact_id": "artifact-id-1",
          "quote": "short exact quote from the artifact text"
        }
      ],
      "evidence_artifact_ids": ["artifact-id-1", "artifact-id-2"],
      "notes": ["short factual note"]
    }
  ]
}

Rules:
- Extract only assignments explicitly evidenced in the artifacts.
- Use null when a field is not supported by the artifacts.
- `rationale` is required for each returned assignment and should be short and specific.
- Include at least one `evidence_spans` row for each returned assignment. Each span must cite
  an `artifact_id` present in this prompt and a short quote that supports the extracted fact.
- Mark practice tests or explicit practice items as optional and not counting toward grade.
- Mark extra credit explicitly when the artifacts say so.
- If an external-platform row shows `Not started`, `--/100`, `0/100`, or similar placeholder progress, you may record `points_earned` and `grade_pct` as zero when that is what the row shows; downstream logic treats any zero score as work not turned in (not a completed attempt).
- Use D2L announcement, content, assignments, grades, and course-home artifacts to decide which external items are actually assigned coursework. Do not treat an entire Cengage or Pearson inventory list as assigned coursework when there is no corroborating D2L evidence and no due date, submission, or grade.
- D2L content items and announcements may list the same assignment title as the external platform, often with extra suffix text after a colon. Treat those as the same assignment when the core title matches.
- If an item is submitted but there is no grade yet, set submitted=true and graded=false.
- If a grade is visible, set graded=true.
- If no assignment facts are present in the chunk, return {"assignments": []}.
- Return JSON only.
""".strip()


CRAWL_RULE_EXTRACTION_PROMPT = """
You are extracting course grading rules from authenticated learning-platform crawl artifacts.

Return a JSON object with exactly this structure:

{
  "course_code": "string or null",
  "course_name": "string or null",
  "grade_categories": [
    {
      "name": "Homework",
      "weight": 0.20,
      "notes": "Weekly assignments"
    }
  ],
  "grading_scale": {
    "A": [93, 100]
  },
  "late_policy": {
    "raw_text": "string or null",
    "accepts_late": true,
    "default_penalty_per_day": 0.02,
    "max_late_days": 5
  },
  "notes": ["short factual note"]
}

Rules:
- Use only what the artifacts explicitly support.
- Category weights must be decimals like 0.20, not percentages like 20.
- If a field is not supported, use null or an empty list/object.
- Return JSON only.
""".strip()


CRAWL_LINK_SELECTION_PROMPT = """
You choose which hyperlinks to follow next while crawling an authenticated learning site.

Goal: find pages that can contain assignment due dates, submission or completion status, grades,
points, rubrics, calendars, quizzes or exams, grading policies,
syllabus rules, or course announcements about deadlines.

Return a JSON object with exactly this shape:

{
  "follow": [0, 2],
  "notes": "optional short reason"
}

Rules:
- `follow` is a list of zero-based indices into the LINKS list provided by the user. Only include
  indices you want the crawler to visit.
- Prefer links that plausibly lead to coursework, grades, or schedule information for this course.
- Skip links that are clearly eText/ebook readers, streaming video, generic help, purchase or
  subscription, account settings, logout, instructor-only tools, class roster or chat, or unrelated
  third-party sites.
- If the page already shows the needed assignment or grade lists, you may return an empty `follow`
  list to avoid redundant navigation.
- Do not invent indices; only use indices that exist in the provided LINKS list.
- Return JSON only.
""".strip()
