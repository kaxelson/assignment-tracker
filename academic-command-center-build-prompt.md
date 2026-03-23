# Academic Command Center — AI Agent Build Prompt

## PROJECT OVERVIEW

Build a Python-based system called "Academic Command Center" that automatically scrapes academic data from multiple learning platforms, synthesizes it using AI, and delivers prioritized daily agendas via email and SMS.

The student attends **Oakton Community College** and uses **D2L Brightspace** as the primary LMS. Some courses use external platforms for homework and grading:

| Course | Code | External Platform | Textbook |
|--------|------|-------------------|----------|
| Physics for Scientists & Engineers | PHY 221 | Pearson MyLab Mastering | Pearson Plus: Physics for Scientists & Engineers — A Strategic Approach |
| Calculus: Early Transcendentals | MAT 252 | Pearson MyLab Mastering | Pearson Plus: Calculus Early Transcendentals |
| Data Structures in Python | CIS 156 | Cengage MindTap | Lambert: Fundamentals of Python Data Structures |

The system must:
1. Log into D2L and scrape course data (syllabus, assignments, grades, calendar)
2. Detect and follow deep links to external platforms (Pearson, Cengage)
3. Scrape assignment, grade, and due date data from those external platforms
4. Use an LLM API to parse syllabi and extract grading policies, weights, late penalties
5. Normalize all data into a unified schema
6. Generate prioritized daily agendas using a scoring algorithm
7. Deliver agendas via email (HTML) and SMS each morning
8. Provide a web dashboard for interactive viewing

---

## TECH STACK

- **Language**: Python 3.11+
- **Browser Automation**: Playwright (async)
- **Database**: PostgreSQL 15+ with SQLAlchemy 2.0 (async) + Alembic for migrations
- **Data Validation**: Pydantic v2
- **AI/LLM**: OpenAI API (GPT-4o or GPT-5.4) for syllabus parsing and agenda tips
- **Email**: SendGrid (or AWS SES as fallback)
- **SMS**: Twilio
- **Web Dashboard**: FastAPI backend + Next.js frontend (or FastAPI with Jinja2 templates as MVP)
- **Scheduling**: APScheduler (in-process) or system cron
- **Configuration**: Pydantic Settings with .env files
- **Testing**: pytest + pytest-asyncio + pytest-playwright
- **Logging**: structlog
- **Containerization**: Docker + docker-compose for local dev (Postgres, the app)

---

## PROJECT STRUCTURE

```
academic-command-center/
├── pyproject.toml
├── alembic.ini
├── alembic/
│   └── versions/
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── README.md
│
├── src/
│   └── acc/                          # main package: "academic command center"
│       ├── __init__.py
│       ├── config.py                 # Pydantic Settings, all env vars
│       ├── main.py                   # CLI entrypoint, orchestrator
│       │
│       ├── db/
│       │   ├── __init__.py
│       │   ├── engine.py             # async SQLAlchemy engine/session factory
│       │   ├── models.py             # SQLAlchemy ORM models
│       │   └── repository.py         # CRUD operations
│       │
│       ├── scrapers/
│       │   ├── __init__.py
│       │   ├── base.py               # Abstract base scraper class
│       │   ├── d2l.py                # D2L Brightspace scraper
│       │   ├── pearson.py            # Pearson MyLab Mastering scraper
│       │   ├── cengage.py            # Cengage MindTap scraper
│       │   └── utils.py              # Shared scraping utilities (waits, selectors, screenshots)
│       │
│       ├── ai/
│       │   ├── __init__.py
│       │   ├── syllabus_parser.py    # LLM-based syllabus analysis
│       │   ├── agenda_tips.py        # LLM-generated study tips
│       │   └── prompts.py            # All LLM prompt templates
│       │
│       ├── engine/
│       │   ├── __init__.py
│       │   ├── normalizer.py         # Cross-platform data normalization
│       │   ├── priority.py           # Priority scoring algorithm
│       │   ├── decomposer.py         # Multi-day task decomposition
│       │   └── agenda.py             # Daily agenda generation
│       │
│       ├── delivery/
│       │   ├── __init__.py
│       │   ├── email_sender.py       # SendGrid email delivery
│       │   ├── sms_sender.py         # Twilio SMS delivery
│       │   ├── templates/
│       │   │   ├── daily_agenda.html  # HTML email template
│       │   │   └── daily_agenda.txt   # Plain text fallback
│       │   └── formatter.py          # Agenda → email/SMS content
│       │
│       ├── dashboard/
│       │   ├── __init__.py
│       │   ├── app.py                # FastAPI app
│       │   ├── routes.py             # API routes
│       │   └── templates/            # Jinja2 templates (MVP dashboard)
│       │
│       └── scheduler/
│           ├── __init__.py
│           └── jobs.py               # Scheduled job definitions
│
└── tests/
    ├── conftest.py
    ├── test_scrapers/
    ├── test_ai/
    ├── test_engine/
    └── test_delivery/
```

---

## DATABASE SCHEMA

Design these SQLAlchemy models in `src/acc/db/models.py`:

### Course
```python
class Course(Base):
    __tablename__ = "courses"
    
    id: Mapped[str] = mapped_column(String(50), primary_key=True)  # e.g. "phy-221-spring-2026"
    code: Mapped[str]                    # "PHY 221"
    name: Mapped[str]                    # "Physics for Scientists & Engineers"
    instructor: Mapped[str | None]
    d2l_course_id: Mapped[str]           # D2L's internal course identifier
    d2l_url: Mapped[str]                 # Direct URL to the course in D2L
    semester: Mapped[str]                # "Spring 2026"
    
    # External platform info (populated after detection)
    external_platform: Mapped[str | None]     # "pearson_mylab" | "cengage_mindtap" | null
    external_platform_url: Mapped[str | None] # Deep link URL
    textbook: Mapped[str | None]
    
    # Grading policy (populated by AI syllabus parser)
    syllabus_raw_text: Mapped[str | None]     # Extracted text from syllabus PDF
    syllabus_parsed: Mapped[dict | None]      # JSON: full parsed syllabus structure
    grading_scale: Mapped[dict | None]        # JSON: {"A": [93,100], "A-": [90,93], ...}
    grade_categories: Mapped[dict | None]     # JSON: [{"name": "Homework", "weight": 0.20}, ...]
    late_policy_global: Mapped[str | None]    # Default late policy for the course
    
    current_grade_pct: Mapped[float | None]   # Current calculated grade
    current_letter_grade: Mapped[str | None]
    
    last_scraped_d2l: Mapped[datetime | None]
    last_scraped_external: Mapped[datetime | None]
    last_syllabus_parse: Mapped[datetime | None]
    
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    
    assignments: Mapped[list["Assignment"]] = relationship(back_populates="course")
```

### Assignment
```python
class Assignment(Base):
    __tablename__ = "assignments"
    
    id: Mapped[str] = mapped_column(String(100), primary_key=True)  # platform-specific unique ID
    course_id: Mapped[str] = mapped_column(ForeignKey("courses.id"))
    
    title: Mapped[str]
    description: Mapped[str | None]
    type: Mapped[str]                    # "homework" | "lab" | "quiz" | "exam" | "project" | "reading" | "discussion"
    source_platform: Mapped[str]         # "d2l" | "pearson_mylab" | "cengage_mindtap"
    external_url: Mapped[str | None]     # Direct link to the assignment
    
    # Dates
    available_date: Mapped[datetime | None]
    due_date: Mapped[datetime | None]
    close_date: Mapped[datetime | None]  # When late submission window closes
    
    # Grading
    grade_category: Mapped[str | None]   # Must match a category in course.grade_categories
    grade_weight_pct: Mapped[float | None]  # This assignment's weight as % of final grade
    points_possible: Mapped[float | None]
    points_earned: Mapped[float | None]
    grade_pct: Mapped[float | None]      # points_earned / points_possible * 100
    
    # Status
    status: Mapped[str]                  # "upcoming" | "available" | "in_progress" | "submitted" | "graded" | "late" | "missing"
    is_submitted: Mapped[bool] = mapped_column(default=False)
    submitted_at: Mapped[datetime | None]
    is_late: Mapped[bool] = mapped_column(default=False)
    days_late: Mapped[int] = mapped_column(default=0)
    
    # Late policy (assignment-level override, or inherited from course)
    late_policy: Mapped[dict | None]     # JSON: {"penalty_per_day": 0.02, "max_late_days": 5, "accepts_late": true}
    
    # Estimation
    estimated_minutes: Mapped[int | None]
    is_multi_day: Mapped[bool] = mapped_column(default=False)
    
    # Scraping metadata
    raw_scraped_data: Mapped[dict | None]  # JSON: raw data from platform for debugging
    last_scraped: Mapped[datetime]
    
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    
    course: Mapped["Course"] = relationship(back_populates="assignments")
```

### AgendaEntry
```python
class AgendaEntry(Base):
    __tablename__ = "agenda_entries"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    assignment_id: Mapped[str] = mapped_column(ForeignKey("assignments.id"))
    
    agenda_date: Mapped[date]            # The date this entry appears on
    priority_score: Mapped[float]        # Computed priority (higher = do first)
    priority_label: Mapped[str]          # "critical" | "high" | "medium" | "low"
    
    # For multi-day tasks
    is_sub_task: Mapped[bool] = mapped_column(default=False)
    sub_task_label: Mapped[str | None]   # "Day 3/5: Review Chapters 12-13"
    daily_minutes: Mapped[int | None]    # Minutes allocated for this specific day
    
    notes: Mapped[str | None]            # AI-generated study tips or context
    
    created_at: Mapped[datetime]
```

### ScrapeLog
```python
class ScrapeLog(Base):
    __tablename__ = "scrape_logs"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    platform: Mapped[str]                # "d2l" | "pearson_mylab" | "cengage_mindtap"
    course_id: Mapped[str | None]
    status: Mapped[str]                  # "success" | "partial" | "failed"
    error_message: Mapped[str | None]
    screenshot_path: Mapped[str | None]  # Path to failure screenshot
    duration_seconds: Mapped[float]
    items_found: Mapped[int] = mapped_column(default=0)
    items_updated: Mapped[int] = mapped_column(default=0)
    started_at: Mapped[datetime]
    completed_at: Mapped[datetime | None]
```

---

## CONFIGURATION

Design `src/acc/config.py` using Pydantic Settings:

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="ACC_")
    
    # Database
    database_url: str = "postgresql+asyncpg://acc:acc@localhost:5432/acc"
    
    # D2L Credentials
    d2l_base_url: str = "https://d2l.oakton.edu"  # Verify actual URL
    d2l_username: str
    d2l_password: str
    d2l_totp_secret: str | None = None  # For MFA if needed
    
    # LLM
    openai_api_key: str
    openai_model: str = "gpt-4o"
    
    # Email (SendGrid)
    sendgrid_api_key: str | None = None
    email_from: str = "agenda@yourdomain.com"
    email_to: str                        # Student's email
    
    # SMS (Twilio)
    twilio_account_sid: str | None = None
    twilio_auth_token: str | None = None
    twilio_from_number: str | None = None
    sms_to_number: str | None = None     # Student's phone
    
    # Scheduling
    agenda_delivery_time: str = "07:00"  # 24hr format, local time
    scrape_interval_hours: int = 6
    timezone: str = "America/Chicago"
    
    # Scraping
    headless: bool = True                # Set False for debugging
    screenshot_on_failure: bool = True
    scrape_timeout_ms: int = 30000
    session_cache_dir: str = ".sessions"
    
    # Agenda
    max_daily_study_hours: float = 6.0
    multi_day_threshold_minutes: int = 120
    lookahead_days: int = 14             # How far ahead to scan
```

Create a `.env.example` file with all keys and placeholder values.

---

## SCRAPER DESIGN

### Base Scraper (`src/acc/scrapers/base.py`)

```python
from abc import ABC, abstractmethod
from playwright.async_api import async_playwright, Browser, Page

class BaseScraper(ABC):
    """
    All scrapers inherit from this. Provides:
    - Shared browser/page lifecycle management
    - Session cookie caching and restoration
    - Screenshot-on-failure wrapper
    - Structured logging
    - Retry logic with exponential backoff
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.browser: Browser | None = None
        self.page: Page | None = None
    
    async def __aenter__(self):
        """Launch browser, attempt to restore cached session."""
        pw = await async_playwright().start()
        self.browser = await pw.chromium.launch(headless=self.settings.headless)
        context = await self.browser.new_context(
            storage_state=self._session_cache_path() if self._session_cache_exists() else None,
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        self.page = await context.new_page()
        return self
    
    async def __aexit__(self, *args):
        """Save session cookies, close browser."""
        if self.page:
            await self.page.context.storage_state(path=self._session_cache_path())
        if self.browser:
            await self.browser.close()
    
    @abstractmethod
    async def login(self) -> bool:
        """Platform-specific login flow. Return True if successful."""
        ...
    
    @abstractmethod
    async def scrape(self) -> list[dict]:
        """Platform-specific scraping logic. Return raw scraped data."""
        ...
    
    async def safe_scrape(self) -> list[dict]:
        """Wrapper: logs, catches errors, takes screenshots on failure."""
        try:
            if not await self._is_session_valid():
                await self.login()
            return await self.scrape()
        except Exception as e:
            if self.settings.screenshot_on_failure:
                await self.page.screenshot(path=f"debug/failure_{self.platform_name}_{datetime.now().isoformat()}.png")
            raise
    
    async def _is_session_valid(self) -> bool:
        """Navigate to a known authenticated page and check if we're still logged in."""
        ...
```

### D2L Scraper (`src/acc/scrapers/d2l.py`)

This is the most critical scraper. It must:

1. **Login Flow**:
   - Navigate to the D2L login page (likely SSO via Oakton's identity provider)
   - Handle username/password entry
   - Handle MFA if present (TOTP support)
   - Verify successful login by checking for the D2L homepage/dashboard

2. **Course Discovery**:
   - From the D2L homepage, enumerate all active courses
   - For each course, extract: course name, code, D2L course ID, URL
   - Store in the `courses` table

3. **Per-Course Scraping** — for each course, scrape these sections:
   
   a. **Syllabus**: 
      - Navigate to Content → look for syllabus document (usually PDF)
      - Download the PDF
      - Extract text using pdfplumber or PyMuPDF
      - Store raw text in `course.syllabus_raw_text`
   
   b. **Assignments/Dropbox**:
      - Navigate to Activities → Assignments
      - Extract: title, due date, points possible, submission status, grade
      - Map to Assignment model
   
   c. **Grades**:
      - Navigate to Progress → Grades
      - Extract: grade items, points earned, points possible, percentages
      - Update Assignment records and Course.current_grade_pct
   
   d. **Calendar**:
      - Navigate to Calendar view
      - Extract upcoming events with dates (exams, due dates, etc.)
   
   e. **External Tool Detection**:
      - In Content modules, look for LTI links (external learning tool links)
      - Identify Pearson MyLab links (look for "pearson", "mylab", "mastering" in URLs or titles)
      - Identify Cengage MindTap links (look for "cengage", "mindtap" in URLs or titles)
      - Store detected platform and URL in Course model
      - NOTE: These are typically embedded via LTI (Learning Tools Interoperability)
        and may open in iframes or new windows. The scraper needs to handle both cases.

**IMPORTANT D2L NOTES**:
- Oakton's D2L URL needs to be verified (commonly `https://d2l.oakton.edu` or `https://mycourses.oakton.edu`)
- D2L uses Brightspace, which has a fairly standard page structure across institutions
- The hamburger menu or navbar typically has: Content, Assignments, Grades, Calendar
- CSS selectors will need to be discovered by inspecting the actual pages
- D2L may use dynamic loading (React/Angular) — use Playwright's `wait_for_selector` and `wait_for_load_state("networkidle")`

### Pearson MyLab Scraper (`src/acc/scrapers/pearson.py`)

1. **Access**: Navigate to the LTI link from D2L — this should auto-authenticate via SSO
2. **Scrape Assignments**: Find the assignments/homework list, extract titles, due dates, scores
3. **Scrape Grades**: Navigate to the gradebook view, extract all grade data
4. **Handle**: Pearson's interface is JavaScript-heavy; expect to need many `wait_for_selector` calls

### Cengage MindTap Scraper (`src/acc/scrapers/cengage.py`)

1. **Access**: Navigate to the LTI link from D2L — auto-authenticates via SSO
2. **Scrape**: Similar pattern to Pearson — assignments, grades, due dates
3. **Handle**: MindTap has a dashboard-style interface with cards/tiles for each activity

---

## AI SYLLABUS PARSER

### `src/acc/ai/syllabus_parser.py`

Use the OpenAI API to parse syllabus text into structured data.

### `src/acc/ai/prompts.py`

```python
SYLLABUS_PARSE_PROMPT = """
You are an expert academic advisor. Analyze this course syllabus and extract structured information.

Return a JSON object with exactly this structure:

{
  "course_name": "string",
  "course_code": "string",
  "instructor": "string",
  "semester": "string",
  
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
    "A-": [90, 93],
    "B+": [87, 90],
    "B": [83, 87],
    "B-": [80, 83],
    "C+": [77, 80],
    "C": [73, 77],
    "C-": [70, 73],
    "D": [60, 70],
    "F": [0, 60]
  },
  
  "late_policy": {
    "default_penalty_per_day": 0.02,
    "max_late_days": 5,
    "accepts_late": true,
    "exceptions": "Exams and quizzes cannot be submitted late",
    "raw_text": "The exact text from the syllabus about late work"
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
      "event": "Spring Break — no classes"
    }
  ],
  
  "external_tools": [
    {
      "name": "Pearson MyLab Mastering",
      "purpose": "Homework and some quizzes",
      "textbook": "Physics for Scientists & Engineers: A Strategic Approach"
    }
  ],
  
  "office_hours": "Tuesday/Thursday 2:00-3:30 PM, Room 2450",
  
  "attendance_policy": "Summary of attendance requirements",
  
  "extra_credit": "Description of any extra credit opportunities, or null"
}

RULES:
- Extract EXACTLY what the syllabus says — do not invent data
- If a field cannot be determined from the syllabus, use null
- Weights in grade_categories must sum to 1.0 (or close to it)
- Dates should be in ISO format (YYYY-MM-DD)
- For penalty_per_day, convert to decimal (e.g., "2% per day" = 0.02)
- Include the raw late policy text so it can be verified

SYLLABUS TEXT:
{syllabus_text}
"""
```

The parser should:
1. Extract text from the syllabus PDF (pdfplumber)
2. Send to OpenAI with the structured prompt
3. Parse the JSON response
4. Validate with a Pydantic model
5. Store in the Course record
6. Log confidence and flag anything that needs human review

---

## PRIORITY SCORING ALGORITHM

### `src/acc/engine/priority.py`

```python
def compute_priority_score(
    assignment: Assignment,
    today: date,
    course_grade: float | None = None,
) -> tuple[float, str]:
    """
    Compute a priority score for an assignment.
    Returns (score: float, label: str) where higher score = do first.
    
    Components:
    - Urgency (40%): Exponential increase as deadline approaches
    - Impact (25%): Grade weight of this assignment
    - Late Risk (20%): Penalty severity if submitted late
    - Momentum (15%): Bonus for partially-completed work (don't lose progress)
    
    Special rules:
    - OVERDUE items that can still be submitted get score = 95+ (just below critical)
    - Items with no late acceptance and due today get score = 100 (absolute top)
    - Already-submitted/graded items get score = 0 (filtered out)
    - Exams get a 1.3x multiplier (high-stakes, can't redo)
    """
    
    if assignment.status in ("submitted", "graded"):
        return (0.0, "completed")
    
    days_until_due = (assignment.due_date.date() - today).days if assignment.due_date else 999
    
    # --- Urgency (0-100) ---
    if days_until_due < 0:  # overdue
        urgency = 100.0
    elif days_until_due == 0:
        urgency = 95.0
    elif days_until_due == 1:
        urgency = 85.0
    elif days_until_due <= 3:
        urgency = 70.0
    elif days_until_due <= 7:
        urgency = 50.0
    else:
        urgency = max(10.0, 40.0 - (days_until_due - 7) * 3)
    
    # --- Impact (0-100) ---
    weight = assignment.grade_weight_pct or 1.0
    impact = min(100.0, weight * 8)  # 12.5% weight -> 100 impact
    
    # --- Late Risk (0-100) ---
    late_policy = assignment.late_policy or {}
    if not late_policy.get("accepts_late", True):
        late_risk = 100.0  # can't be late at all
    else:
        penalty = late_policy.get("penalty_per_day", 0)
        max_days = late_policy.get("max_late_days", 999)
        late_risk = min(100.0, penalty * 500 + (10 if max_days <= 3 else 0))
    
    # --- Momentum (0-100) ---
    # Reward items that are in-progress so they get finished
    if assignment.status == "in_progress":
        momentum = 60.0  # significant boost to finish what you started
    else:
        momentum = 0.0
    
    # --- Composite ---
    score = (
        urgency * 0.40 +
        impact * 0.25 +
        late_risk * 0.20 +
        momentum * 0.15
    )
    
    # --- Multipliers ---
    if assignment.type == "exam":
        score *= 1.3
    
    # Overdue but still submittable
    if days_until_due < 0 and late_policy.get("accepts_late", False):
        score = max(score, 95.0)
    
    # Due today, no late allowed
    if days_until_due <= 0 and not late_policy.get("accepts_late", True):
        score = 100.0
    
    score = min(100.0, score)
    
    # Label
    if score >= 85:
        label = "critical"
    elif score >= 60:
        label = "high"
    elif score >= 35:
        label = "medium"
    else:
        label = "low"
    
    return (round(score, 1), label)
```

### `src/acc/engine/decomposer.py`

```python
def decompose_multi_day_task(
    assignment: Assignment,
    today: date,
    max_daily_minutes: int = 120,
) -> list[dict]:
    """
    Break a large assignment into daily sub-tasks.
    
    Logic:
    1. If estimated_minutes <= max_daily_minutes, return as single task
    2. Calculate available days: from today to (due_date - 1 day buffer)
    3. Divide total time evenly across available days
    4. If daily allocation exceeds max_daily_minutes, extend start date earlier
    5. Generate sub-task entries with labels like "Day 2/5: Focus on problems 12-15"
    
    For exams:
    - Decompose into review sessions
    - Start at least (estimated_minutes / max_daily_minutes) days before exam
    - Label: "Exam Review — Day 3/5: Chapters 12-13"
    
    Returns list of dicts suitable for creating AgendaEntry records.
    """
    ...
```

---

## AGENDA GENERATION

### `src/acc/engine/agenda.py`

```python
async def generate_daily_agenda(
    target_date: date,
    settings: Settings,
    db: AsyncSession,
) -> DailyAgenda:
    """
    Generate the complete agenda for a given date.
    
    Steps:
    1. Fetch all assignments with due_date within lookahead_days of target_date
    2. Fetch all overdue assignments that are still submittable
    3. Fetch all multi-day sub-tasks assigned to target_date
    4. Compute priority scores for everything
    5. Sort by priority score (descending)
    6. Cap total daily minutes at max_daily_study_hours
    7. Fetch current grades for each course
    8. Identify all late/missing work
    9. Generate AI tips (optional — call LLM for study recommendations)
    10. Return structured DailyAgenda object
    
    The DailyAgenda includes:
    - date
    - course_grades: list of {course, grade_pct, letter_grade}
    - priority_items: list of agenda entries for today, sorted by priority
    - upcoming_items: items due in next 3-7 days (preview)
    - late_items: overdue/missing work with penalties
    - daily_tip: AI-generated study tip based on workload
    - total_estimated_minutes: sum of all today's tasks
    """
    ...
```

---

## EMAIL TEMPLATE

### `src/acc/delivery/templates/daily_agenda.html`

Build a responsive HTML email template (compatible with Gmail, Outlook, Apple Mail) that displays:

1. **Header**: "📋 Daily Agenda — [Day of Week], [Date]"
2. **Grade Summary Bar**: Show each course with current grade, letter grade, and a color indicator
3. **🚨 Late/Missing Work Alert** (if any): Red-highlighted section showing overdue items with penalties and remaining submission windows
4. **Today's Priority Items**: Ordered list with:
   - Priority badge (URGENT / HIGH / MEDIUM / LOW) with color coding
   - Course code
   - Assignment title
   - Due date & time
   - Estimated time
   - Grade weight
   - Progress bar (if in-progress)
   - Late policy note
   - Platform (where to submit)
5. **📅 Coming Up This Week**: Brief list of upcoming items
6. **💡 Daily Tip**: AI-generated study recommendation
7. **Footer**: "Generated by Academic Command Center at [time]"

Use inline CSS only (email client compatibility). Use a clean, scannable layout. Mobile-responsive with media queries where supported.

### SMS Format (`src/acc/delivery/formatter.py`)

Condensed SMS (under 160 chars if possible, or multi-part):

```
📋 Mar 22 Agenda
🔴 Calc HW — Polar Coords (DUE TODAY, no late!)
🟡 Physics HW — Ch.12 (due today, 65% done)
🟡 Python Lab 7 (due Mon)
Grades: PHY 87% | MAT 91% | CIS 95%
```

---

## ORCHESTRATION

### `src/acc/main.py`

Build a CLI using `click` or `typer` with these commands:

```bash
# Run a full scrape cycle
acc scrape --all
acc scrape --platform d2l
acc scrape --platform pearson
acc scrape --course "PHY 221"

# Parse/re-parse syllabi
acc parse-syllabi --all
acc parse-syllabi --course "PHY 221"

# Generate and display today's agenda (stdout)
acc agenda --date today
acc agenda --date 2026-03-25

# Send the daily email/SMS now
acc send --email
acc send --sms
acc send --all

# Run the scheduler (continuous)
acc run

# Debug: show all courses and their data
acc status

# Debug: launch browser in headed mode for a platform
acc debug-scrape --platform d2l
```

### Scheduler (`src/acc/scheduler/jobs.py`)

Using APScheduler, define:
1. **Scrape job**: Every `scrape_interval_hours` hours, run full scrape
2. **Agenda job**: Daily at `agenda_delivery_time`, generate agenda + send email + send SMS
3. **Syllabus re-parse**: Weekly, re-parse syllabi to catch updates (rare but possible)

---

## DOCKER SETUP

### docker-compose.yml

```yaml
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_USER: acc
      POSTGRES_PASSWORD: acc
      POSTGRES_DB: acc
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
  
  app:
    build: .
    depends_on:
      - db
    env_file:
      - .env
    volumes:
      - .:/app
      - ./debug:/app/debug        # Screenshots
      - ./.sessions:/app/.sessions # Browser sessions
    command: python -m acc run

volumes:
  pgdata:
```

---

## BUILD ORDER

Implement in this exact order, testing each step before moving to the next:

### Step 1: Project scaffold
- pyproject.toml with all dependencies
- Docker-compose with Postgres
- Config module with .env loading
- Database models + Alembic initial migration
- Verify: `docker-compose up`, run migration, connect to DB

### Step 2: D2L Scraper
- Base scraper class
- D2L login automation
- Course list scraping
- Syllabus PDF download + text extraction
- Assignment scraping
- Grade scraping
- External tool detection
- Verify: Run scraper, check DB has courses + assignments

### Step 3: AI Syllabus Parser
- OpenAI API integration
- Syllabus parse prompt
- Pydantic validation of parsed output
- Store parsed data in Course records
- Verify: Parse each syllabus, review extracted grading policies

### Step 4: External Platform Scrapers
- Pearson MyLab scraper (via D2L LTI link)
- Cengage MindTap scraper (via D2L LTI link)
- Verify: Run scrapers, check DB has external assignments + grades

### Step 5: Agenda Engine
- Priority scoring algorithm
- Multi-day task decomposition
- Daily agenda generation
- CLI `acc agenda` command
- Verify: Generate agenda, review priority ordering

### Step 6: Delivery
- HTML email template
- SendGrid integration
- SMS formatter
- Twilio integration
- CLI `acc send` command
- Verify: Send test email + SMS

### Step 7: Scheduler + Dashboard
- APScheduler setup
- FastAPI dashboard (MVP)
- CLI `acc run` command
- Verify: Run for 24 hours, receive morning agenda

---

## TESTING STRATEGY

- **Unit tests**: Priority algorithm, decomposer, normalizer, formatter
- **Integration tests**: Database operations, API calls (mock external services)
- **Scraper tests**: Use saved HTML snapshots of D2L/Pearson/Cengage pages (capture once, replay in tests)
- **End-to-end**: Full pipeline from scrape → agenda → email (with test credentials)

For scraper development, use `headless=False` so you can watch the browser and debug selector issues interactively.

---

## IMPORTANT NOTES

1. **Start with D2L** — everything else depends on it. Get the login flow working first, even before scraping anything.

2. **Inspect before coding** — Before writing selectors for any platform, manually log in and use browser DevTools to identify the actual HTML structure, CSS classes, and data attributes. Save screenshots and HTML snippets for reference.

3. **Session caching is critical** — Logging in every scrape cycle is slow and may trigger rate limits. Cache session cookies and only re-authenticate when they expire.

4. **Be gentle with scraping** — Add random delays (1-3 seconds) between page navigations. Don't scrape more often than every 6 hours. These platforms serve students, not bots.

5. **The LTI integration is the tricky part** — When D2L links to Pearson or Cengage, it uses LTI (Learning Tools Interoperability). This may involve form POSTs, redirects, and iframe embedding. You may need to intercept the LTI launch URL and follow the redirect chain manually in Playwright.

6. **Verify AI-parsed syllabi** — The first time a syllabus is parsed, print the results for human review. AI extraction is very good but not perfect. Once verified, cache the results and don't re-parse unless the syllabus changes.

7. **Oakton-specific**: The actual D2L URL, login page structure, and SSO flow are specific to Oakton Community College. You will need to discover these by inspecting the live site. Common D2L URLs for community colleges: `https://d2l.[school].edu`, `https://mycourses.[school].edu`, `https://[school].brightspace.com`.
