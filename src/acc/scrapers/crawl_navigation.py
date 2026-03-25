"""Heuristics for crawl navigation (D2L, Pearson, Cengage).

We follow surfaces that plausibly expose assignments, grades, due dates, or
course policy (syllabus, late work, extra credit), and skip obvious eText,
video, chat, and off-course URLs where possible.
"""

from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse, urlunparse

# Substrings in link text OR URL (lowercased combined haystack). Any match -> skip.
_EXTERNAL_SKIP_SUBSTRINGS = (
    "etext",
    "e-text",
    "ebook",
    "etextbook",
    "textbook",
    "read the book",
    "revel",
    "video",
    "lecture",
    "webinar",
    "streaming",
    "media player",
    "podcast",
    "youtube",
    "sam.cengage",
    "help.pearson",
    "support.pearson",
    "plus.pearson",
    "help.cengage",
    "support.cengage",
    "purchase",
    "shop",
    "cart",
    "checkout",
    "subscribe",
    "buy now",
    "instructor",
    "accessibility statement",
    "privacy policy",
    "terms of use",
    "account settings",
    "sign out",
    "log out",
    "logout",
    "profile",
    "dynamic study",
    "study area",
    "study module",
    "mobile app",
    "download app",
    "library",
    "documentary",
    "audio book",
    "audiobook",
    "chapter reading",
    "learning path",
    "mindtap reader",
    "cengage unlimited",
)

# At least one substring must match to follow a nav target (strict).
_EXTERNAL_INCLUDE_SUBSTRINGS = (
    "assignment",
    "assignments",
    "grade",
    "grades",
    "grading",
    "score",
    "scores",
    "result",
    "results",
    "progress",
    "performance",
    "calendar",
    "due",
    "deadline",
    "quiz",
    "quizzes",
    "test",
    "tests",
    "exam",
    "exams",
    "homework",
    "classwork",
    "activity",
    "activities",
    "submission",
    "submissions",
    "feedback",
    "report",
    "analytics",
    "overview",
    "dashboard",
    "announcement",
    "todo",
)

# Substrings that must match as whole tokens (avoid "work" matching "network").
_EXTERNAL_INCLUDE_TOKEN_RE = re.compile(
    r"\b(course\s*work|classwork|schoolwork)\b",
    re.IGNORECASE,
)

def normalize_crawl_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(
        (parsed.scheme, parsed.netloc.lower(), parsed.path, parsed.params, parsed.query, "")
    )


def d2l_href_allowed_for_course(absolute: str, course_id: str, d2l_host: str) -> bool:
    """True if absolute URL stays on the configured D2L host and appears scoped to course_id."""
    parsed = urlparse(absolute.strip())
    if parsed.scheme not in ("http", "https"):
        return False
    if not parsed.netloc or parsed.netloc.lower() != d2l_host.lower():
        return False
    lowered_path = parsed.path.lower()
    if any(token in lowered_path for token in ("/logout", "/log-out")):
        return False
    if "/d2l/" not in lowered_path:
        return False
    qs = parse_qs(parsed.query)
    ou_vals = qs.get("ou", [])
    if ou_vals and course_id not in ou_vals:
        return False
    parts = [segment for segment in parsed.path.split("/") if segment]
    if course_id in parts:
        return True
    if ou_vals and course_id in ou_vals:
        return True
    return False


def nav_target_should_be_crawled(link_text: str, href: str) -> bool:
    haystack = f"{link_text} {href}"
    haystack_lower = haystack.lower()
    for token in _EXTERNAL_SKIP_SUBSTRINGS:
        if token in haystack_lower:
            return False
    if _EXTERNAL_INCLUDE_TOKEN_RE.search(haystack):
        return True
    return any(token in haystack_lower for token in _EXTERNAL_INCLUDE_SUBSTRINGS)


def pearson_href_in_course_scope(absolute_url: str) -> bool:
    if not absolute_url:
        return False
    parsed = urlparse(absolute_url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = parsed.netloc.lower()
    if not host:
        return False
    if "pearson.com" not in host and "pearsoned.com" not in host and "pearsoncmg.com" not in host:
        return False
    if host in {"www.pearson.com", "pearson.com"}:
        return False
    if host.startswith("help.") or host.startswith("support.") or host.startswith("plus."):
        return False
    return True


def cengage_url_same_course(nav_url: str, course_base_url: str) -> bool:
    nav_p = urlparse(nav_url)
    base_p = urlparse(course_base_url)
    if nav_p.scheme not in ("http", "https"):
        return False
    if not nav_p.netloc or nav_p.netloc.lower() != base_p.netloc.lower():
        return False
    nav_q = parse_qs(nav_p.query)
    base_q = parse_qs(base_p.query)
    nav_snap = nav_q.get("snapshotId", [None])[0]
    base_snap = base_q.get("snapshotId", [None])[0]
    if base_snap and nav_snap and nav_snap != base_snap:
        return False
    nav_dep = nav_q.get("deploymentId", [None])[0]
    base_dep = base_q.get("deploymentId", [None])[0]
    if base_dep and nav_dep and nav_dep != base_dep:
        return False
    return True


def is_pearson_mylab_course_tool_frame_url(url: str) -> bool:
    if not url:
        return False
    lowered = url.lower()
    if "mylab.pearson.com/courses/" not in lowered:
        return False
    if any(lowered.endswith(suffix) for suffix in (".js", ".css", ".png", ".jpg", ".gif", ".svg", ".ico")):
        return False
    return True


# D2L navbar / tool extras (Brightspace). Any match in haystack -> skip crawl.
_D2L_TOOL_EXTRA_SKIP_SUBSTRINGS = (
    "classlist",
    "class list",
    "virtual classroom",
    "online rooms",
    "chat",
    "instant message",
    "collaboration",
    "course builder",
    "manage dates",
    "import / export",
    "import/export",
    "permissions",
    "impersonat",
    "media gallery",
    "lecture capture",
    "panopto",
    "kaltura",
    "echo360",
    "teams meeting",
    "live session",
    "bookmark",
    "subscription",
)

# Extra include signals for D2L tool names, URLs, or content topic titles.
_D2L_EXTRA_INCLUDE_SUBSTRINGS = (
    "syllabus",
    "policy",
    "policies",
    "late",
    "grading",
    "grade breakdown",
    "extra credit",
    "weight",
    "outline",
    "course info",
    "course information",
    "expectation",
    "dropbox",
    "rubric",
    "discuss",
    "discussion",
    "module",
    "announce",
    "news",
    "schedule",
)

# Skip individual Content topics that are clearly media-only (syllabus tree).
_D2L_SYLLABUS_TOPIC_SKIP_SUBSTRINGS = (
    "kaltura",
    "panopto",
    "echo360",
    "lecture capture",
    "panopto video",
)


def _d2l_include_haystack(name: str, url: str, module_title: str | None = None) -> str:
    """Name, module, and URL path/query only (avoid 'exam' matching 'example.edu')."""
    parsed = urlparse(url)
    path_part = f"{parsed.path}?{parsed.query}"
    return f"{name} {module_title or ''} {path_part}"


def _d2l_haystack_skip(haystack_lower: str) -> bool:
    for token in _EXTERNAL_SKIP_SUBSTRINGS:
        if token in haystack_lower:
            return True
    for token in _D2L_TOOL_EXTRA_SKIP_SUBSTRINGS:
        if token in haystack_lower:
            return True
    return False


def _d2l_tool_name_force_include(tool_name: str) -> bool:
    n = tool_name.strip().lower()
    if not n:
        return False
    needles = (
        "content",
        "grades",
        "assignment",
        "dropbox",
        "quiz",
        "survey",
        "calendar",
        "class progress",
        "rubric",
        "competenc",
        "outcome",
        "discuss",
        "syllabus",
    )
    return any(needle in n for needle in needles)


_TOOL_NAV_URL_FRAGMENTS = (
    "/dropbox/",
    "/grades",
    "/quizzes",
    "/content/",
    "/calendar/",
    "/assignments",
    "/news/",
    "/le/news/",
    "/lms/news",
)

# Content-module wrappers (LTI) usually live under /content/; matching that alone would
# select every topic. Rely on title/module heuristics for those instead.
_EXTERNAL_WRAPPER_URL_FRAGMENTS = (
    "/dropbox/",
    "/grades",
    "/quizzes",
    "/calendar/",
    "/assignments",
    "/news/",
    "/le/news/",
    "/lms/news",
)


def _d2l_url_matches_course_fragments(url: str, fragments: tuple[str, ...]) -> bool:
    u = url.lower()
    return any(fragment in u for fragment in fragments)


def d2l_tool_nav_should_be_crawled(tool_name: str, url: str) -> bool:
    haystack = f"{tool_name} {url}"
    haystack_lower = haystack.lower()
    if _d2l_haystack_skip(haystack_lower):
        return False
    if _d2l_tool_name_force_include(tool_name):
        return True
    if _d2l_url_matches_course_fragments(url, _TOOL_NAV_URL_FRAGMENTS):
        return True
    include_hay = _d2l_include_haystack(tool_name, url, None).lower()
    if _EXTERNAL_INCLUDE_TOKEN_RE.search(include_hay):
        return True
    if any(token in include_hay for token in _EXTERNAL_INCLUDE_SUBSTRINGS):
        return True
    if any(token in include_hay for token in _D2L_EXTRA_INCLUDE_SUBSTRINGS):
        return True
    return False


def d2l_syllabus_topic_should_be_crawled(title: str, url: str, module_title: str | None) -> bool:
    haystack_lower = f"{title} {url} {module_title or ''}".lower()
    if _d2l_haystack_skip(haystack_lower):
        return False
    for token in _D2L_SYLLABUS_TOPIC_SKIP_SUBSTRINGS:
        if token in haystack_lower:
            return False
    return True


def d2l_content_outline_topic_should_be_crawled(title: str, url: str, module_title: str | None) -> bool:
    haystack_lower = f"{title} {url} {module_title or ''}".lower()
    if _d2l_haystack_skip(haystack_lower):
        return False
    for token in _D2L_SYLLABUS_TOPIC_SKIP_SUBSTRINGS:
        if token in haystack_lower:
            return False
    return True


def d2l_external_tool_wrapper_should_be_crawled(title: str, url: str, module_title: str | None) -> bool:
    haystack = f"{title} {url} {module_title or ''}"
    haystack_lower = haystack.lower()
    if _d2l_haystack_skip(haystack_lower):
        return False
    if _d2l_tool_name_force_include(title) or _d2l_tool_name_force_include(module_title or ""):
        return True
    if _d2l_url_matches_course_fragments(url, _EXTERNAL_WRAPPER_URL_FRAGMENTS):
        return True
    include_hay = _d2l_include_haystack(title, url, module_title).lower()
    if _EXTERNAL_INCLUDE_TOKEN_RE.search(include_hay):
        return True
    if any(token in include_hay for token in _EXTERNAL_INCLUDE_SUBSTRINGS):
        return True
    if any(token in include_hay for token in _D2L_EXTRA_INCLUDE_SUBSTRINGS):
        return True
    return False


def d2l_calendar_url(home_url: str, course_id: str) -> str:
    base = home_url.rsplit("/d2l/home/", 1)[0]
    return f"{base}/d2l/le/calendar/{course_id}"
