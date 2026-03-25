"""AI package."""

from acc.ai.crawl_extractor import CrawlExtractor

__all__ = ["CrawlExtractor", "SyllabusParser", "parse_saved_syllabi"]


def __getattr__(name: str):
    if name == "SyllabusParser":
        from acc.ai.syllabus_parser import SyllabusParser

        return SyllabusParser
    if name == "parse_saved_syllabi":
        from acc.ai.syllabus_parser import parse_saved_syllabi

        return parse_saved_syllabi
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
