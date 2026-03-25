import json

import pytest

from acc.ai.crawl_link_picker import CrawlLinkPicker, heuristic_follow_indices_d2l


class _FakeJsonClient:
    def __init__(self, payload: str) -> None:
        self.payload = payload

    async def complete_json(self, prompt: str) -> str:
        return self.payload


@pytest.mark.asyncio
async def test_pick_follow_indices_drops_out_of_range(tmp_path) -> None:
    from acc.config import Settings

    settings = Settings(
        d2l_snapshot_path=tmp_path / "d2l.json",
        crawl_snapshot_path=tmp_path / "crawl.json",
    )
    client = _FakeJsonClient(json.dumps({"follow": [0, 5, 1], "notes": "test"}))
    picker = CrawlLinkPicker(settings, client=client)
    links = [
        ("https://d2l.example.edu/d2l/home/9", "Home"),
        ("https://d2l.example.edu/d2l/lms/grades/index.d2l?ou=9", "Grades"),
    ]
    got = await picker.pick_follow_indices(
        platform="d2l",
        page_url="https://d2l.example.edu/d2l/home/9",
        page_text="Course home",
        course_code="X",
        course_name="Y",
        links=links,
    )
    assert got == [0, 1]


def test_heuristic_follow_indices_d2l_keeps_grades_link() -> None:
    links = [
        ("https://d2l.example.edu/d2l/lms/classlist/classlist.d2l?ou=1", "Classlist"),
        ("https://d2l.example.edu/d2l/lms/grades/index.d2l?ou=1", "Grades"),
    ]
    assert heuristic_follow_indices_d2l(links) == [1]
