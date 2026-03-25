from pathlib import Path

from acc.config import Settings


def test_crawl_page_concurrency_prefers_new_name_when_both_passed() -> None:
    settings = Settings(
        crawl_page_concurrency=2,
        crawl_d2l_page_concurrency=9,
        _env_file=None,
    )
    assert settings.crawl_page_concurrency == 2


def test_crawl_page_concurrency_legacy_field_when_new_omitted() -> None:
    settings = Settings(crawl_d2l_page_concurrency=4, _env_file=None)
    assert settings.crawl_page_concurrency == 4


def test_runtime_directories_are_created(tmp_path: Path) -> None:
    state_path = tmp_path / ".state" / "d2l-storage.json"
    external_snapshot_path = tmp_path / ".state" / "external-snapshot.json"
    screenshots_dir = tmp_path / "screens"

    settings = Settings(
        d2l_storage_state_path=state_path,
        external_snapshot_path=external_snapshot_path,
        screenshots_dir=screenshots_dir,
    )
    settings.ensure_runtime_dirs()

    assert state_path.parent.exists()
    assert external_snapshot_path.parent.exists()
    assert screenshots_dir.exists()
