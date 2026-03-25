import os
from pathlib import Path

import pytest

from acc.config import Settings
import acc.main as main_module
from acc.main import prepare_runtime_environment, run_refresh_pipeline


def test_prepare_runtime_environment_sets_playwright_and_tmpdir(tmp_path) -> None:
    settings = Settings(
        playwright_browsers_path=tmp_path / "playwright",
        runtime_tmp_dir=tmp_path / "tmp",
    )

    prepare_runtime_environment(settings)

    assert os.environ["PLAYWRIGHT_BROWSERS_PATH"] == str((tmp_path / "playwright").resolve())
    assert os.environ["TMPDIR"] == f"{(tmp_path / 'tmp').resolve()}/"


@pytest.mark.asyncio
async def test_run_refresh_pipeline_passes_refresh_mode_to_crawl_sync(monkeypatch, tmp_path) -> None:
    settings = Settings(
        runtime_tmp_dir=tmp_path / "tmp",
        playwright_browsers_path=tmp_path / "pw",
        openai_api_key="x",
    )
    seen: dict[str, object] = {}

    async def _ok(*args, **kwargs):
        return 0

    async def _crawl_sync(_settings, on_progress=None, *, mode="full"):
        seen["mode"] = mode
        return 0

    monkeypatch.setattr(main_module, "run_d2l_login", _ok)
    monkeypatch.setattr(main_module, "run_d2l_snapshot", _ok)
    monkeypatch.setattr(main_module, "run_crawl_snapshot", _ok)
    monkeypatch.setattr(main_module, "run_crawl_extract", _ok)
    monkeypatch.setattr(main_module, "run_crawl_sync_db", _crawl_sync)

    result = await run_refresh_pipeline(settings, refresh_mode="additive")

    assert seen["mode"] == "additive"
    assert result["refresh_mode"] == "additive"
