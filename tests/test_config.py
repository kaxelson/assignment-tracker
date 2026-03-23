from pathlib import Path

from acc.config import Settings


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
