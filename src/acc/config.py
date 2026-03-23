from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="ACC_",
        extra="ignore",
    )

    env: str = "development"
    database_url: str = "sqlite+aiosqlite:///./.state/acc.db"
    timezone: str = "America/Chicago"

    d2l_base_url: str = "https://d2l.oakton.edu/"
    d2l_username: str | None = None
    d2l_password: SecretStr | None = None

    browser_headless: bool = False
    browser_slow_mo_ms: int = 0
    browser_timeout_ms: int = 30_000
    playwright_browsers_path: Path = Path(".playwright")
    runtime_tmp_dir: Path = Path(".tmp")
    d2l_login_timeout_seconds: int = 240

    d2l_storage_state_path: Path = Path(".state/d2l-storage.json")
    d2l_snapshot_path: Path = Path(".state/d2l-snapshot.json")
    d2l_normalized_path: Path = Path(".state/d2l-normalized.json")
    external_snapshot_path: Path = Path(".state/external-snapshot.json")
    screenshots_dir: Path = Path("screenshots")

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8000

    def ensure_runtime_dirs(self) -> None:
        self.playwright_browsers_path.mkdir(parents=True, exist_ok=True)
        self.runtime_tmp_dir.mkdir(parents=True, exist_ok=True)
        self.d2l_storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.d2l_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.d2l_normalized_path.parent.mkdir(parents=True, exist_ok=True)
        self.external_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
