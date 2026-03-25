from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import Field, SecretStr, model_validator
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
    openai_api_key: SecretStr | None = None
    openai_model: str = "gpt-4o-mini"
    openai_timeout_seconds: int = 90
    openai_retry_max_attempts: int = 5
    openai_retry_base_delay_seconds: float = 1.0

    browser_headless: bool = False
    browser_slow_mo_ms: int = 0
    browser_timeout_ms: int = 30_000
    browser_course_concurrency: int = 4
    cengage_activity_timeout_ms: int = 90_000
    playwright_browsers_path: Path = Path(".playwright")
    runtime_tmp_dir: Path = Path(".tmp")
    d2l_login_timeout_seconds: int = 240

    d2l_storage_state_path: Path = Path(".state/d2l-storage.json")
    d2l_snapshot_path: Path = Path(".state/d2l-snapshot.json")
    d2l_normalized_path: Path = Path(".state/d2l-normalized.json")
    external_snapshot_path: Path = Path(".state/external-snapshot.json")
    crawl_snapshot_path: Path = Path(".state/crawl-snapshot.json")
    crawl_extracted_path: Path = Path(".state/crawl-extracted.json")
    crawl_artifacts_dir: Path = Path(".state/crawl-artifacts")
    crawl_max_external_nav_pages: int = 40
    crawl_ai_navigation: bool = False
    crawl_ai_max_d2l_pages: int = 120
    crawl_ai_max_links_per_page: int = 80
    crawl_extract_concurrency: int = 5
    # All OpenAI chat completions share this global gate (link pick, crawl extract, syllabus parse).
    # Replaces per-extractor LLM semaphores; tune this instead of crawl_extract_concurrency for API pressure.
    openai_max_concurrent_requests: int = 8
    # Throttle Playwright navigations / fetches: global cap plus per-host cap (anti rate-limit / DoS).
    fetch_max_concurrent_global: int = 12
    fetch_max_concurrent_per_host: int = 4
    crawl_page_concurrency: int = 1
    crawl_d2l_page_concurrency: int | None = Field(
        default=None,
        repr=False,
        description="Deprecated: use crawl_page_concurrency / ACC_CRAWL_PAGE_CONCURRENCY.",
    )
    screenshots_dir: Path = Path("screenshots")

    @model_validator(mode="after")
    def _apply_crawl_page_concurrency_alias(self) -> Self:
        if "crawl_page_concurrency" in self.model_fields_set:
            return self
        if (
            "crawl_d2l_page_concurrency" in self.model_fields_set
            and self.crawl_d2l_page_concurrency is not None
        ):
            object.__setattr__(
                self,
                "crawl_page_concurrency",
                max(1, int(self.crawl_d2l_page_concurrency)),
            )
        return self

    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8000

    def ensure_runtime_dirs(self) -> None:
        self.playwright_browsers_path.mkdir(parents=True, exist_ok=True)
        self.runtime_tmp_dir.mkdir(parents=True, exist_ok=True)
        self.d2l_storage_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.d2l_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.d2l_normalized_path.parent.mkdir(parents=True, exist_ok=True)
        self.external_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.crawl_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.crawl_extracted_path.parent.mkdir(parents=True, exist_ok=True)
        self.crawl_artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.screenshots_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_runtime_dirs()
    return settings
