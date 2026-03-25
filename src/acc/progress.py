from typing import Protocol


class ProgressCallback(Protocol):
    """UI / CLI progress sink: headline + optional detail; fraction is 0..1 within the current phase."""

    def __call__(
        self,
        headline: str,
        detail: str | None = None,
        *,
        fraction: float | None = None,
    ) -> None: ...
