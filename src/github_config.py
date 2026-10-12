"""Shared GitHub configuration helpers."""

from __future__ import annotations

import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DOTENV = ROOT_DIR / ".env"


def load_dotenv(path: Path = DEFAULT_DOTENV) -> None:
    """Load .env values without overwriting existing environment variables."""
    if not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if not key or key in os.environ:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def load_tokens() -> list[str]:
    """Load one or more GitHub tokens from GITHUB_TOKEN or GITHUB_TOKENS."""
    token_value = os.getenv("GITHUB_TOKEN") or os.getenv("GITHUB_TOKENS", "")
    return [token.strip() for token in token_value.split(",") if token.strip()]
