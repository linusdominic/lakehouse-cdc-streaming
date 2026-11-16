"""Host-side settings, read from the environment with .env as a fallback."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS_DIR = REPO_ROOT / "artifacts"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    kafka_bootstrap: str
    connect_url: str
    trino_host: str
    trino_port: int
    env: dict[str, str]


def load_settings() -> Settings:
    _load_dotenv(REPO_ROOT / ".env")
    _load_dotenv(REPO_ROOT / ".env.example")
    e = os.environ
    return Settings(
        pg_dsn=(
            f"host=localhost port={e['PG_PORT']} dbname={e['POSTGRES_DB']} "
            f"user={e['POSTGRES_USER']} password={e['POSTGRES_PASSWORD']}"
        ),
        kafka_bootstrap=f"localhost:{e['KAFKA_PORT']}",
        connect_url=f"http://localhost:{e['CONNECT_PORT']}",
        trino_host="localhost",
        trino_port=int(e["TRINO_PORT"]),
        env=dict(e),
    )
