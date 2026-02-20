from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd

from .hashing import sha256_file


@dataclass(frozen=True, slots=True)
class SnapshotResult:
    timestamp_utc: str
    raw_parquet_path: Path
    raw_sha256: str
    row_count_raw: int


def utc_now_iso() -> str:
    # ISO8601 with Z
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_csv_bytes(csv_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(csv_bytes))


def write_raw_snapshot(
    *, df: pd.DataFrame, out_dir: Path, table: str, timestamp_utc: str
) -> SnapshotResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    compact = timestamp_utc.replace("-", "").replace(":", "")
    filename = f"{table}_{compact}.parquet"
    path = out_dir / filename
    df.to_parquet(path, index=False, engine="pyarrow")
    return SnapshotResult(
        timestamp_utc=timestamp_utc,
        raw_parquet_path=path,
        raw_sha256="sha256:" + sha256_file(path),
        row_count_raw=int(df.shape[0]),
    )
