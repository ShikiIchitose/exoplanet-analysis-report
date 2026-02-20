from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .hashing import sha256_file


@dataclass(frozen=True, slots=True)
class CleanResult:
    clean_parquet_path: Path
    clean_sha256: str
    row_count_clean: int
    missingness: dict[str, dict[str, Any]]


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def clean_dataframe(
    *,
    df: pd.DataFrame,
    allowed_methods: list[str],
    metrics: list[str],
    baseline_method: str,
) -> pd.DataFrame:
    # Ensure df intact
    df = df.copy()

    # Required columns contract
    required = ["pl_name", "discoverymethod", "pl_bmassprov", *metrics]
    _require_columns(df, required)

    # Type normalization (invalid => NaN/NA)
    for col in metrics:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if "disc_year" in df.columns:
        disc_year = pd.to_numeric(df["disc_year"], errors="coerce")
        if not isinstance(disc_year, pd.Series):
            raise TypeError("Expected Series from pd.to_numeric for a column input")
        df["disc_year"] = disc_year.astype("Int64")

    # Filter: discoverymethod in allowed set / select row
    mask = df["discoverymethod"].isin(allowed_methods)
    filtered = df.loc[mask, :]
    if not isinstance(filtered, pd.DataFrame):
        raise TypeError("Expected DataFrame after boolean filtering")

    df = filtered.copy()

    # Domain constraints: metric > 0 only where metric is not null.
    for col in metrics:
        bad = df[col].notna() & (df[col] <= 0)
        df.loc[bad, col] = pd.NA

    # Baseline must exist
    if baseline_method not in set(df["discoverymethod"].dropna().unique()):
        raise ValueError(
            f"Baseline method not present after cleaning: {baseline_method}"
        )

    return df


def compute_missingness(
    *,
    df: pd.DataFrame,
    methods: list[str],
    metrics: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Return mapping:
      missingness[method][metric] = {n_total, n_nonnull, missing_rate}
    """
    out: dict[str, dict[str, Any]] = {}
    for m in methods:
        g = df[df["discoverymethod"] == m]
        n_total = int(g.shape[0])
        metric_map: dict[str, Any] = {}
        for k in metrics:
            n_nonnull = int(g[k].notna().sum())
            missing_rate = float(1.0 - (n_nonnull / n_total)) if n_total > 0 else 1.0
            metric_map[k] = {
                "n_total": n_total,
                "n_nonnull": n_nonnull,
                "missing_rate": missing_rate,
            }
        out[m] = metric_map
    return out


def write_clean_dataset(
    *,
    df_clean: pd.DataFrame,
    out_dir: Path,
    table: str,
    timestamp_utc: str,
    method_order: list[str],
    metrics: list[str],
) -> CleanResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    compact = timestamp_utc.replace("-", "").replace(":", "")
    filename = f"{table}_clean_{compact}.parquet"
    path = out_dir / filename
    df_clean.to_parquet(path, index=False, engine="pyarrow")
    miss = compute_missingness(df=df_clean, methods=method_order, metrics=metrics)
    return CleanResult(
        clean_parquet_path=path,
        clean_sha256="sha256:" + sha256_file(path),
        row_count_clean=int(df_clean.shape[0]),
        missingness=miss,
    )
