from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

import pandas as pd

from exoplanet_analysis_report.analyze import compute_metrics_json
from exoplanet_analysis_report.clean import compute_missingness
from exoplanet_analysis_report.config import Config
from exoplanet_analysis_report.hashing import sha256_file
from exoplanet_analysis_report.paths import ProjectPaths
from exoplanet_analysis_report.plots import generate_all_plots
from exoplanet_analysis_report.report import write_report_files
from exoplanet_analysis_report.runlog import (
    RunLog,
    try_get_git_commit,
    try_hash_lockfile,
)
from exoplanet_analysis_report.snapshot import utc_now_iso
from exoplanet_analysis_report.warehouse import load_clean_parquet_to_duckdb


def _rel_or_abs(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def run_offline(
    *, root: Path, config_path: Path | None, clean_parquet_path: Path
) -> int:
    cfg = Config.from_toml(config_path) if config_path else Config()
    paths = ProjectPaths.from_root(root, cfg)
    paths.ensure_dirs()

    # Resolve clean parquet (relative paths are interpreted from repo root)
    clean_parquet_path = (
        (paths.root / clean_parquet_path).resolve()
        if not clean_parquet_path.is_absolute()
        else clean_parquet_path.resolve()
    )
    if not clean_parquet_path.exists():
        raise FileNotFoundError(f"clean parquet not found: {clean_parquet_path}")
    if clean_parquet_path.suffix.lower() != ".parquet":
        raise ValueError(f"--clean must point to a .parquet file: {clean_parquet_path}")

    generated_utc = utc_now_iso()
    git_commit = try_get_git_commit(paths.root)
    schema_hash = cfg.schema_hash()

    # Record the actual invocation (best-effort)
    cmd = " ".join(
        [
            "uv",
            "run",
            "python",
            "scripts/run_offline.py",
            *[shlex.quote(a) for a in sys.argv[1:]],
        ]
    )

    run = RunLog(
        cfg=cfg,
        generated_utc=generated_utc,
        git_commit=git_commit,
        command=cmd,
        schema_hash=schema_hash,
    )

    run.tap = {
        "used": False,
        "reason": "offline: input is clean parquet",
    }

    uv_lock_sha = try_hash_lockfile(paths.root)
    if uv_lock_sha is not None:
        run.outputs["uv_lock_sha256"] = uv_lock_sha

    run_json_path = paths.artifacts_dir / "run.json"
    metrics_json_path = paths.artifacts_dir / "metrics.json"
    report_md_path = paths.artifacts_dir / "report.md"
    report_html_path = paths.artifacts_dir / "report.html"

    try:
        # Offline "data source" declaration (keep top-level schema stable)
        run.data_source = {
            "name": "NASA Exoplanet Archive (offline re-run)",
            "table": cfg.tap.table,
            "tap_endpoint": cfg.tap.endpoint,
            "tap_mode": cfg.tap.mode,
            "format": "parquet",
            "source": "clean_parquet",
            "clean_parquet_path": _rel_or_abs(clean_parquet_path, paths.root),
        }
        # No HTTP in offline mode
        run.http = {}

        # Load clean parquet
        df_clean = pd.read_parquet(clean_parquet_path, engine="pyarrow")

        # Snapshot/audit of the input we actually used
        run.snapshots.update(
            {
                "clean_parquet_path": _rel_or_abs(clean_parquet_path, paths.root),
                "clean_sha256": "sha256:" + sha256_file(clean_parquet_path),
            }
        )
        run.row_counts["clean"] = int(df_clean.shape[0])
        run.missingness = compute_missingness(
            df=df_clean,
            methods=list(cfg.method_order),
            metrics=list(cfg.metrics),
        )

        # Warehouse (rebuild deterministically from the given clean parquet)
        load_clean_parquet_to_duckdb(
            clean_parquet_path=clean_parquet_path,
            warehouse_path=paths.warehouse_path,
        )

        # Analyze
        metrics_dict = compute_metrics_json(
            df_clean=df_clean,
            metrics=list(cfg.metrics),
            method_order=list(cfg.method_order),
            baseline_method=cfg.analysis.baseline_method,
            bootstrap_seed=cfg.bootstrap.seed,
            n_resamples=cfg.bootstrap.n_resamples,
            ci=cfg.bootstrap.ci,
            quantile_method=cfg.bootstrap.quantile_method,
            std_ddof=cfg.analysis.std_ddof,
            min_group_size_for_ci=cfg.thresholds.min_group_size_for_ci,
        )
        metrics_json_path.write_text(
            json.dumps(metrics_dict, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        # Plots
        generate_all_plots(
            df_clean=df_clean,
            metrics=list(cfg.metrics),
            method_order=list(cfg.method_order),
            figures_dir=paths.figures_dir,
            metrics_dict=metrics_dict,
        )

        # Outputs (public contract)
        run.outputs.update(
            {
                "report_md": str(report_md_path.relative_to(paths.root)),
                "report_html": str(report_html_path.relative_to(paths.root)),
                "metrics_json": str(metrics_json_path.relative_to(paths.root)),
                "figures_dir": str(paths.figures_dir.relative_to(paths.root)) + "/",
                "warehouse": str(paths.warehouse_path.relative_to(paths.root)),
            }
        )

        run.finalize_success()
        run.write_json(run_json_path)

        # Report (reads run.json + metrics.json)
        write_report_files(
            run_json_path=run_json_path,
            metrics_json_path=metrics_json_path,
            report_md_path=report_md_path,
            report_html_path=report_html_path,
        )

        return 0

    except Exception as e:
        run.finalize_failure(e)
        run.write_json(run_json_path)
        raise


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run offline pipeline from an existing clean parquet (v0.1.0)."
    )
    p.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root (default: parent of scripts/).",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Optional TOML config file to override defaults.",
    )
    p.add_argument(
        "--clean",
        type=Path,
        required=True,
        help="Path to an existing clean parquet (relative to --root if not absolute).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    run_offline(root=args.root, config_path=args.config, clean_parquet_path=args.clean)


if __name__ == "__main__":
    main()
