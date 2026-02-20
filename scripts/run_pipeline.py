from __future__ import annotations

import argparse
import json
from pathlib import Path

from exoplanet_analysis_report.analyze import compute_metrics_json
from exoplanet_analysis_report.clean import clean_dataframe, write_clean_dataset
from exoplanet_analysis_report.config import Config
from exoplanet_analysis_report.paths import ProjectPaths
from exoplanet_analysis_report.plots import generate_all_plots
from exoplanet_analysis_report.report import write_report_files
from exoplanet_analysis_report.runlog import (
    RunLog,
    try_get_git_commit,
    try_hash_lockfile,
)
from exoplanet_analysis_report.snapshot import (
    parse_csv_bytes,
    utc_now_iso,
    write_raw_snapshot,
)
from exoplanet_analysis_report.tap import fetch_tap_csv
from exoplanet_analysis_report.warehouse import load_clean_parquet_to_duckdb


def run_pipeline(*, root: Path, config_path: Path | None) -> int:
    cfg = Config.from_toml(config_path) if config_path else Config()
    paths = ProjectPaths.from_root(root, cfg)
    paths.ensure_dirs()

    generated_utc = utc_now_iso()
    git_commit = try_get_git_commit(paths.root)
    cmd = " ".join(["uv", "run", "python", "scripts/run_pipeline.py"])
    schema_hash = cfg.schema_hash()

    run = RunLog(
        cfg=cfg,
        generated_utc=generated_utc,
        git_commit=git_commit,
        command=cmd,
        schema_hash=schema_hash,
    )

    uv_lock_sha = try_hash_lockfile(paths.root)
    if uv_lock_sha is not None:
        run.outputs["uv_lock_sha256"] = uv_lock_sha

    run_json_path = paths.artifacts_dir / "run.json"
    metrics_json_path = paths.artifacts_dir / "metrics.json"
    report_md_path = paths.artifacts_dir / "report.md"
    report_html_path = paths.artifacts_dir / "report.html"

    try:
        # 8.1 Fetch
        res = fetch_tap_csv(
            endpoint=cfg.tap.endpoint,
            table=cfg.tap.table,
            fmt=cfg.tap.fmt,
            columns=list(cfg.columns.used),
            discovery_methods=list(cfg.filters.discoverymethod_in),
        )
        run.data_source = {
            "name": "NASA Exoplanet Archive",
            "table": cfg.tap.table,
            "tap_endpoint": cfg.tap.endpoint,
            "tap_mode": cfg.tap.mode,
            "format": cfg.tap.fmt,
            "adql": res.adql,
            "url": res.url,
        }
        run.http = res.http

        # 8.2 Snapshot
        df_raw = parse_csv_bytes(res.csv_bytes)
        snap = write_raw_snapshot(
            df=df_raw,
            out_dir=paths.data_raw_dir,
            table=cfg.tap.table,
            timestamp_utc=generated_utc,
        )
        run.snapshots.update(
            {
                "raw_parquet_path": str(snap.raw_parquet_path.relative_to(paths.root)),
                "raw_sha256": snap.raw_sha256,
            }
        )
        run.row_counts["raw"] = snap.row_count_raw

        # 8.3 Clean
        df_clean = clean_dataframe(
            df=df_raw,
            allowed_methods=list(cfg.filters.discoverymethod_in),
            metrics=list(cfg.metrics),
            baseline_method=cfg.analysis.baseline_method,
        )
        c = write_clean_dataset(
            df_clean=df_clean,
            out_dir=paths.data_clean_dir,
            table=cfg.tap.table,
            timestamp_utc=generated_utc,
            method_order=list(cfg.method_order),
            metrics=list(cfg.metrics),
        )
        run.snapshots.update(
            {
                "clean_parquet_path": str(c.clean_parquet_path.relative_to(paths.root)),
                "clean_sha256": c.clean_sha256,
            }
        )
        run.row_counts["clean"] = c.row_count_clean
        run.missingness = c.missingness

        # 8.4 Warehouse
        load_clean_parquet_to_duckdb(
            clean_parquet_path=c.clean_parquet_path, warehouse_path=paths.warehouse_path
        )

        # 8.5 Analyze
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

        # 8.6 Plots
        generate_all_plots(
            df_clean=df_clean,
            metrics=list(cfg.metrics),
            method_order=list(cfg.method_order),
            figures_dir=paths.figures_dir,
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

        # 8.7 Report
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
    p = argparse.ArgumentParser(description="Run exoplanet analysis pipeline (v0.1.0).")
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
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    run_pipeline(root=args.root, config_path=args.config)


if __name__ == "__main__":
    main()
