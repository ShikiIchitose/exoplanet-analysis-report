from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from markdown_it import MarkdownIt


def _fmt(x: float | None) -> str:
    if x is None:
        return "N/A"
    return f"{x:.4g}"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    head = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return "\n".join([head, sep, body])


def render_report_md(*, run: dict[str, Any], metrics: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Exoplanet Method Comparison Report")
    lines.append("")

    lines.append("## 1. One-line takeaway")
    lines.append("")
    lines.append(
        "- (Write 1–2 sentences that summarize the major differences seen in this snapshot.)"
    )
    lines.append("")

    lines.append("## 2. Run metadata")
    lines.append("")
    lines.append(f"- Generated (UTC): **{run.get('generated_utc', '')}**")
    lines.append(f"- Git commit: **{run.get('git_commit', '')}**")
    ds = run.get("data_source", {})
    lines.append(f"- Source table: `{ds.get('table', '')}`")
    rc = run.get("row_counts", {})
    lines.append(
        f"- Rows: raw **{rc.get('raw', '')}** / clean **{rc.get('clean', '')}**"
    )
    b = run.get("bootstrap", {})
    lines.append(f"- Baseline method: **{b.get('baseline_method', '')}**")
    lines.append(
        f"- Bootstrap: seed **{b.get('seed', '')}**, resamples **{b.get('n_resamples', '')}**, CI **{b.get('ci', '')}**"
    )
    lines.append(f"  - Quantile method: **{b.get('quantile_method', '')}**")
    lines.append(
        f"  - CI eligibility threshold: **n_nonnull ≥ {b.get('min_group_size_for_ci', '')}**"
    )
    lines.append("")

    lines.append("## 3. Data source")
    lines.append("")
    lines.append("- NASA Exoplanet Archive (TAP)")
    lines.append(
        "- Why `pscomppars`: one row per planet → convenient for method-wise summaries."
    )
    lines.append(
        "- Caveat: `pscomppars` is a composite table; values may be derived/filled from different references and may be **not self-consistent within a row**."
    )
    lines.append("")

    lines.append("## 4. Data contract")
    lines.append("")
    snaps = run.get("snapshots", {})
    lines.append(f"- Raw snapshot: `{snaps.get('raw_parquet_path', '')}`")
    lines.append(f"- Clean dataset: `{snaps.get('clean_parquet_path', '')}`")
    lines.append("- Warehouse: `warehouse/warehouse.duckdb`")
    cols = run.get("columns", {}).get("used", [])
    if cols:
        lines.append("- Columns used:")
        for c in cols:
            lines.append(f"  - `{c}`")
    lines.append("")

    lines.append("## 5. Cleaning & validation")
    lines.append("")
    lines.append("- Rows are **not dropped** solely because a metric is null.")
    lines.append(
        "- For each metric: enforce `metric > 0` only where metric is not null."
    )
    lines.append("- `disc_year` is descriptive only; invalid values become null.")
    lines.append("")

    lines.append("## 6. Missingness (per metric × method)")
    lines.append("")
    miss = run.get("missingness", {})
    method_order = metrics.get("method_order", [])
    metric_names = list(metrics.get("metrics", {}).keys())
    for k in metric_names:
        rows = []
        for m in method_order:
            mm = miss.get(m, {}).get(k, {})
            rows.append(
                [
                    m,
                    str(mm.get("n_total", "")),
                    str(mm.get("n_nonnull", "")),
                    _fmt(mm.get("missing_rate")),
                ]
            )
        lines.append(f"### {k}")
        lines.append("")
        lines.append(
            _md_table(["Method", "n_total", "n_nonnull", "missing_rate"], rows)
        )
        lines.append("")
    lines.append("- Note: missingness may be non-random and can bias comparisons.")
    lines.append("")

    lines.append("## 7. Exploratory analysis")
    lines.append("")
    lines.append("- Figures (see `artifacts/figures/`):")
    lines.append("  - `figures/method_counts.png`")
    lines.append("  - `figures/missingness_heatmap.png`")
    for k in metric_names:
        lines.append(f"  - `figures/{k}_by_method.png`")
    lines.append("")

    lines.append("## 8. Statistical analysis")
    lines.append("")
    lines.append(
        "- Primary summaries: quantiles (p05/p25/p50/p75/p95) on non-null values."
    )
    lines.append("- Diagnostics: mean/std/min/max (non-null only).")
    lines.append("- Δ(m) = median(metric|m) − median(metric|baseline).")
    lines.append(
        "- Bootstrap percentile interval is computed only when `n_nonnull` is sufficient."
    )
    lines.append("")

    lines.append("## 9. Results")
    lines.append("")
    lines.append("- Footnotes:")
    lines.append(
        "  - Mean/std are sensitive to heavy tails and outliers; primary comparisons use medians/quantiles."
    )
    lines.append(
        "  - For small `n_nonnull`, tail quantiles (p05/p95) can behave like min/max; interpret cautiously."
    )
    lines.append("")

    for k in metric_names:
        block = metrics["metrics"][k]
        by_method = block["by_method"]

        q_rows = []
        for m in method_order:
            bm = by_method[m]
            q_rows.append(
                [
                    m,
                    str(bm["n_nonnull"]),
                    _fmt(bm["p05"]),
                    _fmt(bm["p25"]),
                    _fmt(bm["p50"]),
                    _fmt(bm["p75"]),
                    _fmt(bm["p95"]),
                ]
            )
        lines.append(f"### {k} — Quantiles (primary)")
        lines.append("")
        lines.append(
            _md_table(
                ["Method", "n_nonnull", "p05", "p25", "p50", "p75", "p95"], q_rows
            )
        )
        lines.append("")

        d_rows = []
        for m in method_order:
            bm = by_method[m]
            d_rows.append(
                [m, _fmt(bm["mean"]), _fmt(bm["std"]), _fmt(bm["min"]), _fmt(bm["max"])]
            )
        lines.append(f"### {k} — Diagnostics")
        lines.append("")
        lines.append(_md_table(["Method", "mean", "std", "min", "max"], d_rows))
        lines.append("")

        diff = block["diff_vs_baseline"]
        d2_rows = []
        for m in method_order:
            if m == metrics["baseline_method"]:
                continue
            dm = diff.get(m, {})
            ci_str = (
                "N/A (insufficient n)"
                if dm.get("ci_low") is None or dm.get("ci_high") is None
                else f"[{_fmt(dm.get('ci_low'))}, {_fmt(dm.get('ci_high'))}]"
            )
            d2_rows.append([m, _fmt(dm.get("point")), ci_str])
        lines.append(f"### {k} — Difference vs baseline")
        lines.append("")
        lines.append(_md_table(["Method", "point (median diff)", "CI"], d2_rows))
        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append("## 10. Interpretation")
    lines.append("")
    lines.append(
        "- Interpret differences as descriptive selection/detection effects; no causal claims."
    )
    lines.append("")

    lines.append("## 11. Limitations")
    lines.append("")
    lines.append(
        "- Composite table caveat (`pscomppars` not necessarily self-consistent)."
    )
    lines.append("- Missingness can be non-random across methods.")
    lines.append(
        "- Group imbalance → larger uncertainty, especially for small-n methods/metrics."
    )
    lines.append("")

    lines.append("## 12. How to reproduce")
    lines.append("")
    fence = "`" * 3
    lines.append(f"{fence}bash")
    lines.append("uv sync --locked")
    lines.append("uv run python scripts/run_pipeline.py")
    lines.append(f"{fence}")
    lines.append("")

    lines.append("## 13. Appendix")
    lines.append("")
    lines.append("### A. Full ADQL")
    lines.append("")
    lines.append(f"{fence}sql")
    lines.append(ds.get("adql", ""))
    lines.append(f"{fence}")
    lines.append("")
    lines.append("### B. TAP sync URL (CSV)")
    lines.append("")
    lines.append(f"{fence}text")
    lines.append(ds.get("url", ""))
    lines.append(f"{fence}")
    lines.append("")

    return "\n".join(lines)


def write_report_files(
    *,
    run_json_path: Path,
    metrics_json_path: Path,
    report_md_path: Path,
    report_html_path: Path,
) -> None:
    run = json.loads(run_json_path.read_text(encoding="utf-8"))
    metrics = json.loads(metrics_json_path.read_text(encoding="utf-8"))

    out_dirs = {report_md_path.parent, report_html_path.parent}
    for d in out_dirs:
        d.mkdir(parents=True, exist_ok=True)

    md = render_report_md(run=run, metrics=metrics)
    if not md.endswith("\n"):
        md += "\n"
    report_md_path.write_text(md, encoding="utf-8", newline="\n")

    md_parser = MarkdownIt("commonmark")
    html = md_parser.render(md)
    report_html_path.write_text(html, encoding="utf-8")
