from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _ensure_outdir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)


def plot_method_counts(
    *, df_clean: pd.DataFrame, method_order: list[str], out_dir: Path
) -> Path:
    _ensure_outdir(out_dir)
    counts = [int((df_clean["discoverymethod"].eq(m)).sum()) for m in method_order]

    fig, ax = plt.subplots()
    ax.bar(method_order, counts)
    ax.set_title("Method counts")
    ax.set_xlabel("Discovery method")
    ax.set_ylabel("Count")

    path = out_dir / "method_counts.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_missingness_heatmap(
    *,
    df_clean: pd.DataFrame,
    method_order: list[str],
    metrics: list[str],
    out_dir: Path,
) -> Path:
    _ensure_outdir(out_dir)

    mat = np.zeros((len(method_order), len(metrics)), dtype=float)
    for i, m in enumerate(method_order):
        g = df_clean[df_clean["discoverymethod"].eq(m)]
        n_total = int(g.shape[0])
        for j, k in enumerate(metrics):
            n_nonnull = int(g[k].notna().sum())
            mat[i, j] = 1.0 - (n_nonnull / n_total) if n_total > 0 else 1.0

    fig, ax = plt.subplots()
    im = ax.imshow(mat, aspect="auto")
    ax.set_title("Missingness rate (method x metric)")
    ax.set_xlabel("Metric")
    ax.set_ylabel("Discovery method")
    ax.set_xticks(range(len(metrics)), labels=metrics, rotation=30, ha="right")
    ax.set_yticks(range(len(method_order)), labels=method_order)
    fig.colorbar(im, ax=ax, label="Missingness rate")

    path = out_dir / "missingness_heatmap.png"
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_metric_by_method(
    *,
    df_clean: pd.DataFrame,
    metric: str,
    method_order: list[str],
    out_dir: Path,
) -> Path:
    _ensure_outdir(out_dir)

    data = []
    labels = []
    for m in method_order:
        col = df_clean.loc[df_clean["discoverymethod"].eq(m), metric]
        if not isinstance(col, pd.Series):
            raise TypeError("Expected pandas.Series for a single column selection")

        arr = col.dropna().to_numpy(dtype=float)  # ここで ndarray に確定

        if metric in ("pl_orbper", "pl_bmasse"):
            arr = arr[arr > 0]
            if arr.size > 0:
                arr = np.log10(arr)

        data.append(arr)  # もう ndarray なので to_numpy は不要
        labels.append(m)

    fig, ax = plt.subplots()
    ax.boxplot(data, tick_labels=labels, showfliers=False)

    title = f"{metric} by method"
    if metric in ("pl_orbper", "pl_bmasse"):
        title += " (log10 scale)"
        ax.set_ylabel(f"log10({metric})")
    else:
        ax.set_ylabel(metric)

    ax.set_title(title)
    ax.set_xlabel("Discovery method")

    filename = f"{metric}_by_method.png"
    path = out_dir / filename
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_diff_ci_vs_baseline(
    *,
    metrics_dict: Mapping[str, Any],
    metric: str,
    method_order: list[str],
    out_dir: Path,
) -> Path:
    """Categorical x-axis plot: median difference vs baseline with optional bootstrap CI.

    This plot uses analysis outputs (`metrics_dict`) so the figure is guaranteed to be consistent
    with the report tables (no re-computation from df_clean).

    Figure:
      - x: discovery method (non-baseline)
      - y: median difference (method - baseline)
      - y errorbar: [ci_low, ci_high] when available (CI gate may suppress it)
      - tick labels include n_nonnull and CI availability to avoid wide margins
    """
    _ensure_outdir(out_dir)

    baseline = str(
        metrics_dict.get("baseline_method")
        or (method_order[0] if method_order else "Transit")
    )
    metrics_root = metrics_dict.get("metrics")
    if not isinstance(metrics_root, Mapping):
        raise KeyError("metrics_dict['metrics'] is missing or not a mapping")

    metric_obj = metrics_root.get(metric)
    if not isinstance(metric_obj, Mapping):
        raise KeyError(
            f"metrics_dict['metrics'][{metric!r}] is missing or not a mapping"
        )

    units = metric_obj.get("units")
    units_s = f" [{units}]" if isinstance(units, str) and units.strip() else ""

    by_method = metric_obj.get("by_method")
    if not isinstance(by_method, Mapping):
        raise KeyError(
            f"metrics_dict['metrics'][{metric!r}]['by_method'] is missing or not a mapping"
        )

    diff_map = metric_obj.get("diff_vs_baseline")
    if not isinstance(diff_map, Mapping):
        raise KeyError(
            f"metrics_dict['metrics'][{metric!r}]['diff_vs_baseline'] is missing or not a mapping"
        )

    methods = [m for m in method_order if m != baseline]
    x = np.arange(len(methods), dtype=float)

    # Collect point/CI/n/reason per method index.
    points: list[float | None] = []
    ci_low: list[float | None] = []
    ci_high: list[float | None] = []
    n_nonnull: list[int | None] = []
    reason: list[str | None] = []

    for m in methods:
        d = diff_map.get(m, {})
        if not isinstance(d, Mapping):
            d = {}
        points.append(
            d.get("point") if isinstance(d.get("point"), (int, float)) else None
        )
        ci_low.append(
            d.get("ci_low") if isinstance(d.get("ci_low"), (int, float)) else None
        )
        ci_high.append(
            d.get("ci_high") if isinstance(d.get("ci_high"), (int, float)) else None
        )
        reason.append(d.get("reason") if isinstance(d.get("reason"), str) else None)

        bm = by_method.get(m, {})
        if isinstance(bm, Mapping) and isinstance(bm.get("n_nonnull"), int):
            n_nonnull.append(int(bm["n_nonnull"]))
        else:
            n_nonnull.append(None)

    # Build tick labels (2 lines) to avoid right-side annotation margin.
    tick_labels: list[str] = []
    for i, m in enumerate(methods):
        n_txt = f"{n_nonnull[i]}" if n_nonnull[i] is not None else "?"
        p = points[i]
        lo = ci_low[i]
        hi = ci_high[i]
        has_ci = (p is not None) and (lo is not None) and (hi is not None)
        if p is None:
            suffix = "N/A"
        elif has_ci:
            suffix = f"n={n_txt}"
        else:
            suffix = f"n={n_txt},\n{reason[i] or 'CI N/A'}"
        tick_labels.append(f"{m}\n{suffix}")

    # Dynamic figure size: scale width with the number of categories.
    fig_w = max(6.5, 1.6 * len(methods))
    fig_h = 4.2
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # Reference line: no difference.
    ax.axhline(0.0, linestyle="--", linewidth=1, zorder=0)

    # Plot CI entries and point-only entries (single pass; type-checker friendly).
    x_ci: list[float] = []
    y_ci: list[float] = []
    low_err: list[float] = []
    high_err: list[float] = []

    x_po: list[float] = []
    y_po: list[float] = []

    for i in range(len(methods)):
        p = points[i]
        if p is None:
            continue

        lo = ci_low[i]
        hi = ci_high[i]
        if lo is not None and hi is not None:
            p_f = float(p)
            lo_f = float(lo)
            hi_f = float(hi)

            x_ci.append(float(x[i]))
            y_ci.append(p_f)
            low_err.append(max(0.0, p_f - lo_f))
            high_err.append(max(0.0, hi_f - p_f))
        else:
            x_po.append(float(x[i]))
            y_po.append(float(p))

    if x_ci:
        ax.errorbar(
            x_ci,
            y_ci,
            yerr=[low_err, high_err],
            fmt="o",
            capsize=8,
            elinewidth=2.0,
            capthick=2.0,
            markersize=7,
        )

    if x_po:
        ax.plot(
            x_po,
            y_po,
            linestyle="None",
            marker="o",
            markerfacecolor="none",
            markeredgewidth=2.0,
            markersize=7,
            color="C0",
        )

    # --- y-axis autoscale around data (point + CI endpoints), with % padding ---
    vals: list[float] = []
    for i in range(len(methods)):
        p = points[i]
        if p is None:
            continue
        vals.append(float(p))

        lo = ci_low[i]
        hi = ci_high[i]
        if lo is not None and hi is not None:
            vals.append(float(lo))
            vals.append(float(hi))

    if vals:
        arr = np.asarray(vals, dtype=float)

        try:
            lo_q, hi_q = np.quantile(arr, [0.05, 0.95], method="linear")
        except TypeError:
            lo_q, hi_q = np.quantile(arr, [0.05, 0.95])

        span = float(hi_q - lo_q)
        pad_frac = 0.20
        pad = (span * pad_frac) if span > 0 else max(abs(float(lo_q)) * pad_frac, 1.0)

        vmin = float(np.min(arr))
        vmax = float(np.max(arr))
        ax.set_ylim(vmin - pad, vmax + pad)

    ax.set_title(f"{metric}: median difference vs {baseline}")
    ax.set_xlabel("Discovery method")
    ax.set_ylabel(f"Median difference\n(method - {baseline}){units_s}")

    ax.set_xticks(x, labels=tick_labels, rotation=20, ha="center")
    ax.margins(x=0.05, y=0.0)
    ax.tick_params(axis="x", pad=6)

    fig.tight_layout()
    path = out_dir / f"diff_ci_{metric}.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def generate_all_plots(
    *,
    df_clean: pd.DataFrame,
    metrics: list[str],
    method_order: list[str],
    figures_dir: Path,
    metrics_dict: Mapping[str, Any] | None = None,
) -> list[Path]:
    paths: list[Path] = []
    paths.append(
        plot_method_counts(
            df_clean=df_clean,
            method_order=method_order,
            out_dir=figures_dir,
        )
    )
    paths.append(
        plot_missingness_heatmap(
            df_clean=df_clean,
            method_order=method_order,
            metrics=metrics,
            out_dir=figures_dir,
        )
    )
    for metric in metrics:
        paths.append(
            plot_metric_by_method(
                df_clean=df_clean,
                metric=metric,
                method_order=method_order,
                out_dir=figures_dir,
            )
        )

    # Optional: use analysis outputs (diff + CI) for additional, report-consistent figures.
    if metrics_dict is not None:
        for metric in metrics:
            paths.append(
                plot_diff_ci_vs_baseline(
                    metrics_dict=metrics_dict,
                    metric=metric,
                    method_order=method_order,
                    out_dir=figures_dir,
                )
            )

    return paths
