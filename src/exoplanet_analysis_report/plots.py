from __future__ import annotations

from pathlib import Path

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


def generate_all_plots(
    *,
    df_clean: pd.DataFrame,
    metrics: list[str],
    method_order: list[str],
    figures_dir: Path,
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
    return paths
