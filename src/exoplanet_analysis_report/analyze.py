from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, TypeAlias, cast

import numpy as np
import pandas as pd

# ---- Add: Quantile method typing + validation ----
QuantileMethod: TypeAlias = Literal[
    "inverted_cdf",
    "averaged_inverted_cdf",
    "closest_observation",
    "interpolated_inverted_cdf",
    "hazen",
    "weibull",
    "linear",
    "median_unbiased",
    "normal_unbiased",
    "lower",
    "higher",
    "midpoint",
    "nearest",
]

_ALLOWED_QUANTILE_METHODS: set[str] = {
    "inverted_cdf",
    "averaged_inverted_cdf",
    "closest_observation",
    "interpolated_inverted_cdf",
    "hazen",
    "weibull",
    "linear",
    "median_unbiased",
    "normal_unbiased",
    "lower",
    "higher",
    "midpoint",
    "nearest",
}


def _normalize_quantile_method(m: str) -> QuantileMethod:
    # External input gate: fail fast and deterministic.
    if m not in _ALLOWED_QUANTILE_METHODS:
        allowed = ", ".join(sorted(_ALLOWED_QUANTILE_METHODS))
        raise ValueError(f"Invalid quantile_method={m!r}. Allowed: {allowed}")
    return cast(QuantileMethod, m)


# Optional: keep percent points as float constants
_Q_POINTS = (0.05, 0.25, 0.50, 0.75, 0.95)


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _as_float(x: float | np.floating[Any] | None) -> float | None:
    if x is None:
        return None
    return float(x)


def _safe_std(values: np.ndarray, ddof: int) -> float | None:
    n = int(values.size)
    if n == 0:
        return None
    if ddof >= n:
        return None
    return float(np.std(values, ddof=ddof))


def _summary_stats(
    values: np.ndarray, *, quantile_method: QuantileMethod, std_ddof: int
) -> dict[str, Any]:
    if values.size == 0:
        return {
            "min": None,
            "p05": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p95": None,
            "max": None,
            "mean": None,
            "std": None,
        }

    qs = np.quantile(values, _Q_POINTS, method=quantile_method)
    return {
        "min": _as_float(np.min(values)),
        "p05": _as_float(qs[0]),
        "p25": _as_float(qs[1]),
        "p50": _as_float(qs[2]),
        "p75": _as_float(qs[3]),
        "p95": _as_float(qs[4]),
        "max": _as_float(np.max(values)),
        "mean": _as_float(np.mean(values)),
        "std": _as_float(_safe_std(values, ddof=std_ddof)),
    }


def _metric_units(metric: str) -> str:
    if metric == "pl_rade":
        return "Earth radii"
    if metric == "pl_orbper":
        return "days"
    if metric == "pl_bmasse":
        return "Earth masses"
    return ""


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    point: float | None
    ci_low: float | None
    ci_high: float | None
    reason: str | None


def _bootstrap_median_diff(
    *,
    x_m: np.ndarray,
    x_b: np.ndarray,
    seed: int,
    n_resamples: int,
    ci: float,
    quantile_method: QuantileMethod,
    min_group_size_for_ci: int,
) -> BootstrapResult:
    # Point estimate always computed if both non-empty.
    if x_m.size == 0 or x_b.size == 0:
        return BootstrapResult(
            point=None, ci_low=None, ci_high=None, reason="insufficient_n"
        )

    point = float(np.median(x_m) - np.median(x_b))

    if x_m.size < min_group_size_for_ci or x_b.size < min_group_size_for_ci:
        return BootstrapResult(
            point=point, ci_low=None, ci_high=None, reason="insufficient_n"
        )

    rng = np.random.default_rng(seed)
    diffs = np.empty(n_resamples, dtype=np.float64)

    nm = int(x_m.size)
    nb = int(x_b.size)

    for i in range(n_resamples):
        sm = x_m[rng.integers(0, nm, size=nm)]
        sb = x_b[rng.integers(0, nb, size=nb)]
        diffs[i] = np.median(sm) - np.median(sb)

    alpha = 1.0 - ci
    lo, hi = np.quantile(
        diffs,
        [(alpha / 2.0), (1.0 - alpha / 2.0)],
        method=quantile_method,
    )
    return BootstrapResult(
        point=point, ci_low=float(lo), ci_high=float(hi), reason=None
    )


def _mass_provenance_counts(series: pd.Series) -> dict[str, int]:
    # Keep categories stable:
    # - "Msini" if contains "Msini"
    # - "Mass" if contains "Mass"
    # - otherwise "Other"
    out = {"Msini": 0, "Mass": 0, "Other": 0}
    for raw in series.dropna():
        s: str = str(raw)  # Ensure it's a str here (keeps the type checker happy).
        if "Msini" in s:
            out["Msini"] += 1
        elif "Mass" in s:
            out["Mass"] += 1
        else:
            out["Other"] += 1
    return out


def compute_metrics_json(
    *,
    df_clean: pd.DataFrame,
    metrics: list[str],
    method_order: list[str],
    baseline_method: str,
    bootstrap_seed: int,
    n_resamples: int,
    ci: float,
    quantile_method: str,
    std_ddof: int,
    min_group_size_for_ci: int,
) -> dict[str, Any]:
    generated_utc = _utc_now_iso()

    qm = _normalize_quantile_method(quantile_method)

    out: dict[str, Any] = {
        "generated_utc": generated_utc,
        "baseline_method": baseline_method,
        "method_order": method_order,
        "analysis": {
            "std_ddof": std_ddof,
            "quantile_method": qm,
        },
        "mass_provenance": {"by_method": {}},
        "metrics": {},
    }

    # Mass provenance by method (independent of metrics missingness)
    for m in method_order:
        g = df_clean[df_clean["discoverymethod"] == m]
        out["mass_provenance"]["by_method"][m] = _mass_provenance_counts(
            g.get("pl_bmassprov", pd.Series(dtype="object"))
        )

    # Metric summaries + diff_vs_baseline
    for k in metrics:
        metric_block: dict[str, Any] = {
            "units": _metric_units(k),
            "by_method": {},
            "diff_vs_baseline": {},
        }

        # Baseline sample for this metric
        gb = df_clean[df_clean["discoverymethod"] == baseline_method]
        xb = gb[k].dropna().to_numpy(dtype=np.float64)

        for m in method_order:
            g = df_clean[df_clean["discoverymethod"] == m]
            n_total = int(g.shape[0])
            x = g[k].dropna().to_numpy(dtype=np.float64)
            n_nonnull = int(x.size)
            missing_rate = float(1.0 - (n_nonnull / n_total)) if n_total > 0 else 1.0

            stats = _summary_stats(x, quantile_method=qm, std_ddof=std_ddof)
            metric_block["by_method"][m] = {
                "n_total": n_total,
                "n_nonnull": n_nonnull,
                "missing_rate": missing_rate,
                **stats,
            }

            if m == baseline_method:
                continue

            br = _bootstrap_median_diff(
                x_m=x,
                x_b=xb,
                seed=bootstrap_seed,
                n_resamples=n_resamples,
                ci=ci,
                quantile_method=qm,
                min_group_size_for_ci=min_group_size_for_ci,
            )
            metric_block["diff_vs_baseline"][m] = {
                "point": br.point,
                "ci_low": br.ci_low,
                "ci_high": br.ci_high,
                "reason": br.reason,
            }

        out["metrics"][k] = metric_block

    return out
