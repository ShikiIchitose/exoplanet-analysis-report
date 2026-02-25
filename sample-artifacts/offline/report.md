# Exoplanet Method Comparison Report

## 1. One-line takeaway

- Using **Transit** as the baseline, non-Transit methods — Radial Velocity (RV), Imaging (Im), and Microlensing (Mi) — show strong selection effects.

  - Median radius: Δmedian(pl_rade) ≈ **+10 R⊕** (all non-Transit methods)
  - Median mass: Δmedian(pl_bmasse) ≈ **+340** (RV) / **+4.1×10³** (Im) / **+164 M⊕** (Mi)
  - Median period: Δmedian(pl_orbper) ≈ **+291 days** (RV), **+3.3×10⁴ days** (Im)
  - Note: Microlensing pl_orbper n_nonnull is insufficient for calculating CI.

- **Transit** を基準 (baseline) にすると, 非 Transit 系の手法 (Radial Velocity (RV), Imaging (Im), Microlensing (Mi)) は, 中央値差ベースで検出法ごとの選択バイアス (selection effects) が強く現れている.

  - Median radius: Δmedian(pl_rade) ≈ **+10 R⊕**（非Transit系全手法）
  - Median mass: Δmedian(pl_bmasse) ≈ **+340**（RV）/ **+4.1×10³**（Im）/ **+164 M⊕**（Mi）
  - Median period: Δmedian(pl_orbper) ≈ **+291 days**（RV）, **+3.3×10⁴ days**（Im）
  - 補足: Microlensing の pl_orbper は CI 算出閾値に満たず欠損.

## 2. Run metadata

- Generated (UTC): **2026-02-25T09:37:24Z**
- Git commit: **da8e7f6**
- Source table: `pscomppars`
- Rows: raw **—** / clean **6025**
- Baseline method: **Transit**
- Bootstrap: seed **18790314**, resamples **10000**, CI **0.95**
  - Quantile method: **linear**
  - CI eligibility threshold: **n_nonnull ≥ 20**

## 3. Data source

- NASA Exoplanet Archive (offline: input is clean parquet)
  - Input clean parquet: `data/clean/pscomppars_clean_20260225T093300Z.parquet`
  - Input sha256: `sha256:2b17ce3c9c00059d389bded1372f77e7774a213a8fb8e6bf0c072cbe8574eebd`
- Why `pscomppars`: one row per planet → convenient for method-wise summaries.
- Caveat: `pscomppars` is a composite table; values may be derived/filled from different references and may be **not self-consistent within a row**.

## 4. Data contract

- Raw snapshot: —
- Clean dataset: `data/clean/pscomppars_clean_20260225T093300Z.parquet`
- Clean sha256: `sha256:2b17ce3c9c00059d389bded1372f77e7774a213a8fb8e6bf0c072cbe8574eebd`
- Warehouse: `warehouse/warehouse.duckdb`
- Columns used:
  - `pl_name`
  - `discoverymethod`
  - `disc_year`
  - `pl_rade`
  - `pl_orbper`
  - `pl_bmasse`
  - `pl_bmassprov`

## 5. Cleaning & validation

- Rows are **not dropped** solely because a metric is null.
- For each metric: enforce `metric > 0` only where metric is not null.
- `disc_year` is descriptive only; invalid values become null.

## 6. Missingness (per metric × method)

### pl_rade

| Method | n_total | n_nonnull | missing_rate |
| --- | --- | --- | --- |
| Transit | 4501 | 4500 | 0.0002222 |
| Radial Velocity | 1166 | 1129 | 0.03173 |
| Imaging | 92 | 88 | 0.04348 |
| Microlensing | 266 | 266 | 0 |

### pl_orbper

| Method | n_total | n_nonnull | missing_rate |
| --- | --- | --- | --- |
| Transit | 4501 | 4501 | 0 |
| Radial Velocity | 1166 | 1166 | 0 |
| Imaging | 92 | 25 | 0.7283 |
| Microlensing | 266 | 12 | 0.9549 |

### pl_bmasse

| Method | n_total | n_nonnull | missing_rate |
| --- | --- | --- | --- |
| Transit | 4501 | 4478 | 0.00511 |
| Radial Velocity | 1166 | 1165 | 0.0008576 |
| Imaging | 92 | 89 | 0.03261 |
| Microlensing | 266 | 266 | 0 |

- Note: missingness may be non-random and can bias comparisons.

## 7. Exploratory analysis

- Figures (see `artifacts/figures/`):
  - `artifacts/figures/method_counts.png`
  - `artifacts/figures/missingness_heatmap.png`
  - `artifacts/figures/pl_rade_by_method.png`
  - `artifacts/figures/pl_orbper_by_method.png`
  - `artifacts/figures/pl_bmasse_by_method.png`

## 8. Statistical analysis

- Primary summaries: quantiles (p05/p25/p50/p75/p95) on non-null values.
- Diagnostics: mean/std/min/max (non-null only).
- Δ(m) = median(metric|m) − median(metric|baseline).
- Bootstrap percentile interval is computed only when `n_nonnull` is sufficient.

## 9. Results

- Footnotes:
  - Mean/std are sensitive to heavy tails and outliers; primary comparisons use medians/quantiles.
  - For small `n_nonnull`, tail quantiles (p05/p95) can behave like min/max; interpret cautiously.

### pl_rade — Quantiles (primary)

| Method | n_nonnull | p05 | p25 | p50 | p75 | p95 |
| --- | --- | --- | --- | --- | --- | --- |
| Transit | 4500 | 1.01 | 1.62 | 2.43 | 3.97 | 14.87 |
| Radial Velocity | 1129 | 1.533 | 4.12 | 12.6 | 13.4 | 14.1 |
| Imaging | 88 | 11.9 | 12.2 | 12.4 | 14.01 | 28.3 |
| Microlensing | 266 | 1.828 | 4.86 | 12.4 | 13.5 | 14.1 |

### pl_rade — Diagnostics

| Method | mean | std | min | max |
| --- | --- | --- | --- | --- |
| Transit | 4.368 | 4.596 | 0.3098 | 32.6 |
| Radial Velocity | 9.76 | 4.78 | 0.637 | 15.58 |
| Imaging | 15.61 | 11.05 | 10.47 | 87.21 |
| Microlensing | 9.851 | 4.535 | 1.09 | 14.3 |

### pl_rade — Difference vs baseline

| Method | point (median diff) | CI |
| --- | --- | --- |
| Radial Velocity | 10.17 | [10.03, 10.28] |
| Imaging | 9.97 | [9.86, 10.2] |
| Microlensing | 9.97 | [9.63, 10.46] |

---

### pl_orbper — Quantiles (primary)

| Method | n_nonnull | p05 | p25 | p50 | p75 | p95 |
| --- | --- | --- | --- | --- | --- | --- |
| Transit | 4501 | 1.367 | 3.833 | 8.159 | 19.5 | 88.07 |
| Radial Velocity | 1166 | 3.663 | 21.81 | 298.9 | 1116 | 7781 |
| Imaging | 25 | 3772 | 9697 | 3.3e+04 | 1.17e+05 | 7.892e+06 |
| Microlensing | 12 | 1533 | 2413 | 3142 | 4757 | 9404 |

### pl_orbper — Diagnostics

| Method | mean | std | min | max |
| --- | --- | --- | --- | --- |
| Transit | 23.97 | 80.99 | 0.112 | 3650 |
| Radial Velocity | 1754 | 5246 | 0.7365 | 7.711e+04 |
| Imaging | 1.703e+07 | 8.024e+07 | 2090 | 4.02e+08 |
| Microlensing | 4176 | 3424 | 1220 | 1.42e+04 |

### pl_orbper — Difference vs baseline

| Method | point (median diff) | CI |
| --- | --- | --- |
| Radial Velocity | 290.7 | [229.6, 363.2] |
| Imaging | 3.299e+04 | [1.003e+04, 1.041e+05] |
| Microlensing | 3134 | N/A (insufficient n) |

---

### pl_bmasse — Quantiles (primary)

| Method | n_nonnull | p05 | p25 | p50 | p75 | p95 |
| --- | --- | --- | --- | --- | --- | --- |
| Transit | 4478 | 1.08 | 3.39 | 6.845 | 19.75 | 597.6 |
| Radial Velocity | 1165 | 3.497 | 17.16 | 346.4 | 1367 | 4475 |
| Imaging | 89 | 1271 | 2860 | 4132 | 6356 | 8772 |
| Microlensing | 266 | 3.99 | 21 | 170.5 | 834.5 | 4215 |

### pl_bmasse — Diagnostics

| Method | mean | std | min | max |
| --- | --- | --- | --- | --- |
| Transit | 123.5 | 500.9 | 0.0364 | 8899 |
| Radial Velocity | 1035 | 1625 | 0.193 | 9333 |
| Imaging | 4456 | 2230 | 635.7 | 9535 |
| Microlensing | 797.7 | 1560 | 1.32 | 9217 |

### pl_bmasse — Difference vs baseline

| Method | point (median diff) | CI |
| --- | --- | --- |
| Radial Velocity | 339.6 | [269.7, 393.7] |
| Imaging | 4125 | [3489, 4570] |
| Microlensing | 163.7 | [110.6, 229] |

---

## 10. Interpretation

- Interpret differences as descriptive selection/detection effects; no causal claims.

## 11. Limitations

- Composite table caveat (`pscomppars` not necessarily self-consistent).
- Missingness can be non-random across methods.
- Group imbalance → larger uncertainty, especially for small-n methods/metrics.

## 12. How to reproduce

```bash
uv sync --locked
uv run python scripts/run_offline.py --clean data/clean/pscomppars_clean_20260225T093300Z.parquet
```

## 13. Appendix

### A. Full ADQL

```sql
N/A (offline run)
```

### B. TAP sync URL (CSV)

```text
N/A (offline run)
```
