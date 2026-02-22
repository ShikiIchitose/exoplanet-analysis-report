# Comparison of Exoplanet Observation Methods Using the Bootstrap Approach (table-centric)

> 日本語版: [README.ja.md](README.ja.md)

A reproducible, end-to-end data pipeline that **fetches exoplanet planet-level records from the NASA Exoplanet Archive (TAP)**, produces a **clean snapshot**, computes **method-by-method summary statistics with guarded uncertainty (bootstrap CI gating)**, and renders a **Markdown + HTML report** with figures.

This repository is designed as a **portfolio-grade** example of:

- Skills in executing data manipulation and **basic statistical calculations using NumPy, Pandas, and DuckDB libraries**
- Statistical Reports and Graph Creation Using **Matplotlib**
- **contracted outputs** (`artifacts/run.json`, `artifacts/metrics.json`)
- **reproducible ingestion** (TAP query + URL recorded; raw/clean snapshots hashed)
- **missingness-aware** summaries (per-metric missingness is always reported)
- deterministic, offline-friendly development practices

---

## One-line takeaway (what this answers)

**How do discovery methods differ in the distribution of key exoplanet properties (radius, orbital period, mass, etc.) when summarized consistently and transparently (including missingness and uncertainty gates)?**

---

## What you get

### Pipeline outputs

After a successful run, you should expect:

```text
artifacts/
  figures/                # PNG plots (counts / missingness / distributions)
  metrics.json             # machine-readable analysis outputs (public contract)
  run.json                 # run metadata (public contract)
  report.md                # human-facing report (Markdown)
  report.html              # report rendered to HTML
data/
  raw/                     # raw snapshot (Parquet)
  clean/                   # clean dataset (Parquet)
warehouse/
  warehouse.duckdb         # DuckDB file containing the clean table
```

### Key design choices (why this pipeline is “honest”)

- **Preserve missingness:** the TAP query does *not* globally filter `metric IS NOT NULL`. Analysis is done **per metric** on non-null rows, and **per-metric missingness** is always emitted.
- **Guarded uncertainty:** bootstrap confidence intervals are computed only when `n_nonnull >= thresholds.min_group_size_for_ci` (default: 20). Otherwise CI fields are `null` with a reason code.
- **Stable quantiles:** quantiles use `numpy.quantile(..., method="linear")` (fixed for stability).
- **Explicit `std` semantics:** standard deviation uses an explicit `ddof` (default `ddof=1`) and the value is recorded in `metrics.json`.

---

## Quickstart

### Requirements

- Python **3.13**
- `uv` for environment + dependency management

### Install

```bash
uv sync --locked
```

### Run the pipeline (defaults)

```bash
uv run python scripts/run_pipeline.py
```

### Run the pipeline (with a TOML config override)

```bash
uv run python scripts/run_pipeline.py --config config.toml
```

Notes:

- `--root` is available if you run from outside the repository root.
- Outputs are written under the repository root by default.

---

## Data Source

### TAP endpoint

- Service: NASA Exoplanet Archive TAP
- Mode: `sync` (synchronous)
- Output format: `csv` (then converted to Parquet)
- Per-run provenance (timestamp, ADQL, URL) is recorded in `artifacts/run.json`.

### Table choice

Use `pscomppars` because it provides **one row per planet**, enabling method-wise summary statistics.

- Data DOI (pscomppars): doi:10.26133/NEA13
- Retrieval timestamp and query details are recorded per run in `artifacts/run.json`
  (e.g., `generated_utc`, `data_source.adql`, `data_source.url`).

**Caveat:**
`pscomppars` is a **composite** table. Values may be derived/filled from multiple references and are **not guaranteed to be self-consistent within a row**.

---

## Configuration format (confirmed)

The **canonical defaults** live in a Python module:

- `src/exoplanet_analysis_report/config.py` (typed `dataclass` config)

Overrides are supported via an **optional TOML file**:

- `Config.from_toml(path)` loads TOML using Python’s `tomllib`
- the pipeline script accepts it as `--config <path>` and applies a **shallow, explicit merge** (predictable overrides)

### Minimal `config.toml` example

```toml
[tap]
endpoint = "https://exoplanetarchive.ipac.caltech.edu/TAP"
table = "pscomppars"
format = "csv"          # note: TOML key is "format" (maps to cfg.tap.fmt)
mode = "sync"

[filters]
discoverymethod_in = ["Transit", "Radial Velocity", "Imaging", "Microlensing"]

[bootstrap]
seed = 18790314
n_resamples = 10000
ci = 0.95
quantile_method = "linear"

[analysis]
baseline_method = "Transit"
std_ddof = 1

[thresholds]
min_group_size = 2
min_group_size_for_ci = 20

[outputs]
data_raw_dir = "data/raw"
data_clean_dir = "data/clean"
artifacts_dir = "artifacts"
figures_dir = "artifacts/figures"
warehouse_path = "warehouse/warehouse.duckdb"

[columns]
used = ["pl_name", "discoverymethod", "disc_year", "pl_rade", "pl_orbper", "pl_bmasse", "pl_bmassprov"]

[metrics]
list = ["pl_rade", "pl_orbper", "pl_bmasse"]

[method_order]
list = ["Transit", "Radial Velocity", "Imaging", "Microlensing"]
```

---

## Statistics & uncertainty (analyze.py)

This project computes per-metric summaries and uncertainty estimates **without complete-case filtering**.  
For each discovery method and each metric, statistics are computed using only the **non-null values for that metric**; missingness is reported explicitly.

## Notation

Let:

- $m$ be a discovery method, and $b$ be the baseline method.
- For a given metric $k$, let the observed (non-null) samples be:
  
```math
  x_{m,k} = \{x_{m,k,1}, \dots, x_{m,k,n_{m,k}}\}
  \qquad (1)
```

```math
  x_{b,k} = \{x_{b,k,1}, \dots, x_{b,k,n_{b,k}}\}
  \qquad (2)
```

- $n_{total}(m)$ is the total number of rows for method $m$ (including nulls for $k$).
- $n_{nonnull}(m,k)=n_{m,k}$ is the number of non-null rows for metric $k$ within method $m$.
- Missing rate is:

```math
  r_{m,k} = 1 - \frac{n_{m,k}}{n_{\text{total}}(m)}
  \quad (n_{\text{total}}(m) > 0)
  \qquad (3)
```

## Summary statistics (by method)

For each $(m, k)$, the following are reported:

- Min/max and mean:

```math
  \min(x_{m,k}), \ \max(x_{m,k}), \
  \overline{x}_{m,k} = \frac{1}{n_{m,k}}\sum_{i=1}^{n_{m,k}} x_{m,k,i}
  \qquad (4)
```

- Quantiles at 5/25/50/75/95 percent:

```math
  q_{p}(x_{m,k})
  \ \text{for}\ p \in \{0.05, 0.25, 0.50, 0.75, 0.95\}
  \qquad (5)
```

  Quantiles use a fixed `quantile_method` (default: `"linear"`) passed to
  `numpy.quantile(..., method=quantile_method)` for reproducibility.

- Standard deviation with configurable `ddof`:

```math
  s_{m,k} =
  \sqrt{\frac{1}{n_{m,k}-\text{ddof}}
  \sum_{i=1}^{n_{m,k}} (x_{m,k,i}-\overline{x}_{m,k})^2 }
  \qquad (6)
```

  If $n_{m,k} = 0$ or $n_{m,k} \le \text{ddof}$, `std` is reported as `null`.

## Effect size vs baseline (median difference)

For each non-baseline method $m \ne b$, the primary effect size is the difference in medians:

```math
\widehat{\Delta}_{m,k}
= \mathrm{median}(x_{m,k}) - \mathrm{median}(x_{b,k})
\qquad (7)
```

A positive value indicates the method-$m$ distribution tends to have **larger typical values** than the baseline for metric $k$ (in the metric’s units).

## Bootstrap confidence interval (quantile method)

Uncertainty for the median difference is estimated via a nonparametric bootstrap (quantile CI).
For bootstrap replicate $j = 1,\dots,B$:

1. Resample with replacement within each group (same sample size):

```math
   I^{(j)}_{m,1},\dots,I^{(j)}_{m,n_m} \overset{\text{iid}}{\sim} \mathrm{Unif}\{0,\dots,n_m-1\},
   \quad
   x^{*(j)}_{m,k} = (x_{m,k,I^{(j)}_{m,1}},\dots,x_{m,k,I^{(j)}_{m,n_m}})
   \qquad (8)
```

```math
   I^{(j)}_{b,1},\dots,I^{(j)}_{b,n_b} \overset{\text{iid}}{\sim} \mathrm{Unif}\{0,\dots,n_b-1\},
   \quad
   x^{*(j)}_{b,k} = (x_{b,k,I^{(j)}_{b,1}},\dots,x_{b,k,I^{(j)}_{b,n_b}})
   \qquad (9)
```

2. Compute the replicate median difference:

```math
   \Delta^{*(j)}_{m,k}
   =
   \mathrm{median}(x^{*(j)}_{m,k})
   -
   \mathrm{median}(x^{*(j)}_{b,k})
   \qquad (10)
```

Let $`S=\{\Delta^{*(j)}_{m,k}\}_{j=1}^{B}`$ be the bootstrap replicates, and let
$y_0 \le \dots \le y_{B-1}$ be $S$ sorted in ascending order.
For `quantile_method="linear"`, define the empirical $p$-quantile $Q_p(S)$ by linear interpolation:

```math
h = p(B-1),\quad j=\lfloor h \rfloor,\quad g=h-j,\quad
Q_p(S) = (1-g)\,y_j + g\,y_{j+1}
\qquad (11)
```

With confidence level $ci$ (e.g., 0.95) and $\alpha = 1 - ci$, the two-sided quantile interval is:

```math
\text{CI}_{m,k}=
\left[
Q_{\alpha/2}(S),
\,
Q_{1-\alpha/2}(S)
\right]
\qquad (12)
```

In code, we compute $Q_p$ using `numpy.quantile(diffs, p, method=quantile_method)`,
fixing `quantile_method` for reproducibility.

> Note (interpretation / alternative convention): if `quantile_method="inverted_cdf"`,
> $Q_p$ matches the inverse-empirical-CDF definition
> $Q_p(S):=\inf\{t\in\mathbb{R}:\hat F(t)\ge p\}$ with
> $\hat F(t)=\frac{1}{B}\sum_{j=1}^{B}\mathbf{1}(\Delta^{*(j)}_{m,k}\le t)$.

### Gate for CI

The point estimate (Eq. 7) is always computed when both samples are non-empty, but the CI is computed only if:

```math
n_{m,k} \ge n_{\min}
\ \text{and}\
n_{b,k} \ge n_{\min}
\qquad (13)
```

where $n_{min}$ is a configured threshold (e.g., `min_group_size_for_ci`).  
If the gate fails, CI fields are reported as `null` with `reason="insufficient_n"`.

## Mass provenance (pl_bmassprov)

Independently of metric missingness, mass provenance strings `pl_bmassprov` are categorized per method into stable buckets: `"Msini"`, `"Mass"`, `"Other"`.

### Output schema example (excerpt from `artifacts/metrics.json`)

This is an excerpt from a real run output. Keys and nesting are the stable contract; **values may vary** with upstream data updates and analysis settings (seed/resamples/CI/filters).
`by_method` reports missingness (`n_total`, `n_nonnull`, `missing_rate`) and summary stats; `diff_vs_baseline` reports the **median difference vs baseline** plus a bootstrap quantile CI when the CI gate passes.

```json
{
  "generated_utc": "2026-02-17T09:11:05Z",
  "baseline_method": "Transit",
  "method_order": ["Transit", "Radial Velocity", "Imaging", "Microlensing"],
  "analysis": {
    "std_ddof": 1,
    "quantile_method": "linear"
  },
  "mass_provenance": {
    "by_method": {
      "Transit": { "Msini": 36, "Mass": 1601, "Other": 2864 },
      "Radial Velocity": { "Msini": 858, "Mass": 298, "Other": 10 }
    }
  },
  "metrics": {
    "pl_rade": {
      "units": "Earth radii",
      "by_method": {
        "Transit": {
          "n_total": 4501,
          "n_nonnull": 4500,
          "missing_rate": 0.00022217285047765323,
          "min": 0.3098,
          "p05": 1.0099500000000001,
          "p25": 1.62,
          "p50": 2.43,
          "p75": 3.9699999999999998,
          "p95": 14.874902168500002,
          "max": 32.6,
          "mean": 4.3681517921,
          "std": 4.596210594142731
        },
        "Radial Velocity": {
          "n_total": 1166,
          "n_nonnull": 1129,
          "missing_rate": 0.03173241852487141,
          "min": 0.637,
          "p05": 1.5332000000000001,
          "p25": 4.12,
          "p50": 12.6,
          "p75": 13.4,
          "p95": 14.1,
          "max": 15.58051,
          "mean": 9.759872661948627,
          "std": 4.780243892568899
        }
      },
      "diff_vs_baseline": {
        "Radial Velocity": {
          "point": 10.17,
          "ci_low": 10.03395691775,
          "ci_high": 10.280563749999999,
          "reason": null
        }
      }
    },
    "pl_orbper": {
      "units": "days",
      "by_method": {
        "Transit": {
          "n_total": 4501,
          "n_nonnull": 4501,
          "missing_rate": 0.0,
          "min": 0.1120067,
          "p05": 1.36665436,
          "p25": 3.833091178,
          "p50": 8.15872,
          "p75": 19.502104,
          "p95": 88.07160187,
          "max": 3650.0,
          "mean": 23.972645441747456,
          "std": 80.99049950360465
        },
        "Microlensing": {
          "n_total": 266,
          "n_nonnull": 12,
          "missing_rate": 0.9548872180451128,
          "min": 1220.0,
          "p05": 1532.675,
          "p25": 2412.75,
          "p50": 3142.5,
          "p75": 4756.875,
          "p95": 9403.999999999993,
          "max": 14200.0,
          "mean": 4175.5575,
          "std": 3424.142541457709
        }
      },
      "diff_vs_baseline": {
        "Microlensing": {
          "point": 3134.34128,
          "ci_low": null,
          "ci_high": null,
          "reason": "insufficient_n"
        }
      }
    }
  }
}
```

Interpretation notes: `diff_vs_baseline[m].point` is `median(method m) - median(baseline)`.
CI fields may be `null` when the CI gate fails (reported via `reason`).
Quantiles and CI quantiles use the fixed `analysis.quantile_method` for reproducibility.

---

## Explore the DuckDB warehouse (optional)

The pipeline loads the clean dataset into DuckDB as a single table:

- database file: `warehouse/warehouse.duckdb`
- table name: `clean_planets` (fixed)

Example (CLI):

```bash
duckdb warehouse/warehouse.duckdb -c "select count(*) from clean_planets;"
```

Example (SQL ideas:run in 1 line):

```sql
-- counts by discovery method
duckdb warehouse/warehouse.duckdb -c "
select discoverymethod, count(*) as n
from clean_planets
group by 1
order by n desc;
"

-- quick missingness check (example metric)
duckdb warehouse/warehouse.duckdb -c "
select
  discoverymethod,
  count(*) as n_total,
  count(pl_rade) as n_nonnull
from clean_planets
group by 1
order by 1;
"
```

> Note: v0.1.0 uses DuckDB primarily as a **local warehouse** and debugging surface.
> The main analysis logic is implemented in Python (pandas/numpy), with DuckDB enabling ad-hoc inspection.

---

## Development

### Run tests

```bash
uv run pytest -q
```

### Style

```bash
uv run ruff check .
uv run ruff format .
```

### Notes on CI

CI currently runs lint/format checks (and a compile smoke check). Tests will be added incrementally; run the pipeline locally to generate artifacts.

---

## References

- NASA Exoplanet Archive (TAP): <https://exoplanetarchive.ipac.caltech.edu/>
- IVOA TAP (protocol overview): <https://www.ivoa.net/documents/TAP/>
- DuckDB Python client: <https://duckdb.org/docs/stable/clients/python/overview.html>
- DuckDB Parquet: <https://duckdb.org/docs/stable/data/parquet/overview.html>
- DuckDB CHECKPOINT: <https://duckdb.org/docs/stable/sql/statements/checkpoint.html>
- Efron, B., & Tibshirani, R. J. (1993). *An Introduction to the Bootstrap*.
- Davison, A. C., & Hinkley, D. V. (1997). *Bootstrap Methods and Their Application*.

## License

MIT License. See `LICENSE` file.

## Expected to add

- [ ] Data Processing Using DuckDB
- [ ] Graph creation by Seabone
