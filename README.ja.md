# ブートストラップ法を用いた系外惑星観測手法比較（table-centric）

NASA Exoplanet Archive の **TAP(Table Access Protocol: 表アクセスプロトコル)** から系外惑星（planet-level）のレコードを取得し、**cleanスナップショット**を生成し、**観測手法（discovery method）ごとの要約統計**を **不確実性のガード（bootstrap CI gating）付き**で算出し、図を含む **Markdown + HTML レポート**を出力する、再現性重視のデータパイプラインです。

このリポジトリは、**ポートフォリオ用途**として次を示すことを目的にしています：

- **NumPy､Pandas､DuckDB**ライブラリによるデータ操作と基礎統計計算実行スキル
- 統計レポート及び**Matplotlib**によるグラフ作成
- **契約（contract）として固定された成果物**：`artifacts/run.json`, `artifacts/metrics.json`
- **再現可能なデータ取得（reproducible ingestion）**：TAPクエリとURLの記録、raw/cleanスナップショットのハッシュ
- **欠損（missingness）に正面から向き合う要約**：メトリックごとの欠損率を必ず出す
- オフライン前提のテストを含む、決定的（deterministic）な開発スタイル

---

## 1行まとめ（このプロジェクトが答える問い）

**観測手法（discovery method）ごとに、主要な惑星パラメータ（半径・公転周期・質量など）の分布はどう違うのかという差を、欠損と不確実性の扱いも含めて一貫した方法で検討します｡**

---

## 生成される成果物

### パイプライン出力

実行が成功すると、概ね次が生成されます：

```text
artifacts/
  figures/                # PNG 図（counts / missingness / distributions）
  metrics.json             # 機械可読な分析出力（public contract）
  run.json                 # 実行メタデータ（public contract）
  report.md                # 人間向けレポート（Markdown）
  report.html              # レポートのHTMLレンダリング
data/
  raw/                     # raw スナップショット（Parquet）
  clean/                   # clean データセット（Parquet）
warehouse/
  warehouse.duckdb         # clean テーブルを格納した DuckDB ファイル
```

### 「誠実な集計」になるための設計（重要）

- **欠損を温存する**：TAPクエリで `metric IS NOT NULL` のような全体フィルタは入れません。代わりに、解析は **メトリックごと**に non-null 行だけで行い、**メトリックごとの欠損率**を必ず出力します。
- **不確実性のガード**：bootstrap による信頼区間（CI(confidence interval: 信頼区間)）は、`n_nonnull >= thresholds.min_group_size_for_ci`（既定：20）を満たすときのみ計算します。満たさない場合、CIは `null` とし、理由コードを出します。
- **分位点（quantile）の安定化**：分位点は `numpy.quantile(..., method="linear")` を固定（テスト安定性のため）。
- **標準偏差（standard deviation）の意味を明示**：`std` は `ddof` を明示（既定 `ddof=1`）し、その値を `metrics.json` に記録します。

---

## クイックスタート

### 必要条件

- Python **3.13**
- 依存管理・環境構築に `uv`

### インストール

```bash
uv sync --locked
```

### パイプライン実行（デフォルト）

```bash
uv run python scripts/run_pipeline.py
```

### パイプライン実行（TOMLで上書き）

```bash
uv run python scripts/run_pipeline.py --config config.toml
```

補足：

- リポジトリ外から実行する場合は `--root` が使えます。
- 出力は既定でリポジトリ直下に生成されます。

---

## Data Source

### TAP endpoint

- サービス: NASA Exoplanet Archive TAP
- モード: `sync`（同期）
- 出力形式: `csv`（その後 Parquet に変換）
- 実行ごとの provenance（取得時刻、ADQL、URL）は `artifacts/run.json` に記録します。

### Table choice

`pscomppars` は **惑星1つにつき1行**の形式で提供されるため、観測手法（discovery method）ごとの要約統計を計算しやすく、本プロジェクトではこのテーブルを使用します。

- データ DOI（pscomppars）: doi:10.26133/NEA13
- 取得時刻およびクエリ詳細は実行ごとに `artifacts/run.json` に記録します  
  （例: `generated_utc`, `data_source.adql`, `data_source.url`）。

**注意（Caveat）:**
`pscomppars` は **composite（複合）**テーブルです。値は複数の参照元から導出・補完されている場合があり、**同一行内で物理的に自己整合していることは保証されません**。

---

## 設定フォーマット（確定）

**標準のデフォルト設定**は Python モジュールにあります：

- `src/exoplanet_analysis_report/config.py`（型付き `dataclass` の設定）

上書き（override）は **任意の TOML ファイル**で行えます：

- `Config.from_toml(path)` が `tomllib` でTOMLを読み込みます
- パイプラインスクリプトが `--config <path>` を受け取り、**予測可能な（浅い）マージ**で適用します

### 最小 `config.toml` 例

```toml
[tap]
endpoint = "https://exoplanetarchive.ipac.caltech.edu/TAP"
table = "pscomppars"
format = "csv"          # TOML側は "format"（cfg.tap.fmt にマップ）
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

## 統計処理概要と不確かさについて（analyze.py）

本プロジェクトは **complete-case filtering（完全ケース分析）** を行わず、メトリクスごとの要約統計と不確かさ推定を計算します。  
各 discovery method（検出法）× 各 metric（指標）について、その metric に対して **non-null（非欠損）** の値のみを用いて統計量を計算し、missingness（欠損性）は明示的に報告します。

## 記法（Notation）

次を定義します。

- $m$ を discovery method（検出法）、$b$ を baseline method（基準となる検出法）とします。
- metric $k$ について、観測された（non-null の）サンプルを次で表します。

```math
  x_{m,k} = \{x_{m,k,1}, \dots, x_{m,k,n_{m,k}}\}
  \qquad (1)
```

```math
  x_{b,k} = \{x_{b,k,1}, \dots, x_{b,k,n_{b,k}}\}
  \qquad (2)
```

- $n_{total}(m)$ は method $m$ に属する行数の総数（metric $k$ の null を含む）です。
- $n_{nonnull}(m,k)=n_{m,k}$ は method $m$ における metric $k$ の non-null 行数です。
- 欠損率（missing rate）は次で定義します。

```math
  r_{m,k} = 1 - \frac{n_{m,k}}{n_{\text{total}}(m)}
  \quad (n_{\text{total}}(m) > 0)
  \qquad (3)
```

## 要約統計（method 別）

各 $(m, k)$ について、次を報告します。

- 最小値／最大値と平均：

```math
  \min(x_{m,k}), \ \max(x_{m,k}), \
  \overline{x}_{m,k} = \frac{1}{n_{m,k}}\sum_{i=1}^{n_{m,k}} x_{m,k,i}
  \qquad (4)
```

- 分位点（quantile: 分位点） 5/25/50/75/95%：

```math
  q_{p}(x_{m,k})
  \ \text{for}\ p \in \{0.05, 0.25, 0.50, 0.75, 0.95\}
  \qquad (5)
```

  分位点は再現性のため、固定の `quantile_method`（既定: `"linear"`）を用いて  
  `numpy.quantile(..., method=quantile_method)` により計算します。

- 標準偏差（standard deviation: 標準偏差） `ddof` 付き：

```math
  s_{m,k} =
  \sqrt{\frac{1}{n_{m,k}-\text{ddof}}
  \sum_{i=1}^{n_{m,k}} (x_{m,k,i}-\overline{x}_{m,k})^2 }
  \qquad (6)
```

  $n_{m,k} = 0$ または $n_{m,k} \le \text{ddof}$ の場合、`std` は `null` として報告します。

## baseline に対する効果量（中央値差）

baseline 以外の method $m \ne b$ について、主要な効果量は median（中央値）の差です。

```math
\widehat{\Delta}_{m,k}
= \mathrm{median}(x_{m,k}) - \mathrm{median}(x_{b,k})
\qquad (7)
```

値が正であれば、metric $k$ について method $m$ の分布が baseline よりも **典型値（中央値）が大きい傾向** にあることを示します（単位は metric の units）。

## Bootstrap confidence interval (quantile method)

中央値差の不確かさは、非パラメトリック・ブートストラップ（quantile CI）で推定します。  
ブートストラップ反復 $j = 1,\dots,B$ について：

1. 各グループ内で、元標本と同じサイズで復元抽出（sampling with replacement）します：

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

2. 反復 $j$ における中央値差を計算します：

```math
   \Delta^{*(j)}_{m,k}
   =
   \mathrm{median}(x^{*(j)}_{m,k})
   -
   \mathrm{median}(x^{*(j)}_{b,k})
   \qquad (10)
```

ブートストラップ複製 $`S=\{\Delta^{*(j)}_{m,k}\}_{j=1}^{B}`$ を考え、 $S$ を昇順に並べたものを
$y_0 \le \dots \le y_{B-1}$ とします。`quantile_method="linear"` の場合、経験的な $p$ 分位点 $Q_p(S)$ は線形補間により次で定義します：

```math
h = p(B-1),\quad j=\lfloor h \rfloor,\quad g=h-j,\quad
Q_p(S) = (1-g)\,y_j + g\,y_{j+1}
\qquad (11)
```

信頼水準 $ci$（例：0.95）および $\alpha = 1 - ci$ に対して、両側 quantile 区間は：

```math
\text{CI}_{m,k}
=
\left[
Q_{\alpha/2}(S),
\,
Q_{1-\alpha/2}(S)
\right]
\qquad (12)
```

実装では、`numpy.quantile(diffs, p, method=quantile_method)` により $Q_p$ を計算し、
再現性のため `quantile_method` を固定します。

> 注（解釈／別規約）：`quantile_method="inverted_cdf"` の場合、$Q_p$ は経験 CDF の逆関数としての定義
> $Q_p(S):=\inf\{t\in\mathbb{R}:\hat F(t)\ge p\}$ と直接対応します。
> ここで $\hat F(t)=\frac{1}{B}\sum_{j=1}^{B}\mathbf{1}(\Delta^{*(j)}_{m,k}\le t)$ です。

### CI のゲート（Gate for CI）

点推定（式 (7)）は両群が空でない限り常に計算しますが、CI は次を満たす場合にのみ計算します。

```math
n_{m,k} \ge n_{\min}
\ \text{and}\
n_{b,k} \ge n_{\min}
\qquad (13)
```

ここで $n_{min}$ は閾値（例: `min_group_size_for_ci`）です。  
ゲートを満たさない場合、CI は `null` として出力し、`reason="insufficient_n"` を付与します。

## 質量 provenance（pl_bmassprov）

metric の欠損性とは独立に、`pl_bmassprov` の文字列を method ごとに次の安定カテゴリへ分類して集計します：`"Msini"`, `"Mass"`, `"Other"`。

### 出力スキーマ例（artifacts/metrics.json からの抜粋）

以下は実際の実行結果からの抜粋です。キー名とネスト構造が安定した契約であり、値は上流データの更新や解析設定（seed / resamples / CI / filters）により変動し得ます。
by_method には欠損状況（n_total, n_nonnull, missing_rate）と要約統計が入り、diff_vs_baseline には baseline に対する中央値差と（ゲートを満たす場合の）ブートストラップ quantiles CI が入ります。

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

解釈メモ：diff_vs_baseline[m].point は median(method m) - median(baseline) です。
CI のゲート条件を満たさない場合、CI は null になり、その理由が reason に入ります。
分位点と CI のパーセンタイルは、再現性のため analysis.quantile_method を固定して計算します。

---

## DuckDB warehouse を探索する（任意）

このパイプラインは、クリーンデータセットを DuckDB に 1 つのテーブルとしてロードします。

- データベースファイル: `warehouse/warehouse.duckdb`
- テーブル名: `clean_planets`（固定）

例（CLI）:

```bash
duckdb warehouse/warehouse.duckdb -c "select count(*) from clean_planets;"
```

例（SQL のアイデア）:

```sql
-- discovery method ごとの件数
duckdb warehouse/warehouse.duckdb -c "
select discoverymethod, count(*) as n
from clean_planets
group by 1
order by n desc;
"

-- 欠損状況の簡易チェック（例: ある metric）
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

> 注: v0.1.0 では DuckDB は主に **ローカル warehouse（倉庫）** およびデバッグ用の閲覧面として使います。  
> メインの分析ロジックは Python（pandas/numpy）で実装しており、DuckDB はアドホックな確認を可能にします。

---

## 開発

### テスト実行

```bash
uv run pytest -q
```

### フォーマット／静的解析

```bash
uv run ruff check .
uv run ruff format .
```

### CIに関して

CI は現時点で lint/format（＋構文コンパイル）までを実行します。テストは段階的に追加予定です。成果物生成はローカルでパイプライン実行してください。

---

## 参考文献

- NASA Exoplanet Archive (TAP): <https://exoplanetarchive.ipac.caltech.edu/>
- IVOA TAP (protocol overview): <https://www.ivoa.net/documents/TAP/>
- DuckDB Python client: <https://duckdb.org/docs/stable/clients/python/overview.html>
- DuckDB Parquet: <https://duckdb.org/docs/stable/data/parquet/overview.html>
- DuckDB CHECKPOINT: <https://duckdb.org/docs/stable/sql/statements/checkpoint.html>
- Efron, B., & Tibshirani, R. J. (1993). *An Introduction to the Bootstrap*.
- Davison, A. C., & Hinkley, D. V. (1997). *Bootstrap Methods and Their Application*.

## License

MIT License. `LICENSE` を参照してください｡

## 今後の予定

- [ ] DuckDBを使ったデータ処理
- [ ] Seaboneによるグラフ作成
