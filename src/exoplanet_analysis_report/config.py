from __future__ import annotations

import json
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .hashing import sha256_text


@dataclass(frozen=True, slots=True)
class TapConfig:
    endpoint: str = "https://exoplanetarchive.ipac.caltech.edu/TAP"
    table: str = "pscomppars"
    fmt: str = "csv"
    mode: str = "sync"


@dataclass(frozen=True, slots=True)
class FiltersConfig:
    discoverymethod_in: tuple[str, ...] = (
        "Transit",
        "Radial Velocity",
        "Imaging",
        "Microlensing",
    )


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    seed: int = 18_790_314
    n_resamples: int = 10_000
    ci: float = 0.95
    quantile_method: str = "linear"


@dataclass(frozen=True, slots=True)
class AnalysisConfig:
    baseline_method: str = "Transit"
    std_ddof: int = 1


@dataclass(frozen=True, slots=True)
class ThresholdsConfig:
    min_group_size: int = 2
    min_group_size_for_ci: int = 20


@dataclass(frozen=True, slots=True)
class OutputsConfig:
    data_raw_dir: str = "data/raw"
    data_clean_dir: str = "data/clean"
    artifacts_dir: str = "artifacts"
    figures_dir: str = "artifacts/figures"
    warehouse_path: str = "warehouse/warehouse.duckdb"


@dataclass(frozen=True, slots=True)
class ColumnsConfig:
    # Keep this explicit for “public contract” clarity.
    used: tuple[str, ...] = (
        "pl_name",
        "discoverymethod",
        "disc_year",
        "pl_rade",
        "pl_orbper",
        "pl_bmasse",
        "pl_bmassprov",
    )


@dataclass(frozen=True, slots=True)
class Config:
    tap: TapConfig = field(default_factory=TapConfig)
    filters: FiltersConfig = field(default_factory=FiltersConfig)
    columns: ColumnsConfig = field(default_factory=ColumnsConfig)

    metrics: tuple[str, ...] = ("pl_rade", "pl_orbper", "pl_bmasse")
    method_order: tuple[str, ...] = (
        "Transit",
        "Radial Velocity",
        "Imaging",
        "Microlensing",
    )

    bootstrap: BootstrapConfig = field(default_factory=BootstrapConfig)
    analysis: AnalysisConfig = field(default_factory=AnalysisConfig)
    thresholds: ThresholdsConfig = field(default_factory=ThresholdsConfig)
    outputs: OutputsConfig = field(default_factory=OutputsConfig)

    def to_public_dict(self) -> dict[str, Any]:
        """
        Return a JSON-serializable dict suitable for hashing and emitting into run.json.
        (Avoid non-deterministic types like Path.)
        """
        d = asdict(self)
        return d

    def schema_hash(self) -> str:
        """
        Hash of the canonical config (sorted JSON) to detect pipeline contract changes.
        """
        canonical = json.dumps(
            self.to_public_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return "sha256:" + sha256_text(canonical)

    @classmethod
    def from_toml(cls, path: Path) -> "Config":
        """
        Load partial overrides from a TOML file.
        TOML structure example:
            [tap]
            endpoint = "..."
            [bootstrap]
            seed = 123
        """
        data = tomllib.loads(path.read_text(encoding="utf-8"))
        return cls.from_mapping(data)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "Config":
        # Shallow, explicit merge to keep behavior predictable.
        def get_opt(section: str, key: str, default: Any) -> Any:
            return data.get(section, {}).get(key, default)

        tap = TapConfig(
            endpoint=get_opt("tap", "endpoint", TapConfig().endpoint),
            table=get_opt("tap", "table", TapConfig().table),
            fmt=get_opt("tap", "format", TapConfig().fmt),
            mode=get_opt("tap", "mode", TapConfig().mode),
        )
        filters = FiltersConfig(
            discoverymethod_in=tuple(
                get_opt(
                    "filters",
                    "discoverymethod_in",
                    list(FiltersConfig().discoverymethod_in),
                )
            )
        )
        columns = ColumnsConfig(
            used=tuple(get_opt("columns", "used", list(ColumnsConfig().used)))
        )
        bootstrap = BootstrapConfig(
            seed=int(get_opt("bootstrap", "seed", BootstrapConfig().seed)),
            n_resamples=int(
                get_opt("bootstrap", "n_resamples", BootstrapConfig().n_resamples)
            ),
            ci=float(get_opt("bootstrap", "ci", BootstrapConfig().ci)),
            quantile_method=str(
                get_opt(
                    "bootstrap", "quantile_method", BootstrapConfig().quantile_method
                )
            ),
        )
        analysis = AnalysisConfig(
            baseline_method=str(
                get_opt("analysis", "baseline_method", AnalysisConfig().baseline_method)
            ),
            std_ddof=int(get_opt("analysis", "std_ddof", AnalysisConfig().std_ddof)),
        )
        thresholds = ThresholdsConfig(
            min_group_size=int(
                get_opt(
                    "thresholds", "min_group_size", ThresholdsConfig().min_group_size
                )
            ),
            min_group_size_for_ci=int(
                get_opt(
                    "thresholds",
                    "min_group_size_for_ci",
                    ThresholdsConfig().min_group_size_for_ci,
                )
            ),
        )
        outputs = OutputsConfig(
            data_raw_dir=str(
                get_opt("outputs", "data_raw_dir", OutputsConfig().data_raw_dir)
            ),
            data_clean_dir=str(
                get_opt("outputs", "data_clean_dir", OutputsConfig().data_clean_dir)
            ),
            artifacts_dir=str(
                get_opt("outputs", "artifacts_dir", OutputsConfig().artifacts_dir)
            ),
            figures_dir=str(
                get_opt("outputs", "figures_dir", OutputsConfig().figures_dir)
            ),
            warehouse_path=str(
                get_opt("outputs", "warehouse_path", OutputsConfig().warehouse_path)
            ),
        )

        metrics = (
            tuple(get_opt("metrics", "list", list(cls().metrics)))
            if "metrics" in data
            else cls().metrics
        )
        method_order = (
            tuple(get_opt("method_order", "list", list(cls().method_order)))
            if "method_order" in data
            else cls().method_order
        )

        return cls(
            tap=tap,
            filters=filters,
            columns=columns,
            metrics=metrics,
            method_order=method_order,
            bootstrap=bootstrap,
            analysis=analysis,
            thresholds=thresholds,
            outputs=outputs,
        )
