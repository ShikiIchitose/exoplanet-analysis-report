from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import Config


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """
    Resolve all filesystem paths from a single project root.

    Rule: internal modules must use these paths instead of ad-hoc Path("artifacts") etc.
    """

    root: Path
    data_raw_dir: Path
    data_clean_dir: Path
    artifacts_dir: Path
    figures_dir: Path
    warehouse_path: Path

    @classmethod
    def from_root(cls, root: Path, cfg: Config) -> "ProjectPaths":
        root = root.resolve()
        return cls(
            root=root,
            data_raw_dir=(root / cfg.outputs.data_raw_dir),
            data_clean_dir=(root / cfg.outputs.data_clean_dir),
            artifacts_dir=(root / cfg.outputs.artifacts_dir),
            figures_dir=(root / cfg.outputs.figures_dir),
            warehouse_path=(root / cfg.outputs.warehouse_path),
        )

    def ensure_dirs(self) -> None:
        self.data_raw_dir.mkdir(parents=True, exist_ok=True)
        self.data_clean_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.figures_dir.mkdir(parents=True, exist_ok=True)
        self.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
