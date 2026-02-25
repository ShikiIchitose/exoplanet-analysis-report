from __future__ import annotations

import json
import sys
import traceback
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from .config import Config
from .hashing import sha256_file


def _safe_version(dist: str) -> str | None:
    try:
        return metadata.version(dist)
    except metadata.PackageNotFoundError:
        return None


def collect_library_versions() -> dict[str, str]:
    libs = [
        "numpy",
        "pandas",
        "pyarrow",
        "duckdb",
        "httpx",
        "matplotlib",
        "markdown-it-py",
    ]
    out: dict[str, str] = {}
    for name in libs:
        v = _safe_version(name)
        if v is not None:
            out[name] = v
    return out


@dataclass(slots=True)
class RunLog:
    cfg: Config
    generated_utc: str
    git_commit: str
    command: str
    schema_hash: str

    data_source: dict[str, Any] = field(default_factory=dict)
    http: dict[str, Any] = field(default_factory=dict)
    tap: dict[str, Any] = field(default_factory=dict)
    snapshots: dict[str, Any] = field(default_factory=dict)
    row_counts: dict[str, Any] = field(default_factory=dict)
    missingness: dict[str, Any] = field(default_factory=dict)
    outputs: dict[str, Any] = field(default_factory=dict)

    status: str = "running"
    error_summary: str | None = None

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "generated_utc": self.generated_utc,
            "git_commit": self.git_commit,
            "python": {"version": sys.version.split()[0]},
            "command": self.command,
            "data_source": self.data_source,
            "http": self.http,
            "tap": self.tap,
            "filters": {
                "discoverymethod_in": list(self.cfg.filters.discoverymethod_in)
            },
            "columns": {"used": list(self.cfg.columns.used)},
            "row_counts": self.row_counts,
            "schema_hash": self.schema_hash,
            "bootstrap": {
                "seed": self.cfg.bootstrap.seed,
                "n_resamples": self.cfg.bootstrap.n_resamples,
                "ci": self.cfg.bootstrap.ci,
                "baseline_method": self.cfg.analysis.baseline_method,
                "quantile_method": self.cfg.bootstrap.quantile_method,
                "min_group_size_for_ci": self.cfg.thresholds.min_group_size_for_ci,
                "std_ddof": self.cfg.analysis.std_ddof,
            },
            "snapshots": self.snapshots,
            "missingness": self.missingness,
            "outputs": self.outputs,
            "libraries": collect_library_versions(),
            "status": self.status,
            "error_summary": self.error_summary,
        }

    def finalize_success(self) -> None:
        self.status = "success"
        self.error_summary = None

    def finalize_failure(self, exc: BaseException) -> None:
        self.status = "failed"
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        self.error_summary = tb.splitlines()[-1] if tb else str(exc)

    def write_json(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_public_dict(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def _resolve_gitdir(repo_root: Path) -> Path | None:
    git_path = repo_root / ".git"
    if not git_path.exists():
        return None

    if git_path.is_dir():
        return git_path

    # Worktree: .git is a file: "gitdir: <path>"
    try:
        txt = git_path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return None

    prefix = "gitdir:"
    if not txt.lower().startswith(prefix):
        return None

    p = txt[len(prefix) :].strip()

    # If p is absolute, (repo_root / p) becomes p (pathlib behavior).
    return (repo_root / p).resolve()


def _resolve_commondir(gitdir: Path) -> Path:
    """
    In a linked worktree, refs usually live under the common git dir.
    gitdir/commondir contains a path (often relative) to that common dir.
    """
    commondir_file = gitdir / "commondir"
    if not commondir_file.exists():
        return gitdir

    try:
        rel = commondir_file.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return gitdir

    if not rel:
        return gitdir

    return (gitdir / rel).resolve()


def try_get_git_commit(repo_root: Path) -> str:
    repo_root = repo_root.resolve()

    gitdir = _resolve_gitdir(repo_root)
    if gitdir is None:
        return "UNKNOWN"

    common_gitdir = _resolve_commondir(gitdir)

    head = gitdir / "HEAD"
    try:
        txt = head.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return "UNKNOWN"

    if not txt:
        return "UNKNOWN"

    # Detached HEAD: HEAD contains commit hash
    if not txt.startswith("ref: "):
        return txt[:7]

    # Symbolic ref: resolve ref file (try gitdir, then common_gitdir)
    ref = txt[len("ref: ") :].strip()
    if not ref:
        return "UNKNOWN"

    for base in (gitdir, common_gitdir):
        ref_path = base / ref
        try:
            if ref_path.exists():
                sha = ref_path.read_text(encoding="utf-8", errors="replace").strip()
                if sha:
                    return sha[:7]
        except OSError:
            return "UNKNOWN"

    # Fast mode: do not scan packed-refs
    return "UNKNOWN"


def try_hash_lockfile(repo_root: Path) -> str | None:
    lock = repo_root / "uv.lock"
    if not lock.exists():
        return None
    return "sha256:" + sha256_file(lock)
