from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

ALLOWED_DISCOVERY_METHODS: set[str] = {
    "Transit",
    "Radial Velocity",
    "Imaging",
    "Microlensing",
    "Eclipse Timing Variations",
    "Transit Timing Variations",
    "Astrometry",
    "Pulsar Timing",
    "Pulsation Timing Variations",
    "Orbital Brightness Modulation",
    "Disk Kinematics",
}


@dataclass(frozen=True, slots=True)
class TapFetchResult:
    csv_bytes: bytes
    adql: str
    url: str
    http: dict[str, Any]


def _validate_discovery_methods(discovery_methods: list[str]) -> None:
    unknown = sorted(set(discovery_methods) - ALLOWED_DISCOVERY_METHODS)
    if unknown:
        msg = "Unknown discovery method(s): " + ", ".join(repr(x) for x in unknown)
        raise ValueError(msg)


def build_adql(*, table: str, columns: list[str], discovery_methods: list[str]) -> str:
    """
    Build ADQL with only the filters required by the spec:
      - discoverymethod IN (...)
    Do NOT add "metric IS NOT NULL" constraints (avoid complete-case bias).
    """
    _validate_discovery_methods(discovery_methods)

    cols = ", ".join(columns)
    methods = ", ".join(f"'{m}'" for m in discovery_methods)
    # Keep ADQL human-readable (spaces etc.) and let httpx encode it via params.
    return f"select {cols} from {table} where discoverymethod in ({methods})"


def _sync_url(endpoint: str) -> str:
    return endpoint.rstrip("/") + "/sync"


def fetch_tap_csv(
    *,
    endpoint: str,
    table: str,
    fmt: str,
    columns: list[str],
    discovery_methods: list[str],
    timeout_s: float = 30.0,
    max_retries: int = 3,
) -> TapFetchResult:
    """
    Fetch TAP sync CSV.

    Retry policy:
      - retry on transport errors
      - retry on transient 5xx (502/503/504)
    """
    adql = build_adql(table=table, columns=columns, discovery_methods=discovery_methods)
    base_url = _sync_url(endpoint)

    params = {"query": adql, "format": fmt}

    timeout = httpx.Timeout(timeout_s)
    headers = {"User-Agent": "exoplanet-analysis-report/0.1.0 (httpx)"}

    last_exc: Exception | None = None

    with httpx.Client(
        timeout=timeout, headers=headers, follow_redirects=True
    ) as client:
        for attempt in range(1, max_retries + 1):
            try:
                # 1) GET with params (correct encoding; avoid double-encoding).
                resp = client.get(base_url, params=params)
                # 2) If URL too long or rejected, fallback to POST (form-encoded).
                if resp.status_code in (414, 400, 431):
                    resp = client.post(base_url, data=params)

                if resp.status_code in (502, 503, 504) and attempt < max_retries:
                    continue

                resp.raise_for_status()

                csv_bytes = resp.content
                # Record URL as a fully materialized string (auditable).
                url_str = str(resp.request.url)

                http_meta: dict[str, Any] = {
                    "status": resp.status_code,
                    "content_type": resp.headers.get("content-type"),
                    "response_bytes": len(csv_bytes),
                }
                if "content-length" in resp.headers:
                    try:
                        http_meta["content_length_bytes"] = int(
                            resp.headers["content-length"]
                        )
                    except ValueError:
                        pass

                return TapFetchResult(
                    csv_bytes=csv_bytes, adql=adql, url=url_str, http=http_meta
                )

            except (httpx.TransportError, httpx.HTTPStatusError) as e:
                last_exc = e
                if attempt >= max_retries:
                    break

    msg = f"TAP fetch failed after {max_retries} attempts"
    raise RuntimeError(msg) from last_exc
