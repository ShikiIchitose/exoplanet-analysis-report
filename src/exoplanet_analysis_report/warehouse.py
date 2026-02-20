from __future__ import annotations

from pathlib import Path

import duckdb

CLEAN_TABLE_NAME: str = "clean_planets"


def load_clean_parquet_to_duckdb(
    *, clean_parquet_path: Path, warehouse_path: Path
) -> None:

    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(warehouse_path)) as con:
        con.execute(f"drop table if exists {CLEAN_TABLE_NAME}")
        con.execute(
            f"create table {CLEAN_TABLE_NAME} as select * from read_parquet(?)",
            [str(clean_parquet_path)],
        )
        con.execute("checkpoint")
