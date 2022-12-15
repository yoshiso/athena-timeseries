from typing import Optional, List, Callable
import pandas as pd
from datetime import datetime
import awswrangler

from ..dt import (
    to_quarter_start_dt,
    to_month_start_dt,
    to_quarter_end_dt,
    to_month_end_dt,
)


def _assert_dt(dt: Optional[str]):
    if dt is not None:
        datetime.strptime(dt, "%Y-%M-%d")


def to_where(
    start_dt: Optional[str],
    end_dt: Optional[str],
    partition_key: str = "quarter",
    partition_interval: str = "quarterly",
    type: str = "DATE",
    tz: Optional[str] = None,
):
    assert partition_interval in ("quarterly", "monthly")

    start_dt_offset_fn = {
        "quarterly": to_quarter_start_dt,
        "monthly": to_month_start_dt,
    }[partition_interval]

    end_dt_offset_fn = {
        "quarterly": to_quarter_end_dt,
        "monthly": to_month_end_dt,
    }[partition_interval]

    where = []

    if start_dt is not None:
        _start_dt = pd.Timestamp(start_dt, tz=tz)
        if tz is not None:
            _start_dt = _start_dt.tz_convert("UTC").tz_localize(None)
        _start_dt = start_dt_offset_fn(_start_dt)

        where += [
            f"{partition_key} >= CAST('{_start_dt:%Y-%m-%d}' AS DATE)",
            f"dt >= CAST('{start_dt}' AS {type})",
        ]

    if end_dt is not None:
        _end_dt = pd.Timestamp(end_dt, tz=tz)
        if tz is not None:
            _end_dt = _end_dt.tz_convert("UTC").tz_localize(None)
        _end_dt = end_dt_offset_fn(_end_dt)

        where += [
            f"{partition_key} <= CAST('{_end_dt:%Y-%m-%d}' AS DATE)",
            f"dt <= CAST('{end_dt}' AS {type})",
        ]

    return where


def query(
    boto3_session,
    glue_db_name: str,
    table_name: str,
    field: str,
    symbols: Optional[List[str]] = None,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    max_cache_expires: int = 1,
    partition_key: str = "partition_dt",
    partition_interval: str = "quarterly",
    type: str = "Timestamp",
    ctas_approach: bool = False,
) -> pd.DataFrame:
    # _assert_dt(start_dt)
    # _assert_dt(end_dt)

    where = to_where(
        start_dt=start_dt,
        end_dt=end_dt,
        partition_key=partition_key,
        partition_interval=partition_interval,
        type=type,
    )

    if symbols is not None and len(symbols) > 0:
        predicated = "'" + "','".join(symbols) + "'"

        where += [f"symbol in ({predicated})"]

    stmt = f"""
SELECT 
    {field}, 
    symbol, 
    dt
FROM
    {table_name}
        """

    if len(where) > 0:
        condition = " AND ".join(where)
        stmt += f"WHERE {condition}"

    df = awswrangler.athena.read_sql_query(
        stmt,
        database=glue_db_name,
        boto3_session=boto3_session,
        max_cache_seconds=max_cache_expires,
        ctas_approach=ctas_approach,
    )

    df["dt"] = pd.to_datetime(df["dt"])

    return df.set_index(["dt", "symbol"])[field].unstack().sort_index()
