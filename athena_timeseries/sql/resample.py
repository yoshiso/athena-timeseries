from dataclasses import dataclass
from tempfile import TemporaryDirectory
import numpy as np

from typing import List, Optional, Any
from logging import getLogger
import awswrangler
import pandas as pd
import re

from .basic import to_where, _assert_dt


logger = getLogger(__name__)


@dataclass(frozen=True)
class Expr:
    def to_repr(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class And(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " AND ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class Or(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " OR ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class GT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} > {self.value}"


@dataclass(frozen=True)
class GTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} >= {self.value}"


@dataclass(frozen=True)
class LT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} < {self.value}"


@dataclass(frozen=True)
class LTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} <= {self.value}"


@dataclass(frozen=True)
class Like(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, str)
        return f"{self.field} like {self.value}"


@dataclass(frozen=True)
class Eq(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        if isinstance(self.value, str):
            assert isinstance(self.value, str)
            return f"{self.field} = '{self.value}'"
        if isinstance(self.value, bool):
            str_value = "true" if self.value is True else "false"
            return f"{self.field} = {str_value}"
        if isinstance(self.value, int):
            return f"{self.field} = {self.value}"
        raise NotImplementedError


def resample_query(
    *,
    boto3_session,
    glue_db_name: str,
    table_name: str,
    field: str,
    symbols: Optional[List[str]] = None,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    interval: str = "day",
    tz: Optional[str] = None,
    op: str = "last",
    where: Optional[Expr] = None,
    cast: Optional[str] = None,
    verbose: int = 0,
    fast: bool = True,
    offset_repr: Optional[str] = None,
):

    inner_stmt = _build_inner_view(
        table_name=table_name,
        field=field,
        symbols=symbols,
        start_dt=start_dt,
        end_dt=end_dt,
        interval=interval,
        tz=tz,
        where=where,
        offset_repr=offset_repr,
    )

    field_name = "value"
    if cast is not None:
        field_name = f"cast(value as {cast})"

    operator = {
        "last": "max_by({field_name}, timestamp)",
        "first": "min_by({field_name}, timestamp)",
        "max": "max({field_name})",
        "min": "min({field_name})",
        "sum": "sum({field_name})",
    }[op].format(field_name=field_name)

    stmt = f"""
select
    dt,
    {operator} AS value,
    symbol
from
    ({inner_stmt}) as t
group by dt, symbol
order by dt
    """

    if verbose > 0:
        print(stmt)

    if fast:
        execute_fn = _fast_read_sql_query
    else:
        execute_fn = awswrangler.athena.read_sql_query

    df = execute_fn(
        sql=stmt,
        database=glue_db_name,
        boto3_session=boto3_session,
        max_cache_query_inspections=60 * 10,
        ctas_approach=False,
    )

    df = df.set_index(["dt", "symbol"])["value"].unstack()

    df.index = pd.to_datetime(
        df.index,
        format="%Y-%m-%d %H:%M:%S.%f %Z",
        cache=True,
        infer_datetime_format=True,
    )

    if tz is not None:
        if df.index.tz is None:
            df = df.tz_localize(tz)
        else:
            df = df.tz_convert(tz)

    df.index.name = "dt"

    return df.sort_index()


def _fast_read_sql_query(
    *,
    sql: str,
    database: str,
    boto3_session,
    max_cache_query_inspections: int,
    ctas_approach: bool,
    dtype=np.float64,
):
    execution_id = awswrangler.athena.start_query_execution(
        sql=sql,
        database=database,
        boto3_session=boto3_session,
    )
    try:
        execution_result = awswrangler.athena.wait_query(
            execution_id, boto3_session=boto3_session
        )
    except (Exception, KeyboardInterrupt) as e:
        awswrangler.athena.stop_query_execution(
            execution_id, boto3_session=boto3_session
        )
        raise e from e

    if execution_result["Status"]["State"] != "SUCCEEDED":
        raise RuntimeError(f"Failed athena query: {execution_result}")

    with TemporaryDirectory() as tempdir:
        output_path = tempdir + "/temp.csv"

        awswrangler.s3.download(
            execution_result["ResultConfiguration"]["OutputLocation"], output_path
        )

        df = pd.read_csv(output_path, dtype={"dt": str, "symbol": str, "value": dtype})

    return df


def _build_inner_view(
    table_name: str,
    field: str,
    start_dt: Optional[str] = None,
    end_dt: Optional[str] = None,
    symbols: Optional[List[str]] = None,
    interval: str = "day",
    tz: Optional[str] = None,
    where: Optional[Expr] = None,
    extras: Optional[List[str]] = None,
    offset_repr: Optional[str] = None,
):
    where_conditions = to_where(
        start_dt,
        end_dt,
        partition_key="partition_dt",
        partition_interval="monthly",
        type="TIMESTAMP",
        tz=tz,
    )

    where_term = ""
    if len(where_conditions) > 0:
        where_term = " AND ".join(where_conditions)

    in_term = ""
    if symbols is not None:
        in_term = "symbol in ('" + "','".join(symbols) + "')"

    all_term = where_term

    if len(in_term) > 0:
        if len(all_term) > 0:
            all_term = f"{all_term} AND {in_term}"
        else:
            all_term = in_term

    if where is not None:
        if len(all_term) > 0:
            all_term = f"{all_term} AND ({where.to_repr()})"
        else:
            all_term = where.to_repr()

    if len(all_term) > 0:
        all_term = f" where {all_term}"

    dt_term = to_resampled_dt(interval=interval, tz=tz, offset_repr=offset_repr)

    dt_repr = "dt" if tz is None else f"dt AT TIME ZONE '{tz}'"

    extra_repr = ""
    if extras is not None:
        extra_repr = "," + ",".join(extras)

    return f"""
select
    ({dt_term}) as dt,
    ({dt_repr}) as timestamp,    
    {field} as value,
    symbol{extra_repr}
from
    {table_name} 
{all_term}
order by timestamp asc
    """


def to_resampled_dt(interval: str, tz: str, offset_repr: Optional[str] = None):
    """
    Args:
        tz: timezone to be used to resample for date intervals.
        offset_repr: offset term used to shift timestamp before resampling.
    """
    dt_repr = "dt" if tz is None else f"dt AT TIME ZONE '{tz}'"

    if offset_repr is not None:
        dt_repr = f"({dt_repr} {offset_repr})"

    if interval in ("hour", "day", "minute", "week", "month"):
        return f"date_trunc('{interval}', {dt_repr})"

    match = re.match("([0-9]+)(minute|hour)", interval)

    if match is None:
        raise ValueError(f"{interval} is invalid")

    steps = match[1]
    interval = match[2]

    upper_interval = {
        "minute": "hour",
        "hour": "day",
    }[interval]

    multiples = {"minute": f"minute({dt_repr})", "hour": f"hour({dt_repr})"}[interval]

    return f"date_trunc('{upper_interval}', {dt_repr}) + {multiples} / {steps} * interval '{steps}' {interval}"
