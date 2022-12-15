import pandas as pd
from datetime import datetime


def is_date(dt: str) -> bool:
    try:
        datetime.strptime(dt, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def to_quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def to_month_start_dt(dt: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=dt.year, month=dt.month, day=1)


def to_month_end_dt(dt: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=dt.year, month=dt.month, day=1) + pd.offsets.MonthEnd()


def to_quarter_start_dt(dt: pd.Timestamp, offset: int = 0) -> pd.Timestamp:
    """Compute beginning date of quarter from given timestamp"""
    quarter = to_quarter(dt.month)

    quarter_start_month = (quarter - 1) * 3 + 1

    dt = pd.Timestamp(year=dt.year, month=quarter_start_month, day=1)

    if offset > 0:
        dt = dt - pd.offsets.MonthBegin(3 * offset)

    return dt


def to_quarter_end_dt(dt: pd.Timestamp, offset: int = 0):
    return to_quarter_start_dt(dt, offset) + pd.offsets.MonthEnd(3)


def compute_intervals(
    start_dt: str,
    end_dt: str,
    days: int = 5,
    fmt: str = "%Y-%m-%d",
    offset=pd.offsets.Day(),
):
    _start_dt = pd.Timestamp(start_dt)
    _end_dt = pd.Timestamp(end_dt)

    while True:
        current_end_dt = (_start_dt + pd.offsets.Day(days)).normalize()

        if current_end_dt >= _end_dt:
            yield _start_dt.strftime(fmt), _end_dt.strftime(fmt)
            return

        yield _start_dt.strftime(fmt), current_end_dt.strftime(fmt)

        _start_dt = current_end_dt + offset


def compute_monthly_intervals(start_dt: str, end_dt: str):
    fmt = "%Y-%m-%d"
    datetime.strptime(start_dt, fmt)
    datetime.strptime(end_dt, fmt)

    _start_dt = pd.Timestamp(start_dt)
    _end_dt = pd.Timestamp(end_dt)

    while True:
        current_end_dt = _start_dt.replace(day=1) + pd.offsets.MonthEnd()

        if current_end_dt >= _end_dt:
            yield _start_dt.strftime(fmt), _end_dt.strftime(fmt)
            return

        yield _start_dt.strftime(fmt), current_end_dt.strftime(fmt)

        _start_dt = current_end_dt + pd.offsets.Day()
