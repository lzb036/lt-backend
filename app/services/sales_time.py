from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


SALES_TIMEZONE_NAME = "Asia/Shanghai"

try:
    SALES_TIMEZONE = ZoneInfo(SALES_TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    SALES_TIMEZONE = timezone(
        timedelta(hours=8),
        name=SALES_TIMEZONE_NAME,
    )


def as_sales_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=SALES_TIMEZONE)
    return value.astimezone(SALES_TIMEZONE)


def to_sales_local_naive(value: datetime) -> datetime:
    return as_sales_datetime(value).replace(tzinfo=None)


def sales_now_naive() -> datetime:
    return datetime.now(SALES_TIMEZONE).replace(tzinfo=None)


def iso_sales_datetime(
    value: datetime | None,
    *,
    empty: str | None = None,
) -> str | None:
    if value is None:
        return empty
    return as_sales_datetime(value).isoformat(timespec="seconds")
