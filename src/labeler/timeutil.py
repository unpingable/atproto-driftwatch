import datetime
from typing import Optional, Union


def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def to_utc_datetime(value: Optional[Union[str, int, float, datetime.datetime]]) -> datetime.datetime:
    if value is None:
        return now_utc()
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)
    if isinstance(value, str):
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            dt = datetime.datetime.fromisoformat(v)
        except Exception:
            return now_utc()
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    return now_utc()


def to_utc_iso(value: Optional[Union[str, int, float, datetime.datetime]]) -> str:
    return to_utc_datetime(value).isoformat()
