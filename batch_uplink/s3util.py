"""S3キーの組み立てと時間範囲の列挙。"""

from __future__ import annotations

import datetime as dt
from typing import Iterator

RAW_PREFIX = "raw"
EVENTS_PREFIX = "events"


def _dt(us: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(us / 1e6, tz=dt.timezone.utc)


def raw_key(device_id: int, batch_start_us: int) -> str:
    """raw/YYYY/MM/DD/HH/<device>-<startus>.bin。測定開始時刻から決める（冪等）。"""
    d = _dt(batch_start_us)
    return (f"{RAW_PREFIX}/{d:%Y/%m/%d/%H}/"
            f"{device_id:04d}-{batch_start_us:020d}.bin")


# 多重防御: 1回の列挙で辿る時別prefixの上限（暴走防止。正当な用途は最大でも数時間）。
MAX_HOUR_PREFIXES = 24 * 8  # 8日

def raw_hour_prefixes(start_us: int, end_us: int) -> Iterator[str]:
    """[start,end] の時間帯をまたぐ raw/ の時別prefixを列挙。"""
    cur = _dt(start_us).replace(minute=0, second=0, microsecond=0)
    end = _dt(end_us)
    count = 0
    while cur <= end and count < MAX_HOUR_PREFIXES:
        yield f"{RAW_PREFIX}/{cur:%Y/%m/%d/%H}/"
        cur += dt.timedelta(hours=1)
        count += 1


def event_meta_key(event_id: str) -> str:
    return f"{EVENTS_PREFIX}/{event_id}/meta.json"


def event_batch_key(event_id: str, batch_start_us: int) -> str:
    return f"{EVENTS_PREFIX}/{event_id}/{batch_start_us:020d}.bin"
