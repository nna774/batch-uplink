"""デバイスの生存台帳（DynamoDB namazu-devices）。

ingest がバッチ受信ごとに1項目を upsert し、watchdog が定期起動して
「最後の受信からの経過」を見て欠測を判定する。events テーブルとは別に、
「今このデバイスが喋っているか」の単一の真実をここに持つ。

生存の主信号は **last_ingest_at_us（ingest が受信した壁時計時刻）**。
firmware はWiFi断のあとバックフィルするので、測定時刻(last_batch_start_us)だけでは
「復旧直後で追いつき中」と「本当に沈黙」が区別できない。受信壁時計で生存を、
last_batch_start_us との差でデータ遅延を、別々に見る。

欠測通知の状態(offline_notified_at_us)とデータ遅延通知の状態(lag_notified_at_us)は
watchdog だけが書く。ingest が書く受信系フィールドとは互いに素なので、両者を
UpdateItem で分けて更新すれば read-modify-write の競合は起きない。
"""

from __future__ import annotations

import os
from decimal import Decimal

import boto3

_table_cache = None


def _table():
    global _table_cache
    if _table_cache is None:
        _table_cache = boto3.resource("dynamodb").Table(os.environ["NAMZ_DEVICES_TABLE"])
    return _table_cache


def _dec(v) -> Decimal:
    return Decimal(str(int(v)))


def record_batch(device_id: int, batch_start_us: int, ingest_at_us: int,
                 last_batch_key: str = "") -> None:
    """バッチ受信を台帳に反映（upsert）。ingest から毎バッチ呼ぶ。

    - last_ingest_at_us  : 受信壁時計。生存の主信号。常に前進（値は常に "今"）。
    - last_batch_start_us: 受け取ったバッチの測定開始時刻。データの新しさの目安。
      通常は順送りなので単調増加。バックフィル中は一時的に巻き戻り得るが、
      これは表示上の「データ鮮度」であって生存判定には使わないので許容する。
    - batches_total      : 累積受信数。
    """
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression=(
            "SET last_ingest_at_us = :now, last_batch_start_us = :bs, "
            "last_batch_key = :key ADD batches_total :one"
        ),
        ExpressionAttributeValues={
            ":now": _dec(ingest_at_us),
            ":bs": _dec(batch_start_us),
            ":key": last_batch_key,
            ":one": Decimal(1),
        },
    )


def get_device(device_id: int) -> dict | None:
    return _table().get_item(Key={"device_id": device_id}).get("Item")


def list_devices() -> list[dict]:
    """全デバイスを device_id 昇順で返す（台数は多くて数台の想定）。"""
    items = _table().scan().get("Items", [])
    items.sort(key=lambda x: int(x.get("device_id", 0)))
    return items


def mark_offline_notified(device_id: int, at_us: int) -> None:
    """欠測通知を送ったことを記録（watchdog 専用）。"""
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="SET offline_notified_at_us = :t",
        ExpressionAttributeValues={":t": _dec(at_us)},
    )


def clear_offline(device_id: int) -> None:
    """欠測状態を解除（受信再開＝復帰時に watchdog が呼ぶ）。"""
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="REMOVE offline_notified_at_us",
    )


def mark_lag_notified(device_id: int, at_us: int) -> None:
    """データ遅延の通知を送ったことを記録（watchdog 専用）。"""
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="SET lag_notified_at_us = :t",
        ExpressionAttributeValues={":t": _dec(at_us)},
    )


def clear_lag(device_id: int) -> None:
    """データ遅延状態を解除（遅延が解消したら watchdog が呼ぶ）。"""
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="REMOVE lag_notified_at_us",
    )


def staleness_us(item: dict, now_us: int) -> int:
    """最後の受信からの経過[us]。未受信(記録なし)は now_us をそのまま返す。"""
    return now_us - int(item.get("last_ingest_at_us", 0))


def lag_us(item: dict, now_us: int) -> int | None:
    """最新データの遅延[us] = now - 最終バッチ測定開始時刻。

    受信は続いていても測定時刻が実時刻に追いつかない状態（バックフィル追いつき中や
    デバイス時計ずれ）を表す。api の /devices の lag_s と同じ定義。バッチ未記録なら None。
    """
    last_batch = int(item.get("last_batch_start_us", 0))
    if not last_batch:
        return None
    return now_us - last_batch


def evaluate(item: dict, now_us: int, offline_after_us: int,
             renotify_after_us: int) -> str | None:
    """欠測状態を判定し、取るべき通知アクションを返す（副作用なし・テスト用）。

    返り値:
      - None            … 何もしない（オンライン継続 / 欠測だが再送待ち）
      - "offline"       … 初めて欠測を検知した
      - "offline_again" … 欠測が続いており再送間隔を過ぎた（1日ごとの再送）
      - "recovery"      … 欠測通知後に受信が再開した（復帰）
    """
    age = staleness_us(item, now_us)
    notified_at = item.get("offline_notified_at_us")
    notified = notified_at is not None
    if age > offline_after_us:
        if not notified:
            return "offline"
        if now_us - int(notified_at) >= renotify_after_us:
            return "offline_again"
        return None
    if notified:
        return "recovery"
    return None


def evaluate_lag(item: dict, now_us: int, lag_after_us: int,
                 renotify_after_us: int, offline_after_us: int) -> str | None:
    """データ遅延を判定し、取るべき通知アクションを返す（副作用なし・テスト用）。

    生存(受信)とは別軸。**欠測中は offline 側の通知に任せて黙る**（online の時だけ
    見る）。offline と lag は同一サイクルで二重に鳴らない。

    返り値:
      - None           … 何もしない（遅延なし / 欠測中 / 遅延中だが再送待ち）
      - "lag"          … 初めて遅延を検知した（受信は継続中）
      - "lag_again"    … 遅延が続いており再送間隔を過ぎた
      - "lag_recovery" … 遅延通知後に遅延が解消した
    """
    lag = lag_us(item, now_us)
    if lag is None:
        return None
    if staleness_us(item, now_us) > offline_after_us:
        return None  # 欠測中は欠測通知の担当。lag は黙る
    notified_at = item.get("lag_notified_at_us")
    notified = notified_at is not None
    if lag > lag_after_us:
        if not notified:
            return "lag"
        if now_us - int(notified_at) >= renotify_after_us:
            return "lag_again"
        return None
    if notified:
        return "lag_recovery"
    return None
