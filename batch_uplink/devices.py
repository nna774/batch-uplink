"""デバイスの生存台帳（DynamoDB namazu-devices）。

ingest がバッチ受信ごとに1項目を upsert し、watchdog が定期起動して
「最後の受信からの経過」を見て欠測を判定する。events テーブルとは別に、
「今このデバイスが喋っているか」の単一の真実をここに持つ。

生存の主信号は **last_ingest_at_us（ingest が受信した壁時計時刻）**。
firmware はWiFi断のあとバックフィルするので、測定時刻(last_batch_start_us)だけでは
「復旧直後で追いつき中」と「本当に沈黙」が区別できない。受信壁時計で生存を、
last_batch_start_us との差でデータ遅延を、別々に見る。

last_batch_start_us は単調増加（条件付き更新で巻き戻りを拒否）。送信側がRAM
キュー（最新）を優先し退避ファイル（古い）を後回しにする実装（v2.11.0）のため、
バックフィル中は新旧のバッチが交互に届く。ここが無条件上書きだと、退避ファイル
受信のたびにこの値が過去へ戻り、遅延判定(evaluate_lag)とダッシュボードの
「データ鮮度」表示の両方が誤って暴れる。

欠測通知の状態(offline_notified_at_us)とデータ遅延通知の状態(lag_notified_at_us)は
watchdog だけが書く。ingest が書く受信系フィールドとは互いに素なので、両者を
UpdateItem で分けて更新すれば read-modify-write の競合は起きない。

再起動要求(pending_restart_requested_at_us)は手元CLIが書き、ingest が次回バッチ
受信時に読んでレスポンスへ反映した直後に消す一回性のフィールド。これも受信系
フィールドとは互いに素。
"""

from __future__ import annotations

import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

_table_cache = None


def _table():
    global _table_cache
    if _table_cache is None:
        _table_cache = boto3.resource("dynamodb").Table(os.environ["NAMZ_DEVICES_TABLE"])
    return _table_cache


def _dec(v) -> Decimal:
    return Decimal(str(int(v)))


def record_batch(device_id: int, batch_start_us: int, ingest_at_us: int,
                 last_batch_key: str = "", fw_version: str = "",
                 track_prev_key: bool = False) -> None:
    """バッチ受信を台帳に反映（upsert）。ingest から毎バッチ呼ぶ。

    - last_ingest_at_us  : 受信壁時計。生存の主信号。常に前進（値は常に "今"）。
    - last_batch_start_us: 受け取ったバッチの測定開始時刻。データの新しさの目安。
      条件付き更新で単調増加を強制する（下記）。
    - batches_total      : 累積受信数。
    - fw_version         : 呼び出し側がリクエストヘッダ等から読んだ、送信元が
      いま動かしているビルド版数（空文字なら書かない。ヘッダを送らない
      呼び出し側や未対応の旧ファームとの互換のため）。
    - track_prev_key     : Trueなら、上書き前の`last_batch_key`を`prev_batch_key`へ
      退避してから`last_batch_key`を新値で上書きする。DynamoDBの`UpdateExpression`は
      同一呼び出し内のSET節の右辺が全て更新前の値を参照するため、追加のGetItem無しに
      アトミックに「1つ前のバッチキー」を残せる。呼び出し側がバッチ境界をまたぐ
      連続性のために直前バッチを引きたい用途向け（Electabuzz detect固有の要件。
      Namazuは使わないのでデフォルトFalse——常時書くと使わない側の属性が増えるだけ）。
      台帳が無い(初回)場合は`last_batch_key`属性自体が無く参照できないので、
      `if_not_exists`で空文字にフォールバックする（「直前バッチ無し」を正しく表す）。
    """
    set_parts = [
        "last_ingest_at_us = :now",
        "last_batch_key = :key",
    ]
    values = {
        ":now": _dec(ingest_at_us),
        ":key": last_batch_key,
        ":one": Decimal(1),
    }
    if track_prev_key:
        set_parts.append("prev_batch_key = if_not_exists(last_batch_key, :empty)")
        values[":empty"] = ""
    if fw_version:
        set_parts.append("fw_version = :fw")
        values[":fw"] = fw_version
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="SET " + ", ".join(set_parts) + " ADD batches_total :one",
        ExpressionAttributeValues=values,
    )
    # last_batch_start_us は別のUpdateItemで条件付き更新する。上のSETに混ぜて
    # 全体をConditionExpressionで守ると、退避ファイル（古いバッチ）受信時に
    # last_ingest_at_us・batches_totalまで一緒に更新拒否されてしまう
    # （生存台帳が受信を見逃した扱いになる）ため、必ず分ける。
    try:
        _table().update_item(
            Key={"device_id": device_id},
            UpdateExpression="SET last_batch_start_us = :bs",
            ConditionExpression=(
                "attribute_not_exists(last_batch_start_us) OR last_batch_start_us < :bs"
            ),
            ExpressionAttributeValues={":bs": _dec(batch_start_us)},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def get_device(device_id: int) -> dict | None:
    return _table().get_item(Key={"device_id": device_id}).get("Item")


def record_batch_fragments(item: dict | None, batch_start_us: int, ingest_at_us: int,
                           last_batch_key: str = "", fw_version: str = "") -> list[tuple[str, dict]]:
    """record_batch()と同じ書き込み内容を、実行せず断片のリストとして返す。

    呼び出し側が他の関心事（自前のローカル属性等）と合流させて1回のupdate_itemに
    まとめたい場合向け。record_batch()自身は「毎バッチ即executeする」関数のままなので、
    こちらは別関数として追加した（既存の呼び出し元・track_prev_keyを使う呼び出し元は
    record_batch()を変更なく使い続けられる）。

    record_batch()はlast_batch_start_usの単調増加をConditionExpressionで守る
    （そのため内部でupdate_itemを2回に分けている）。こちらは呼び出し側が既に
    GetItemで取得済みの現在の項目を`item`（未登録デバイスならNone）として受け取り、
    その場でPython側で比較することで、追加のDynamoDB呼び出し無しに単調性を保つ。
    ConditionExpressionによる「同時に来た書き込み同士のアトミック性」までは
    提供しない——呼び出し元が既にGetItemしている前提であり、そのGetItemと
    このupdate_itemの間に他の書き込みが割り込む可能性を許容できる場合にのみ使うこと
    （デバイス1台からのリクエストが事実上直列な用途を想定）。

    track_prev_keyは持たない（Electabuzz detect固有の機能。そちらは従来通り
    record_batch()を使う）。
    """
    set_parts = [
        "last_ingest_at_us = :now",
        "last_batch_key = :key",
    ]
    values = {
        ":now": _dec(ingest_at_us),
        ":key": last_batch_key,
    }
    if fw_version:
        set_parts.append("fw_version = :fw")
        values[":fw"] = fw_version

    prev_batch_start = int(item.get("last_batch_start_us", 0)) if item else 0
    if batch_start_us > prev_batch_start:
        set_parts.append("last_batch_start_us = :bs")
        values[":bs"] = _dec(batch_start_us)

    return [
        ("SET " + ", ".join(set_parts), values),
        ("ADD batches_total :one", {":one": Decimal(1)}),
    ]


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


def request_restart(device_id: int, at_us: int) -> None:
    """再起動要求を立てる（手元CLI専用）。ingest が次回バッチ受信時に読んで
    レスポンスへ反映し、反映した直後に clear_restart_request() で消す想定
    （一度伝えたら消える一回性の要求）。
    """
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="SET pending_restart_requested_at_us = :t",
        ExpressionAttributeValues={":t": _dec(at_us)},
    )


def clear_restart_request(device_id: int) -> None:
    """再起動要求を消す（ingest がレスポンスへ反映した直後に呼ぶ）。"""
    _table().update_item(
        Key={"device_id": device_id},
        UpdateExpression="REMOVE pending_restart_requested_at_us",
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
