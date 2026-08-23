"""devices.record_batch_fragments() の純粋関数テスト（DynamoDBに触らない）。"""

import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from batch_uplink import devices


def _values(fragments):
    merged = {}
    for _, values in fragments:
        merged.update(values)
    return merged


def test_first_batch_no_prior_item_writes_last_batch_start_us():
    frags = devices.record_batch_fragments(None, 1_000_000, 2_000_000, last_batch_key="k")
    set_expr, _ = frags[0]
    assert "last_batch_start_us = :bs" in set_expr
    assert _values(frags)[":bs"] == Decimal(1_000_000)


def test_newer_batch_start_us_included():
    item = {"last_batch_start_us": Decimal(1_000_000)}
    frags = devices.record_batch_fragments(item, 2_000_000, 3_000_000, last_batch_key="k")
    set_expr, _ = frags[0]
    assert "last_batch_start_us = :bs" in set_expr
    assert _values(frags)[":bs"] == Decimal(2_000_000)


def test_older_batch_start_us_omitted():
    # バックフィルで過去のバッチが後から届いた場合、last_batch_start_usは
    # 巻き戻さない（record_batch()のConditionExpressionと同じ意図）。
    item = {"last_batch_start_us": Decimal(2_000_000)}
    frags = devices.record_batch_fragments(item, 1_000_000, 3_000_000, last_batch_key="k")
    set_expr, _ = frags[0]
    assert "last_batch_start_us" not in set_expr


def test_equal_batch_start_us_omitted():
    item = {"last_batch_start_us": Decimal(1_000_000)}
    frags = devices.record_batch_fragments(item, 1_000_000, 3_000_000, last_batch_key="k")
    set_expr, _ = frags[0]
    assert "last_batch_start_us" not in set_expr


def test_always_bumps_batches_total():
    frags = devices.record_batch_fragments(None, 1_000_000, 2_000_000, last_batch_key="k")
    add_expr, add_values = frags[1]
    assert add_expr == "ADD batches_total :one"
    assert add_values == {":one": Decimal(1)}


def test_fw_version_written_when_given():
    frags = devices.record_batch_fragments(None, 1_000_000, 2_000_000, fw_version="1.2.3")
    set_expr, values = frags[0]
    assert "fw_version = :fw" in set_expr
    assert values[":fw"] == "1.2.3"


def test_fw_version_omitted_when_empty():
    frags = devices.record_batch_fragments(None, 1_000_000, 2_000_000)
    set_expr, _ = frags[0]
    assert "fw_version" not in set_expr
