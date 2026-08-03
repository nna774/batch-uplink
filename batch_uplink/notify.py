"""通知層。Notifier インターフェイスで差し替え可能にする（初期実装は Slack）。"""

from __future__ import annotations

import json
import os
import urllib.request
from abc import ABC, abstractmethod


class Notifier(ABC):
    @abstractmethod
    def notify(self, title: str, text: str, fields: dict | None = None,
               *, image_url: str | None = None, image_alt: str | None = None) -> None:
        ...


class NullNotifier(Notifier):
    def notify(self, title, text, fields=None, *, image_url=None, image_alt=None):
        pass


class SlackNotifier(Notifier):
    """Slack Incoming Webhook。channel を指定すると payload に載せる
    （レガシーwebhookでは送信先を上書きできる。アプリ版webhookでは無視される点に注意）。

    image_url を渡すと image ブロックで画像を添付する。Incoming Webhook は
    ファイルアップロードができないので、Slack が取得できる公開URL（例: Gyazo）を渡す。"""

    def __init__(self, webhook_url: str, channel: str = ""):
        self.webhook_url = webhook_url
        self.channel = channel

    def notify(self, title, text, fields=None, *, image_url=None, image_alt=None):
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        ]
        if fields:
            blocks.append({
                "type": "section",
                "fields": [{"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in fields.items()],
            })
        if image_url:
            blocks.append({
                "type": "image",
                "image_url": image_url,
                "alt_text": image_alt or title,
            })
        body = {"text": title, "blocks": blocks}
        if self.channel:
            body["channel"] = self.channel
        payload = json.dumps(body).encode()
        req = urllib.request.Request(
            self.webhook_url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()


def event_url(eid: str) -> str:
    """ダッシュボードの該当イベントページの生URL。NAMZ_DASHBOARD_URL 未設定なら空文字。"""
    base = os.environ.get("NAMZ_DASHBOARD_URL", "").rstrip("/")
    return f"{base}/#event/{eid}" if base else ""


def event_field(eid: str) -> str:
    """イベントIDを、ダッシュボードの該当ページへの Slack mrkdwn リンクにする。
    NAMZ_DASHBOARD_URL 未設定ならID文字列のまま返す。"""
    url = event_url(eid)
    return f"<{url}|{eid}>" if url else eid


def from_env() -> Notifier:
    """環境変数から Notifier を構築。将来 Discord 等を足すならここに分岐を追加。"""
    kind = os.environ.get("NAMZ_NOTIFIER", "slack").lower()
    if kind == "slack":
        url = os.environ.get("NAMZ_SLACK_WEBHOOK_URL")
        channel = os.environ.get("NAMZ_SLACK_CHANNEL", "")
        return SlackNotifier(url, channel) if url else NullNotifier()
    return NullNotifier()
