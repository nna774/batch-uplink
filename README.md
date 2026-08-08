# batch-uplink

**測って貯めて送る**基盤。センサを繋いだ小さな機械が固定長のレコードを溜め、
署名を付けてクラウドへ投げ、落ちている間はローカルに退避して復旧後に追いつく——
という経路の、**測る対象に依存しない部分だけ**を集めたもの。

[NamazuHaUrokoGaNai](https://github.com/nna774/NamazuHaUrokoGaNai)（家庭用地震計）で
実稼働していたコードを切り出したもので、[Electabuzz](https://github.com/nna774/Electabuzz)
（商用電力の周波数モニタ）と共有する。

**ワイヤ形式は含まない。** magic もバージョンもフィールド定義も、載せる側が自分で持つ。

## 何が入っているか

### C++ (ESP32 / Arduino / PlatformIO)

| | |
|---|---|
| `Batch` | 送信バッファ。`[ヘッダ領域][固定長レコード × N][tail]` の3領域だけを知る |
| `Uploader` | 送信キュー・LittleFS退避・指数バックオフ・バックフィル・HMAC署名POST。既定は退避先も一杯なら諦めて積む（データを捨てない）が、`dropOldestWhenFull=true` を渡すと満杯時に最古から捨てる。`oldestQueuedStartUs()` で最古の未送信バッチの開始時刻が取れる（呼び出し側で現在時刻と比べれば遅延表示に使える）。`watchResponseHeaders`（配列）を渡すとバッチPOST成功時にそれらのレスポンスヘッダの値を保持し `lastResponseHeaderValue(name)` で読める（既定nullptr/0で従来通り何も読まない。最大`kMaxWatchedHeaders`件まで。ヘッダの意味づけは呼び出し側の責務）。`flushToSpill()` はRAMキューを即座に全部LittleFSへ退避する（OTA更新など、この後RAMの内容が失われうる場面向け）。`caCert`（PEM文字列）を渡すとingest/alert接続のTLS検証に`setCACert()`を使う（既定nullptrでは従来通り`setInsecure()`にフォールバックするので、証明書を用意でき次第渡すのを推奨する） |
| `HmacSha256` | 署名。ボディのバイト列にしか依存しない |
| `TimeSync` | NTP(smooth同期。大きなずれは一度だけstep) |

**依存ライブラリは無い**（Arduino コアと ESP32 の標準機能のみ）。

### Python (AWS Lambda)

| | |
|---|---|
| `auth` | HMAC-SHA256 検証。`NAMZ_HMAC_SECRET_<id>` で個体別鍵 |
| `devices` | デバイス生存台帳（DynamoDB）。受信壁時計で生存を、測定時刻との差で遅延を見る |
| `notify` | 通知層（Slack Incoming Webhook / Null。差し替え可能） |
| `s3util` | S3キー組み立て。**20桁ゼロ埋めで辞書順＝時系列順**になる命名 |

**stdlib + boto3 のみ**。numpy に依存しないので platform wheel の問題が起きない。

## 使いかた

### C++

`platformio.ini` に**タグで**足す。

```ini
lib_deps =
    https://github.com/nna774/batch-uplink.git#v1.0.0
```

`Batch` はワイヤ形式を知らないので、ヘッダは**呼び出し側が書く**。

```cpp
Batch* b = new Batch(capacityRecords, recordBytes, headerBytes, tailCapacity);
b->begin(startUs);
while (...) b->addRecord(&rec, sizeof(rec));

// レコードを積み終えてから。この時点で recordCount() も records() も確定している
// （レコード数や payload の CRC をヘッダに書けるのはここ）。
myFormat::fillHeader(b->headerPtr(), b->recordCount(), b->records(), b->recordsSize());
uploader.enqueue(b);   // 以降 bytes()/size() がそのまま送られる
```

`tail` は「レコード列の後ろに置く可変長領域」。TLV などの枠組みは呼び出し側が組む。
`addRecord()` のたびに1レコード分だけ後ろへ押し出すので、**バッファはいつ見ても
完結したバイト列**になっている。確定用の呼び出しを忘れる、という失敗の形が無い。

### Python

```bash
pip install "git+https://github.com/nna774/batch-uplink@v1.0.0"
```

```python
from batch_uplink import auth, devices, notify, s3util

auth.verify(device_id, body, signature_hex)
key = s3util.raw_key(device_id, batch_start_us)
devices.record_batch(device_id, batch_start_us, ingest_at_us, key)
notify.from_env().notify("title", "text")
```

Lambda の zip へ同梱する場合、**pip の呼び出しを numpy 等と分けること**。
`--platform` は `--only-binary=:all:` を要求するが `git+` はソースツリーなので両立しない。

```bash
pip install --target "$stage" --platform manylinux2014_x86_64 --only-binary=:all: \
  --implementation cp --python-version 3.12 --abi cp312 numpy
pip install --target "$stage" --no-deps "git+https://github.com/nna774/batch-uplink@v1.0.0"
```

## タグで pin しろ。ブランチ追従にするな

`#main` や `@main` にすると、片方のプロジェクトのために入れた変更が、
**もう片方の次回ビルドで黙って混入する。**「何も変えていないのに再ビルドで壊れる」
という最悪の壊れ方をするので、必ずタグを指すこと。

## 設定（環境変数）

| | |
|---|---|
| `NAMZ_HMAC_SECRET_<id>` / `NAMZ_HMAC_SECRET` | デバイスの共有鍵 |
| `NAMZ_DEVICES_TABLE` | 生存台帳の DynamoDB テーブル名 |
| `NAMZ_NOTIFIER` | `slack` / それ以外は無通知 |
| `NAMZ_SLACK_WEBHOOK_URL` / `NAMZ_SLACK_CHANNEL` | Slack 通知先 |
| `NAMZ_DASHBOARD_URL` | 通知に載せるリンクの基点 |

`NAMZ_` という接頭辞は切り出し元の名残で、**プロジェクト名とは無関係**。
別テーブル・別チャンネルを指すだけで別スタックとして動くので、
使う側は自分のスタックの値を渡せばよい（改名すると既存の稼働系が壊れるので据え置いている）。

## テスト

```bash
test/run.sh
```

`Batch` は Arduino に依存しないのでホストの `g++` で走る。

## License

MIT. [LICENSE](LICENSE) を参照。
