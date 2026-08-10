#include "Uploader.h"

#include <Arduino.h>
#include <HTTPClient.h>
#include <LittleFS.h>
#include <WiFiClientSecure.h>

#include <esp_heap_caps.h>

#include <cstdio>
#include <cstring>

#include "HmacSha256.h"

#include "esp_timer.h"

static constexpr uint32_t kBackoffStartMs = 1000;
static constexpr uint32_t kBackoffMaxMs = 60000;

// WiFiClientSecureの既定は120000ms。呼び出し側(uploaderTask)の10秒task
// watchdogより長いため、ネット瞬断でTLSハンドシェイクが詰まるとハンドシェイクの
// タイムアウトより先にWDTが強制パニック再起動させてしまう（実機で確認、
// NamazuHaUrokoGaNai docs/log/2026-08-09-device1-outage-reboot-loop-and-data-loss.md）。
// パニックは`flushToSpill()`を経由しないためRAM上のバッチを毎回失う。ここを
// WDTの10秒未満に縮め、詰まった時はライブラリ自身が先に諦めて接続失敗を返す
// ようにし、既存の指数バックオフによる通常の再試行に落とす。
//
// HTTPClientの`_connectTimeout`/`_tcpTimeout`は既定5000ms
// (`HTTPCLIENT_DEFAULT_TCP_TIMEOUT`、TCP接続確立と送受信を別々にカバー)なので
// 既に10秒未満で安全。唯一守られていないのがこのハンドシェイクタイムアウトだった。
// 「TCP接続に近い5秒＋ハンドシェイクここ」が直列に積み上がっても10秒に収まるよう、
// design.mdの当初案(8000ms)よりさらに切り詰めて4000msにする。
static constexpr uint32_t kHandshakeTimeoutMs = 4000;

// postBatch()の各段階(http_.begin()/http_.POST()前後)にタイムスタンプ付きログを
// 出す。既定オフ(呼び出し側のbuild_flagsで-DUPLINK_DEBUG_TIMINGを渡した時だけ
// 有効)、コンパイル時に消えるのでオフ時はコストゼロ。「詰まって戻ってこない」
// 系の障害調査用——begin()とPOST()のどちらの前で最後のログが止まったかを見れば、
// TCP接続確立とTLSハンドシェイク+送受信のどちらでブロックしているか切り分けられる。
#ifdef UPLINK_DEBUG_TIMING
#define UPLINK_DEBUG_LOG(...) Serial.printf(__VA_ARGS__)
#else
#define UPLINK_DEBUG_LOG(...) \
  do {                        \
  } while (0)
#endif

// nullptr終端の配列(argv方式)の要素数を数える。arr自体がnullptrなら0。
static size_t countSentinelArray(const char* const* arr) {
  if (!arr) return 0;
  size_t n = 0;
  while (arr[n]) ++n;
  return n;
}

namespace {
// mutex_のRAIIラッパー。スコープを抜けると必ず解放する（早期returnで解放し
// 忘れるミスを防ぐ）。非再入——同じタスクが二重に取るとデッドロックするので、
// ロック区間の中でロックを取る別のpublicメソッドを呼ばないこと。
class UploaderLock {
 public:
  explicit UploaderLock(SemaphoreHandle_t sem) : sem_(sem) {
    xSemaphoreTake(sem_, portMAX_DELAY);
  }
  ~UploaderLock() { xSemaphoreGive(sem_); }
  UploaderLock(const UploaderLock&) = delete;
  UploaderLock& operator=(const UploaderLock&) = delete;

 private:
  SemaphoreHandle_t sem_;
};
}  // namespace

Uploader::Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
                   uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir,
                   bool dropOldestWhenFull, const char* const* watchResponseHeaders,
                   const char* const* extraRequestHeaderNames,
                   const char* const* extraRequestHeaderValues, const char* caCert,
                   size_t maxSpillReadBytes, bool discardSpillOn400)
    : ingestUrl_(ingestUrl), alertUrl_(alertUrl), hmacSecret_(hmacSecret),
      deviceId_(deviceId), maxRam_(maxRamBatches), spillDir_(spillDir),
      dropOldestWhenFull_(dropOldestWhenFull), watchResponseHeaders_(watchResponseHeaders),
      watchResponseHeaderCount_(countSentinelArray(watchResponseHeaders)),
      extraRequestHeaderNames_(extraRequestHeaderNames),
      extraRequestHeaderValues_(extraRequestHeaderValues),
      extraRequestHeaderCount_(countSentinelArray(extraRequestHeaderNames)),
      caCert_(caCert), discardSpillOn400_(discardSpillOn400),
      lastResponseHeaderValues_(watchResponseHeaderCount_),
      maxSpillReadBytes_(maxSpillReadBytes) {}

Uploader::~Uploader() {
  free(spillReadBuf_);
  if (mutex_) vSemaphoreDelete(mutex_);
}

bool Uploader::begin() {
  if (caCert_) {
    client_.setCACert(caCert_);
  } else {
    client_.setInsecure();  // caCert未指定時の後方互換フォールバック（検証なし）
  }
  client_.setHandshakeTimeout(kHandshakeTimeoutMs);
  if (!LittleFS.begin(true)) {
    Serial.println("[uploader] LittleFS mount failed");
    return false;
  }
  if (!LittleFS.exists(spillDir_)) LittleFS.mkdir(spillDir_);
  // 起動時に退避ファイル数を数える
  File dir = LittleFS.open(spillDir_);
  spillCount_ = 0;
  for (File f = dir.openNextFile(); f; f = dir.openNextFile()) {
    if (!f.isDirectory()) ++spillCount_;
  }
  Serial.printf("[uploader] spill files on boot: %u\n", (unsigned)spillCount_);
  if (maxSpillReadBytes_ > 0) {
    spillReadBuf_ = (uint8_t*)malloc(maxSpillReadBytes_);
    if (!spillReadBuf_) {
      Serial.printf(
          "[uploader] spill read buffer malloc failed (%u B), falling back to "
          "per-read malloc/free\n",
          (unsigned)maxSpillReadBytes_);
    }
  }
  return true;
}

void Uploader::enqueue(Batch* batch) {
  if (!batch || !batch->valid()) {
    UPLINK_DEBUG_LOG("[uplink-debug] enqueue: invalid batch, dropped (batch=%p)\n",
                      (void*)batch);
    delete batch;
    return;
  }
  static uint32_t sEnqueueCount = 0;
  UploaderLock lock(mutex_);
  UPLINK_DEBUG_LOG(
      "[uplink-debug] enqueue #%u len=%u ram=%u spill=%u t=%lld\n",
      (unsigned)++sEnqueueCount, (unsigned)batch->size(), (unsigned)ram_.size(),
      (unsigned)spillCount_, (long long)esp_timer_get_time());
  while (ram_.size() >= maxRam_) {
    if (spillOldestRam()) continue;
    if (!dropOldestWhenFull_) break;  // 従来通り: 諦めて積む（メモリ許す範囲）
    // LittleFSも一杯: 退避済みの最古を1本消して空きを作り、退避を再試行する。
    // 退避ファイルが無ければ(=RAMキューの先頭が一番古い)、その先頭を諦める。
    if (evictOldestSpill()) continue;
    Batch* oldest = ram_.front();
    ram_.pop_front();
    delete oldest;
    ++droppedCount_;
    Serial.println("[uploader] spill full: dropped oldest queued batch");
  }
  ram_.push_back(batch);
}

bool Uploader::pump() {
  // 送信が連続して詰まり、バックオフが上限(kBackoffMaxMs)に張り付いている間は、
  // ram_に居座っている未送信バッチをspillへ逃がしておく。
  //
  // spillへの退避は本来enqueue()がRAM満杯を検知した時の副作用としてしか起きない
  // 設計だが、送信が詰まったままRAMも満杯だと、呼び出し側(NamazuHaUrokoGaNai)の
  // Batchバッファプールも同時に枯渇し、新しいバッチを作れなくなる。すると
  // enqueue()が二度と呼ばれなくなり、退避のトリガー自体が永遠に来ない飢餓状態に
  // 陥る——実機のWiFi遮断試験で確認した(newBatch()が延々失敗し続けるだけで
  // 何時間経っても回復しない、NamazuHaUrokoGaNai
  // docs/log/2026-08-11-uploader-task-split-realhw-check.md)。
  // WiFi接続状態に関わらず呼んでよい(flushToSpill()はLittleFSのみで完結する)。
  if (backoffMs_ >= kBackoffMaxMs) {
    flushToSpill();
  }
  if (WiFi.status() != WL_CONNECTED) {
    UPLINK_DEBUG_LOG("[uplink-debug] pump: wifi not connected (status=%d) t=%lld\n",
                      (int)WiFi.status(), (long long)esp_timer_get_time());
    return false;
  }
  uint32_t now = millis();
  if (now < nextAttemptMs_) {
    UPLINK_DEBUG_LOG("[uplink-debug] pump: backoff active (now=%u next=%u) t=%lld\n",
                      (unsigned)now, (unsigned)nextAttemptMs_, (long long)esp_timer_get_time());
    return false;
  }

  // 1) RAMキューの古い順を優先で送る。
  //
  // 元は退避ファイル（spill）を常に優先していたが、大量backlog（spillが
  // 尽きない）が続く間RAMキューが一本も送信されず`ram_`が上限で慢性的に
  // 張り付き、呼び出し側(NamazuHaUrokoGaNai)のBatchバッファプール枯渇→
  // 一般malloc()フォールバックによるヒープ断片化の根本原因になっていた
  // （NamazuHaUrokoGaNai docs/log/2026-08-11-batch-pool-fallback-heap-corruption.md）。
  // RAMキューを毎回先に払うことで`ram_`を空/低占有に保ち、この詰まりを防ぐ。
  // 代償として、大量backlog復旧中はspillの吐き出しが新規バッチに割り込まれ
  // やすくなり遅くなる（データを失うわけではない、待ち順のトレードオフ）。
  //
  // ram_からの取り出しは「popしてから送る」（front()を覗いたまま送るのでは
  // ない）。覗いたまま送ると、送信中（postBatch()はロック外・数秒〜数十秒
  // かかりうる）にもう一方のタスクのenqueue()がRAM満杯を検知して同じ先頭要素を
  // spillOldestRam()で退避・deleteしてしまう競合windowができる
  // （NamazuHaUrokoGaNai docs/log/2026-08-11-uploader-task-split-design.md）。
  // popして所有権をローカルへ移しておけば、送信中はram_から見えなくなるので
  // enqueue()側のオーバーフロー処理と衝突しない。失敗時はpush_front()で戻す。
  Batch* ramBatch = nullptr;
  {
    UploaderLock lock(mutex_);
    if (!ram_.empty()) {
      ramBatch = ram_.front();
      ram_.pop_front();
    }
  }
  if (ramBatch) {
    UPLINK_DEBUG_LOG("[uplink-debug] pump: ram branch t=%lld\n",
                      (long long)esp_timer_get_time());
    bool ok = postBatch(ramBatch->bytes(), ramBatch->size());
    UPLINK_DEBUG_LOG("[uplink-debug] pump: postBatch(ram) -> %d t=%lld\n", (int)ok,
                      (long long)esp_timer_get_time());
    if (ok) {
      delete ramBatch;
      backoffMs_ = 0;
      nextAttemptMs_ = now;
      return true;
    }
    {
      UploaderLock lock(mutex_);
      ram_.push_front(ramBatch);
    }
    backoffMs_ = backoffMs_ ? min(backoffMs_ * 2, kBackoffMaxMs) : kBackoffStartMs;
    nextAttemptMs_ = now + backoffMs_;
    return false;
  }

  // 2) 退避ファイル（常に古い）。パスの決定・オープン・読み込みまでを
  // 1回のロックでまとめて行い、「pump()がパスを決めた直後にenqueue()側の
  // evictOldestSpill()が同じファイルを消す」隙間を作らない。
  char path[64] = {0};
  uint64_t startUs;
  uint8_t* body = nullptr;
  size_t len = 0;
  bool usingFixedBuf = false;
  bool readOk = false;
  {
    UploaderLock lock(mutex_);
    if (spillCount_ > 0 && loadOldestSpillPath(path, sizeof(path), startUs)) {
      // spillCount_>0なのにloadOldestSpillPath()が見つけられない場合だけを
      // 「FAILED」とログしたい（spillCount_==0で退避ファイルが単に無い、という
      // 普段の状態と、カウンタと実ファイルがズレている異常を混同しないため）。
      // &&の短絡評価に頼って両者を1本のログにまとめると、平常時(spillCount_==0)
      // 毎回「FAILED」が出て紛らわしくなる——実際に踏んだ(pump()が空振りする
      // たびに出続けていた)ので、下のelse節で明示的に分けた。
      UPLINK_DEBUG_LOG("[uplink-debug] pump: loadOldestSpillPath -> %s t=%lld\n", path,
                        (long long)esp_timer_get_time());
      // LittleFS.open()はArduino-ESP32のFS実装内部でstd::make_shared<VFSFileImpl>を
      // 使っており、ヒープが極端に逼迫した状態でそのnew()が失敗すると
      // std::bad_alloc相当の例外を投げる。ここをcatchせずに素通しすると
      // std::terminate()->abort()で機体ごと再起動する（実機で確認）。
      // ヒープ枯渇自体はここで直せる問題ではないので、例外は「開けなかった」
      // という通常の失敗として扱い、既存のバックオフ経路に流す。
      File f;
      try {
        f = LittleFS.open(path, "r");
      } catch (...) {
        Serial.printf("[uploader] LittleFS.open(%s) threw (heap exhausted?) t=%lld\n", path,
                      (long long)esp_timer_get_time());
      }
      if (f) {
        len = f.size();
        UPLINK_DEBUG_LOG("[uplink-debug] pump: opened %s len=%u t=%lld\n", path, (unsigned)len,
                          (long long)esp_timer_get_time());
        // 固定バッファ(begin()で確保済み)に収まるならそちらを使い、都度の
        // malloc/freeを避ける（コンストラクタのmaxSpillReadBytesコメント参照）。
        // 収まらない/未確保なら従来通り都度malloc()する。
        usingFixedBuf = spillReadBuf_ && len <= maxSpillReadBytes_;
        body = usingFixedBuf ? spillReadBuf_ : (uint8_t*)malloc(len);
        int readLen = body ? f.read(body, len) : -1;
        f.close();
        // ESP.getMaxAllocHeap()はMALLOC_CAP_INTERNAL基準でmalloc()の実際の基準
        // (MALLOC_CAP_8BIT)を過大報告することがある(NamazuHaUrokoGaNai PR #54で
        // 実測済み)。ここのmalloc()失敗の原因切り分けには8BIT側の実測値が要る。
        UPLINK_DEBUG_LOG(
            "[uplink-debug] pump: read -> %d (body=%p) heap_free=%u "
            "maxblock_internal=%u maxblock_8bit=%u t=%lld\n",
            readLen, (void*)body, (unsigned)ESP.getFreeHeap(), (unsigned)ESP.getMaxAllocHeap(),
            (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT),
            (long long)esp_timer_get_time());
        readOk = body && readLen == (int)len;
        if (!readOk && body && !usingFixedBuf) {
          free(body);
          body = nullptr;
        }
      } else {
        UPLINK_DEBUG_LOG("[uplink-debug] pump: LittleFS.open(%s) FAILED t=%lld\n", path,
                          (long long)esp_timer_get_time());
      }
    } else if (spillCount_ > 0) {
      // spillCount_はある(>0)のに実ファイルが見つからない——カウンタと実体が
      // ズレている異常なので、平常時の「spillが単に空」と区別してログする。
      UPLINK_DEBUG_LOG("[uplink-debug] pump: loadOldestSpillPath FAILED (spillCount_=%u) t=%lld\n",
                        (unsigned)spillCount_, (long long)esp_timer_get_time());
    }
    // spillCount_==0の場合はここで何もログしない(退避ファイルが単に無いだけの
    // 平常状態、pump()呼び出しのたびに出ると紛らわしい)。
  }
  if (path[0] == '\0') {
    // 送るものが無い: 使い回していた接続を明示的に閉じる。繋ぎっぱなしにすると
    // 次にバッチが来るまでTLSセッション分のRAMを無駄に予約し続けてしまう
    // （使い回しの狙いはバックフィルの連続POST中のハンドシェイク省略であって、
    // 常時接続の維持ではない）。
    UPLINK_DEBUG_LOG("[uplink-debug] pump: nothing to send, closing idle connection t=%lld\n",
                      (long long)esp_timer_get_time());
    closeConnection();
    return false;
  }
  if (!readOk) {
    // パスは決まったが開けなかった/読めなかった -> バックオフ
    backoffMs_ = backoffMs_ ? min(backoffMs_ * 2, kBackoffMaxMs) : kBackoffStartMs;
    nextAttemptMs_ = now + backoffMs_;
    return false;
  }
  bool ok = postBatch(body, len);
  UPLINK_DEBUG_LOG("[uplink-debug] pump: postBatch(spill) -> %d t=%lld\n", (int)ok,
                    (long long)esp_timer_get_time());
  if (!usingFixedBuf) free(body);
  if (ok) {
    UploaderLock lock(mutex_);
    removeSpill(path);
    backoffMs_ = 0;
    nextAttemptMs_ = now;
    return true;
  }
  // ingestが「このボディは壊れている」とHTTP 400で明確に判定した場合に限り、
  // この退避ファイルは何度リトライしても成功しないとみなして諦める
  // （Uploader.h冒頭コメント「不変条件 例外2」）。403やコード無しの接続失敗は
  // ここに来ない(lastPostCode_はHTTP応答が実際に返った時だけ更新される)ので、
  // それらは従来通り下のバックオフへ流れて再試行される。
  if (discardSpillOn400_ && lastPostCode_ == 400) {
    Serial.printf(
        "[uploader] spill %s rejected with HTTP 400 (malformed body, discarding) t=%lld\n", path,
        (long long)esp_timer_get_time());
    UploaderLock lock(mutex_);
    removeSpill(path);
    backoffMs_ = 0;
    nextAttemptMs_ = now;
    return false;
  }
  backoffMs_ = backoffMs_ ? min(backoffMs_ * 2, kBackoffMaxMs) : kBackoffStartMs;
  nextAttemptMs_ = now + backoffMs_;
  return false;
}

bool Uploader::postBatch(const uint8_t* body, size_t len) {
  UPLINK_DEBUG_LOG(
      "[uplink-debug] postBatch begin len=%u wifi=%d client_connected=%d heap_free=%u "
      "maxblock=%u t=%lld\n",
      (unsigned)len, (int)WiFi.status(), (int)client_.connected(), (unsigned)ESP.getFreeHeap(),
      (unsigned)ESP.getMaxAllocHeap(), (long long)esp_timer_get_time());
  // client_/http_ はUploaderの寿命だけ生きるメンバ（バックフィル中の連続POSTで
  // TLSハンドシェイクを省略するための使い回し。docs参照）。setReuse(true)で
  // 「切断時に使い回せるなら閉じない」をHTTPClientへ伝える。前回の接続が
  // まだ生きていればHTTPClient::connect()が自動でハンドシェイクを省略する。
  bool began = http_.begin(client_, ingestUrl_);
  UPLINK_DEBUG_LOG("[uplink-debug] http_.begin() -> %d t=%lld\n", (int)began,
                    (long long)esp_timer_get_time());
  if (!began) return false;
  http_.setReuse(true);
  if (watchResponseHeaderCount_ > 0) {
    // HTTPClient::collectHeaders は const-correct でない(const char* headerKeys[])。
    // 中身を書き換えないことは分かっているのでconst_castで橋渡しする。
    http_.collectHeaders(const_cast<const char**>(watchResponseHeaders_),
                         watchResponseHeaderCount_);
  }
  http_.addHeader("Content-Type", "application/octet-stream");
  http_.addHeader("X-Namz-Device", String(deviceId_));
  http_.addHeader("X-Namz-Signature", hmacSha256Hex(hmacSecret_, body, len).c_str());
  for (size_t i = 0; i < extraRequestHeaderCount_; ++i) {
    http_.addHeader(extraRequestHeaderNames_[i], extraRequestHeaderValues_[i]);
  }
  UPLINK_DEBUG_LOG("[uplink-debug] http_.POST() start len=%u t=%lld\n", (unsigned)len,
                    (long long)esp_timer_get_time());
  int code = http_.POST(const_cast<uint8_t*>(body), len);
  UPLINK_DEBUG_LOG("[uplink-debug] http_.POST() -> code=%d t=%lld\n", code,
                    (long long)esp_timer_get_time());
  // HTTPClientは接続確立自体に失敗すると負値(HTTPC_ERROR_*)を返す。実際に
  // サーバから応答コードが返った時だけここに載るので、discardSpillOn400_の
  // 「サーバが明確に拒否した」判定はこの区別に乗っかれる。
  lastPostCode_ = code;
  bool ok = (code >= 200 && code < 300);
  if (ok) {
    for (size_t i = 0; i < watchResponseHeaderCount_; ++i) {
      lastResponseHeaderValues_[i] = http_.header(watchResponseHeaders_[i]);
    }
  }
  // end()はここで必ず呼ぶ（次のaddHeader()向けにヘッダ状態をクリアするため）。
  // ソケット自体はsetReuse(true)とサーバのkeep-alive応答次第で閉じずに残る。
  http_.end();
  if (!ok) {
    // TLSハンドシェイクは大きな連続ブロックを要求するので、空きの総量より
    // 「取れる最大ブロック」が効く。code=-1 が続く時はここを見る。
    Serial.printf("[uploader] POST failed code=%d (heap free=%u maxblock=%u)\n",
                  code, ESP.getFreeHeap(), ESP.getMaxAllocHeap());
    // 失敗した接続を次回に持ち越さない。壊れた/不明な状態のソケットを
    // 使い回そうとして再度失敗し続けるのを避け、次回は必ず繋ぎ直す。
    client_.stop();
  }
  return ok;
}

void Uploader::closeConnection() {
  if (client_.connected()) client_.stop();
}

bool Uploader::sendAlert(const char* json, size_t len) {
  if (WiFi.status() != WL_CONNECTED) return false;
  WiFiClientSecure client;
  if (caCert_) {
    client.setCACert(caCert_);
  } else {
    client.setInsecure();  // caCert未指定時の後方互換フォールバック（検証なし）
  }
  client.setHandshakeTimeout(kHandshakeTimeoutMs);
  HTTPClient http;
  if (!http.begin(client, alertUrl_)) return false;
  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-Namz-Device", String(deviceId_));
  http.addHeader("X-Namz-Signature",
                 hmacSha256Hex(hmacSecret_, (const uint8_t*)json, len).c_str());
  int code = http.POST((uint8_t*)const_cast<char*>(json), len);
  http.end();
  return code >= 200 && code < 300;
}

bool Uploader::spillOldestRam() {
  if (ram_.empty()) return false;
  Batch* b = ram_.front();
  char path[64];
  // 20桁ゼロ埋め startUs でファイル名 -> 辞書順=時系列順
  snprintf(path, sizeof(path), "%s/%020llu.bin", spillDir_,
           (unsigned long long)b->startUs());
  File f = LittleFS.open(path, "w");
  if (!f) return false;
  size_t w = f.write(b->bytes(), b->size());
  f.close();
  if (w != b->size()) {
    LittleFS.remove(path);
    return false;
  }
  ram_.pop_front();
  delete b;
  ++spillCount_;
  return true;
}

size_t Uploader::flushToSpill() {
  UploaderLock lock(mutex_);
  size_t n = 0;
  while (!ram_.empty()) {
    if (!spillOldestRam()) break;
    ++n;
  }
  return n;
}

size_t Uploader::ramQueued() const {
  UploaderLock lock(mutex_);
  return ram_.size();
}

size_t Uploader::spillCount() const {
  UploaderLock lock(mutex_);
  return spillCount_;
}

size_t Uploader::droppedCount() const {
  UploaderLock lock(mutex_);
  return droppedCount_;
}

String Uploader::lastResponseHeaderValue(const char* headerName) const {
  for (size_t i = 0; i < watchResponseHeaderCount_; ++i) {
    if (strcmp(watchResponseHeaders_[i], headerName) == 0) return lastResponseHeaderValues_[i];
  }
  return String();
}

bool Uploader::oldestQueuedStartUs(uint64_t& outStartUs) const {
  UploaderLock lock(mutex_);
  if (spillCount_ > 0) {
    char path[64];
    return loadOldestSpillPath(path, sizeof(path), outStartUs);
  }
  if (!ram_.empty()) {
    outStartUs = ram_.front()->startUs();
    return true;
  }
  return false;
}

bool Uploader::loadOldestSpillPath(char* out, size_t outLen, uint64_t& startUs) const {
  // LittleFS.open()/File::openNextFile()はどちらもArduino-ESP32のFS実装内部で
  // std::make_shared<VFSFileImpl>を使う。ヒープ逼迫時にそのnew()が失敗すると
  // 未捕捉例外->abort()で機体ごと再起動する（pump()側の同種コメント参照、
  // 実機で確認済み）。ここは呼び出し元がpump()とoldestQueuedStartUs()の両方に
  // またがるため、失敗を「退避ファイルが見つからなかった」扱いに落として
  // 呼び出し元に伝える。
  String oldest;
  try {
    File dir = LittleFS.open(spillDir_);
    for (File f = dir.openNextFile(); f; f = dir.openNextFile()) {
      if (f.isDirectory()) continue;
      String name = f.name();
      if (oldest.isEmpty() || name < oldest) oldest = name;
    }
  } catch (...) {
    Serial.printf("[uploader] loadOldestSpillPath(%s) threw (heap exhausted?)\n", spillDir_);
    return false;
  }
  if (oldest.isEmpty()) return false;
  snprintf(out, outLen, "%s/%s", spillDir_, oldest.c_str());
  startUs = strtoull(oldest.c_str(), nullptr, 10);
  return true;
}

void Uploader::removeSpill(const char* path) {
  if (LittleFS.remove(path) && spillCount_ > 0) --spillCount_;
}

bool Uploader::evictOldestSpill() {
  char path[64];
  uint64_t startUs;
  if (!loadOldestSpillPath(path, sizeof(path), startUs)) return false;
  removeSpill(path);
  ++droppedCount_;
  return true;
}
