#include "Uploader.h"

#include <Arduino.h>
#include <HTTPClient.h>
#include <LittleFS.h>
#include <WiFiClientSecure.h>

#include <cstdio>
#include <cstring>

#include "HmacSha256.h"

static constexpr uint32_t kBackoffStartMs = 1000;
static constexpr uint32_t kBackoffMaxMs = 60000;

// nullptr終端の配列(argv方式)の要素数を数える。arr自体がnullptrなら0。
static size_t countSentinelArray(const char* const* arr) {
  if (!arr) return 0;
  size_t n = 0;
  while (arr[n]) ++n;
  return n;
}

Uploader::Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
                   uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir,
                   bool dropOldestWhenFull, const char* const* watchResponseHeaders,
                   const char* const* extraRequestHeaderNames,
                   const char* const* extraRequestHeaderValues, const char* caCert)
    : ingestUrl_(ingestUrl), alertUrl_(alertUrl), hmacSecret_(hmacSecret),
      deviceId_(deviceId), maxRam_(maxRamBatches), spillDir_(spillDir),
      dropOldestWhenFull_(dropOldestWhenFull), watchResponseHeaders_(watchResponseHeaders),
      watchResponseHeaderCount_(countSentinelArray(watchResponseHeaders)),
      extraRequestHeaderNames_(extraRequestHeaderNames),
      extraRequestHeaderValues_(extraRequestHeaderValues),
      extraRequestHeaderCount_(countSentinelArray(extraRequestHeaderNames)),
      caCert_(caCert), lastResponseHeaderValues_(watchResponseHeaderCount_) {}

bool Uploader::begin() {
  if (caCert_) {
    client_.setCACert(caCert_);
  } else {
    client_.setInsecure();  // caCert未指定時の後方互換フォールバック（検証なし）
  }
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
  return true;
}

void Uploader::enqueue(Batch* batch) {
  if (!batch || !batch->valid()) {
    delete batch;
    return;
  }
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
  if (WiFi.status() != WL_CONNECTED) return false;
  uint32_t now = millis();
  if (now < nextAttemptMs_) return false;

  // 1) 退避ファイル（常に古い）を優先で送る
  if (spillCount_ > 0) {
    char path[64];
    uint64_t startUs;
    if (loadOldestSpillPath(path, sizeof(path), startUs)) {
      File f = LittleFS.open(path, "r");
      if (f) {
        size_t len = f.size();
        uint8_t* body = (uint8_t*)malloc(len);
        if (body && f.read(body, len) == (int)len) {
          f.close();
          bool ok = postBatch(body, len);
          free(body);
          if (ok) {
            removeSpill(path);
            backoffMs_ = 0;
            nextAttemptMs_ = now;
            return true;
          }
        } else {
          if (body) free(body);
          f.close();
        }
      }
    }
    // 送れなかった -> バックオフ
    backoffMs_ = backoffMs_ ? min(backoffMs_ * 2, kBackoffMaxMs) : kBackoffStartMs;
    nextAttemptMs_ = now + backoffMs_;
    return false;
  }

  // 2) RAMキューの古い順
  if (!ram_.empty()) {
    Batch* b = ram_.front();
    if (postBatch(b->bytes(), b->size())) {
      ram_.pop_front();
      delete b;
      backoffMs_ = 0;
      nextAttemptMs_ = now;
      return true;
    }
    backoffMs_ = backoffMs_ ? min(backoffMs_ * 2, kBackoffMaxMs) : kBackoffStartMs;
    nextAttemptMs_ = now + backoffMs_;
    return false;
  }
  // 送るものが無い: 使い回していた接続を明示的に閉じる。繋ぎっぱなしにすると
  // 次にバッチが来るまでTLSセッション分のRAMを無駄に予約し続けてしまう
  // （使い回しの狙いはバックフィルの連続POST中のハンドシェイク省略であって、
  // 常時接続の維持ではない）。
  closeIdleConnection();
  return false;
}

bool Uploader::postBatch(const uint8_t* body, size_t len) {
  // client_/http_ はUploaderの寿命だけ生きるメンバ（バックフィル中の連続POSTで
  // TLSハンドシェイクを省略するための使い回し。docs参照）。setReuse(true)で
  // 「切断時に使い回せるなら閉じない」をHTTPClientへ伝える。前回の接続が
  // まだ生きていればHTTPClient::connect()が自動でハンドシェイクを省略する。
  if (!http_.begin(client_, ingestUrl_)) return false;
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
  int code = http_.POST(const_cast<uint8_t*>(body), len);
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

void Uploader::closeIdleConnection() {
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
  size_t n = 0;
  while (!ram_.empty()) {
    if (!spillOldestRam()) break;
    ++n;
  }
  return n;
}

String Uploader::lastResponseHeaderValue(const char* headerName) const {
  for (size_t i = 0; i < watchResponseHeaderCount_; ++i) {
    if (strcmp(watchResponseHeaders_[i], headerName) == 0) return lastResponseHeaderValues_[i];
  }
  return String();
}

bool Uploader::oldestQueuedStartUs(uint64_t& outStartUs) const {
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
  File dir = LittleFS.open(spillDir_);
  String oldest;
  for (File f = dir.openNextFile(); f; f = dir.openNextFile()) {
    if (f.isDirectory()) continue;
    String name = f.name();
    if (oldest.isEmpty() || name < oldest) oldest = name;
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
