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

Uploader::Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
                   uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir)
    : ingestUrl_(ingestUrl), alertUrl_(alertUrl), hmacSecret_(hmacSecret),
      deviceId_(deviceId), maxRam_(maxRamBatches), spillDir_(spillDir) {}

bool Uploader::begin() {
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
    if (!spillOldestRam()) break;  // 退避できなければ諦めて積む（メモリ許す範囲）
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
  return false;
}

bool Uploader::postBatch(const uint8_t* body, size_t len) {
  WiFiClientSecure client;
  client.setInsecure();  // TODO: Function URL のルート証明書をピン留めする
  HTTPClient http;
  if (!http.begin(client, ingestUrl_)) return false;
  http.addHeader("Content-Type", "application/octet-stream");
  http.addHeader("X-Namz-Device", String(deviceId_));
  http.addHeader("X-Namz-Signature", hmacSha256Hex(hmacSecret_, body, len).c_str());
  int code = http.POST(const_cast<uint8_t*>(body), len);
  http.end();
  bool ok = (code >= 200 && code < 300);
  if (!ok) {
    // TLSハンドシェイクは大きな連続ブロックを要求するので、空きの総量より
    // 「取れる最大ブロック」が効く。code=-1 が続く時はここを見る。
    Serial.printf("[uploader] POST failed code=%d (heap free=%u maxblock=%u)\n",
                  code, ESP.getFreeHeap(), ESP.getMaxAllocHeap());
  }
  return ok;
}

bool Uploader::sendAlert(const char* json, size_t len) {
  if (WiFi.status() != WL_CONNECTED) return false;
  WiFiClientSecure client;
  client.setInsecure();
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

bool Uploader::loadOldestSpillPath(char* out, size_t outLen, uint64_t& startUs) {
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
