#pragma once
// バッチの送信キュー・LittleFS退避・リトライ・バックフィルを担う。
//
// 不変条件: 「2xx が返るまでバッチを捨てない」。失敗理由は区別しない。
// 送信順序: LittleFSの退避ファイル（常に古い）を先に、次にRAMキューの古い順。

#include <deque>
#include <cstdint>

#include "Batch.h"

class Uploader {
 public:
  Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
           uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir);

  // 起動時に LittleFS をマウントし退避ファイル数を数える。
  bool begin();

  // 完成したバッチを送信キューに積む。所有権を受け取る。
  // RAMが一杯なら最も古いRAMバッチをLittleFSへ退避してから積む。
  void enqueue(Batch* batch);

  // 送信を1歩進める（送信タスクから周期的に呼ぶ）。送れたら true。
  bool pump();

  // 速報を即時POST。失敗は握りつぶす（速報は best-effort）。
  // 本文はプロジェクト固有なので呼び出し側が組む（震度・最大加速度といった
  // 中身をここに置くと、この層が地震計専用になり他プロジェクトと共有できない）。
  bool sendAlert(const char* json, size_t len);

  size_t ramQueued() const { return ram_.size(); }
  size_t spillCount() const { return spillCount_; }

 private:
  bool postBatch(const uint8_t* body, size_t len);
  bool spillOldestRam();               // RAM先頭をファイルへ
  bool loadOldestSpillPath(char* out, size_t outLen, uint64_t& startUs);
  void removeSpill(const char* path);

  const char* ingestUrl_;
  const char* alertUrl_;
  const char* hmacSecret_;
  uint32_t deviceId_;
  uint32_t maxRam_;
  const char* spillDir_;

  std::deque<Batch*> ram_;
  size_t spillCount_ = 0;
  uint32_t backoffMs_ = 0;
  uint32_t nextAttemptMs_ = 0;
};
