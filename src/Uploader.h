#pragma once
// バッチの送信キュー・LittleFS退避・リトライ・バックフィルを担う。
//
// 不変条件: 「2xx が返るまでバッチを捨てない」。失敗理由は区別しない。
// 送信順序: LittleFSの退避ファイル（常に古い）を先に、次にRAMキューの古い順。
//
// 例外: dropOldestWhenFull=true の時だけ、LittleFSも一杯でこれ以上退避できない
// 場面に限り、一番古いデータ（退避ファイルの最古、無ければRAMキューの先頭）を
// 捨てて新しいデータを残す。既定は false でこの例外は発動しない
// （呼び出し側が明示的に選ばない限り不変条件は変わらない）。

#include <Arduino.h>

#include <deque>
#include <cstdint>

#include "Batch.h"

class Uploader {
 public:
  // watchResponseHeader: 設定すると、バッチPOSTが成功した時にそのレスポンス
  // ヘッダの値を保持し lastResponseHeaderValue() で読めるようになる（オプトイン、
  // 既定nullptrで従来通りヘッダを一切見ない）。この層はヘッダを読んで渡すだけで
  // 意味づけは持たない——「何のヘッダか・値をどう解釈するか」は呼び出し側の責務。
  // Electabuzz等の他プロジェクトを巻き込まないための設計（dropOldestWhenFullと同じ考え方）。
  Uploader(const char* ingestUrl, const char* alertUrl, const char* hmacSecret,
           uint32_t deviceId, uint32_t maxRamBatches, const char* spillDir,
           bool dropOldestWhenFull = false, const char* watchResponseHeader = nullptr);

  // 起動時に LittleFS をマウントし退避ファイル数を数える。
  bool begin();

  // 完成したバッチを送信キューに積む。所有権を受け取る。
  // RAMが一杯なら最も古いRAMバッチをLittleFSへ退避してから積む。
  // 退避先も一杯なら dropOldestWhenFull_ に従う（コンストラクタ参照）。
  void enqueue(Batch* batch);

  // 送信を1歩進める（送信タスクから周期的に呼ぶ）。送れたら true。
  bool pump();

  // 速報を即時POST。失敗は握りつぶす（速報は best-effort）。
  // 本文はプロジェクト固有なので呼び出し側が組む（震度・最大加速度といった
  // 中身をここに置くと、この層が地震計専用になり他プロジェクトと共有できない）。
  bool sendAlert(const char* json, size_t len);

  size_t ramQueued() const { return ram_.size(); }
  size_t spillCount() const { return spillCount_; }

  // キュー中で最も古いバッチの開始時刻[us]（送信順序と同じ基準：退避ファイル優先、
  // 無ければRAMキューの先頭）。キューが空なら false。表示で「どれだけ遅れているか」
  // を出す用途を想定（呼び出し側が現在時刻との差を取る）。
  bool oldestQueuedStartUs(uint64_t& outStartUs) const;

  // dropOldestWhenFull=true の時、満杯で捨てた本数の累計。
  // 既定(false)では常に0のまま（捨てないので増えようがない）。
  size_t droppedCount() const { return droppedCount_; }

  // watchResponseHeader が設定されている時、直近成功したバッチPOSTのレスポンスに
  // そのヘッダがあればその値、無ければ空文字列。watchResponseHeader==nullptrなら
  // 常に空文字列。POSTが失敗した回は更新しない（前回成功時の値を保持）。
  String lastResponseHeaderValue() const { return lastResponseHeaderValue_; }

 private:
  bool postBatch(const uint8_t* body, size_t len);
  bool spillOldestRam();               // RAM先頭をファイルへ
  bool loadOldestSpillPath(char* out, size_t outLen, uint64_t& startUs) const;
  void removeSpill(const char* path);
  bool evictOldestSpill();             // 退避ファイルの最古を1本消す（空きを作る）

  const char* ingestUrl_;
  const char* alertUrl_;
  const char* hmacSecret_;
  uint32_t deviceId_;
  uint32_t maxRam_;
  const char* spillDir_;
  bool dropOldestWhenFull_;
  const char* watchResponseHeader_;

  std::deque<Batch*> ram_;
  size_t spillCount_ = 0;
  size_t droppedCount_ = 0;
  String lastResponseHeaderValue_;
  uint32_t backoffMs_ = 0;
  uint32_t nextAttemptMs_ = 0;
};
