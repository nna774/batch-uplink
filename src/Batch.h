#pragma once
// ワイヤ形式に依存しない送信バッファ。
//
//   [ヘッダ領域 headerBytes][固定長レコード × recordCount][tail]
//
// ヘッダの中身はこのクラスの関知しない領域で、呼び出し側が headerPtr() 越しに書く。
// magic・バージョン・レコード数・CRC はどれもプロジェクト固有だからだ（NAMZ は
// lib/NamzWire、周波数モニタ側は別の形を持つ）。書く時期は「レコードを積み終えた後・
// Uploader へ渡す前」——その時点で recordCount() も records() も確定している。
//
// tail は「レコード列の後ろに置く可変長領域」。TLV 等の枠組みは呼び出し側が組む。
// addRecord() のたびに1レコード分だけ後ろへ押し出すので、tail は常に正しい位置に
// 居続ける。ゆえにバッファはいつ見ても完結したバイト列であり、bytes()/size() は
// 純粋な getter でよい（確定用の呼び出しを忘れる、という失敗の形が存在しない）。
// 押し出しの費用は tail の長さぶんの memmove だけで、実測上無視できる。

#include <cstddef>
#include <cstdint>

class Batch {
 public:
  // capacityRecords 件ぶんの領域を確保する。失敗時 valid()==false。
  Batch(uint32_t capacityRecords, size_t recordBytes, size_t headerBytes,
        size_t tailCapacity = 0);
  ~Batch();

  Batch(const Batch&) = delete;
  Batch& operator=(const Batch&) = delete;

  bool valid() const { return buf_ != nullptr; }

  // 先頭レコードの時刻を設定し、レコード列と tail を空にする（1件目投入前に呼ぶ）。
  // startUs は順序付け（退避ファイル名・送信順）にのみ使う。
  void begin(uint64_t startUs);

  // 固定長レコードを1件追記。満杯、または len が recordBytes と違うなら false。
  bool addRecord(const void* rec, size_t len);

  // tail へ追記。begin() で消えるので、バッチ毎に入れ直すこと。
  bool appendTail(const void* data, size_t len);

  // tail の残り容量。複数回に分けて書くものを、途中で溢れて中途半端な状態に
  // しないために、呼び出し側が先に総量を確かめられるようにしてある。
  size_t tailRemaining() const { return tailCapacity_ - tailLen_; }

  // 呼び出し側がヘッダを書き込むための可変ポインタ。
  uint8_t* headerPtr() { return buf_; }

  // ヘッダに載せる値（レコード数・CRC 等）を作るための読み取り口。
  const uint8_t* records() const { return buf_ + headerBytes_; }
  size_t recordsSize() const { return static_cast<size_t>(count_) * recordBytes_; }

  bool isFull() const { return count_ >= capacity_; }
  uint32_t recordCount() const { return count_; }
  size_t recordBytes() const { return recordBytes_; }
  uint64_t startUs() const { return startUs_; }

  const uint8_t* bytes() const { return buf_; }
  size_t size() const { return headerBytes_ + recordsSize() + tailLen_; }

 private:
  uint8_t* tailPtr() const { return buf_ + headerBytes_ + recordsSize(); }

  uint8_t* buf_ = nullptr;
  size_t headerBytes_ = 0;
  size_t recordBytes_ = 0;
  size_t tailCapacity_ = 0;
  size_t tailLen_ = 0;
  uint32_t capacity_ = 0;
  uint32_t count_ = 0;
  uint64_t startUs_ = 0;
};
