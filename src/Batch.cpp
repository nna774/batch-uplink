#include "Batch.h"

#include <cstdlib>
#include <cstring>

Batch::Batch(uint32_t capacityRecords, size_t recordBytes, size_t headerBytes,
             size_t tailCapacity)
    : headerBytes_(headerBytes),
      recordBytes_(recordBytes),
      tailCapacity_(tailCapacity),
      capacity_(capacityRecords) {
  size_t total = headerBytes + static_cast<size_t>(capacityRecords) * recordBytes
                 + tailCapacity;
  buf_ = static_cast<uint8_t*>(malloc(total));
  if (buf_) std::memset(buf_, 0, headerBytes);
}

Batch::Batch(uint8_t* buf, size_t bufSize, uint32_t capacityRecords, size_t recordBytes,
             size_t headerBytes, size_t tailCapacity, ReleaseFn onRelease, void* releaseCtx)
    : headerBytes_(headerBytes),
      recordBytes_(recordBytes),
      tailCapacity_(tailCapacity),
      capacity_(capacityRecords),
      onRelease_(onRelease),
      releaseCtx_(releaseCtx) {
  size_t need = headerBytes + static_cast<size_t>(capacityRecords) * recordBytes
                + tailCapacity;
  if (buf && bufSize >= need) {
    buf_ = buf;
    std::memset(buf_, 0, headerBytes);
  } else if (buf && onRelease) {
    // 領域が小さすぎて使えない場合も、所有権は最初から呼び出し側にあるので
    // ここで突き返す。返し忘れるとプールのスロットが1つ消えたままになる。
    onRelease(releaseCtx, buf);
  }
}

Batch::~Batch() {
  if (!buf_) return;
  if (onRelease_) {
    onRelease_(releaseCtx_, buf_);
  } else {
    free(buf_);
  }
}

void Batch::begin(uint64_t startUs) {
  startUs_ = startUs;
  count_ = 0;
  tailLen_ = 0;
  // ヘッダは呼び出し側が後から書く。それまでの中身を前バッチの残骸にしない。
  std::memset(buf_, 0, headerBytes_);
}

bool Batch::addRecord(const void* rec, size_t len) {
  if (len != recordBytes_ || isFull()) return false;
  uint8_t* at = tailPtr();
  // tail はレコード列の直後に居る。1レコード分だけ後ろへ押し出し、空いた場所へ書く。
  if (tailLen_) std::memmove(at + recordBytes_, at, tailLen_);
  std::memcpy(at, rec, recordBytes_);
  ++count_;
  return true;
}

bool Batch::appendTail(const void* data, size_t len) {
  if (tailLen_ + len > tailCapacity_) return false;
  std::memcpy(tailPtr() + tailLen_, data, len);
  tailLen_ += len;
  return true;
}
