// Batch の契約（ヘッダ領域 + 固定長レコード列 + tail）のテスト。
// ワイヤ形式には一切触れない。載せる側の形式ごとの検証は各プロジェクトの責務。

#include <cstdint>
#include <cstdio>
#include <cstring>

#include "Batch.h"

static int gFailures = 0;

static void check(const char* name, bool ok) {
  printf("%s %s\n", ok ? "ok  " : "FAIL", name);
  if (!ok) ++gFailures;
}

static void checkEq(const char* name, long got, long want) {
  if (got == want) { printf("ok   %s\n", name); return; }
  printf("FAIL %s: want %ld, got %ld\n", name, want, got);
  ++gFailures;
}

int main() {
  // --- 寸法どおりに並ぶ ---
  {
    Batch b(4, 8, 16, 0);
    check("valid", b.valid());
    b.begin(1234);
    checkEq("空のsize", (long)b.size(), 16);
    checkEq("startUsを保つ", (long)b.startUs(), 1234);
    for (int i = 0; i < 4; ++i) {
      uint8_t rec[8];
      std::memset(rec, i + 1, sizeof(rec));
      check("addRecord", b.addRecord(rec, sizeof(rec)));
    }
    check("満杯", b.isFull());
    checkEq("recordCount", b.recordCount(), 4);
    checkEq("size", (long)b.size(), 16 + 4 * 8);
    checkEq("recordsSize", (long)b.recordsSize(), 32);
    checkEq("records()はヘッダの直後", (long)(b.records() - b.bytes()), 16);
    checkEq("1件目", b.records()[0], 1);
    checkEq("4件目", b.records()[24], 4);
    check("満杯なら拒否", !b.addRecord("xxxxxxxx", 8));
  }

  // --- レコード長が違えば拒否する（形式の取り違えを黙って通さない）---
  {
    Batch b(2, 8, 16, 0);
    b.begin(0);
    check("短いレコードを拒否", !b.addRecord("short", 5));
    check("長いレコードを拒否", !b.addRecord("toolongrecord", 13));
    checkEq("拒否は数に入らない", b.recordCount(), 0);
  }

  // --- ヘッダは呼び出し側のもの。レコードを侵さない ---
  {
    Batch b(2, 4, 12, 0);
    b.begin(7);
    b.addRecord("AAAA", 4);
    b.addRecord("BBBB", 4);
    std::memset(b.headerPtr(), 0xEE, 12);
    checkEq("ヘッダが書ける", b.bytes()[11], 0xEE);
    checkEq("レコードが無傷", b.records()[0], 'A');
    checkEq("レコードが無傷(2件目)", b.records()[4], 'B');
  }

  // --- tail は常にレコード列の直後に居続ける ---
  {
    Batch b(3, 4, 8, 16);
    b.begin(0);
    checkEq("tail残量", (long)b.tailRemaining(), 16);
    b.appendTail("TT", 2);          // レコードより先に入れても
    checkEq("tail残量が減る", (long)b.tailRemaining(), 14);
    checkEq("tailだけのsize", (long)b.size(), 8 + 2);
    for (int i = 0; i < 3; ++i) b.addRecord("....", 4);  // 後からレコードを積む
    checkEq("size", (long)b.size(), 8 + 12 + 2);
    const uint8_t* end = b.bytes() + 8 + 12;
    check("tailはレコード列の直後", end[0] == 'T' && end[1] == 'T');
    check("レコードがtailに潰されていない", b.records()[11] == '.');
    // 追記は既存の tail の後ろに続く
    b.appendTail("UU", 2);
    check("追記は後ろに続く", end[2] == 'U' && end[3] == 'U');
    checkEq("追記後のsize", (long)b.size(), 8 + 12 + 4);
  }

  // --- tail が容量を超えたら拒否する（中途半端に書かない）---
  {
    Batch b(1, 4, 8, 4);
    b.begin(0);
    check("収まる分は入る", b.appendTail("AB", 2));
    check("溢れる分は拒否", !b.appendTail("CDE", 3));
    checkEq("拒否しても長さは変わらない", (long)b.size(), 8 + 2);
    check("残量ぴったりは入る", b.appendTail("CD", 2));
    checkEq("tail残量ゼロ", (long)b.tailRemaining(), 0);
  }

  // --- begin() はレコードもtailも巻き戻し、ヘッダを消す ---
  {
    Batch b(2, 4, 8, 8);
    b.begin(1);
    b.addRecord("AAAA", 4);
    b.appendTail("TT", 2);
    std::memset(b.headerPtr(), 0xFF, 8);
    b.begin(2);
    checkEq("recordCountが戻る", b.recordCount(), 0);
    checkEq("tailが消える", (long)b.tailRemaining(), 8);
    checkEq("sizeが戻る", (long)b.size(), 8);
    checkEq("startUsが更新される", (long)b.startUs(), 2);
    checkEq("ヘッダが前バッチの残骸を残さない", b.bytes()[0], 0);
  }

  // --- tail 無し（tailCapacity=0）でも成立する ---
  {
    Batch b(2, 12, 64, 0);
    b.begin(0);
    checkEq("tail残量ゼロ", (long)b.tailRemaining(), 0);
    check("tailへの追記は拒否", !b.appendTail("X", 1));
    b.addRecord("123456789012", 12);
    checkEq("size", (long)b.size(), 64 + 12);
  }

  printf("\n%s (%d failure%s)\n", gFailures ? "FAILED" : "PASSED", gFailures,
         gFailures == 1 ? "" : "s");
  return gFailures ? 1 : 0;
}
