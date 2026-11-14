#include "TimeSync.h"

#include <sys/time.h>

#include <atomic>

#include "esp_sntp.h"

namespace timesync {

// step判定の閾値[us]。begin で設定。
static uint64_t sStepThresholdUs = 0;

// この起動でSNTPの応答を一度でも受け取ったか。onSync(lwIPのタスク)が立て、
// isSynced(任意のタスク)が読む。RTC_NOINIT等には置かない——ソフトリセット後も
// システム時刻はRTCから引き継がれるが、その値はこの起動では誰も検証していない。
static std::atomic<bool> sReceived{false};

// SNTPが時刻を受信するたびに呼ばれる。SMOOTHモードでは受信時刻は adjtime で
// slew適用済みだが、オフセットが大きい（slewでは現実的な時間で詰まらない）場合だけ
// settimeofday で一発 step し、slew を上書きする。小さいずれはそのまま slew に任せる。
static void onSync(struct timeval* tv) {
  struct timeval now;
  gettimeofday(&now, nullptr);
  int64_t server_us = static_cast<int64_t>(tv->tv_sec) * 1000000LL + tv->tv_usec;
  int64_t now_us = static_cast<int64_t>(now.tv_sec) * 1000000LL + now.tv_usec;
  int64_t delta = server_us - now_us;
  if (delta < 0) delta = -delta;
  if (static_cast<uint64_t>(delta) > sStepThresholdUs) {
    settimeofday(tv, nullptr);
  }
  // step後に立てる。isSynced()を見てから時刻を読む側が、step前の時刻を掴まないように。
  sReceived.store(true, std::memory_order_release);
}

void begin(const char* server1, const char* server2, uint64_t stepThresholdUs) {
  sStepThresholdUs = stepThresholdUs;
  // 通常は slew(なめらか)同期。ステップ補正で時刻が飛ぶのを避ける。
  esp_sntp_setoperatingmode(ESP_SNTP_OPMODE_POLL);
  sntp_set_sync_mode(SNTP_SYNC_MODE_SMOOTH);
  esp_sntp_setservername(0, server1);
  esp_sntp_setservername(1, server2);
  // 大ずれだけ step させるコールバックを登録（init 前に）。
  sntp_set_time_sync_notification_cb(onSync);
  esp_sntp_init();
}

// sntp_get_sync_status() は使えない。COMPLETED は一度読むと RESET に戻る一回限りの
// 通知で、SMOOTH モードでは slew が終わるまで IN_PROGRESS のままでもある。
// 「時刻がそれらしい年か」でも判定しない。パニックやWDTによるソフトリセットでは
// 直前の時刻がRTCから引き継がれ、SNTP応答前から年判定を通ってしまう。
bool isSynced() { return sReceived.load(std::memory_order_acquire); }

uint64_t nowUs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + tv.tv_usec;
}

}  // namespace timesync
