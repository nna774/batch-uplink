#include "TimeSync.h"

#include <sys/time.h>

#include "esp_sntp.h"

namespace timesync {

// step判定の閾値[us]。begin で設定。
static uint64_t sStepThresholdUs = 0;

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

bool isSynced() {
  return sntp_get_sync_status() == SNTP_SYNC_STATUS_COMPLETED ||
         time(nullptr) > 1700000000;  // 2023-11以降なら同期済とみなす
}

uint64_t nowUs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000ULL + tv.tv_usec;
}

}  // namespace timesync
