#pragma once
// NTP時刻同期。測定中に時刻が飛ばないよう smooth(slew) 同期を使う。
// ただし slew では詰まらない大きなずれ（長時間オフライン後のドリフト等）は
// 一度だけ step で補正する（閾値は begin の引数）。

#include <cstdint>

namespace timesync {

// SNTPを開始（smoothモード）。WiFi接続後に呼ぶ。
// stepThresholdUs を超えるオフセットを検知したら slew でなく step で一発補正する。
void begin(const char* server1, const char* server2, uint64_t stepThresholdUs);

// 一度でも時刻同期できたか。
bool isSynced();

// 現在のUNIX時刻[us]。
uint64_t nowUs();

}  // namespace timesync
