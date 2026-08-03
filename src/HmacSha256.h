#pragma once
// mbedTLS を使った HMAC-SHA256（ESP-IDF に同梱）。

#include <cstdint>
#include <cstddef>
#include <string>

#include "mbedtls/md.h"

inline std::string hmacSha256Hex(const char* key, const uint8_t* data, size_t len) {
  uint8_t out[32];
  const mbedtls_md_info_t* info = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
  mbedtls_md_hmac(info, reinterpret_cast<const uint8_t*>(key), std::string(key).size(),
                  data, len, out);
  static const char* hex = "0123456789abcdef";
  std::string s;
  s.reserve(64);
  for (int i = 0; i < 32; ++i) {
    s.push_back(hex[out[i] >> 4]);
    s.push_back(hex[out[i] & 0xF]);
  }
  return s;
}
