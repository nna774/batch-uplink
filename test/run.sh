#!/usr/bin/env bash
# Batch はArduinoに依存しないのでホストのg++で走る。実機もPlatformIOも要らない。
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' EXIT
g++ -std=gnu++17 -Wall -Wextra -Werror -I "$HERE/../src" \
  -o "$OUT/test_batch" "$HERE/test_batch.cpp" "$HERE/../src/Batch.cpp"
"$OUT/test_batch"
