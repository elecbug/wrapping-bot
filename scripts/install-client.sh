#!/usr/bin/env sh
set -eu

SOURCE="${1:-./bin/wrapping-bot}"
DESTINATION="${2:-/usr/local/bin/wrapping-bot}"

if [ ! -f "$SOURCE" ]; then
  echo "Client binary not found: $SOURCE" >&2
  exit 1
fi

install -m 0755 "$SOURCE" "$DESTINATION"
echo "Installed wrapping-bot to $DESTINATION"
