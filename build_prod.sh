#!/bin/bash
# build pansou with new plugins on prod
set -x
SRC=/root/pansou-build
rm -rf "$SRC" && mkdir -p "$SRC"
tar xzf /root/pansou-src.tar.gz -C "$SRC"
cd "$SRC" || exit 1
export PATH=/usr/local/bin:$PATH
export GOTOOLCHAIN=local
export CGO_ENABLED=0
go build -trimpath -ldflags="-s -w" -o /root/pansou-new . 2>&1
echo "BUILD_EXIT=$?"
ls -la /root/pansou-new 2>/dev/null
