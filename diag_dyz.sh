#!/bin/bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
echo "--- curl same headers:"
curl -sS -m 12 -o /dev/null -w "HTTP:%{http_code} TIME:%{time_total} IP:%{remote_ip}\n" \
  -A "$UA" \
  -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" \
  -H "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8" \
  -H "Connection: keep-alive" \
  --compressed "https://www.dayanzai.me/?s=photoshop" 2>&1 | tail -3
echo "--- DNS from prod:"
getent hosts www.dayanzai.me || echo "getent failed"
echo "--- verbose error sample:"
grep -a "dayanzai" /tmp/pansou-test.log | tail -5
