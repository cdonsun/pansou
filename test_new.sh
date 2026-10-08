#!/bin/bash
# smoke-test new plugins on a temp port before touching the live service
cd /root/pansou-build || exit 1
export PATH=/usr/local/bin:$PATH
export PORT=9999
export HOST=127.0.0.1
export CHANNELS=tgsearchers7
export ENABLED_PLUGINS=dayanzai,guohe
export CACHE_ENABLED=false
export ASYNC_PLUGIN_ENABLED=true
export ASYNC_RESPONSE_TIMEOUT=6
export GOMEMLIMIT=300MiB
nohup /root/pansou-new > /tmp/pansou-test.log 2>&1 &
echo $! > /tmp/pansou-test.pid
sleep 3
echo "--- health:"
curl -s -m 5 "http://127.0.0.1:9999/api/search?kw=ping" -o /dev/null -w "HTTP:%{http_code} TIME:%{time_total}\n"
echo "--- dayanzai search (photoshop):"
curl -s -m 25 "http://127.0.0.1:9999/api/search?kw=photoshop&plugins=dayanzai&res=merge" | head -c 1200
echo ""
echo "--- guohe search (photoshop):"
curl -s -m 25 "http://127.0.0.1:9999/api/search?kw=photoshop&plugins=guohe&res=merge" | head -c 1200
echo ""
kill $(cat /tmp/pansou-test.pid) 2>/dev/null
echo "--- log tail:"
tail -20 /tmp/pansou-test.log
