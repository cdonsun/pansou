#!/bin/bash
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36"

# 用 pansou 后端直接搜，看各插件来源分布
for kw in "%E8%80%83%E7%A0%94%E8%B5%84%E6%96%99" "%E4%B9%A6%E7%B1%8D" "%E5%8A%9E%E5%85%AC%E8%BD%AF%E4%BB%B6" "photoshop" "%E7%BD%91%E8%AF%BE"; do
  echo "=== $kw ==="
  curl -s -m 25 "http://127.0.0.1:8888/api/search?kw=$kw&res=merge" | python3 -c "
import sys, json
d = json.load(sys.stdin)
mbt = d.get('data', {}).get('merged_by_type', {})
srcs = {}
for k, v in mbt.items():
    for it in v:
        s = it.get('source', '')
        if s not in srcs: srcs[s] = 0
        srcs[s] += 1
print('total:', d.get('data',{}).get('total'), '| plugins:', ', '.join(f'{k}={v}' for k,v in sorted(srcs.items(), key=lambda x:-x[1])[:10]))
"
  echo ""
done
