#!/usr/bin/env bash
# 逐栈验收：端口 → chain_id → 索引进度
# 用法：PORTS="8001:56" bash deploy/verify.sh    # 单栈（端口:期望chainId）
#       PORTS="8001:56 8002:97 8003:" bash deploy/verify.sh   # 一次查多栈
#       PORT=8001 CHAIN_ID=56 bash deploy/verify.sh
set -uo pipefail

PORTS="${PORTS:-${PORT:-8001}:${CHAIN_ID:-}}"

for pair in $PORTS; do
    port="${pair%%:*}"
    expect="${pair#*:}"
    echo "---- 127.0.0.1:$port (期望 chainId=${expect:-未指定})"
    about="$(curl -s --max-time 5 "http://127.0.0.1:${port}/api/v1/about/" || true)"
    if [ -z "$about" ]; then
        echo "  ✗ 无响应：栈未启动或端口不对（docker compose ps / logs -f indexer-worker）"
        continue
    fi
    chain_id="$(printf '%s' "$about" | grep -o '"chain_id":[0-9]*' | head -1 | grep -o '[0-9]*$')"
    if [ -z "$chain_id" ]; then
        # /api/v1/about/ 不返回 chain_id：经宿主端口反查该栈的 db 容器，直接读 Chain 表
        proj="$(docker ps --filter "publish=$port" --format '{{.ID}}' | head -1 | \
            xargs -r docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null)"
        dbid="$(docker ps --filter "label=com.docker.compose.project=$proj" \
            --filter "label=com.docker.compose.service=db" --format '{{.ID}}' | head -1)"
        [ -n "$dbid" ] && chain_id="$(docker exec "$dbid" psql -U postgres -tAc \
            'select chain_id from history_chain limit 1;' 2>/dev/null | tr -d ' \r')"
    fi
    echo "  chain_id = ${chain_id:-?}"
    if [ -n "$expect" ] && [ "$chain_id" != "$expect" ]; then
        echo "  ✗ 链不匹配！检查该栈 .env 的 ETHEREUM_NODE_URL（RPC 指错链会导致 check_chainid_matches 拒绝启动）"
    fi
    indexing="$(curl -s --max-time 5 "http://127.0.0.1:${port}/api/v1/about/indexing/" || true)"
    printf '  indexing = %s\n' "$(printf '%s' "$indexing" | grep -o '"synced":[a-z]*' | head -1)"
done

echo
echo "索引日志：docker compose logs -f indexer-worker（各栈目录下执行）"
echo "若 worker 反复重启且报 chainId 不匹配：该栈换链了，只能 docker compose down -v 清库重来"
