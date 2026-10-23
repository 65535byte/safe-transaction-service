#!/usr/bin/env bash
# 部署「一条链」：生成栈目录（只放 docker-compose.yml + .env，不复制源码）+ .env（幂等），
# 可选直接构建并启动。部署第 2、3 条链时，改参数重跑本脚本即可。
#
# 用法（在代码目录里执行）：
#   NAME=tx-bsc PORT=8001 RPC=https://bsc-dataseed.bnbchain.org/ DOMAIN=bsc-tx.dplife.org \
#   START=1 bash deploy/setup_stack.sh
#
# 参数：
#   NAME             栈名（同时是目录名与 COMPOSE_PROJECT_NAME，启用后不可改）
#   PORT             该栈 nginx 在宿主的监听端口（只绑 127.0.0.1），每条链一个
#   RPC              本链 RPC
#   DOMAIN           本链对外域名
#   START            1=顺带 build + up -d（默认 0，只生成配置）
#   SRC              代码源目录（默认：本脚本所在目录的上一级，即仓库根）
#   ROOT             栈根目录（默认 /root/safe）
#   CSRF_ORIGIN      覆盖 CSRF_TRUSTED_ORIGINS（默认 https://$DOMAIN；子域通配填 https://*.主域）
# 镜像 tag（DOCKER_IMAGE）由模板固定为 safe-tx:6.11.0，脚本不读也不写
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SRC:-$(dirname "$DEPLOY_DIR")}"
ROOT="${ROOT:-/root/safe}"
CSRF_ORIGIN="${CSRF_ORIGIN:-}"

: "${NAME:?需要 NAME=栈名，如 tx-bsc}"
: "${PORT:?需要 PORT=宿主端口，如 8001}"
: "${RPC:?需要 RPC=本链 RPC}"
: "${DOMAIN:?需要 DOMAIN=本链对外域名}"

[ -f "$SRC/docker-compose.yml" ] || { echo "找不到 $SRC/docker-compose.yml，用 SRC= 指定代码目录"; exit 1; }
command -v docker >/dev/null || { echo "未找到 docker"; exit 1; }
# 源码目录需为绝对路径：栈目录里没有源码，容器要用它做构建上下文与 nginx.conf 挂载
case "$SRC" in /*) ;; *) SRC="$(cd "$SRC" && pwd)";; esac

# 已存在的键就地替换，缺失则追加（不删注释、不动其他行）
set_env() {
    local f="$1" k="$2" v="$3"
    if grep -qE "^${k}=" "$f"; then
        sed -i "s#^${k}=.*#${k}=${v}#" "$f"
    else
        printf '%s=%s\n' "$k" "$v" >>"$f"
    fi
}

# 栈目录只放配置：docker-compose.yml（从源码目录同步）+ .env，
# 源码不复制——构建上下文与 nginx.conf 挂载由 .env 的 SRC_DIR 指回源码目录
dst="$ROOT/$NAME"
mkdir -p "$dst"
if ! cmp -s "$SRC/docker-compose.yml" "$dst/docker-compose.yml"; then
    cp "$SRC/docker-compose.yml" "$dst/docker-compose.yml"
    echo "== 已同步 $dst/docker-compose.yml（源码目录 $SRC）"
fi

if [ -f "$dst/.env" ]; then
    echo "== $dst/.env 已存在，未覆盖（改配置请直接编辑它，再 docker compose up -d）"
    cur="$(grep -E '^COMPOSE_PROJECT_NAME=' "$dst/.env" | tail -1 | cut -d= -f2- || true)"
    [ -z "$cur" ] || [ "$cur" = "$NAME" ] || echo "  ⚠️ 其中 COMPOSE_PROJECT_NAME=$cur 与 NAME=$NAME 不一致：改它会换数据卷，务必确认"
else
    cp "$DEPLOY_DIR/env/stack.env.example" "$dst/.env"
    [ -n "$CSRF_ORIGIN" ] || CSRF_ORIGIN="https://$DOMAIN"
    # 只写与「栈身份」绑定的键；DJANGO_SECRET_KEY / SETUP_SAFE_ADDRESSES 等保持模板占位符，由人工填写
    set_env "$dst/.env" COMPOSE_PROJECT_NAME "$NAME"
    set_env "$dst/.env" NGINX_HOST_BIND 127.0.0.1
    set_env "$dst/.env" NGINX_HOST_PORT "$PORT"
    set_env "$dst/.env" ETHEREUM_NODE_URL "$RPC"
    set_env "$dst/.env" CSRF_TRUSTED_ORIGINS "$CSRF_ORIGIN"
    echo "== 已生成 $dst/.env（RPC=$RPC 域名=$DOMAIN 端口=$PORT）"
    echo "  ℹ️ DJANGO_SECRET_KEY / SETUP_SAFE_ADDRESSES 沿用模板里固定的值，脚本不读也不写"
fi

# 每次都写（含已有 .env）：栈目录没有源码，构建上下文与 nginx.conf 挂载都靠它
set_env "$dst/.env" SRC_DIR "$SRC"

if [ "${START:-0}" = 1 ]; then
    cd "$dst"
    # 总是 build：有缓存时是增量（秒级），无缓存时才真构建；只在首次过滤会导致改了代码不生效
    docker compose build
    docker compose up -d
    echo "== 已启动 $NAME（docker compose logs -f indexer-worker 看追块）"
fi

cat <<EOF

下一步：
  1) 宿主 nginx（需 root，证书不存在会提示 certbot 命令）：
       NAME=$NAME DOMAIN=$DOMAIN PORT=$PORT bash $DEPLOY_DIR/setup_host_nginx.sh
  2) 验收（chainId 可省，只查响应）：
       PORTS="$PORT:" bash $DEPLOY_DIR/verify.sh
  3) SETUP_SAFE_ADDRESSES=0 的自定义链：登录 https://$DOMAIN/admin/ 录入
     Master Copies 与 Proxy Factories（address / version 1.3.0+L2 / 勾 l2 /
     initial block number=部署块 / tx block number 填同值）
EOF
