#!/usr/bin/env bash
# 「一条链」一键部署：按顺序跑完 DEPLOYMENT.md 5.1 的全部步骤（幂等，失败可重跑）
#   1) setup_stack.sh       生成栈目录 + .env（只写栈身份项）→ build → up -d
#   2) certbot              签发 Let's Encrypt 证书（需 root，用 sudo；证书已存在则跳过）
#   3) setup_host_nginx.sh  宿主 nginx 加转发（需 root，用 sudo）
#   4) 创建后台账户          已有 superuser 则跳过
#   5) verify.sh            验收
#
# 用法（在代码目录里执行；非 root 登录时会用 sudo 跑需要 root 的步骤）：
#   NAME=tx-bsc PORT=8001 RPC=https://bsc-dataseed.bnbchain.org/ DOMAIN=bsc-tx.dplife.org \
#   CHAIN_ID=56 bash deploy/deploy_stack.sh
#
# 参数：
#   NAME / PORT / RPC / DOMAIN      必填，含义同 setup_stack.sh
#   CHAIN_ID                        验收时校验的 chainId（可省，只查响应）
#   ROOT / SRC / CSRF_ORIGIN 同 setup_stack.sh（镜像 tag 由模板固定，不作为参数）
#   SKIP_CERT=1                     跳过 certbot（已有证书 / 已放自签证书）
#   SKIP_SUPERUSER=1                跳过创建后台账户
#   SU_USER / SU_EMAIL / SU_PASS    三个都给则非交互创建后台账户，否则交互式输入
#   ADMIN_EMAIL                     certbot 通知邮箱（可选）
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SRC:-$(dirname "$DEPLOY_DIR")}"
ROOT="${ROOT:-$HOME/safe}"
CSRF_ORIGIN="${CSRF_ORIGIN:-}"
CHAIN_ID="${CHAIN_ID:-}"

: "${NAME:?需要 NAME=栈名，如 tx-bsc}"
: "${PORT:?需要 PORT=宿主端口，如 8001}"
: "${RPC:?需要 RPC=本链 RPC}"
: "${DOMAIN:?需要 DOMAIN=本链对外域名}"

case "$SRC" in /*) ;; *) SRC="$(cd "$SRC" && pwd)";; esac
STACK_DIR="$ROOT/$NAME"
if [ "$(id -u)" = 0 ]; then SUDO=""; else SUDO="sudo"; fi

env_val() { grep -E "^$1=" "$STACK_DIR/.env" | tail -1 | cut -d= -f2-; }

echo "===== 1/5 生成栈目录 + .env ====="
env NAME="$NAME" PORT="$PORT" RPC="$RPC" DOMAIN="$DOMAIN" ROOT="$ROOT" SRC="$SRC" \
    CSRF_ORIGIN="$CSRF_ORIGIN" START=0 bash "$DEPLOY_DIR/setup_stack.sh"

# .env 除「栈身份」项外的所有值（含 DJANGO_SECRET_KEY / SETUP_SAFE_ADDRESSES）都由
# deploy/env/stack.env.example 固定，脚本不读也不写；要改请编辑模板或改栈自己的 .env 后 up -d

echo "===== 2/5 构建并启动 ====="
cd "$STACK_DIR"
docker compose build
docker compose up -d
for _ in $(seq 1 60); do
    curl -sf --max-time 3 "http://127.0.0.1:$PORT/api/v1/about/" >/dev/null && break
    sleep 5
done
echo "== 栈已就绪：http://127.0.0.1:$PORT/api/v1/about/"

echo "===== 3/5 证书 + 宿主 nginx 转发 ====="
CERT="/etc/letsencrypt/live/$DOMAIN/fullchain.pem"
if [ "${SKIP_CERT:-0}" = 1 ] || { [ -f "$CERT" ] && [ -f "/etc/letsencrypt/live/$DOMAIN/privkey.pem" ]; }; then
    echo "== 证书已存在（或 SKIP_CERT=1），跳过签发：$CERT"
else
    command -v certbot >/dev/null || { echo "未找到 certbot：sudo apt install -y certbot python3-certbot-nginx"; exit 1; }
    echo "== 签发证书（域名必须已解析到本机，否则会失败）"
    $SUDO certbot certonly --nginx -d "$DOMAIN" \
        --agree-tos --no-eff-email ${ADMIN_EMAIL:+-m "$ADMIN_EMAIL"} --keep-until-expiring
fi
$SUDO env NAME="$NAME" DOMAIN="$DOMAIN" PORT="$PORT" bash "$DEPLOY_DIR/setup_host_nginx.sh"

echo "===== 4/5 后台账户 ====="
if [ "${SKIP_SUPERUSER:-0}" = 1 ]; then
    echo "== SKIP_SUPERUSER=1，跳过"
else
    has_su="$(docker compose exec -T web python manage.py shell -c \
        "from django.contrib.auth import get_user_model; print(get_user_model().objects.filter(is_superuser=True).exists())" \
        2>/dev/null | tr -d ' \r' | tail -1)"
    if [ "$has_su" = "True" ]; then
        echo "== 已存在 superuser，跳过"
    elif [ -n "${SU_USER:-}" ] && [ -n "${SU_EMAIL:-}" ] && [ -n "${SU_PASS:-}" ]; then
        docker compose exec -T -e DJANGO_SUPERUSER_PASSWORD="$SU_PASS" web \
            python manage.py createsuperuser --noinput --username "$SU_USER" --email "$SU_EMAIL"
        echo "== 已创建 $SU_USER"
    else
        echo "== 下面交互式创建（不想建可 Ctrl-C，下次带 SKIP_SUPERUSER=1）"
        docker compose exec web python manage.py createsuperuser
    fi
fi

echo "===== 5/5 验收 ====="
env PORTS="$PORT:$CHAIN_ID" bash "$DEPLOY_DIR/verify.sh"

cat <<EOF

===== 完成 =====
  栈目录   $STACK_DIR
  对外地址 https://$DOMAIN/
  管理后台 https://$DOMAIN/admin/
  索引日志 cd $STACK_DIR && docker compose logs -f indexer-worker
EOF
if [ "$(env_val SETUP_SAFE_ADDRESSES)" = "0" ]; then
    cat <<EOF

⚠️ SETUP_SAFE_ADDRESSES=0（自定义链）：登录 https://$DOMAIN/admin/ 录入
   Master Copies（勾 l2、version 填 1.3.0+L2、initial/tx block number 填部署块）
   与 Proxy Factories，保存即开始索引，无需重启。
EOF
fi
