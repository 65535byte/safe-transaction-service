#!/usr/bin/env bash
# 宿主 nginx：给「一条链」生成转发配置（按域名 → 该栈 127.0.0.1:<PORT>）并 reload。需 root。
#
# 用法（用 sudo env 而不是 sudo VAR=，否则 sudo 会吞掉环境变量）：
#   sudo env NAME=tx-bsc DOMAIN=bsc-tx.dplife.org PORT=8001 bash deploy/setup_host_nginx.sh
#
# 参数：
#   NAME    栈名（决定文件名 /etc/nginx/conf.d/safe-tx-<NAME>.conf，重跑覆盖同名文件）
#   DOMAIN  对外域名（证书默认路径按它推导）
#   PORT    该栈 nginx 在宿主的监听端口
#   CERT    证书路径（默认 /etc/letsencrypt/live/<DOMAIN>/fullchain.pem）
#   KEY     私钥路径（默认 /etc/letsencrypt/live/<DOMAIN>/privkey.pem）
set -euo pipefail

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${NAME:?需要 NAME=栈名，如 tx-bsc}"
: "${DOMAIN:?需要 DOMAIN=本链对外域名}"
: "${PORT:?需要 PORT=该栈在宿主的监听端口}"

CERT="${CERT:-/etc/letsencrypt/live/$DOMAIN/fullchain.pem}"
KEY="${KEY:-/etc/letsencrypt/live/$DOMAIN/privkey.pem}"

[ "$(id -u)" = 0 ] || {
    echo "需 root 执行："
    echo "  sudo env NAME=$NAME DOMAIN=$DOMAIN PORT=$PORT bash $DEPLOY_DIR/setup_host_nginx.sh"
    exit 1
}
command -v nginx >/dev/null || { echo "未找到 nginx"; exit 1; }

if [ ! -f "$CERT" ] || [ ! -f "$KEY" ]; then
    echo "证书或私钥不存在："
    echo "  $CERT"
    echo "  $KEY"
    echo
    echo "先签发（域名须已解析到本机、80 端口可达）："
    echo "  sudo certbot certonly --nginx -d $DOMAIN"
    echo "或先放自签证书，然后带 CERT=/KEY= 重跑本脚本。"
    exit 1
fi

# 日志格式只能定义一次，单独放 00- 前缀文件（按文件名字母序先于各栈 conf 被 include）
LOGCONF="/etc/nginx/conf.d/00-safe-tx-logging.conf"
cat >"$LOGCONF" <<'EOF'
log_format safe_tx '$remote_addr [$time_local] host=$host "$request" status=$status '
                   'upstream=$upstream_addr ustatus=$upstream_status '
                   'rt=$request_time urt=$upstream_response_time xff="$http_x_forwarded_for"';
EOF

out="/etc/nginx/conf.d/safe-tx-$NAME.conf"
sed -e "s#@NAME@#$NAME#g" -e "s#@DOMAIN@#$DOMAIN#g" -e "s#@PORT@#$PORT#g" \
    -e "s#@CERT@#$CERT#g" -e "s#@KEY@#$KEY#g" \
    "$DEPLOY_DIR/host-nginx/site.conf.example" >"$out"

nginx -t && { systemctl reload nginx 2>/dev/null || nginx -s reload; }

echo "== 已写入 $out 并 reload nginx"
echo "   https://$DOMAIN/ → 127.0.0.1:$PORT"
echo "   检查：curl -s https://$DOMAIN/api/v1/about/ | head -c 200"
echo "   访问日志（含 upstream=127.0.0.1:$PORT 即证明转发到了本栈）："
echo "     sudo tail -f /var/log/nginx/safe-tx-$NAME.access.log"
