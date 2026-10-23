# Safe Transaction Service v6.11.0 — Docker 部署文档

```bash
cd /root/safe/transactionService/safe-transaction-service   # 后文所有命令的工作目录
```

## 1. 架构概览

`docker-compose.yml` 启动 8 个服务：

| 服务 | 端口 | 作用 |
|---|---|---|
| `nginx` | **8000** | 统一入口（反向代理） |
| `web` | 8888 | Gunicorn Django API |
| `db` | 5432 | PostgreSQL 18 |
| `redis` | 6379 | 缓存 |
| `rabbitmq` | 5672 | Celery broker |
| `indexer-worker` / `contracts-tokens-worker` / `scheduler` | — | Celery worker / 定时任务 |
| `flower`（可选，`--profile debug`） | 5555 | 队列监控 |

### 1.1 各容器职责

| 容器 | 关键职责 | 备注 |
|---|---|---|
| `nginx` | 反向代理，upstream 指向 gunicorn unix socket | 与 `web` 共用 `nginx-shared` 卷传 socket |
| `web` | Gunicorn Django API，处理 HTTP（`/api/v1/*`、`/admin`） | **不依赖 rabbitmq**；改 `.env` 后必须 `up -d --build`（镜像内嵌） |
| `db` | PostgreSQL 18，存全部业务数据 | **一库 = 一链**（`Chain` 表单行 + `check_chainid_matches` 启动硬校验） |
| `redis` | 缓存层（`cache_txs_view_for_address` 等） | **不是** Celery broker |
| `rabbitmq` | Celery broker + 事件队列（`events/services/queue_service.py`） | 多链共用 broker 时 `EVENTS_QUEUE_EXCHANGE_NAME` 必须改名 |
| `indexer-worker` | 消费 `default` / `indexing` / `processing` 队列；**唯一执行迁移 + `setup_service` + `check_chainid_matches`** | `RUN_MIGRATIONS=1`，链的初始化入口 |
| `contracts-tokens-worker` | 消费 `contracts` / `tokens` 队列（ABI、token 元信息抓取） | 与主索引器隔离，避免慢 IO 阻塞追块 |
| `scheduler` | `celery beat`（DatabaseScheduler，按 `CeleryTaskConfiguration` 周期触发） | **单实例**，否则重复调度 |
| `ganache` | 本地测试链（chainId 1337） | 仅 `--profile develop` 启用，生产不启 |
| `flower` | Celery 任务监控 UI（`http://localhost:5555/`） | 仅 `--profile debug` 启用，无鉴权，生产勿开

关键机制：

- **数据库迁移自动完成**（`indexer-worker` 的 `RUN_MIGRATIONS=1`），无需手动执行。
- 启动时 `check_chainid_matches` 校验 RPC 链 ID 与数据库一致，防止换链导致数据错乱。
- 两种索引模式：**L2 事件索引（推荐，普通 RPC 即可，适用于 Safe L2 合约 v1.3.0+ 的绝大多数 EVM 链）**；tracing 模式（仅旧版非 L2 合约的链，需支持 `trace_*` 的节点，如 Erigon）。

## 2. 部署步骤

### 2.1 环境核对

要求：Docker ≥ 24、Compose v2、可用内存 ≥ 4GB、磁盘剩余 ≥ 10GB、端口 8000/8888/5432/6379/5672 空闲。当前系统均已满足。

### 2.2 配置 `.env`

```bash
cp .env.l2.sample .env
openssl rand -hex 32    # 输出替换 .env 中的 DJANGO_SECRET_KEY
vim .env
```

需修改的变量（其余保持默认；`DATABASE_URL`/`REDIS_URL`/`CELERY_BROKER_URL` 中主机名是 compose 服务名，勿改成 localhost）：

| 变量 | 值 |
|---|---|
| `DJANGO_SECRET_KEY` | ⚠️ 必改，用上面的随机串 |
| `ETHEREUM_NODE_URL` | ⚠️ 必改，目标链 RPC，如 BSC 测试网 `https://bsc-testnet.publicnode.com`（chain ID 97，合约自动配置）或 Polygon `https://polygon-rpc.com/` |
| `ETH_L2_NETWORK` | L2 模式保持 `1` |
| `SETUP_SAFE_ADDRESSES` | 默认 `1`：启动时自动把内置链（safe-eth-py 收录）的官方 Safe 合约地址写入索引表；设 `0`：**不**写入这些内置地址，只索引你在 Admin 手动录入的合约（即第 4 节的手动模式，内置链同样适用），重启也不会写回 |
| `CSRF_TRUSTED_ORIGINS` | ⚠️ 必改。填浏览器实际访问地址，本地：`http://127.0.0.1:8000,http://localhost:8000`；生产挂域名：`https://你的域名` |

> `CSRF_TRUSTED_ORIGINS` 规则：填浏览器地址栏的 `协议://域名[:端口]`（与实际访问地址一致，不带路径和尾斜杠，多个用逗号分隔）。不设置或与访问地址不符时，admin 登录等浏览器 POST 会返回 403；API/SDK 调用（curl 等）不发 Origin 头，不受影响。修改后需 `docker compose up -d` 重建生效。
>
> 同主域下子域名多或易变时，域名段可用 `*` 通配子域，例如 `http://*.dplife.org` 一次配置覆盖 `wallet-multi-sig.dplife.org`、`wallet.dplife.org` 等所有子域，换子域名时无需再改。

> 内置支持的链（见 [safe-eth-py 支持列表](https://github.com/safe-global/safe-eth-py/blob/master/gnosis/safe/addresses.py)）会自动配置合约；自定义 EVM 链见第 4 节。
> 公开 RPC 可能限制 `eth_getLogs` 块范围，索引慢或报错时换带 API key 的端点。**免费公共节点通常直接拒绝历史区块查询**（BSC 的 `bsc-dataseed*.bnbchain.org` 对 2021 年区块返回 `-32005 limit exceeded`，`publicnode` 提示 archive 需 token）：需要从合约部署块追历史的链必须用带 key / 支持 archive 的 RPC，否则索引一直卡在部署块（`/api/v1/about/indexing/` 的 `masterCopiesBlockNumber` 不动，日志刷 `Request error retrieving events`）。

**Tracing 模式**（仅旧版非 L2 合约的链）：改用 `cp .env.tracing.sample .env`，额外配置 `ETHEREUM_TRACING_NODE_URL`（支持 `trace_*` 的节点），`ETH_L2_NETWORK=0`。

### 2.3 构建与启动

```bash
docker compose build --force-rm   # 首次约 5–15 分钟
docker compose up -d
```

首次启动：`indexer-worker` 自动执行迁移并初始化（`setup_service`），随后从 Safe 合约部署块开始追块。追平需数小时属正常，观察日志：

```bash
docker compose logs -f indexer-worker   # 出现 "Running Celery worker for queues ..." 即正常
```

`SETUP_SAFE_ADDRESSES=0` 时启动**不会写入任何内置合约**，日志中以下两行属预期（录入自定义合约后才会开始索引）：

```
Skipping built-in Safe MasterCopy/ProxyFactory setup (SETUP_SAFE_ADDRESSES is disabled). Only manually added addresses will be used
Cannot find any SafeMasterCopy and ProxyFactory for chain id 97
```

> `.env` 会被打进镜像（`Dockerfile` 的 `COPY . .`），改动后必须 `docker compose up -d --build`（或先 `docker compose build`）才生效，`restart` 不读新 `.env`。

### 2.4 验证

```bash
docker compose ps                                     # 8 个服务全部 Up
curl 'http://localhost:8000/api/v1/about/'             # 返回 JSON 即部署成功
curl 'http://localhost:8000/api/v1/about/indexing/'    # 索引进度，追平后 synced 为 true

# 管理后台（⚠️ 重要：自定义链部署必用——录入合约的唯一入口；内置链可选）
docker exec -it safe-transaction-service-web-1 python manage.py createsuperuser
# 浏览器访问 http://127.0.0.1:8000/admin/（用 127.0.0.1 或 localhost 访问，勿用局域网 IP）
# 登录返回 403 → 未配 CSRF_TRUSTED_ORIGINS，见 2.2 节；配好后需 docker compose up -d 重建

# 队列监控（可选）
docker compose --profile debug up -d flower    # http://localhost:5555/
```

## 3. 运维速查

```bash
docker compose logs -f <service>     # 日志（web / indexer-worker / ...）
docker compose restart web           # 重启单服务进程（不读新 .env）
docker compose up -d                 # 修改 .env 后精确重建受影响容器（无变化则零操作，幂等可随时执行）

# 全部停止 / 启动（数据卷均保留）
docker compose stop                  # 停止全部容器（容器保留）
docker compose start                 # 原样拉起保留的容器（秒级）
docker compose down                  # 停止并删除容器 + 网络（数据卷保留，数据不丢）
docker compose restart               # 全部重启（沿用旧环境变量，不读新 .env）

# 彻底重来（含重建镜像，改了源码/依赖时用）
docker compose down && docker compose build --force-rm && docker compose up -d
```

> 语义速记：`stop`/`start` 是"暂停键"；`down`/`up -d` 是"关机重开"；`restart` 只重启进程、**不读新 `.env`**（改配置后必须用 `up -d`）。仅 `down -v` 会删数据库（见第 6 节）。

索引修复（在 web 容器内，`docker exec -it safe-transaction-service-web-1 bash`）：

| 命令 | 用途 |
|---|---|
| `python manage.py check_index_problems` | 自动修复缺失交易（较慢） |
| `python manage.py reindex_master_copies --from-block-number <块高> [--addresses 0x111 ...]` | 已知问题块时手动重索引（更快） |
| `python manage.py reindex_erc20 --from-block-number <块高> [--addresses ...]` | 修复缺失的 ERC20/721 转账 |

常见问题：

| 现象 | 处理 |
|---|---|
| `safes/{address}` 的 nonce 已更新但交易列表没有 | 正常：前者实时 `eth_call`，后者依赖索引，延迟约半分钟 |
| ERC20 未索引 | 代币需实现 `name`/`symbol`/`decimals`/`balanceOf`，否则记入 `TokenNotValid` |
| worker 反复重启报 chainid 不匹配 | RPC 换到了另一条链，须清库重部署（见第 6 节） |
| 同链换 RPC 供应商 | 直接改 `ETHEREUM_NODE_URL` 后 `docker compose up -d`，链 ID 不变不触发保护 |
| 登录 admin 返回 403（JSON） | `.env` 加 `CSRF_TRUSTED_ORIGINS=http://*.dplife.org`（子域名通配，今后换子域名无需再改），`docker compose up -d` 重建后重试 |
| 登录成功又跳回登录页 | 会话 cookie 仅 HTTPS 生效：用 Chrome 访问 `http://127.0.0.1:8000`，或前置 HTTPS |
| 不想索引内置链的官方合约，或删除后重启又写回 | `.env` 设 `SETUP_SAFE_ADDRESSES=0` 后 `docker compose up -d --build`（重启不再写入）；库中已有的旧记录需删一次：Admin 的 `SafeMasterCopy` / `ProxyFactory` 两张表，或 `docker compose exec -T web python manage.py shell -c "from safe_transaction_service.history.models import SafeMasterCopy, ProxyFactory; SafeMasterCopy.objects.all().delete(); ProxyFactory.objects.all().delete()"` |

链重组自动处理，无需干预：未达确认深度（`ETH_REORG_BLOCKS`，L2 默认 200）的块会定期校验，重组时自动回退重扫。

## 4. 自定义 EVM 链部署

内置支持列表之外的链（自建/联盟链等），`setup_service` 不会自动配置合约；**内置链**若也想完全手动管理合约地址（不用官方数据），把 `.env` 的 `SETUP_SAFE_ADDRESSES` 设为 `0`（见 2.2 节），之后流程与本节相同，需：

**1）链上已部署 Safe L2 合约**：`GnosisSafeL2`（v1.3.0+）与 `ProxyFactory`，未部署则按 [safe-contracts 部署说明](https://github.com/safe-global/safe-contracts/tree/v1.3.0/contracts) 操作（单例为确定性部署，各链地址相同，如 L2 单例 `0x69f4d1788e39c87893c980c06edf4b7f686429d2`）。记录两个合约的部署块高。

**2）`.env`**：同 2.2 节，`ETHEREUM_NODE_URL` 指向自建节点，`ETH_L2_NETWORK=1`（旧版非 L2 合约只能走 tracing 模式）。

**3）启动后登录 `http://localhost:8000/admin/` 手动录入**（保存即开始索引，无需重启）：

| 模型 | 录入字段 |
|---|---|
| **Master Copies** | `address`（GnosisSafeL2 地址）、`version`（如 `1.3.0+L2`）、**`l2` 勾选**（事件索引按此字段启用，务必勾上）、`initial block number`（部署块高）、`tx block number`（填同 initial 的值，见下方说明） |
| **Proxy Factories** | `address`（ProxyFactory 地址）、`initial block number`（部署块高）、`tx block number`（填同 initial 的值） |

> `tx block number` 含义是"索引器**已扫描到**的块高"（扫描断点，重启后从此续扫），与 `initial block number`（合约部署块）含义不同。新录入时填与 initial 相同的值，索引即从部署块开始扫。**拿不准往小填**（重扫无害）；**勿填大**——填大于实际进度会跳过中间区块，漏掉的数据不会自动补回。`deployer` 仅为备注标签，随意。

**4）自建链建议**：

- 几乎无重组的链可在 `.env` 设 `ETH_REORG_BLOCKS=10`（默认 200）加快确认。
- 使用唯一固定的 EIP-155 chain ID（勿用 1337 等默认值）；chain ID 入库受启动保护，换链必须清库。
- 想被官方收录：先在 [ethereum-lists/chains](https://github.com/ethereum-lists/chains) 注册，再向 safe-eth-py 提 PR（自用无需）。

## 5. 部署一条链（脚本化）

**一实例 = 一链**（`Chain` 表单行 + `check_chainid_matches` 启动硬校验）：每条链一套完整 stack（nginx + web + db + redis + rabbitmq + 2 个 worker + beat），中间件各起各的，互不干扰。

所以**部署 N 条链 = 把本节流程跑 N 次**（只有 4 个参数不同）。本节是推荐流程（脚本自动化），第 2 节是手工步骤的原理说明，照本节跑时不用看。

端口与目录编排自己定，脚本不依赖这张表：

| 链 | `NAME`（= 目录名 = 项目名） | 宿主端口 | chainId |
|---|---|---|---|
| BSC 主网 | `tx-bsc` | 8001 | 56 |
| BSC 测试网 | `tx-bsctest` | 8002 | 97 |
| FUNS 主网 | `tx-funs` | 8003 | 由 RPC 自动识别 |

各栈 nginx 只绑 `127.0.0.1`，对外统一由**宿主 nginx 按域名转发**。

### 5.1 部署第一条链

**一行版**（下面 4 步按顺序自动串起来，幂等，失败修完重跑即可）：

```bash
cd ~/safe/transactionService/safe-transaction-service   # 代码目录（内含 deploy/）；root 时 ~ 即 /root
NAME=tx-bsc PORT=8001 RPC=https://bsc-dataseed.bnbchain.org/ DOMAIN=bsc-tx.dplife.org \
CHAIN_ID=56 bash deploy/deploy_stack.sh
```
可选：`SKIP_CERT=1`（已有证书）、`SKIP_SUPERUSER=1`、`SU_USER/SU_EMAIL/SU_PASS`（免交互建账户）、`ADMIN_EMAIL`（certbot 通知邮箱）。

拆开手动执行（想逐步看输出时）：

```bash
cd ~/safe/transactionService/safe-transaction-service   # 代码目录（内含 deploy/）；root 时 ~ 即 /root

# 0) 非 root 登录时先做一次：把自己加入 docker 组，之后免 sudo 跑 compose（重登录后生效）
sudo usermod -aG docker $USER && newgrp docker

# 1) 生成栈目录 + .env 并启动（START=1 才启动，不加则只生成配置）
#    ROOT 默认 /root/safe，非 root 用户 mkdir 会 Permission denied，必须显式指到自己的家目录
#    DOMAIN 必须是具体主机名（证书路径与 certbot 都按它走），不能填 *.dplife.org
#    CSRF_ORIGIN 可选：想让同主域下换子域不再改配置，就传通配（只在首次生成 .env 时生效）
NAME=tx-bsc PORT=8001 RPC=https://bsc-dataseed.bnbchain.org/ DOMAIN=bsc-tx.dplife.org \
CSRF_ORIGIN=https://*.dplife.org ROOT=$HOME/safe START=1 bash deploy/setup_stack.sh

# 2) 宿主 nginx：证书 + 转发（需 root；证书不存在时脚本会提示 certbot 命令）
#    用 sudo env VAR=... 而不是 sudo VAR=...，否则 sudo 会吞掉环境变量
sudo certbot certonly --nginx -d bsc-tx.dplife.org
sudo env NAME=tx-bsc DOMAIN=bsc-tx.dplife.org PORT=8001 bash deploy/setup_host_nginx.sh

# 3) 管理后台账户（⚠️ 自定义链必做：Admin 是录入合约的唯一入口；内置链可选）
cd $HOME/safe/tx-bsc && docker compose exec web python manage.py createsuperuser
# 浏览器访问 https://bsc-tx.dplife.org/admin/（走域名，不要直连 127.0.0.1:8001，
# 否则会话 cookie 仅 HTTPS 生效 + CSRF Origin 不匹配，会登录失败）

# 4) 验收
PORTS="8001:56" bash deploy/verify.sh
```

> 第 2 步的两个前置条件（本机首次执行时）：① `certbot` 需预装 —— `sudo apt install -y certbot python3-certbot-nginx`，未装时命令报 `command not found`；② `DOMAIN` 必须**已解析到本机**（A/AAAA 都指向本机公网 IP，Cloudflare 等代理后要确保子域记录存在），否则 `certbot certonly --nginx` 签发失败，`setup_host_nginx.sh` 会在证书检查处退出并打印签发命令。
> 域名暂不可解析时，可用自签证书先验证转发链路：`sudo mkdir -p /etc/letsencrypt/live/$DOMAIN && sudo openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=$DOMAIN" -keyout /etc/letsencrypt/live/$DOMAIN/privkey.pem -out /etc/letsencrypt/live/$DOMAIN/fullchain.pem`，然后照常跑第 2 步，用 `curl -sk --resolve $DOMAIN:443:127.0.0.1 https://$DOMAIN/api/v1/about/` 检查（应返回 JSON 且 `secure=true`，`/admin/login/` 返回 200）。验证完记得删掉该配置与证书目录，否则证书失效后 nginx 会加载失败。

### 5.2 再部署一条链

同一条命令，只换 4 个参数（镜像已存在，不会再构建）：

```bash
NAME=tx-funs PORT=8003 RPC=<FUNS RPC> DOMAIN=funs-tx.dplife.org ROOT=$HOME/safe START=1 \
bash deploy/setup_stack.sh
sudo env NAME=tx-funs DOMAIN=funs-tx.dplife.org PORT=8003 bash deploy/setup_host_nginx.sh
cd $HOME/safe/tx-funs && docker compose exec web python manage.py createsuperuser   # 后台账户
PORTS="8003:" bash deploy/verify.sh
```

### 5.3 脚本替你做了什么

`setup_stack.sh` 从 `deploy/env/stack.env.example` 生成 `.env` 并自动填好这些键（**已有的 `.env` 不覆盖**）：

| 你提供的参数 | 脚本写入 |
|---|---|
| `NAME` | `COMPOSE_PROJECT_NAME`（同时是目录名） |
| `PORT` | `NGINX_HOST_PORT`（并写 `NGINX_HOST_BIND=127.0.0.1`） |
| `RPC` | `ETHEREUM_NODE_URL` |
| `DOMAIN` | `CSRF_TRUSTED_ORIGINS=https://$DOMAIN`（子域通配用 `CSRF_ORIGIN=https://*.主域` 覆盖；**仅在首次生成 `.env` 时写入**，已存在的 `.env` 需自己 `sed` 改这一行后 `docker compose up -d`） |
| `ROOT` | 栈根目录，默认 `/root/safe`；**非 root 登录必须显式传 `ROOT=$HOME/safe`**，否则 `mkdir` 报 Permission denied |
| — | 其余（`PYTHONPATH` / `DJANGO_SETTINGS_MODULE` / `DEBUG` / `DATABASE_URL` / `ETH_L2_NETWORK` / `REDIS_URL` / `CELERY_BROKER_URL`）沿用模板默认值，各栈相同，无需改动 |

**脚本不碰、值固定在模板里的三项**（`setup_stack.sh` / `deploy_stack.sh` 都不读也不写）：

- `DJANGO_SECRET_KEY`：每栈各自一份，`openssl rand -hex 32`
- `SETUP_SAFE_ADDRESSES`：`1` = 内置链（`safe-eth-py` 收录）自动写入官方合约；`0` = 自定义链（如 FUNS），稍后 Admin 手动录入
- `DOCKER_IMAGE`：镜像 tag，固定 `safe-tx:6.11.0`，多栈共用、只构建一次（脚本不再接受 `IMAGE=` 参数）

要改请**编辑 `deploy/env/stack.env.example`**（只影响之后新建的栈；已有栈改各自 `.env` 后 `docker compose up -d`）。
脚本只写「栈身份」相关的 6 个键：`COMPOSE_PROJECT_NAME` / `NGINX_HOST_BIND` / `NGINX_HOST_PORT` / `ETHEREUM_NODE_URL` / `CSRF_TRUSTED_ORIGINS` / `SRC_DIR`。

其余人工事项：**证书签发**（certbot，一次）、**后台账户创建**（每栈一次，`docker compose exec web python manage.py createsuperuser`）和**自定义链的 Admin 录入**（见 5.6）。

### 5.4 ⚠️ 数据卷跟着 `NAME` 走，别改

`db` 服务没挂具名卷，pgdata 落在**按项目名命名的匿名卷**（`<COMPOSE_PROJECT_NAME>_xxx`）。`NAME` 一改 = 卷名变了 = 服务建一个新空库，已索引的数据"消失"（旧卷其实还在，只是没人挂）。

已存在的栈要纳入这套管理：**不要迁目录**，只做两件事——

```bash
# ① 在其 .env 补齐（COMPOSE_PROJECT_NAME 填它现在的项目名，通常是目录名，别改）
COMPOSE_PROJECT_NAME=<原项目名>
NGINX_HOST_BIND=127.0.0.1
NGINX_HOST_PORT=<分配的端口>
DOCKER_IMAGE=safe-tx:6.11.0
# ② 加宿主 nginx 转发
sudo env NAME=<原项目名> DOMAIN=<域名> PORT=<分配的端口> bash deploy/setup_host_nginx.sh
```

改完 `docker compose up -d`，先 `curl http://127.0.0.1:<端口>/api/v1/about/` 确认 chainId 没变，再动别的。

### 5.5 `docker-compose.yml` 为多栈做的三处调整

| 改动 | 说明 |
|---|---|
| `nginx.ports: ${NGINX_HOST_BIND:-0.0.0.0}:${NGINX_HOST_PORT:-80}:8000` | 每栈用 `.env` 指定自己的宿主端口，避免 80 冲突 |
| 删掉 `db`/`redis`/`rabbitmq`/`web` 的宿主端口 | 彻底消除端口冲突；调试用 `docker compose exec db psql -U postgres` |
| 加 `image: ${DOCKER_IMAGE:-safe-tx:6.11.0}` | 多栈复用同一个镜像，只构建一次 |

### 5.6 HTTPS 与自定义链录入

所有栈都在 nginx 之后，`config/settings/production.py` 已加 `SECURE_PROXY_SSL_HEADER`，容器 `docker/nginx/nginx.conf` 也已改为**透传**宿主 nginx 的 `X-Forwarded-Proto`（不要再写回 `$scheme`，否则 Django 永远认为请求是 http，admin 的 CSRF Origin 校验会 403）。

`SETUP_ADDRESSES=0`（自定义链）启动后，日志出现 `Skipping built-in ...` / `Cannot find any SafeMasterCopy and ProxyFactory` 属预期。登录 `https://<本链域名>/admin/` 按第 4 节第 3 步录入 **Master Copies**（务必勾 `l2`、`version` 填 `1.3.0+L2`）与 **Proxy Factories**，保存即开始索引，无需重启。

### 5.7 资源与运维

- 建议 8 核 / 32GB 内存 / NVMe ≥ 500GB：BSC 主网全量索引的库增长最快，定期 `df -h` 与各栈 `docker compose exec db psql -U postgres -c "select pg_size_pretty(pg_database_size('postgres'))"`。
- `CELERYD_CONCURRENCY` 默认 5000；追块期打满 RPC 时往该栈 `.env` 加一行 `CELERYD_CONCURRENCY=1000` 后 `docker compose up -d`。
- 备份（每栈独立）：`docker compose exec -T db pg_dump -U postgres postgres > /backup/<链>-$(date +%F).sql`
- 升级代码：各栈 `git pull` 后 `docker compose build && docker compose up -d`，逐链滚动。
- 某条链要换到另一条链：只对该栈 `docker compose down -v` 清库（其余栈不受影响）。

## 6. 停止与清理

```bash
docker compose down      # 停止并移除容器（数据库数据保留）
docker compose down -v   # ⚠️ 清空数据库（不可逆）：换链 / 换 chain ID 前必须执行
```
