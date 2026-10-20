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
> 公开 RPC 可能限制 `eth_getLogs` 块范围，索引慢或报错时换带 API key 的端点。

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

> 语义速记：`stop`/`start` 是"暂停键"；`down`/`up -d` 是"关机重开"；`restart` 只重启进程、**不读新 `.env`**（改配置后必须用 `up -d`）。仅 `down -v` 会删数据库（见第 5 节）。

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
| worker 反复重启报 chainid 不匹配 | RPC 换到了另一条链，须清库重部署（见第 5 节） |
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

## 5. 停止与清理

```bash
docker compose down      # 停止并移除容器（数据库数据保留）
docker compose down -v   # ⚠️ 清空数据库（不可逆）：换链 / 换 chain ID 前必须执行
```
