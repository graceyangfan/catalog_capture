# Predict.fun 执行适配器设计

> 状态：设计草案，仅文档，不包含实现<br>
> 日期：2026-10-11<br>
> 目标：在现有 `nautilus-predict` 行情适配器上增加 NautilusTrader 风格的执行侧，覆盖订单、账户、成交、持仓、私有 WebSocket、重连和对账。

## 1. 结论

建议新增独立的 `PredictExecutionClient`，不要把执行逻辑塞进
`PredictDataClient`。两个客户端可以共享纯协议模型、市场元数据和签名代码，但应各自
拥有连接、任务、取消令牌和生命周期。

第一版采用下面的边界：

- `OmsType::Netting`、`AccountType::Cash`，一个 Predict 钱包对应一个
  `AccountId`。
- 每个 outcome 继续使用现有 `{market_id}-{index_set}.PREDICT` 作为
  `InstrumentId`；`raw_symbol` 中的 outcome `onChainId` 是下单的 `tokenId`。
- REST 负责认证、下单、查询和恢复；`predictWalletEvents/{jwt}` 负责实时订单和成交
  生命周期；BNB Chain RPC 负责 USDT 余额、授权和强取消的链上事实。
- `orderHash` 作为 Nautilus `VenueOrderId`。Predict 的十进制 `orderId` 必须保留在
  side map 和事件 `info` 中，但不能与 hash 混用。
- `orderTransactionSuccess` 才产生 `OrderFilled`；
  `orderTransactionSubmitted` 只是内部 `SETTLING` 状态，不能先发 fill 再在失败时回滚。
- `/orders/remove-by-hash` 只从订单簿移除，官方明确说明它不会做链上取消。因此 HTTP
  200 不能直接转换成 `OrderCanceled`。
- 初版支持单笔 LIMIT/MARKET、查询、批量报告和强取消。`ModifyOrder` 先明确拒绝；在
  第二阶段实现“确认旧单不可成交后再提交新单”的 cancel-replace。
- 行情快照里的 `lastOrderSettled`、`settlementsPending` 是市场级信息，绝不能作为某个
  账户的 fill、position 或 account 真相源。

最接近的 Nautilus 参考实现是 Polymarket，而不是中心化交易所适配器：两者都是 outcome
token、现金账户、链下订单簿加链上结算，并且都有“流事件 + REST 恢复 + 链上余额”的多源
一致性问题。结构和生命周期应参考 Polymarket，协议字段和签名必须以 Predict 官方资料为准，
不能复制 Polymarket 的 wire model。

## 2. 调研基线与证据边界

### 2.1 本仓库现状

当前 `crates/nautilus-predict` 已经有：

- `PredictDataClient`：公共 WebSocket、心跳、重连、订阅引用计数和动态市场轮换；
- `PredictHttpClient`：市场、分类、搜索和订单簿等公共 REST；
- `PredictInstrumentProvider`：二元市场的 YES/NO `BinaryOption` 定义；
- `PredictMarketContext`：`market_id`、两个 outcome instrument、价格/数量精度；
- 自定义 `PredictOrderbookSnapshot` 及 catalog 序列化。

当前没有：

- `ExecutionClient` 实现和 `ExecutionClientFactory`；
- 私有 JWT 认证、钱包事件订阅；
- EIP-712 order builder、签名和 order hash；
- 私有订单、账户活动、positions REST models；
- 订单关联表、pending submit/cancel、fill 去重和启动对账；
- BNB Chain balance/approval/cancel 交互。

当前 instrument 设计有一个已知表示边界：Predict 链上金额是 18 位 wei，Nautilus fixed
quantity 最多 16 位；`Currency::USDT()` 当前是 8 位。实现必须在 wire 层一直保留整数 wei，
只在 Nautilus 事件边界做一次显式、可测试的舍入，并在 `info` 中保留原始 wei。禁止经过
`f64`。

### 2.2 Nautilus 对照版本

设计审计了相邻 `../nautilus_trader` 的本地提交
`52b6b2427d412d5214908d1bd7f4afb0478a0fe8`，重点包括：

- `crates/common/src/clients/execution.rs` 的 `ExecutionClient` 合约；
- `crates/live/src/execution/emitter.rs` 和 `ExecutionClientCore`；
- `crates/adapters/polymarket/src/execution/` 的 lifecycle、orders、reports、
  reconciliation、pending 和 websocket dispatch；
- `crates/adapters/polymarket/src/config.rs`、`factories.rs` 和集成测试。

该 sibling checkout 目前有未提交的 execution-action 相关改动。因此本文只把当前
`ExecutionClient` 的稳定订单/报告接口作为实现目标；`ExecuteAction` 用于 approve、redeem、
merge、split 的扩展方案必须等上游接口稳定后再单独设计，不能让第一版执行适配器依赖一个
分支私有 API。

### 2.3 Predict 官方事实

以下事实来自 2026-10-11 可见的官方文档和官方 SDK：

- Mainnet 所有 API 请求需要 `x-api-key`，个人操作还需要 wallet JWT；JWT 通过
  `/v1/auth/message` 与 `/v1/auth` 获取。[认证指南](https://dev.predict.fun/ts-how-to-authenticate-your-api-requests-663127m0)
- 创建订单是 `POST /v1/orders`，要求 JWT；返回 `orderId`、`orderHash`、`code` 和
  `removalLockedUntil`。[创建订单](https://dev.predict.fun/create-an-order-32534694e0)
- 私有 WebSocket topic 是 `predictWalletEvents/{jwt}`；事件包括 accepted、rejected、
  expired、cancelled、transaction submitted/success/failed。
  [订阅 topics](https://dev.predict.fun/subscription-topics-1915507m0)
- WebSocket 在握手时使用 API key，服务端每 15 秒发送 heartbeat；客户端必须原样回显
  timestamp，并在重连后重订阅。[WebSocket 总览](https://dev.predict.fun/general-information-1915499m0)、
  [心跳](https://dev.predict.fun/heartbeats-1915508m0)
- 重复 subscribe 是 no-op 并重新 ack；unsubscribe 即使 topic 不存在也成功。请求通过
  `requestId` 关联，错误包括 `invalid_credentials`。
  [请求格式](https://dev.predict.fun/request-format-1915501m0)
- 官方 SDK 的 LIMIT 计算会把 price 保留 3 个有效数字、quantity 保留 5 个有效数字；订单
  经过 EIP-712 签名并计算 hash。MARKET 订单要基于新鲜 orderbook 计算 maker/taker
  amount 和 slippage。[官方 TypeScript SDK](https://github.com/PredictDotFun/sdk)
- orderbook 只存 YES 价格；NO book 需要交换 bid/ask 并按市场精度取补数。
  [Understanding the Orderbook](https://dev.predict.fun/understanding-the-orderbook-685654m0)
- `/orders/remove-by-hash` 最多 100 条，只做 orderbook removal，不做 on-chain cancel。
  [Remove orders by hash](https://dev.predict.fun/remove-orders-by-hash-38139973e0)
- positions 通过 `/v1/positions` 分页返回，含 market、outcome、amount、value、average buy
  price 和 PnL。[Get positions](https://dev.predict.fun/get-positions-43539045e0)
- `/v1/account` 只返回账户身份、referral 和 points，不返回可交易余额。余额必须从链上
  USDT `balanceOf` 读取。[Get connected account](https://dev.predict.fun/get-connected-account-25326917e0)
- account activity 包含 order、match、convert、merge、split；公共 match endpoint 可按
  signer 和 maker/taker 过滤。[Account activity](https://dev.predict.fun/get-account-activity-32534697e0)、
  [Order matches](https://dev.predict.fun/get-order-match-events-25663812e0)

官方 API 仍标记 Beta，文档 schema 示例没有证明历史 activity/match 的每条记录都携带可与
`orderHash` 无歧义关联的 ID。历史 `FillReport` 的完整性因此是实现前必须用真实 fixture 或
testnet 录制确认的证据缺口；不能靠 market、price、quantity 和时间猜测关联。

## 3. 不变量

实现前应把下面这些条目作为验收合同，而不是建议。

### 3.1 Identity

1. `InstrumentId` 的完整身份是 `(market_id, index_set, PREDICT)`。
2. `tokenId` 必须来自该 instrument 的 `raw_symbol`，不能从 market ID 推导。
3. wallet event 的 `outcomeIndex` 不得直接当 `index_set` 使用。对于二元市场通常是
   `index_set = 1 << outcome_index`，仍必须与缓存的 market outcomes 验证后才接受。
4. `ClientOrderId` 只属于 Nautilus；Predict signed order 没有 client ID 字段。
5. `orderHash` 是 canonical `VenueOrderId`；`orderId` 是另一个协议标识，保留但不互换。
6. `settlementId`、`transactionHash`、WebSocket `requestId` 都是独立字段。
7. fill 去重键至少包含 `(order_hash, settlement_id, fill_leg)`；如果正式 payload 提供 log
   index 或 match ID，应纳入完整键。
8. 每个异步消息还属于一个 client generation；旧 generation 的回调不能写入新状态。

### 3.2 Lifecycle

1. 本地校验通过后，先记录 pending context 和 reservation，再发 `OrderSubmitted`，最后才
   执行外部 side effect。
2. POST 超时或断连是“结果未知”，不是 rejection。由于 order hash 在发送前就能计算，必须
   按 hash 查询恢复。
3. mutation 先完成，再发布事件；同步 msgbus 回调按可重入处理。
4. 任何 fill/cancel/expire 早于 accepted 到达时，先用已保存 context 合成且只合成一次
   `OrderAccepted`，再发布终态事件。
5. reconnect 不是清空订单真相；只替换 generation-owned socket、ack、buffer 和 task。
6. `stop`、`reset`、`dispose` 幂等。reset 必须先 drain 旧 task generation，再开新 generation。
7. JWT 失效时进入不可交易状态，刷新认证并完整重连；不能继续接受新的 submit command。

### 3.3 Routing

1. 公共 market data 与私有 wallet events 使用独立客户端连接和独立 event sender。
2. `PredictOrderbookSnapshot` 永远只进入 data path；wallet order/fill/account 只进入 execution
   path。
3. wallet event 的 `walletAddress` 必须与配置的 maker/deposit address 匹配。
4. topic 必须精确匹配当前 generation 的 `predictWalletEvents/{jwt}`；日志必须把 JWT
   redact，不能打印完整 topic。
5. private subscribe 的 ack 是 execution readiness 的一部分，不能像当前公共行情客户端
   那样把 ack 当纯 informational frame 忽略。

### 3.4 Money and state

1. wire order amount、balance、fill、fee 一律用 `U256`/十进制整数或 `Decimal`；禁止
   `f64` 往返。
2. USDT total 来自链上 `balanceOf(maker)`；free balance 等于 total 减去未完成 BUY 的
   最大剩余 maker liability，且最小为零。
3. reservation 必须在 submit 前建立，在 rejection/cancel/expire/fill 后按剩余量更新；失败
   不能先删 reservation 再假定请求未执行。
4. account mutation 完整后再发 `AccountState`；position 和 account 更新不能被一次早返回
   吞掉。
5. 缺失 position row 不默认表示 flat。第一版
   `provides_bulk_position_coverage` 应返回 `false`，直到通过 resolved/redeemed/zero-position
   fixtures 证明 API 的缺失语义。

## 4. 代码结构

建议的最终目录如下。第一阶段不要求一次建齐所有文件，按后文实施切片落地。

```text
crates/nautilus-predict/src/
├── common.rs                  # venue、instrument/market/outcome identity
├── config.rs                  # Data + Execution config
├── factories.rs               # Data + Execution factories
├── models.rs                  # public REST wire models
├── http/
│   ├── mod.rs
│   ├── client.rs              # shared transport/error envelope
│   ├── public.rs              # markets/orderbook
│   ├── private.rs             # auth/orders/activity/positions
│   └── models.rs              # exact private REST DTOs
├── websocket/
│   ├── mod.rs
│   ├── protocol.rs            # R/M envelopes, heartbeat, topic/ack
│   ├── data.rs                # existing public market session
│   └── execution.rs           # authenticated wallet session
├── signing/
│   ├── mod.rs
│   ├── order_builder.rs       # exact wei amount math
│   ├── eip712.rs              # typed data/hash/signature
│   └── contracts.rs           # chain/address selection and cancel calldata
└── execution/
    ├── mod.rs                 # PredictExecutionClient + ExecutionClient impl
    ├── actor.rs               # single writer; command/raw/completion select loop
    ├── lifecycle.rs           # connect/reconnect/reset/JWT/session generation
    ├── orders.rs              # validate/submit/cancel/cancel-all
    ├── dispatch.rs            # wallet event -> Nautilus events
    ├── pending.rs             # pending submit/cancel and unknown outcomes
    ├── context.rs             # client ID/hash/order ID/instrument correlation
    ├── reservations.rs        # cash and token reservation accounting
    ├── reports.rs             # Order/Fill/Position/Account reports
    └── reconciliation.rs      # startup/reconnect gap recovery and pagination
```

不要让 `execution/` 直接读取 `PredictDataClient` 的 mutable maps。可提取一个不可变、可 clone
的 `PredictInstrumentContext`，由 instrument 事件或 provider 提供给两个客户端。共享的是定义，
不是生命周期所有权。

### 4.1 `PredictExecutionClientConfig`

建议字段：

```rust
pub struct PredictExecutionClientConfig {
    pub account_id: AccountId,              // default PREDICT-001
    pub api_key: Option<SecretString>,       // or PREDICT_API_KEY
    pub private_key: Option<SecretString>,   // EOA or Privy signer key
    pub account_mode: PredictAccountMode,    // Eoa | PredictAccount { address }
    pub chain_id: u64,                       // 56 mainnet, 97 testnet
    pub rpc_url: SecretString,
    pub base_url_http: Option<String>,
    pub base_url_ws: Option<String>,
    pub http_timeout_secs: u64,
    pub max_retries: u32,
    pub retry_delay_initial_ms: u64,
    pub retry_delay_max_ms: u64,
    pub jwt_refresh_skew_secs: u64,
    pub market_slippage_bps: u16,
    pub transport_backend: TransportBackend,
}
```

安全规则：

- secrets 的 `Debug` 必须固定输出 `[REDACTED]`；
- EOA 模式要求 signer == maker；Predict Account 模式要求显式 deposit address，签名按官方
  Kernel/ECDSA validator 规则生成；
- chain ID、合约地址、market 的 `isNegRisk`/`isYieldBearing` 必须共同选择 verifying
  contract，不能只按一个布尔字段；
- connect 只检查 approvals 并给出明确错误，默认不自动发送 approve 交易。

### 4.2 Factory

`PredictExecutionClientFactory` 应与 Polymarket factory 一致：

```rust
let core = ExecutionClientCore::new(
    trader_id,
    ClientId::from(name),
    *PREDICT_VENUE,
    OmsType::Netting,
    config.account_id,
    AccountType::Cash,
    None,
    cache,
);
```

factory 只做配置 downcast、core 组装和 client 构造，不发网络请求。网络认证和 on-chain
preflight 属于 `connect()`。

## 5. Channel 与并发模型

推荐采用 actor 单写者模型。它与当前 Predict data session 的 command/raw-frame select loop
一致，同时避免多个 HTTP task 直接竞争订单状态。

```text
Nautilus core thread
    │ submit/cancel/query (sync trait method)
    ▼
bounded exec_command_tx ───────────────┐
                                      │
WebSocket EpochMessageHandler         ▼
    │                          PredictExecutionActor
bounded raw_ws_tx ───────────►  tokio::select!
                               - commands
HTTP/on-chain task completion ─►       - private WS frames
bounded completion_tx ─────────►       - task completions
                               - cancellation/JWT timer
                                      │
                                      ▼
                         mutate state, then emit ExecutionEvent
```

### 5.1 Channel 职责

| Channel | Payload | Writer | Reader | 满载策略 |
|---|---|---|---|---|
| `exec_command` | submit/cancel/query + captured context | Nautilus core | actor | `try_send` 返回显式错误；不静默丢弃 |
| `raw_ws` | `(generation, Message)` | socket callback | actor | 标记 generation unhealthy，主动重连；execution frame 不可 drop |
| `completion` | `(generation, operation_id, Result)` | spawned HTTP/RPC task | actor | 取消 generation；不能丢 result 后假装成功 |
| `reconcile_buffer` | parsed wallet event | actor startup path | actor | 有上限；溢出则放弃本次启动并重连 |

建议初值为 command 256、raw WS 2,048、completion 512，但必须通过 burst 测试校准；容量不是
正确性机制。

### 5.2 为什么 HTTP 不能直接在 actor 内 await

如果 actor 在 `POST /orders` 或 RPC receipt 上阻塞，它就不能及时回显 15 秒 heartbeat，也
不能处理可能先于 HTTP response 到达的 wallet event。正确方式是：

1. actor 建立 pending state，发 `OrderSubmitted`；
2. spawn 一个 generation-owned task 执行 REST/RPC；
3. task 只把结果写回 `completion_tx`；
4. actor 按 operation ID 和 generation 串行应用结果。

### 5.3 WebSocket request/ack

维护：

```text
next_request_id: u64
pending_requests: Map<request_id, SubscribeWallet { jwt_fingerprint }>
active_wallet_topic: Option<RedactedTopicIdentity>
```

收到 `type=R` 时先解析显式 error，再处理 success。只有当前 generation、request ID 和 JWT
fingerprint 都匹配时，subscribe ack 才能把 private stream 标记 ready。
`invalid_credentials` 触发 JWT refresh；未知 ack 记录 debug 后忽略，不能改变 readiness。

### 5.4 启动与重连无缝对账

为避免 REST-first 与 WS-first 各自的时间缺口，使用“先开流、缓冲、再快照”：

1. 获取/刷新 JWT；
2. 建立带 API key 的 WebSocket；
3. subscribe `predictWalletEvents/{jwt}` 并等待成功 ack；
4. 开始缓冲 wallet events，但暂不对外发布；
5. 并发拉取 open/all orders、account activity/fills、positions、链上 USDT balance；
6. 用 REST/on-chain snapshot 对 Nautilus cache 做 reconciliation；
7. 按稳定 event identity 去重并 drain 缓冲事件；
8. 发布完整 `AccountState`；
9. `core.set_connected(true)`，开放新 command。

任何一步失败都保持 disconnected。重连重复相同流程，不能仅重订阅 topic 后立刻报告 connected。

## 6. Order 映射

### 6.1 支持矩阵

| Nautilus 输入 | Predict 映射 | 第一版 |
|---|---|---|
| `Limit + GTC` | `strategy=LIMIT`，远期 expiration | 支持 |
| `Limit + GTD` | `strategy=LIMIT`，使用 `expire_time` | 支持，需过期边界测试 |
| `Limit + post_only` | protocol 存在 `rejectedPostOnly`，但公开 body 示例未完整展示字段 | 在真实 request fixture 确认前拒绝 |
| `Market + IOC` | 新鲜 book + `strategy=MARKET` + slippage | 支持，必须确认 venue 即时语义 |
| quote-quantity market BUY | SDK 的 `valueWei` 路径 | 第二阶段；不能误当 base quantity |
| FOK/其他 TIF | 无足够官方证据 | 拒绝 |
| stop/trailing/pegged | 无协议映射 | 拒绝 |
| reduce-only | outcome token 是 long-only 资产，不等于 venue reduce-only flag | 先拒绝，不能悄悄忽略 |
| modify | 无原子 amend | 第一版拒绝；后续 cancel-replace |
| order list | API 未证明原生 batch create | 可逐单 fan-out，每单独立结果；非原子 |

对 unsupported command 返回 domain error/`OrderDenied`，不能 panic，也不能发出一个被悄悄
降级的订单。

### 6.2 Amount 计算

LIMIT 订单必须与官方 SDK parity：

```text
BUY:  makerAmount = price * quantity / 1e18, takerAmount = quantity
SELL: makerAmount = quantity,               takerAmount = price * quantity / 1e18
```

在计算前应用官方的有效数字规则，使用整数运算，并验证转换后的价格和数量仍符合 instrument
increment。若用户输入会被改变，建议在发 `OrderSubmitted` 前发 `OrderUpdated` 并明确更新后的
base quantity/price，或者直接 deny；不能只在 wire body 中静默截断。

MARKET 订单必须在提交时重新读取 REST orderbook，不使用 catalog 快照或可能过期的策略 cache。
对 NO instrument，先把 YES book 按相同 `decimalPrecision` 交换两侧并取补数，再执行 SDK
amount math。空深度、深度不足、最后价格为零或超出 slippage 时 deny，不发送 side effect。

### 6.3 Submit 顺序

```text
validate instrument/order/account/approvals
  -> calculate exact wei amounts
  -> build typed data and deterministic orderHash
  -> reserve collateral/tokens
  -> register OrderContext(client_id, hash, instrument, side, qty, price, strategy)
  -> emit OrderSubmitted
  -> POST /v1/orders
  -> apply REST completion and/or wallet event
```

`OrderContext` 至少保存：

```text
client_order_id, strategy_id, instrument_id, market_id, index_set, token_id,
order_side, order_type, time_in_force, original_qty, signed_qty, limit_px,
salt, order_hash, optional order_id, maker, signer, fee_rate_bps,
is_neg_risk, is_yield_bearing, generation_created, ts_submitted
```

### 6.4 POST 结果分类

| 结果 | 处理 |
|---|---|
| 本地 serialization/signing 错误，未发请求 | `OrderDenied`，释放 reservation |
| HTTP 4xx 且明确订单未创建 | `OrderRejected`，保留完整 venue reason，释放 reservation |
| 201 + IDs | 记录 `orderId/orderHash/removalLockedUntil`；等待/合并 wallet lifecycle |
| timeout、连接重置、5xx，无法证明未创建 | 标记 `UnknownSubmit`，不 reject、不释放 reservation；按预计算 hash 查询 |
| wallet `orderAccepted` 先到 | 绑定 context 并 accepted；后到的 201 只能补充字段，不重复事件 |
| wallet `orderNotAccepted` 先到 | rejected；后到的 HTTP result 不得复活订单 |

建议 `retain_unresolved_submissions()` 返回 `true`。只有 venue evidence 能关闭 unknown
submission。

## 7. 私有事件状态机

| Predict wallet event | 内部动作 | Nautilus 输出 |
|---|---|---|
| `orderAccepted` | 绑定 hash/orderId；状态 OPEN | `OrderAccepted`，一次 |
| `orderNotAccepted` | 终止 pending；释放 reservation | `OrderRejected` |
| `orderExpired` | 终态；释放剩余 reservation | `OrderExpired` |
| `orderCancelled` | 终态；释放剩余 reservation | `OrderCanceled` |
| `orderTransactionSubmitted` | 记录 settlement inflight | 无 fill；可触发健康/延迟指标 |
| `orderTransactionSuccess` | 去重、应用 fill、更新 reservation/position/account | 必要时先 accepted，再 `OrderFilled` |
| `orderTransactionFailed` | settlement 失败；不应用 fill；触发 targeted REST/RPC resolution | 通常无订单终态，除非查询证明确切状态 |

状态机必须接受 duplicate、late 和 re-entrant delivery。推荐内部状态：

```text
Prepared -> Submitted -> Open -> Settling -> PartiallyFilled/Open -> Filled
                    \-> Rejected
                    \-> Canceled
                    \-> Expired
Submitted -> UnknownSubmit -> {Open | Rejected | Canceled | Filled}
```

`Settling` 不是 Nautilus 的新增 public order status，只是 adapter 私有状态。

### 7.1 Fill 构造

`OrderFilled`/`FillReport` 字段：

- `instrument_id`：由 market + verified outcome/indexSet 解析；
- `venue_order_id`：order hash；
- `trade_id`：稳定 composite ID；
- `last_qty`：`executedSizeWei` 精确转换到 instrument quantity；
- `last_px`：`executedPriceWei` 精确转换；
- `commission`：按 `fee.type` 分别解释 COLLATERAL 或 SHARES。若 Nautilus event 只能放一种
  Money currency，SHARES fee 不能伪装成 USDT；需先确认模型策略，必要时把 share fee 反映到
  net quantity 并在 `info` 保留原值；
- `liquidity_side`：仅当 `isMaker` 明确存在时使用 Maker/Taker，否则 `NoLiquiditySide`，不能猜；
- `ts_event`：venue timestamp；`ts_init`：本地收到/恢复时间。

transaction failed 永远不生成负 fill 或 `OrderFillVoided`，因为本设计从未把 submitted settlement
作为 fill 发布。只有将来协议证明“先成功后链上重组/回滚”时，才另行设计 void。

## 8. Cancel 与 Modify

### 8.1 强取消

Nautilus `cancel_order` 的语义是订单不能继续成交。Predict 有两个不同动作：

1. REST remove-by-hash：快速从订单簿删除，但不取消链上签名；
2. SDK/合约 `cancelOrders`：链上取消，需要按 `isNegRisk` 和 `isYieldBearing` 选择合约。

因此建议流程：

```text
mark PendingCancel
  -> optional REST remove-by-hash (快速降低撮合机会)
  -> submit on-chain cancel transaction
  -> wait successful receipt and/or wallet cancellation evidence
  -> targeted GET order confirmation
  -> emit OrderCanceled exactly once
```

如果 REST 成功但链上失败，订单仍处于 unresolved cancel，不能发 canceled。若 fill 与 cancel
并发，先按 venue timestamps 和 settlement identity 应用已发生的 fill，再关闭剩余量。

`removalLockedUntil` 表示 remove 可能暂时不允许；任务调度应使用 venue timestamp/明确响应，
而不是固定 sleep。

### 8.2 Cancel all / batch cancel

- 从 cache 选出属于当前 client/account/instrument filter 的 active orders；
- 每批 remove-by-hash 不超过 100；
- 按 `(isNegRisk, isYieldBearing, maker)` 分组链上 cancel；
- 每个 child order 都有独立 pending/result；
- 一组失败不应把其他组标记失败或成功；
- 返回/事件必须让部分成功可观察。

### 8.3 Modify

Predict 没有已证实的原子 amend。第一版 `modify_order` 应明确 deny。后续实现时：

1. 锁定 client order ID，防止 cancel/modify 重叠；
2. 强取消旧 order hash；
3. 只有旧单不可再成交得到证明后，才签名并提交 replacement；
4. replacement 失败时，旧单保持 canceled，发布准确终态，不能把 registry 假装恢复成旧单；
5. 新 hash 绑定同一个 client order ID，并保留 hash history 供迟到 fill 路由。

## 9. Account

`/v1/account` 不能提供 balance。账户状态采用以下来源：

```text
on-chain USDT balanceOf(maker)        -> total
sum(open BUY remaining makerAmount)   -> locked
max(total - locked, 0)                -> free
```

生成：

```rust
AccountBalance::from_total_and_locked(total, locked, Currency::USDT())
```

并通过 `ExecutionEventEmitter::emit_account_state` 发布，`reported=true`。`info` 建议包含经过
脱敏/可序列化的：

```text
chain_id, maker_address, signer_mode, raw_total_wei,
reservation_count, unresolved_submit_count, block_number
```

刷新时机：

- connect/reconnect 对账完成；
- `query_account`；
- 每个成功 settlement、强 cancel、redeem/merge/split 后；
- 可选低频周期校验，但不能用周期轮询代替事件驱动更新。

AccountState 发布前必须同时完成 total、locked、free 和 reservation mutation。测试必须既检查
cache 最终值，也检查 subscriber 收到的事件。

## 10. Position

Predict position 是 outcome token 余额，映射为每个 outcome instrument 的 long-only net
position：

| Predict | Nautilus |
|---|---|
| `market.id + outcome.indexSet` | `InstrumentId` |
| `amount > 0` | `PositionSide::Long` |
| `amount` | `quantity` |
| `averageBuyPriceUsd` | `avg_px_open`，注明 venue precision |
| position `id` | `venue_position_id` |
| row timestamp 若缺失 | 不能伪装 venue time；使用 snapshot init 并在 `info`/日志注明 |

`generate_position_status_reports` 必须：

1. 遍历所有 cursor，直到明确终止；
2. 先按请求 instrument/market 过滤，再构造 report；
3. 用 outcome `indexSet/onChainId` 双重验证 instrument；
4. 保留非空 zero amount row，不能在 parse 前过滤；
5. 对缺失 row 不生成 flat，除非后续证据证明 bulk coverage 完整；
6. resolved/redeemed/includeLost 组合建立 fixtures。

`PositionStatusReport` 是 reconciliation snapshot，不替代实时 fill。fill 成功后 engine 先更新
position，随后异步 REST position refresh 用于校验偏差。

## 11. Reports 与启动恢复

### 11.1 Order reports

- `generate_order_status_report`：优先按 canonical order hash 调 `/v1/orders/{hash}`；只有
  client order ID 时从 context registry 解析 hash；
- bulk：分页 `/v1/orders`，不可只取第一页；
- status 映射必须从实际 enum fixture 建表。未知新状态返回显式错误/Unknown，不默认 OPEN；
- original/signed quantity、filled quantity、price、expiration 均从 structured wire fields 解析；
- `client_order_id` 只有 registry/cache 有可靠关联时才填。

### 11.2 Fill reports

优先级：

1. wallet event 录制的 canonical fill journal；
2. authenticated account activity；
3. signer-filtered order matches；
4. 必要时链上 receipt/log。

如果第 2/3 层不能提供 order hash 或 stable match ID，就不能声称 mass status 完整。应让
`generate_fill_reports` 返回 evidence-gap error，或只返回明确可关联的 rows 并让 mass
reconciliation fail closed。绝不通过相同 market/price/size/time 猜一个 order。

### 11.3 Mass status

`generate_mass_status` 组合 order、fill、position 三类报告前后都要检查 unresolved settlement/
unknown submit gate。读取期间如果出现新的 unresolved state，本次报告失败，不交付一个混合时点
的“完整”快照。

### 11.4 External orders

启动时遇到当前 wallet 创建但 registry 中不存在的订单，交给 Nautilus reconciliation 创建
external order；随后 `register_external_order` 必须建立 `(client_order_id, order_hash,
instrument_id)` 映射，使迟到 wallet events 能继续路由。

## 12. HTTP 与错误模型

拆分错误：

```rust
enum PredictHttpError {
    Transport { outcome: KnownNotSent | Unknown },
    Http { status, code, message, retry_after },
    AuthExpired,
    RateLimited,
    Decode,
    Protocol,
}
```

规则：

- 先解析 `success=false`/error envelope，再解析 success body；
- 401/`invalid_credentials` 刷新 JWT，但 submit 请求是否执行仍按具体时点分类；
- 429 尊重 `Retry-After`，重试不得改变 order salt/hash；
- POST submit 只可重放同一个 signed payload/hash，不能每次生成新 salt；
- GET 可按 bounded exponential backoff 重试；
- cancel/submit 的重试必须有最大总时长，并在停止时被 generation cancellation 中断；
- 日志中 redact API key、JWT、private key、signature 和完整 auth body。

## 13. 分阶段实施

下面是推荐的实际编码顺序。除 Step 0 外，每一步都必须满足四个条件后才能进入下一步：

1. 该步新增代码可以独立编译；
2. 至少一个测试在没有该步实现时失败；
3. 没有把 mock serialization 描述成 venue acceptance；
4. 文档里的字段名、状态和当前实现一致。

### Step 0：冻结基线和协议证据

先不要写交易代码。完成以下准备：

1. 记录本仓库和 sibling Nautilus 的基线：

   ```bash
   git rev-parse HEAD
   git status --short
   git -C ../nautilus_trader rev-parse HEAD
   git -C ../nautilus_trader status --short
   ```

2. 确认本次实现针对哪个 Nautilus commit。当前 sibling 有 execution-action 未提交改动；在这些
   改动提交或移除前，不要把它们当成公开合约。
3. 从 Predict testnet 或官方 SDK 生成并脱敏保存以下 fixtures：

   ```text
   auth_message.json
   auth_success.json
   market_execution_metadata.json
   create_limit_request.json
   create_limit_response.json
   get_order_open.json
   get_order_partially_filled.json
   get_order_terminal.json
   wallet_order_accepted.json
   wallet_order_rejected.json
   wallet_settlement_submitted.json
   wallet_settlement_success.json
   wallet_settlement_failed.json
   positions_page_1.json
   positions_page_last.json
   account_activity_page.json
   ```

4. fixtures 放在 `crates/nautilus-predict/test_data/execution/`。不得包含 API key、JWT、私钥、
   真实签名者地址或可重放签名。
5. 为第 16 节待确认项建立结果表；每一项标记 `confirmed`、`blocked` 或 `not needed in v1`，
   并附证据 URL/fixture 名。

完成条件：可以只凭 fixtures 写出完整 wire structs；仍缺稳定 ID 的字段被明确标为 blocker，
没有靠推测填空。

### Step 1：建立最小模块骨架和依赖

先保持现有 `http.rs` 和 `data_client.rs` 不动，避免第一步同时重构已经工作的行情路径。新增：

```text
src/private_http.rs
src/private_models.rs
src/signing/mod.rs
src/signing/order_builder.rs
src/signing/eip712.rs
src/execution/mod.rs
```

在 `Cargo.toml` 只加入当前步骤实际使用的依赖。预计需要：

```toml
alloy = { workspace = true, features = ["contract", "signer-local", "provider-http", "reqwest", "reqwest-rustls-tls"] }
alloy-primitives.workspace = true
futures-util.workspace = true
parking_lot.workspace = true
rand.workspace = true
zeroize.workspace = true
```

如果根 workspace 尚未声明这些依赖，先对齐 sibling Nautilus 所用版本，不在 adapter crate 单独
选择另一个 major version。不要因为 Polymarket 用了某个 crate 就全部复制依赖。

`lib.rs` 暂时只声明 private modules，不导出未完成的 public client：

```rust
mod private_http;
mod private_models;
mod signing;

pub mod execution;
```

测试：

```bash
cargo check -p nautilus-predict
cargo test -p nautilus-predict --lib
```

完成条件：空骨架编译通过；没有改变现有 data client 行为，也没有引入 Python 或 CLI surface。

### Step 2：实现精确 wire models 和错误 envelope

在 `private_models.rs` 建立以下类型，所有链上整数都先保留为字符串或 `U256`，不能直接反序列化
为 `f64`：

```text
PredictAuthMessageResponse
PredictAuthRequest / PredictAuthResponse
PredictContractOrder
PredictCreateOrderRequest / Response
PredictOrderData / PredictOrderPage
PredictActivityEvent / PredictActivityPage
PredictPositionData / PredictPositionPage
PredictWalletEnvelope / PredictWalletEvent
PredictApiErrorEnvelope
```

具体规则：

- 使用 `#[serde(rename_all = "camelCase")]`，但对 `type`、`tokenId` 等关键字段写显式 rename；
- API 的 integer-like string 不先转有符号整数；
- optional 与 missing 分开：只有协议确认等价时才使用 `#[serde(default)]`；
- status/type 先保留 raw string，再通过 `classify_*` 转 domain enum。未知值返回 protocol error，
  不用 `#[serde(other)]` 静默降级；
- `success=false` envelope 在 generic success body 之前解析；
- error 保存 HTTP status、venue code、message 和 `Retry-After`。

建议测试文件：

```text
src/private_models.rs              # 纯 serde unit tests
tests/predict_private_protocol.rs  # 完整 fixture tests
```

最低测试：

- 每个 fixture decode 后检查非空稳定 ID；
- missing、null、unknown enum、超大 wei、负数和错误 envelope；
- JSON round-trip 只用于 outbound DTO，inbound DTO 不要求重新序列化后文本相同；
- 同时有 error 和 data 时 error 优先。

完成条件：所有 execution fixtures 都能无网络解析；未知状态明确失败；没有任何 `f64` 金额字段。

### Step 3：补齐 market/outcome 执行元数据

当前 `PredictMarket` 只保留行情需要的字段。执行需要增加：

```rust
pub struct PredictMarketExecutionMeta {
    pub market_id: u64,
    pub condition_id: String,
    pub fee_rate_bps: u64,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub decimal_precision: u8,
    pub outcomes: Vec<PredictOutcomeExecutionMeta>,
}

pub struct PredictOutcomeExecutionMeta {
    pub name: String,
    pub index_set: u8,
    pub on_chain_id: String,
}
```

实施方式：

1. 扩充 `models.rs` 的 wire market 字段；
2. 在 `providers.rs` 构建 `BinaryOption` 时，把执行元数据写入 instrument `info`，或放入一个
   明确的 `PredictInstrumentExecutionMeta` registry；
3. 不从 instrument symbol 推导 token ID；token ID 始终来自 `raw_symbol`/metadata；
4. `PredictMarketContext` 增加 outcome lookup，但保持原有 YES orderbook identity 不变；
5. 动态市场注册必须携带同一份 execution metadata，避免 bootstrap 与 dynamic path 不一致。

测试：

- YES/NO instrument 只差一个 outcome 维度，token ID、indexSet 都正确；
- market metadata 缺少 `isNegRisk`、fee 或 outcome 时 fail closed；
- instrument serialization/readback 后仍能恢复下单需要的字段；
- 原有 orderbook tests 不回归。

完成条件：给定任意一个已加载 Predict instrument，可以无网络得到
`market_id/index_set/token_id/fee/is_neg_risk/is_yield_bearing`。

### Step 4：实现 amount math、typed data、hash 和签名

在 `signing/order_builder.rs` 实现纯函数：

```rust
fn limit_order_amounts(
    side: OrderSide,
    price_wei: U256,
    quantity_wei: U256,
) -> Result<PredictOrderAmounts>;

fn market_order_amounts(
    side: OrderSide,
    quantity_or_value: PredictMarketQuantity,
    book: &PredictExactBook,
    slippage_bps: u16,
) -> Result<PredictOrderAmounts>;

fn complement_yes_book(
    book: &PredictExactBook,
    precision: u8,
) -> Result<PredictExactBook>;
```

在 `signing/eip712.rs` 实现：

```text
build_order
build_typed_data
typed_data_hash
sign_eoa_order
sign_predict_account_order
```

关键点：

- limit price 3 个有效数字、quantity 5 个有效数字的规则必须与官方 SDK parity；
- salt generator 通过 trait/closure 注入，测试使用固定 salt，生产使用 CSPRNG；
- retry 重用同一个 signed payload 和 hash；
- verifying contract 由 `(chain_id, is_neg_risk, is_yield_bearing)` 完整选择；
- EOA 和 Predict Account 的 maker/signer/signatureType 不能共用含糊配置；
- quantity/price 的 Nautilus→wei 转换先做 range、precision 和 overflow 检查；
- `complement_yes_book` 在整数 tick 空间算 `1 - price`，交换两侧后重新检查排序。

最低 parity vectors：

```text
LIMIT BUY / SELL
MARKET BUY by shares
MARKET BUY by value
MARKET SELL
YES / NO
standard / neg-risk
yield / non-yield
EOA / Predict Account
```

对每个 vector 断言 `makerAmount`、`takerAmount`、typed-data JSON、digest、signature 和
order hash。官方 SDK 输出是 oracle；Rust 自己 encode/decode round-trip 不能替代跨实现 parity。

完成条件：离线 vectors byte-for-byte 一致；任一字段变化会导致测试失败；还没有真实 POST。

### Step 5：实现 private HTTP client 和 JWT manager

在 `private_http.rs` 建立一个 cloneable、无 Nautilus cache 引用的 client。建议方法：

```rust
async fn get_auth_message(&self) -> Result<PredictAuthMessage>;
async fn create_jwt(&self, request: PredictAuthRequest) -> Result<PredictJwt>;
async fn create_order(&self, request: &PredictCreateOrderRequest) -> Result<PredictCreateOrderData>;
async fn get_order(&self, order_hash: &str) -> Result<Option<PredictOrderData>>;
async fn list_orders_page(&self, query: &OrderQuery) -> Result<PredictOrderPage>;
async fn remove_orders_by_hash(&self, hashes: &[String]) -> Result<PredictRemoveOrdersResult>;
async fn list_activity_page(&self, query: &ActivityQuery) -> Result<PredictActivityPage>;
async fn list_positions_page(&self, query: &PositionQuery) -> Result<PredictPositionPage>;
```

再建立：

```rust
struct PredictJwtManager {
    current: RwLock<Option<PredictJwt>>,
    refresh_skew: Duration,
}
```

JWT manager 的所有日志只输出 token fingerprint，不能输出 JWT。多个并发请求发现 token 过期时，
只能有一个 refresh in flight；其他请求等待同一结果。

分页 helper 要求：

- cursor 原样传回；
- 检测重复 cursor，防止死循环；
- 有可配置 page/record hard cap；
- 到达 cap 时返回错误，不返回被截断的“成功”结果；
- 测试至少两页且每页非空。

使用本地 mock server 精确断言 method、path、headers 和 structured JSON body。不要把 API key
或 Authorization 写入 snapshot failure output。

完成条件：private endpoints 和 auth 在 mock 上可用；pagination、401 refresh、429 retry 和
unknown POST outcome 分类均有测试。

### Step 6：实现私有 WebSocket parser 与 session

不要先重构 `PredictDataClient` 的公共 socket。新增
`execution/websocket.rs`，只处理 wallet stream：

```rust
enum PredictWsEnvelope {
    Response(PredictWsResponse),
    Message(PredictWsMessage),
}

enum PredictWsMessage {
    Heartbeat(u64),
    Wallet(PredictWalletEvent),
}
```

session 必须维护：

```text
generation
current_epoch
next_request_id
pending_subscribe_ack
jwt_fingerprint
heartbeat_deadline
overflow flag
```

实现顺序：

1. API key 放握手 header；
2. connect 后发送一个 `predictWalletEvents/{jwt}` subscribe；
3. 等同 request ID 的 `type=R, success=true`；
4. 收到 heartbeat 立即原样 echo，不等待 actor 的 REST 操作；
5. wallet message 附上 generation 后送入 actor；
6. `invalid_credentials` 通知 lifecycle 刷新 JWT；
7. reconnect 后生成新 request ID 并重新订阅；
8. raw queue overflow 时主动 fail generation，不能丢帧继续运行。

测试使用 paused Tokio time 或显式 barrier，不用固定 sleep：

- successful/failed/unknown ack；
- heartbeat exact echo、stale echo、missing data；
- JWT 不出现在 Debug/log；
- old epoch message 被忽略；
- duplicate subscribe ack 不产生第二个 ready；
- queue overflow 触发 reconnect signal。

完成条件：session 可以稳定输出 parsed wallet events，但还不生成 Nautilus execution events。

### Step 7：增加 execution config、factory 和 read-only client

在 `config.rs` 新增 `PredictExecutionClientConfig`，不要把私钥字段塞进
`PredictDataClientConfig`。在 `factories.rs` 新增 `PredictExecutionClientFactory`，并按第 4.2
节构造 `ExecutionClientCore`。

`execution/mod.rs` 第一版结构：

```rust
pub struct PredictExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: PredictExecutionClientConfig,
    emitter: ExecutionEventEmitter,
    http: PredictPrivateHttpClient,
    jwt: Arc<PredictJwtManager>,
    actor_tx: Option<mpsc::Sender<PredictExecCommand>>,
    session_tasks: TaskGroup,
    operation_tasks: TaskGroup,
    connected: Arc<AtomicBool>,
}
```

先实现：

```text
client_id/account_id/venue/oms_type/get_account
start/stop/reset/dispose
connect/disconnect
on_instrument
query_account
generate_*_reports
```

交易方法在这个阶段全部返回明确的 read-only error，不调用默认 no-op：

```text
submit_order
submit_order_list
modify_order
cancel_order
cancel_all_orders
batch_cancel_orders
```

这一步要先证明 factory、core、cache、emitter 和生命周期拼接正确，再开放 side effect。

完成条件：Nautilus 可以注册、连接和关闭 execution client；调用交易命令明确失败；重复 stop/
reset/dispose 不 panic。

### Step 8：实现 actor 和 generation 生命周期

新增：

```rust
enum PredictExecCommand {
    QueryAccount(QueryAccount),
    QueryOrder(QueryOrder),
    Submit(PreparedSubmit),
    Cancel(PreparedCancel),
    Reconcile,
}

enum PredictExecCompletion {
    Auth { generation, result },
    Rest { generation, operation_id, result },
    Chain { generation, operation_id, result },
}

struct PredictExecutionActor {
    generation: u64,
    command_rx: mpsc::Receiver<PredictExecCommand>,
    ws_rx: mpsc::Receiver<GenerationMessage<PredictWalletEvent>>,
    completion_rx: mpsc::Receiver<PredictExecCompletion>,
    state: PredictExecutionState,
    cancellation: CancellationToken,
}
```

所有 mutable execution state 只由 actor 写。HTTP/RPC tasks 只拿 immutable request、cloneable
client 和 generation，完成后写 `completion_tx`。

实现 lifecycle：

```text
start generation
  -> auth
  -> private WS subscribe ack
  -> buffer wallet events
  -> REST/RPC snapshot
  -> reconcile
  -> drain buffer
  -> emit account
  -> connected=true
```

disconnect/reset：

1. `connected=false`，关闭 command intake；
2. increment/invalidate generation；
3. cancel session 和 operation TaskGroup；
4. await drain；
5. 替换 generation-owned queues/maps，而不是只 `clear()`；
6. 保留 durable order correlation 和 unresolved evidence；
7. 需要 reconnect 时从同一 connect pipeline 重建。

完成条件：故意让旧 generation 的 HTTP completion 在 reset 后到达，它不能改变 state，也不能
发事件。

### Step 9：实现 read-only Account、Order、Position、Fill reports

先完成 read side，暂不下单。

Account：

1. Alloy provider 调 `USDT.balanceOf(maker)`；
2. 18-decimal raw wei 转 Decimal；
3. 按 `Currency::USDT()` 边界转换并保留 `raw_total_wei`；
4. 当前没有本地订单时 locked=0；
5. 通过 emitter 发布 `AccountState`。

Order reports：

1. `/orders/{hash}` 构造 single report；
2. `/orders` 遍历 cursor 构造 bulk reports；
3. unknown status fail closed；
4. 没有可靠 client ID 时保留 `None`，不猜。

Position reports：

1. `/positions` 遍历 cursor；
2. `(market.id, outcome.indexSet/onChainId)` 双重绑定 instrument；
3. amount>0 为 LONG；
4. non-empty zero row 保留为 FLAT evidence；
5. 缺失 row 不生成 flat；
6. `provides_bulk_position_coverage=false`。

Fill reports：

1. 先只接受 fixture 中能稳定关联 order hash 的 activity/match row；
2. 没有 stable association 时返回 evidence-gap error；
3. 不用价格、数量、时间启发式拼接。

测试必须同时观察：

- 返回 report 的字段；
- account cache 的最终 state；
- execution event subscriber 收到的 `AccountState`。

完成条件：read-only connect 后能生成非空 mass status；若 fill 证据不完整则明确失败，而不是返回
看似完整的空 fills。

### Step 10：实现启动/重连 reconciliation

新增 registries：

```text
OrderContextRegistry: client_order_id <-> order_hash <-> order_id
FillJournal: full fill identity -> applied state
SettlementRegistry: settlement_id -> submitted/success/failed/unresolved
ExternalOrderRegistry: venue order -> reconciled client order
```

启动 reconciliation 顺序：

1. private WS ready 后开始 buffer；
2. 拉 open/all orders；
3. 拉最近 activity/fills；
4. 拉 positions 和 on-chain balance；
5. 建立 reports，交给 Nautilus execution reconciliation；
6. 注册 external orders；
7. drain buffered wallet events；
8. 以 full event identity 去重；
9. 前后各检查 unresolved settlement gate；
10. 全部成功才 connected。

需要定义 total ordering，例如：

```text
(venue_timestamp, event_rank, order_hash, settlement_id, fill_leg)
```

`event_rank` 只用于相同 timestamp 的确定性应用，不得改变 venue 明确因果。推荐 accepted <
settlement success < cancel/expire；若真实协议能在同 timestamp 给 sequence，应优先用 sequence。

完成条件：构造 REST snapshot 中有 open order、WS buffer 中同一订单已 fill/cancel 的测试，最终只
产生一次正确事件和正确 position/account。

### Step 11：开放 LIMIT submit

新增 `execution/context.rs`、`pending.rs`、`reservations.rs` 和 `orders.rs`。

同步 `submit_order` 只做轻量校验和 enqueue；详细校验由 actor 在 side effect 前完成：

```text
client/venue/account ownership
instrument exists and has execution metadata
Limit + supported TIF
price/quantity increment and range
maker/signer mode
market trading status
fee/contract selection
approval and available balance
```

actor 的原子逻辑顺序：

```rust
let prepared = prepare_limit_order(...)?;
state.reserve(&prepared)?;
state.register_context(&prepared)?;
emitter.emit_order_submitted(...);
spawn_create_order(prepared.clone());
```

若 spawn 失败，在发布 submitted 后必须发有因果关系的 rejected/denied，并回滚 reservation；
不能留下永久 pending。

HTTP result 分类实现第 6.4 节。`UnknownSubmit` 保存 signed body 和预计算 hash，recovery 只查询或
重放同一个 hash。绝不重新生成 salt。

reservation：

```text
BUY locked = remaining makerAmount in USDT
SELL locked = remaining outcome token quantity
```

Cash `AccountState` 只展示 USDT reservation；SELL token reservation 单独用于本地下单校验，不能
伪造成 MarginBalance。

完成条件：LIMIT BUY/SELL、YES/NO 在 mock 中能完成 submitted→accepted/rejected；unknown POST
不会错误 rejection；每次 reservation 改变都有 subscriber-visible AccountState。

### Step 12：实现 wallet dispatch 和实时 fill

新增 `execution/dispatch.rs`。入口只接受已验证 wallet owner 和 current generation 的事件：

```rust
fn apply_wallet_event(
    state: &mut PredictExecutionState,
    event: PredictWalletEvent,
    emitter: &ExecutionEventEmitter,
) -> Result<Vec<FollowUp>>;
```

每个 event handler 固定顺序：

1. 验证 wallet、hash、market、outcome；
2. 查 context；未知但属于当前 wallet 的订单进入 external-order buffer；
3. 检查 duplicate/terminal conflict；
4. mutation order/fill/reservation/settlement state；
5. 构造 Nautilus event；
6. 发布 event；
7. 返回 account/position refresh 等 follow-up，由 actor spawn，handler 本身不 await。

乱序规则：

- fill/cancel 早于 accepted：先合成一次 accepted；
- HTTP accepted 晚于 WS accepted：只补字段；
- transaction submitted：只写 settlement registry；
- transaction failed：不生成 fill；
- transaction success duplicate：full identity dedupe；
- success 与 failed 对同 settlement 冲突：标 unresolved，停止交易并做 targeted resolution。

完成条件：状态机测试既断言事件顺序，也断言 registry、reservation、account 和 position 最终值。

### Step 13：实现强 cancel

新增 `execution/cancellations.rs` 和最小 on-chain contract wrapper。

单笔 cancel：

1. 从 cache/context 解析 canonical order hash；
2. 标记 `PendingCancel`，防止重复 cancel/modify；
3. 若 `removalLockedUntil` 已过，调用 REST remove-by-hash；
4. 根据 market flags 选择 exchange contract；
5. 提交相同 order payload 的 on-chain `cancelOrders`；
6. 等 receipt 并验证成功；
7. 再用 wallet event 或 GET order 确认终态；
8. mutation state/release reservation；
9. emit `OrderCanceled` 一次。

若步骤 3 成功、步骤 5/6 失败，保持 unresolved cancel；不能发 canceled。若期间收到 fill，先应用
fill 并缩减 remaining reservation。

batch/cancel-all：

- REST hash chunks 最大 100；
- chain calls 按 `(is_neg_risk, is_yield_bearing, maker)` 分组；
- 每个 child 独立 operation ID 和结果；
- partial success 可观察；
- 对已经 filled/canceled 的 child 幂等处理。

完成条件：REST 200 + chain revert 测试不发 `OrderCanceled`；partial batch 只关闭成功 child；
cancel/fill race 的累计 filled quantity 与 reservation 正确。

### Step 14：开放 MARKET 和 NO outcome

MARKET submit 比 LIMIT 多一个 preflight：

1. 用 REST 拉当前 market orderbook；
2. 校验 timestamp freshness；
3. NO instrument 执行 exact complement transform；
4. 校验 depth 足够覆盖请求；
5. 使用官方 parity math 计算 maker/taker/amount/slippage；
6. 再执行 Step 11 的 reserve/register/submitted/POST 流程。

不要从 `PredictOrderbookSnapshot` catalog 或 data client 内存读取 book：execution 和 data client
生命周期不同，且 MARKET 必须在 side effect 前使用可证明新鲜的 book。

第一小步只支持 base quantity。quote-quantity BUY 要单独加入 `valueWei` 路径，并测试
`amount != takerAmount` 的 `isMinAmountOut=true` 情况。

完成条件：YES/NO、BUY/SELL、空 book、深度不足、过期 book、slippage 边界都有测试；NO book
价格与 side 顺序严格正确。

### Step 15：最后才做 Modify 和链上 actions

`ModifyOrder` 用 cancel-replace：

1. 锁定原 client order；
2. 执行 Step 13 强取消；
3. 证明旧 hash 不可成交；
4. 计算 replacement；
5. 提交新 hash；
6. 保留 old hash history 处理迟到 fill；
7. replacement 失败时保持旧单已取消，不伪造 rollback。

approve/redeem/merge/split/convert 不属于普通 `submit_order`。若最终采用 `ExecuteAction`：

- 每个 action 有 typed request/response，不接受任意 JSON；
- 钱包、chain、market flags 和数量完整校验；
- receipt 成功后刷新 account/positions；
- action 不生成虚假的 OrderFilled；
- Rust/Python/stub surface 一起交付。

完成条件：只有在 action API 已进入目标 Nautilus 基线后才实现；分支私有接口不能成为发布依赖。

### 每一步的本地验证命令

开发期间先跑最小相关集：

```bash
cargo test -p nautilus-predict --lib
cargo test -p nautilus-predict --test predict_private_protocol
cargo test -p nautilus-predict --test execution_client
cargo check -p nautilus-predict --all-features
cargo clippy -p nautilus-predict --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

如果某个 integration test 尚未创建，命令从它首次出现的步骤开始执行。准备 PR 前再跑仓库门槛：

```bash
make build
make test
make fmt
make clippy
make cargo-deny
make pre-commit
```

任何 testnet/live probe 必须默认 dry-run 或使用独立测试钱包，并由调用者显式开启；常规 unit/
integration test 不能发送真实订单或链上交易。

### 推荐 PR 切分

#### PR 1：协议模型与 parity fixtures

- 扩充 market model：`isNegRisk`、`isYieldBearing`、`feeRateBps`、condition/outcome fields；
- private REST DTOs、R/M WebSocket envelopes、wallet event enum；
- EIP-712 order builder、amount math、hash/signature；
- 与官方 TS/Python SDK 生成的固定 vectors 做 byte-for-byte parity；
- 不接 `ExecutionClient`，不发真实订单。

#### PR 2：只读执行客户端

- config、factory、`ExecutionClientCore`；
- JWT、private WS heartbeat/ack/reconnect；
- account balance、orders、positions、reports 和 startup reconciliation；
- 只读模式下 submit/cancel 明确 deny；
- 用 testnet/recorded fixtures 关闭历史 fill 关联的证据缺口。

#### PR 3：LIMIT submit

- context、reservation、pending submit、unknown outcome recovery；
- wallet order/fill dispatch；
- 单笔 LIMIT GTC/GTD；
- subscriber-visible account/order/fill tests。

#### PR 4：强取消

- REST remove + on-chain cancel；
- single/batch/cancel-all；
- partial failure、fill race、removal lock 和 receipt failure。

#### PR 5：MARKET 和 NO outcome

- fresh book、YES→NO complement、depth/slippage；
- base quantity，随后再考虑 quote quantity；
- market order settlement failure 与 reconnect 测试。

#### 后续

- cancel-replace modify；
- approve/redeem/merge/split/convert actions；
- Python/PyO3/stub/export surface（如果该 crate 进入上游 Nautilus 公共 adapter）；
- 性能基准与 live soak。

## 14. 测试门槛

### 14.1 Identity / protocol

- 两个 instrument 只差 `index_set`，订单 token、事件路由和 position 必须彼此隔离；
- `outcomeIndex=0/1` 与 `indexSet=1/2` 的转换和矛盾输入；
- orderId/hash/settlementId/txHash/requestId 不可互换；
- structured JSON body 精确断言 `data.order.*`、`pricePerShare`、`strategy`、slippage fields；
- 官方 SDK typed-data/hash/signature parity vectors；
- 18-decimal wei 到 16/8-decimal Nautilus boundary 的 exact、round、overflow、dust cases。

### 14.2 Lifecycle / channel

- command queue full/closed 有显式错误；
- private subscribe ack 失败、JWT 过期、heartbeat stale/mismatch；
- reconnect 时 WS buffer + REST snapshot 无 gap、无 duplicate；
- 旧 generation HTTP completion/WS frame 不写新 generation；
- raw WS overflow 触发 fail/reconnect，不静默丢 execution event；
- repeated stop/reset/dispose 安全。

### 14.3 Order state

- WS accepted 早于 POST response；
- fill/cancel 早于 accepted；
- duplicate accepted/fill/cancel；
- definitive reject 与 unknown submit 分离；
- unknown submit 按预计算 hash 恢复；
- settlement submitted→failed 不产生 fill；submitted→success 只产生一次 fill；
- partial fill 后 cancel；fill 与强取消竞态；
- replacement 失败不复活旧单（实现 modify 时）。

### 14.4 Account / position / reports

- 非空正常 balance、非空零 balance、缺失 RPC summary；
- reservation 建立、部分 fill、release、超出 total clamp；
- 每次 account change 同时断言 stored state 和 subscriber event；
- positions 多页、空页、非空 zero row、resolved/redeemed/includeLost；
- 两个 owner/wallet 不互相读取或清理状态；
- exact page cap + 非空 control，避免 vacuous pass；
- equal timestamps 使用 stable total ordering，不依赖 map/SQL/runner 顺序。

### 14.5 Live evidence

mock 只能证明 serialization 和 adapter state machine，不能证明 venue 接受。进入可交易状态前至少
完成以下 testnet/live 证据：

- EOA 与 Predict Account 各一组 auth + wallet topic；
- LIMIT BUY/SELL、YES/NO；
- accepted、post-only reject（若计划支持）、partial/full fill；
- REST removal 与链上 cancel 的差异；
- settlement success/failure；
- reconnect gap recovery；
- position/balance 在 fill 和 redeem 后的一致性；
- account activity/match 是否提供稳定 fill↔order 关联。

## 15. 非目标

本设计不在第一版中：

- 自动发送 token approvals；
- 自动 redeem/merge/split/convert；
- 用公共 orderbook settlement 字段推断个人成交；
- 用 `f64` 简化签名金额；
- 把 REST removal 当链上 cancel；
- 在没有真实证据时承诺 FOK、post-only、atomic batch 或完整历史 fill；
- 给 `catalog-capture-cli` 增加交易开关。当前产品仍是 capture 工具，执行客户端先作为
  `nautilus-predict` library surface；是否新增交易 binary/上游 adapter 需另行确定产品边界。

## 16. 实现前待确认清单

以下问题会改变 public contract，必须在写交易代码前用官方 schema、testnet fixture 或维护者
确认关闭：

1. `CreateOrderRequest` 的完整 body，尤其 post-only、STP、`amount`、`isMinAmountOut`；
2. 所有 order status 枚举及其 terminal/open 语义；
3. 201 create response 是否足以证明 accepted，还是只证明 request created；
4. `orderId` 与 `orderHash` 在 wallet/REST/on-chain 各边界的稳定性；
5. wallet fill 是否可能拆成多个 legs，以及每个 leg 的稳定唯一 ID；
6. transaction success 后是否仍可能发生链重组/补偿失败；
7. account activity/matches 能否无歧义关联 order hash；
8. positions 缺失 row 是否能证明 flat；
9. BNB mainnet/testnet USDT decimals、所有 exchange/adapter 地址和升级策略；
10. Nautilus `Money` 表达 share-denominated fee 的最终方案；
11. LIMIT GTC 的官方 expiration 约定与 MARKET 的真实即时成交语义；
12. 同一 wallet 多 client 实例是否允许；若允许，reservation 和 WebSocket owner identity 必须
    扩展到 `(account_id, client_id)`，不能用进程全局单例覆盖。

这些项目没有关闭前，可以完成只读 client、协议 parser 和签名 parity，但不应把适配器标记为
production-ready。
