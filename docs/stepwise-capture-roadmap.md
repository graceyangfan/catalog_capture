# 分步采集实施路线图

## 文档目的

本文档是 `nautilus_catalog_capture` 的**执行级**实施计划，回答三个问题：

1. 下一步先做什么、后做什么？
2. 每个阶段录哪些交易所、哪些数据？
3. 怎样验证「写得出、读得回、能复算」？

排序原则（全文贯穿）：

| 维度 | 顺序 |
|---|---|
| 数据形态 | **交易所内置标准类型** → **adapter `CustomData`** |
| 传输方式 | **WebSocket 实时订阅** → **HTTP 请求 / 历史回填** |
| 业务复杂度 | **单 venue 单标的** → **多 venue 期权链** → **跨所研究层** |
| 存储压力 | **Top-of-book + 状态** → **成交流** → **盘口深度** |

相关文档：

- 采什么：`docs/options-ml-data-capture-plan.md`
- CustomData 清单：`docs/native-custom-data-targets.md`
- 产品阶段：`docs/implementation-plan.md`、`ROADMAP.md`
- 首次 live 验证：`docs/live-validation.md`

---

## 当前基线（2026-06）

### 已完成

| 层级 | 状态 | 说明 |
|---|---|---|
| `catalog-capture-core` | ✅ | `CapturePlan`、分区、chunk flush、catalog sink |
| `CatalogCaptureActor` | ✅ | 覆盖全部内置家族 + `custom_data` 回调 |
| CLI TOML 解析 | ✅ | 内置家族 + `index_prices` / `funding_rates` / `custom_data` 字段已存在 |
| 合成 / fixture 读回 | ✅ | quotes、mark、greeks、instruments 等 PyO3 读回 smoke |
| Binance Futures live | ✅ | Step 1–2：`quotes` / mark / index / funding + `instruments` bootstrap |
| Deribit CLI | ✅ | `venue.kind = "deribit"` + `capture.deribit-btc.toml`（Step 3） |

### 关键约束

1. **Actor 主路径仍是 subscribe + cache bootstrap**
   `on_start` 先 `bootstrap_instruments`（`subscribe_instrument` + cache 快照，冷 cache 时 `request_instrument`），再订阅行情。
   **WS 实时流 = 主路径**；全量 universe HTTP 回填仍属 Step 8。

2. **CLI runtime 已支持 `binance_futures`、`deribit`、`bybit`、`okx`**
   Derive 仍待 Step 4d；多 venue 单 job 已验证（Deribit + Binance）。

3. **期权 universe 仍靠手写 `instrument_id`**
   尚无按 underlying / expiry / OI 的发现与筛选。

4. **不发明 schema**
   `CustomData` 必须原样录制 adapter 已发出的 `type_name` 与 payload（见 `docs/custom-data-contract.md`）。

---

## 分层模型

### 第一层：内置标准类型（Nautilus Model）

按「先易后难」推荐的实施顺序：

| 批次 | 数据家族 | 典型来源 | 难度 | 研究价值 |
|---|---|---|---|---|
| A1 | `quotes` | WS | ★ | 定价、spread、微观结构 |
| A2 | `mark_prices` | WS | ★ | 标记价、期权 IV 锚点 |
| A3 | `index_prices` | WS | ★★ | 跨所对齐、basis |
| A4 | `funding_rates` | WS | ★★ | carry、资金费率套利 |
| A5 | `instruments` | WS 事件 + HTTP 引导 | ★★ | 链重建、元数据 |
| A6 | `instrument_statuses` / `instrument_closes` | WS | ★★ | 停盘、到期、结算 |
| A7 | `option_greeks` | WS | ★★★ | IV 曲面、GEX、VRP |
| A8 | `trades` | WS | ★★★ | Trade tape、RV、order flow |
| A9 | `bars` | WS 或离线聚合 | ★★★ | RV 补充、降采样研究 |
| A10 | `book_deltas` | WS | ★★★★ | 深度、冲击成本（量大） |

### 第二层：Adapter CustomData

按「先 WS 流、后 HTTP 请求」：

| 批次 | 类型 | Adapter | 来源 | 难度 |
|---|---|---|---|---|
| B1 | `DeribitVolatilityIndex` | Deribit | WS | ★★ |
| B2 | `BinanceFuturesLiquidation` | Binance Futures | WS | ★★ |
| B3 | `HyperliquidOpenInterest` | Hyperliquid | WS | ★★ |
| B4 | `BinanceFuturesOpenInterest` | Binance Futures | HTTP request | ★★★ |
| B5 | `BinanceFuturesOpenInterestHist` | Binance Futures | HTTP batch | ★★★★ |

### 第三层：HTTP / 回填（非 runtime subscribe）

当前 Actor **不覆盖**，需新增「回填模式」或独立 job：

- `request_trades` / `request_bars` 历史成交与 K 线
- `request_funding_rates` 历史资金费率
- `request_instruments` 冷启动 universe
- `RequestCustomData` 驱动的 OI 快照与 OI 历史

---

## 总览：九个 Step

```mermaid
flowchart LR
    S0[Step 0\n基线巩固] --> S1[Step 1\n内置 WS 单层]
    S1 --> S2[Step 2\n内置 WS 衍生品状态]
    S2 --> S3[Step 3\nDeribit 期权 WS]
    S3 --> S4[Step 4\n多 venue 内置 WS]
    S4 --> S5[Step 5\nCustomData WS]
    S5 --> S6[Step 6\n内置 WS 深度与成交]
    S6 --> S7[Step 7\nCustomData HTTP]
    S7 --> S8[Step 8\nHTTP 历史回填]
    S8 --> S9[Step 9\n研究层与 DM 派生]
```

| Step | 主题 | 数据层 | 传输 | 预估工期 |
|---|---|---|---|---|
| 0 | 基线巩固 | 内置 A1–A2 | WS | 2–3 天 |
| 1 | 单所永续标准流 | 内置 A1–A4 | WS | 3–5 天 |
| 2 | 元数据与合约状态 | 内置 A5–A6 | WS + 轻量 HTTP | 3–5 天 |
| 3 | Deribit 期权 WS 栈 | 内置 A7–A8 | WS | 1–2 周 |
| 4 | 四所多 venue 内置 WS | 内置 A1–A8 | WS | 1–2 周 |
| 5 | CustomData 实时流 | Custom B1–B3 | WS | 1 周 |
| 6 | 成交与精选深度 | 内置 A8–A10 | WS | 1–2 周 |
| 7 | CustomData 请求型 | Custom B4–B5 | HTTP | 1 周 |
| 8 | 历史回填模式 | 内置 + Custom | HTTP | 2–3 周 |
| 9 | Universe 选择 + 离线派生 | 研究层 | — | 持续 |

---

## Step 0：基线巩固（最容易）

### 目标

确认「在线写入 → rollover → PyO3 读回」闭环稳定，不扩展 scope。

### 范围

- **交易所**：Binance Futures（testnet → mainnet）
- **标的**：`ETHUSDT-PERP`（或 `BTCUSDT-PERP`）
- **数据**：`quotes`（主），`mark_prices`（辅）

### 工作项

1. 跑通 `examples/capture.toml` CLI 与 `binance_futures_quote_capture` example
2. 用 `tests/python_catalog_probe.py` 验证时间窗与条数
3. 记录 queue depth、flush 原因、丢数策略（`overflow_policy`）
4. 固化「合格 live run」检查清单（写入目录、文件数、首尾 ts）

### 完成标准

- [ ] 连续 3 次 30s+ live run 无 panic、有 parquet 产出
- [ ] PyO3 读回 tick 序列单调递增
- [ ] 文档化 ops 参数：`flush_rows`、`queue_capacity`、`overflow_policy`

### 不做什么

- 不接入新交易所
- 不录 `trades` / `book_deltas`（避免过早引入吞吐压力）

---

## Step 1：单所永续 — 内置 WS 标准流（A1–A4）

### 目标

在 Binance Futures 上补齐 **basis / carry 研究** 所需的最小永续数据面。

### 范围

| 数据家族 | 订阅方式 | 说明 |
|---|---|---|
| `quotes` | WS | 已有 |
| `mark_prices` | WS | 已有 |
| `index_prices` | WS | **新增 live 验证** |
| `funding_rates` | WS | **新增 live 验证** |

### 工作项

1. 扩展 `examples/capture.toml`：显式打开 `index_prices`、`funding_rates`
2. 新增 example / smoke：`binance_futures_derivatives_state_capture`
3. 扩展 `python_catalog_probe.py` 或新增 probe 覆盖 index / funding 分区
4. 对比交易所 UI/API，确认 funding 更新频率与字段合理性

### 完成标准

- [x] 四类数据均有独立 parquet 分区
- [x] 同一 `instrument_id` 下 `ts_event` 语义正确
- [ ] 能离线计算简单 basis：`mark` vs `index`（可选 notebook）

### 支撑的未来 DM 功能

- Basis Tracker（永续腿）
- Vol Regime 的 RV 输入（配合后续 `trades`）

---

## Step 2：元数据与合约状态（A5–A6）

### 目标

让研究数据知道「录的是什么合约、何时不可交易」。

### 范围

- **交易所**：仍先 Binance Futures
- **数据**：`instruments`、`instrument_statuses`、`instrument_closes`

### 工作项

1. ✅ `bootstrap_instruments`：connect 后 cache 快照 + 冷 cache `request_instrument`
2. ✅ PyO3 `instruments` 读回（develop 扩展）
3. ✅ `capture.binance-perp.ws.toml` 含 `instrument_statuses` / `instrument_closes` 订阅
4. fixture smoke 验证 status/close；Binance live 短跑以 instruments 为主（status 轮询 ~3600s）

### 完成标准

- [x] `instruments` 分区含正确 `instrument_id`、精度、合约类型
- [x] status / close 分区与 PyO3 读回（fixture）；live 短跑允许无 status 行

### 难度说明

比 Step 1 略难：instrument 事件频率低，但分区与读回路径需与 quotes 一致。

---

## Step 3：Deribit 期权 — 内置 WS 栈（A7–A8）

### 目标

第一次接入 **期权研究主战场**，仍只使用内置类型 + WS。

### 范围

**交易所**：Deribit
**标的**：BTC、ETH
**Profile**：`targeted_derivatives`（见 `docs/native-custom-data-targets.md`）

| 腿 | 合约示例 | 数据家族 |
|---|---|---|
| 期权链（手写 MVP） | 近月 ATM call/put 若干 | `instruments`, `quotes`, `option_greeks` |
| 对冲永续 | `BTC-PERPETUAL`, `ETH-PERPETUAL` | `quotes`, `mark_prices`, `index_prices`, `funding_rates` |

### 工作项

1. CLI 增加 `venue.kind = "deribit"` 与 `DeribitDataClientFactory` 接线
2. 新增 `examples/capture.deribit-btc.toml`（少量手写 option `instrument_id`）
3. Live 验证 `option_greeks` 含：`delta/gamma/vega/theta/rho`、`mark_iv`、`open_interest`
4. 可选：同期权 `quotes`（top-of-book）

### 完成标准

- [x] Deribit mainnet 至少一条期权 + 一条永续同跑 60s+（`capture.deribit-btc.toml`）
- [x] `option_greeks` PyO3 读回字段完整（`python_catalog_deribit_probe.py`）
- [ ] 离线可画单所单到期 IV skew（哪怕只有 2–4 个 strike）

### 暂不做

- `trades`（Step 6）
- `DeribitVolatilityIndex`（Step 5）
- 自动链发现（Step 9）

### 支撑的未来 DM 功能

- Dashboard、Greeks View、Term Structure（单所）

---

## Step 4：多 venue 内置 WS 扩展

### 目标

把 Step 1 + Step 3 的「内置 WS 栈」复制到 DM 所需的其余期权所。

### 范围

按接入难度排序：

| 顺序 | 交易所 | 内置 WS 栈 | CLI 新增 `kind` |
|---|---|---|---|
| 4a | **Deribit** | Step 3 已完成 | `deribit` |
| 4b | **Bybit** | quotes, mark, index, funding, option_greeks, instruments | `bybit` |
| 4c | **OKX** | 同上 | `okx` |
| 4d | **Derive** | 同上 | `derive` |
| 4e | **Binance Futures** | Step 1–2 永续腿（对冲参考） | 已有 |

每个 venue 首期只录：**BTC + ETH，近月 4–8 个期权合约 + 1 条永续**。

### 工作项

1. 抽象 `VenueRuntimeConfig` 枚举，统一 `add_data_client` 注册
2. 支持 `[[venues]]` 多条目：一个 capture job 内 Deribit + Binance 并行
3. 每所一个 TOML profile + 一个 live smoke test
4. /catalog 分区按 `instrument_id` 自然隔离，无需项目级 merge

### 完成标准

- [x] 至少 **Deribit + Binance** 双 venue 同跑成功（`capture.multi-deribit-binance.toml`）
- [x] Bybit / OKX 各有「最小期权 + 永续」配置模板（Derive 待接）
- [x] 跨所读取时 `instrument_id` 后缀区分 venue（`.BINANCE` / `.DERIBIT` / `.BYBIT` / `.OKX`）

### 支撑的未来 DM 功能

- IV Divergence（四所 IV 对比）
- Lead-Lag（多所 greeks 时间戳对齐）

### 已知缺口（本 Step 不解决）

- **Binance Options**、**Thalex**：Nautilus 尚无 adapter → 列入 Step 9 评估

---

## Step 5：CustomData — WS 实时流（B1–B3）

### 目标

在 **不发明 schema** 前提下，录制高价值 adapter 原生 custom 流。

### 范围

按 `docs/native-custom-data-targets.md` 推荐顺序：

| 顺序 | CustomData | 交易所 | 标识符示例 |
|---|---|---|---|
| 5a | `DeribitVolatilityIndex` | Deribit | metadata `index_name=btc_usd` |
| 5b | `BinanceFuturesLiquidation` | Binance Futures | per instrument |
| 5c | `HyperliquidOpenInterest` | Hyperliquid（可选） | per instrument |

### 工作项

1. CLI 取消 `custom_data`「仅注释、未验证」状态；补 `examples/capture.deribit-dvol.toml` / `capture.hyperliquid-open-interest.toml`
2. 启动前调用 adapter `register_*_custom_data()`（参考 `write_hyperliquid_open_interest_fixture.rs`）
3. 新增 probe / smoke：`tests/python_deribit_dvol_smoke.py`、`tests/python_catalog_deribit_dvol_probe.py`、`tests/python_catalog_hyperliquid_open_interest_probe.py`
4. 文档化每个 `type_name` 的 `identifier` / `metadata` 填法

### 完成标准

- [x] `DeribitVolatilityIndex` fixture 写入且 PyO3 可读；live profile 已补
- [ ] `BinanceFuturesLiquidation` 与永续 capture 并行无串分区
- [x] `HyperliquidOpenInterest` fixture 写入可读；CLI venue/profile/probe 已补
- [ ] custom parquet 路径与 `type_name` 一致，满足 `custom-data-contract.md`

### 支撑的未来 DM 功能

- Vol Regime（DVOL + IV）
- 清算 / 拥挤度监控

---

## Step 6：内置 WS — 成交与精选深度（A8–A10）

### 目标

提高微观结构与 order flow 复现能力；控制存储成本。

### 范围

| 数据 | 策略 |
|---|---|
| `trades` | 期权 + 永续均开，优先近月活跃合约 |
| `bars` | 仅作 RV 补充（1m/5m），不替代 tick |
| `book_deltas` | **仅精选**：近月 ATM ±2 strike、25d put/call |

### 工作项

1. 压测：`trades` 开启后的 queue / flush / 丢数观测
2. 为 `book_deltas` 单独 TOML profile（与普通期权 profile 分离）
3. 制定 family 级默认：`book_deltas` 更小 `flush_rows`、更大 `queue_capacity`
4. Deribit block trade：确认 `TradeTick` 是否携带 block 标记；若无则记 adapter gap

### 完成标准

- [ ] 30min soak 无静默丢数（或丢数可 metrics 化）
- [ ] 精选深度合约 ≤ 20 个时磁盘增速可接受
- [ ] 离线可算签名 delta flow（基于 trades + greeks）

### 支撑的未来 DM 功能

- Trade Tape、Order Flow、RV / VRP

---

## Step 7：CustomData — HTTP 请求型（B4–B5）

### 目标

支持 **非流式** custom 数据，仍以 adapter 原生类型落盘。

### 范围

| 类型 | 方式 | 说明 |
|---|---|---|
| `BinanceFuturesOpenInterest` | `RequestCustomData` | 快照型 OI |
| 其他 venue OI | 视 adapter 能力 | 无则跳过 |

### 工作项

1. **扩展 capture 运行时**：二选一
   - **方案 A（推荐）**：新增 `CaptureScheduler` actor，定时 `request_data` → `on_data` 落盘
   - **方案 B**：独立 `catalog-capture-backfill` CLI，与 live 分离
2. 配置面：`[[capture.custom_data_requests]]`（interval_secs、type_name、identifier）
3. 验证 OI 快照与 WS greeks 内嵌 OI 可交叉核对

### 完成标准

- [ ] 每小时（可配置）OI 快照入库
- [ ] 请求失败可重试、可日志观测，不阻塞 WS capture

### 难度说明

这是第一个 **Actor 架构变更**（从纯 subscribe 到 subscribe + request），比 Step 5 难。

---

## Step 8：HTTP 历史回填模式

### 目标

冷启动 universe、补齐历史，服务回测与 ML 训练集。

### 范围

| 类型 | API 形态 | 模式 |
|---|---|---|
| `request_instruments` | HTTP | 冷启动链 |
| `request_trades` | HTTP | 历史成交 |
| `request_bars` | HTTP | 历史 K 线 |
| `request_funding_rates` | HTTP | 历史 funding |
| `BinanceFuturesOpenInterestHist` | HTTP batch | OI 历史曲线 |

### 工作项

1. 新建 `catalog-capture-backfill`（或在 CLI 增加 `mode = "backfill"`）
2. 支持时间窗、`start`/`end`、rate limit
3. 与 live catalog 目录约定兼容（同分区布局，不同 `capture_job` 元数据）
4. 样本外回测：回填 + live 接缝处无重复/缺口

### 完成标准

- [ ] 可回填至少 7 天 Deribit BTC 近月 option_greeks 或 trades 之一（视 API 限流）
- [ ] 回填数据与 live 数据 PyO3 同一套读 API

### 原则

- 回填是 **`historical_backfill` 模式**，不替代 Step 1–6 的 runtime WS（见 `native-custom-data-targets.md`）

---

## Step 9：Universe 选择 + 离线派生（面向 DM）

### 目标

从「手写 instrument_id」升级为「可维护的研究数据仓」，并离线复算 DM 面板。

### 工作项

**9a — 配置抽象（中等难度）**

- `underlying = "BTC"` + `expiry_days <= 45` + `top_n_by_open_interest`
- 定时 `request_instruments` 刷新 universe
- 设计参考见 `docs/option-universe-manager-design.md`

**9b — 离线派生 job（独立仓库或 `research/`）**

| 派生指标 | 输入原始数据 |
|---|---|
| IV surface / term structure | option_greeks + instruments |
| GEX / max pain | option_greeks（gamma × OI） |
| IV Divergence | 多 venue option_greeks 对齐 |
| Vol Regime / VRP | mark_iv + trades/bars（RV）+ DVOL |
| Basis / carry | index + mark + funding |
| Lead-Lag | 多 venue ts_event 序列相关 |
| Order flow | trades + delta |

**9c — 外部 adapter 缺口**

- Binance Options、Thalex：评估自研 adapter vs 第三方数据 vs 放弃

### 完成标准

- [ ] 一份配置可描述「BTC 近月全链」而无需列出 50+ instrument_id
- [ ] 离线 job 可从 raw catalog 产出至少 3 类派生面板（IV term、GEX、basis）

---

## 交易所 × Step 对照矩阵

| 交易所 | Step 0–2 | Step 3–4 | Step 5 | Step 6 | Step 7–8 | 备注 |
|---|---|---|---|---|---|---|
| Binance Futures | ✅ 主战场 | 对冲腿 | Liquidation | trades | OI 请求/历史 | 无期权 |
| Deribit | — | ✅ 期权主所 | DVOL | trades + 深度 | trades/bars 回填 | 流动性最大 |
| Bybit | — | ✅ | — | 同 Deribit | 视 API | — |
| OKX | — | ✅ | — | 同 Deribit | 视 API | — |
| Derive | — | ✅ | — | 同 Deribit | 视 API | — |
| Hyperliquid | — | 可选 | OI WS | — | — | 非 DM 六所 |
| Binance Options | — | — | — | — | 待 adapter | DM 缺口 |
| Thalex | — | — | — | — | 待 adapter | DM 缺口 |

---

## 推荐 TOML Profile 演进

### `capture.binance-perp.ws.toml`（Step 1）

```toml
[[venues]]
id = "binance_futures"
kind = "binance_futures"
environment = "mainnet"
product_type = "usd_m"

[[capture.quotes]]
instrument_id = "BTCUSDT-PERP.BINANCE"

[[capture.mark_prices]]
instrument_id = "BTCUSDT-PERP.BINANCE"

[[capture.index_prices]]
instrument_id = "BTCUSDT-PERP.BINANCE"

[[capture.funding_rates]]
instrument_id = "BTCUSDT-PERP.BINANCE"
```

### `capture.deribit-options.ws.toml`（Step 3）

```toml
[[venues]]
id = "deribit"
kind = "deribit"
environment = "mainnet"

# 近月 BTC 永续（对冲腿）
[[capture.quotes]]
instrument_id = "BTC-PERPETUAL.DERIBIT"

[[capture.mark_prices]]
instrument_id = "BTC-PERPETUAL.DERIBIT"

[[capture.index_prices]]
instrument_id = "BTC-PERPETUAL.DERIBIT"

[[capture.funding_rates]]
instrument_id = "BTC-PERPETUAL.DERIBIT"

# 手写近月期权（示例，需按实盘链更新）
[[capture.instruments]]
instrument_id = "BTC-28JUN26-100000-C.DERIBIT"

[[capture.quotes]]
instrument_id = "BTC-28JUN26-100000-C.DERIBIT"

[[capture.option_greeks]]
instrument_id = "BTC-28JUN26-100000-C.DERIBIT"
```

### `capture.multi-venue.ws.toml`（Step 4–5）

在 Step 3 基础上增加 `[[venues]]` 条目与 `[[capture.custom_data]]`（DVOL、liquidation）。

---

## 验证清单（每 Step 必做）

1. **写入**：catalog 目录出现预期分区（family + instrument / type_name）
2. **读回**：`tests/python_*_smoke.py` 或 probe 通过
3. **时间**：`ts_event` 单调、与 wall clock 偏差可解释
4. **运维**：queue depth、drop count、flush reason 可观测
5. **复算**：至少 1 个离线 notebook/job 证明「raw → 派生指标」可行

---

## 风险与依赖

| 风险 | 缓解 |
|---|---|
| WS 吞吐导致 `drop_oldest` 静默丢数 | 分 profile 拆 job；per-family metrics；关键家族 `block` 策略 |
| 期权 instrument_id 频繁变更 | Step 9 universe 刷新；instruments 分区保留历史 |
| HTTP 回填与 live 重复 | 分区 metadata 记 `capture_mode=live|backfill` |
| 无 Binance Options / Thalex adapter | 四所 MVP 先覆盖 DM ~80% 功能 |
| Actor 仅 subscribe | Step 7 前不承诺 OI 快照；Step 7 明确架构方案 |

---

## 建议的「下一步」

如果今天只做一个动作，执行 **Step 1**：

1. 在 `examples/capture.toml` 打开 `index_prices` + `funding_rates`
2. 对 Binance Futures mainnet 跑 60s live
3. 用 probe 验证 4 类内置 WS 数据均入库

这是投入最小、且直接解锁 basis / carry 研究的路径，也完全符合「先内置、先 WS、先易后难」。

---

## 修订记录

| 日期 | 说明 |
|---|---|
| 2026-06-18 | 初版：内置 → CustomData、WS → HTTP 分步路线图 |
