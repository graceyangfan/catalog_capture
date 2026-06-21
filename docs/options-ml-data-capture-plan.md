# Options / ML Data Capture Plan

## 目标

这份文档面向本项目第一阶段建设，目标不是先做一个展示型应用，而是先把 **可回放、可复算、可用于 ML 与期权策略研究** 的底层数据采集打稳。

我们当前的主要目标有三个：

1. 为 `Derive.xyz`、`Deribit`、`Bybit`、`OKX` 等衍生品交易所的期权策略开发准备研究级数据。
2. 为后续的特征工程、训练集构建、回测复现提供统一的原始数据底座。
3. 为未来做一个类似 `Derivatives Monkey` 的分析应用保留足够细的原始事件流，而不是只保存最终面板指标。

## 核心原则

### 1. 先采原始事件，再离线派生

对于 ML 和期权研究，最容易后悔的事情是“采少了”。
`GEX`、`skew`、`term structure`、`implied move`、`lead-lag`、`vol regime` 这些都应该尽量由离线任务从原始数据复算，而不是把派生结果当成唯一真相源。

### 2. 同时采集标的层与期权层

期权策略不能只靠期权本身的数据。

至少要同步覆盖：

- 现货 / 指数 / 永续
- 期权报价 / 成交 / Greeks
- 对冲腿相关的 `mark` / `index` / `funding`
- 交易所特有状态数据，如 `open interest`、`liquidation`、`volatility index`

### 3. 保留事件时间与接收时间

所有高频与跨 venue 分析都依赖严格的时间语义。
采集时必须尽可能保留：

- `ts_event`
- `ts_init`
- `venue`
- `instrument_id`
- 数据家族类型

这样后续才能分析：

- venue 之间谁先动
- 本地接收延迟
- 同一事件是否发生重排序

### 4. 采集阶段少做不可逆降采样

不要在 capture 阶段就把 tick 数据强行聚成分钟线。
`Bar` 可以作为补充，但不能替代 `QuoteTick`、`TradeTick`、`OrderBookDelta`、`OptionGreeks` 这些原始事件。

### 5. 按“研究用途”设计采集，而不是按“页面组件”设计采集

如果未来做类似 `Derivatives Monkey` 的应用，页面上看到的 `GEX`、`flow`、`skew`、`basis`、`regime` 都应来自研究数据仓，而不是反过来让页面指标决定底层采样口径。

## 当前仓库已经支持的采集面

基于当前代码，`CatalogCaptureActor` 和 `CapturePlan` 已经能覆盖以下数据家族：

- `instruments`
- `quotes`
- `trades`
- `bars`
- `book_deltas`
- `mark_prices`
- `instrument_statuses`
- `instrument_closes`
- `option_greeks`
- `custom_data`

但当前 CLI 配置层仍有明显边界：

- 已暴露到 TOML 的家族包括：
  - `instruments`
  - `quotes`
  - `trades`
  - `mark_prices`
  - `instrument_statuses`
  - `instrument_closes`
  - `option_greeks`
  - `bars`
  - `book_deltas`
- `custom_data` 已有示例 profile（如 DVOL / OI），但尚未纳入标准 `option_universe`
  families 自动展开。
- `index_prices`、`funding_rates` 已通过 `capture.option_universe.families` 和显式
  `[[capture.*]]` 支持；`option_chains` 与显式 `ForwardPrice` capture 仍待补齐。
- 期权 universe 已支持 Deribit / Bybit / OKX；Binance / Derive 仍在 roadmap。

这意味着：
当前仓库已经具备“研究级采集器”的基本雏形，但离“真正适合期权 ML 与跨 venue 衍生品研究”的第一版，还缺关键数据面和更好的配置抽象。

## 我们应该优先采集哪些数据

下面按优先级拆分。

## P0：必须采集的原始数据

### A. Instrument 定义与元数据

必须采：

- `InstrumentAny`
- 期权合约定义
- 永续 / 期货 / 现货定义

重点字段：

- `instrument_id`
- `venue`
- `underlying`
- `quote_currency`
- `expiry`
- `strike`
- `option_kind`
- `multiplier`
- `tick_size`
- `lot_size`

为什么必须采：

- 这是期权链重建、按 expiry/strike 分桶、生成 surface 的基础。
- 没有稳定的 instrument 元数据，后续很难做跨天研究和统一特征。

### B. 标的层 Top-of-Book 与成交

必须采：

- `QuoteTick`
- `TradeTick`
- `MarkPriceUpdate`

覆盖对象：

- 交易所本地现货
- 交易所本地永续 / 期货
- 期权对应的主要对冲腿
- 跨 venue 参考市场的核心现货 / 永续

为什么必须采：

- 方向、波动、微观流动性和 hedging 条件都依赖标的层。
- 期权 IV 和 Greeks 的解释必须结合 underlying 行为。
- 后续 `basis`、`lead-lag`、`microstructure alpha` 都靠这层数据。

### C. 期权层 Top-of-Book 与成交

必须采：

- 期权 `QuoteTick`
- 期权 `TradeTick`
- 期权 `MarkPriceUpdate`

为什么必须采：

- 期权研究不能只依赖 Greeks。
- 实盘研究一定要知道真实报价、成交、价差、活跃时段和流动性断层。
- `flow`、`whale trade`、主动买卖方向判断都来自成交流。

### D. Option Greeks

必须采：

- `OptionGreeks`

当前 `OptionGreeks` 至少应保留：

- `delta`
- `gamma`
- `vega`
- `theta`
- `rho`
- `mark_iv`
- `bid_iv`
- `ask_iv`
- `underlying_price`
- `open_interest`

为什么必须采：

- 这是重建 smile、term structure、IV percentile、GEX 的核心半成品。
- 对 ML 来说，Greeks 往往比单纯价格更稳定、更可泛化。
- 对策略来说，它是“期权事件空间”里最关键的结构化状态。

### E. Instrument 状态类事件

建议纳入 P0：

- `InstrumentStatus`
- `InstrumentClose`

为什么必须采：

- 研究数据需要知道合约是否暂停、到期、关闭、结算。
- 不采这类状态，后续很容易误把停盘/切换造成的缺口当成 alpha。

## P1：应尽快补齐的原始数据

### F. Index Price

必须尽快增加：

- `IndexPriceUpdate`

为什么重要：

- 期权定价、永续偏离、basis、套利判断都不能只看 mark。
- 不同 venue 的标记价格往往各自有平滑逻辑，`index_price` 更适合做跨 venue 对齐。

### G. Funding Rate

必须尽快增加：

- `FundingRateUpdate`

为什么重要：

- 永续是很多期权策略的 hedge leg。
- `funding` 本身也是情绪、拥挤度和 carry 的重要特征。
- 后续做 `basis + carry`、`hedge cost`、`regime` 都需要它。

### H. Venue Custom Data

必须尽快支持通过 CLI 显式录制 `custom_data`。

优先 custom data 类型：

- `open interest` 更新或快照
- `open interest history`
- `liquidation`
- 交易所波动率指数，例如 `DVOL`
- block trade / RFQ / 大宗成交
- venue 特有的 order flow 或聚合统计

为什么重要：

- 这些数据通常不是标准 quote/trade 能替代的。
- 如果目标是做类似 `Derivatives Monkey` 的分析，`OI`、`DVOL`、`flow`、`blocks` 都是高价值信号源。

### I. Option Chain Snapshot

建议增加：

- `OptionChainSlice`

为什么建议采：

- 虽然可以从逐个合约的 `quotes + greeks + instruments` 离线重建期权链，但 `option_chain` 快照作为“便捷研究层”非常有价值。
- 对下游应用、面板、训练集拼接更友好。

建议定位：

- `OptionChainSlice` 不是唯一真相源。
- 它应该是方便读取和快速研究的二级派生采集面，而不是替代逐合约原始事件。

### J. Forward Price / Synthetic Forward

建议增加：

- 显式 `ForwardPrice` 数据类型，或定期离线保存 forward bootstrap 结果

为什么重要：

- 期权期限结构、carry、moneyness 正规化都需要 forward 视角。
- 没有 forward，会影响跨 expiry 和跨 venue 的标准化研究质量。

## P2：后续增强数据

### K. 期权盘口深度

建议只对精选合约录制：

- `OrderBookDelta`

适用范围：

- 近月 ATM
- 近月 25d put / 25d call 附近
- 高 OI / 高成交的核心 strike

为什么不建议一开始全量采：

- 全链路期权深度数据非常重。
- 对第一阶段 ML 和面板建设，top-of-book + trade + greeks 的性价比更高。

### L. 跨 venue 参考市场

建议补充：

- 同时录制若干“参考定价市场”的现货 / 永续

例如：

- `BTCUSDT-PERP`
- 主流现货 `BTC/USD` 或 `BTC/USDT`
- 目标期权 venue 的本地 perp / index

为什么重要：

- 这是 `lead-lag`、跨 venue mispricing、hedge slippage 研究的基础。

## 这些原始数据将支持哪些派生研究结果

以下内容建议 **离线计算**，不要作为 capture 阶段唯一输出：

- `ATM IV`
- `IV smile`
- `term structure`
- `25d risk reversal / skew`
- `butterfly`
- `implied move`
- `GEX`
- `max pain`
- `call wall / put wall`
- `basis`
- `spot-perp premium`
- `lead-lag correlation`
- `flow imbalance`
- `whale / block trade detector`
- `vol regime`
- `anomaly detection`
- ML 特征面板

## 采集设计建议：不要一个任务录所有东西

建议把采集拆成几类 profile，而不是一个超大 capture job。

## Profile A：标的高频层

目标：

- 记录现货 / 永续 / 期货的高频报价、成交、mark、后续补 index/funding

建议录制：

- `quotes`
- `trades`
- `mark_prices`
- `book_deltas`，只对极少数核心 hedge leg 打开

用途：

- microstructure
- lead-lag
- basis
- hedge execution 研究

## Profile B：期权面板基础层

目标：

- 记录期权链研究最核心的数据，而不急着上全深度

建议录制：

- `instruments`
- 期权 `quotes`
- 期权 `trades`
- 期权 `mark_prices`
- `option_greeks`
- `instrument_statuses`
- `instrument_closes`

用途：

- IV surface
- skew
- term structure
- implied move
- OI/GEX 研究

## Profile C：慢速状态 / 自定义数据层

目标：

- 记录频率不高、但研究价值很高的 venue state

建议录制：

- `custom_data`（OI、DVOL、liquidations）
- `index_prices` / `funding_rates`（hedge leg；`option_universe` profile 已默认包含）
- 定时 instrument universe snapshot（`metadata/option_universe_resolutions.jsonl`）

用途：

- OI 曲线
- liquidation 研究
- DVOL / vol regime
- 跨 venue 归因

## Profile D：精选期权深度层

目标：

- 只对少量关键合约录制 `book_deltas`

建议选择：

- 近 1 到 3 个到期
- ATM 周围若干 strike
- 25d put / call 附近
- 高频成交或高 OI 合约

用途：

- 做市行为
- order book alpha
- 盘口韧性与冲击成本

## 期权 universe 选择建议

第一阶段不建议一开始对所有远端 long-tail strike 做全深度录制。
更合理的顺序是：

1. 全量录制期权 `quotes + trades + mark_prices + option_greeks`
2. 只对精选合约录制 `book_deltas`
3. 如果存储和吞吐稳定，再扩展更多到期和 strike 的深度

对于 BTC / ETH 这类主流资产，建议按以下逻辑选 universe：

- 近月和次近月全链路优先
- 远月优先保留 top-of-book 与 Greeks
- 深度盘口只覆盖高流动性关键 strike

## 数据录制时的关键要求

### 1. 事件排序必须稳定

同一 `instrument_id` 下必须优先保证事件顺序和时间语义稳定。
对于跨 venue 分析，也要尽量避免不同数据家族在写入时出现不可解释的乱序。

### 2. 必须有丢数可观测性

当前默认配置里存在 `overflow_policy`。
如果使用 `drop_oldest`，必须同步记录：

- queue depth
- dropped item count
- 每类数据的丢弃计数

否则 ML 训练集会在你不知道的情况下被悄悄污染。

### 3. 原始数据与派生数据分层保存

建议保持两层：

- `raw capture catalog`
- `derived research datasets`

不要把离线生成的 `GEX`、`surface snapshot`、`skew snapshot` 混进原始 market data 分区里。

### 4. 记录环境与版本信息

建议额外记录：

- venue environment
- adapter version
- capture config hash
- capture job name

这样回头才能解释“为什么这个月和上个月的字段、精度或覆盖范围不一样”。

## Derivatives Monkey 能力映射（capture 视角）

| DM 面板 / 概念 | 所需原始数据 | 当前 `option_universe` 覆盖 | 缺口 |
|---|---|---|---|
| Trade Tape | 期权 `trades` | 未默认纳入 universe families | 在 profile 中加 `trades` |
| IV surface / skew | 全链 quotes + greeks | 近月 ATM±N | Step 9a 全链 / OI-ranked |
| GEX / Key Levels | greeks + OI + 参考 spot | greeks 有；OI / 参考 spot 无 | `custom_data` + 参考现货 profile |
| ATM IV vs RV / DVOL | 连续 ATM IV + 波动率指数 | 近月窗口 greeks | DVOL `custom_data` |
| Cross-venue divergence | 多 venue 同时采集 + provenance | 单 job 单 venue；JSONL lineage 有 | 多 profile 并行 + 更广 universe |

原则不变：**capture 采原始事件，DM 面板离线复算**。Strike 选型使用 per-expiry
forward（`underlying_price` / forward API），perp 仅作 hedge leg。

## 对照 Derivatives Monkey BTC 文档后的录制收敛

参考 Derivatives Monkey 的公开 BTC 文档导航，产品能力大致收敛为五类：

- `Volatility`
- `Greeks`
- `Flow`
- `Multi-Exchange`
- `Quant`

公开文档入口：

- [BTC Docs](https://www.derivativesmonkey.com/btc/docs)
- [BTC Homepage](https://www.derivativesmonkey.com/)

从 capture 角度看，这五类页面并不要求我们“先做面板”，而是要求我们把下面这几层原始数据录扎实。

### Tier 1：必须 7x24 稳定录制

这是最核心的一层，缺任何一个都会直接限制 DM 风格分析的上限。

| 数据层 | 目的 | 当前状态 | 结论 |
|---|---|---|---|
| 期权 `instruments` | expiry/strike/kind 基础真相源 | 已支持 | 必须长期保留 |
| 期权 `quotes` | IV / skew / surface / spread | 已支持 | 必须长期保留 |
| 期权 `option_greeks` | IV / Greeks / GEX / OI | 已支持 | 必须长期保留 |
| 期权 `trades` | tape / flow / scanner | 已支持，但仍需长稳验证 | 必须长期保留 |
| 对冲腿 `quotes` / `mark_prices` / `index_prices` / `funding_rates` | basis / carry / hedge 解释 | 已支持 | 必须长期保留 |
| `instrument_statuses` / `instrument_closes` | 到期/停牌/结算边界 | 已支持，已纳入标准 universe profiles | 必须长期保留 |
| `forward_prices.jsonl` / `option_universe_resolutions.jsonl` | 解析 lineage / rollover 可追溯性 | 已支持 | 必须长期保留 |

这一层基本对应 DM 文档里的：

- `IV Percentile`
- `Term Structure`
- `Surfaces`
- `Vol Regime`
- `Skew Analytics`
- `GEX Profile`
- `Levels`
- `Greeks`
- `Greek Exposure`
- `Trade Tape`
- `Basis Spread`

### Tier 2：应尽快补齐的研究增强层

这一层不是“没有就不能录”，但没有它们，DM 风格页面会明显偏弱。

| 数据层 | 对应 DM 能力 | 为什么重要 | 当前状态 |
|---|---|---|---|
| 标的 `bars`（至少 perp 1m） | `Vol Regime`, `Quant`, `Backtest` | RV / VRP / regime 更稳 | 代码支持，标准 profile 未纳入 |
| 精选 `book_deltas` | `Order Flow`, `Scanner`, `Levels` | 观察盘口吸收/撤单/冲击 | 代码支持，尚无标准 options profile |
| `DeribitVolatilityIndex` | `Vol Regime` | 直接提供波动率指数锚点 | 已支持 |
| 多 venue 同时录制 | `Divergence`, `Lead-Lag`, `Basis Spread` | 单所无法做 cross-venue | 已有基础，缺 operator 级长期验证 |
| 参考 spot / index 录制 | `Basis Spread`, `Arb Guide`, `Lead-Lag` | 不能只看 perp/option | 当前偏 perp/index，现货参考仍弱 |

### Tier 3：DM 风格深度 flow 所需，但可晚于主线

| 数据层 | 对应 DM 能力 | 说明 |
|---|---|---|
| block / RFQ 专用流 | `Block RFQ` | 当前主仓库暂无专门 adapter-native 录制面 |
| liquidation / crowding custom data | `Scanner`, `Quant` | 已有 Binance liquidation 路线，但未打通 parquet 主线 |
| HTTP OI 快照 / 历史 | `OI by Strike`, `Quant` | 有助于低频 OI 研究与回填 |
| 更广全链 universe / 全市场扫描 | `Scanner`, `Heatmaps`, `Tail Strike` | 需要 `all` / 更宽 universe 定时 batch |

## 结论：为了对齐 DM，我们真正要“做踏实”的不是更多页面，而是四条录制主线

### 1. Rolling live 主线

目标：支持 `Volatility / Greeks / Basis / 基础 Flow`

必须稳定录：

- `instruments`
- `quotes`
- `trades`
- `option_greeks`
- `forward_prices`
- `mark_prices`
- `index_prices`
- `funding_rates`
- `instrument_statuses`
- `instrument_closes`

对应 profile：

- Deribit / Bybit / OKX `*-universe-autorefresh.toml`

### 2. Research live 主线

目标：支持 `Vol Regime / 基础 Quant / 多所对照`

在 rolling live 基础上，再保证：

- Deribit `DeribitVolatilityIndex`
- 多 venue 同时长稳录制
- 至少 perp 级 `bars`

### 3. OI-ranked 主线

目标：支持 `OI by Strike / GEX / Levels / Scanner`

关键点：

- `oi_ranked` universe + runtime refresh
- `option_greeks.open_interest`
- resolution lineage 持久化

注意：

- startup `oi_ranked` preflight 目前只在 Bybit 路径上更接近可用
- Deribit / OKX 现阶段应更多依赖 runtime warmup 后的 refresh

### 4. Full-chain batch 主线

目标：支持 `Surfaces / Heatmaps / Tail Strike / 离线全链研究`

关键点：

- `all` strike policy
- 控制 capture duration，而不是一上来 7x24
- readback / metadata / flush 行为必须稳定

## 下一阶段开发优先级（按 DM 对齐重新排序）

1. 把 `rolling live` 三家长稳录制跑顺
2. 把 `instrument_statuses` / `instrument_closes` 当成标准 family，而不是可选补充
3. 给 research baseline 增加 `bars`
4. 为 options 单独补一个精选 `book_deltas` profile
5. 把多 venue 长稳验证做成 operator 标准流程
6. 再考虑 block / liquidation / HTTP OI / 更广全链扫描

## 对当前仓库的具体建议

## 第一优先级：把它从“Binance 验证器”推进成“期权研究采集器”

建议按以下顺序推进：

1. CLI 暴露 `custom_data` 采集配置
2. 在 `CapturePlan` 与 `CatalogCaptureActor` 中增加：
   - `index_prices`
   - `funding_rates`
   - `option_chains`
3. 在 CLI runtime 中增加多 venue 支持：
   - `derive`
   - `deribit`
   - `bybit`
   - `okx`
4. 增加期权 universe 发现与选择机制

具体设计草案见 `docs/option-universe-manager-design.md`。

其中第 4 点非常关键。
期权不能像单一永续那样把 `instrument_id` 硬编码完事，更现实的做法是支持：

- 按 underlying 发现
- 按 expiry 窗口筛选
- 按 `option_kind`
- 按 OI / 成交 / 流动性阈值筛选

## 第二优先级：支持更适合研究的配置抽象

当前配置是“逐个 instrument 明确列出”的方式。
对于期权研究，建议后续扩展两类 selector：

### 家族选择器

例如：

- `underlying = "BTC"`
- `venue = "DERIBIT"`
- `expiry_days <= 45`
- `option_kind = "all"`

### 流动性选择器

例如：

- `top_n_by_open_interest`
- `top_n_by_volume`
- `delta_bucket`

这样后续既能支持研究，也能支撑类似 `Derivatives Monkey` 的定期全市场扫描。

## 第三优先级：派生层不要写死在 capture actor 里

建议将下面这些指标放在后续离线任务或 research job 中：

- `surface builder`
- `skew builder`
- `GEX builder`
- `basis builder`
- `lead-lag builder`
- `vol regime builder`
- `feature panel builder`

capture actor 的职责应尽量保持简单：

- 订阅
- 接收
- 分区
- flush
- 保证可读回

## 推荐的第一阶段落地顺序

### Phase 1：先打通研究必需数据

目标：

- 标的 `quotes/trades/mark`
- 期权 `quotes/trades/mark/greeks`
- `instruments`
- `instrument_statuses`
- `instrument_closes`
- `custom_data`

产出：

- 能做 IV、term、skew、基础 flow、OI 研究

### Phase 2：补齐衍生品定价关键状态

目标：

- `index_prices`
- `funding_rates`
- `option_chains`
- `forward price` 或对应离线 bootstrap

产出：

- 能做 basis、carry、cross-venue normalization、便捷研究快照

### Phase 3：补部分深度与多 venue

目标：

- 关键期权 `book_deltas`
- Derive / Deribit / Bybit / OKX 多 venue runtime

产出：

- 能做 lead-lag、depth alpha、跨 venue mispricing

### Phase 4：研究层和应用层

目标：

- 生成统一派生数据集
- 为仪表盘或分析应用提供稳定读取层

产出：

- 类似 `Derivatives Monkey` 的内部研究应用或面板基础

## 一个实用判断标准

如果某个数据在未来回答下面任一问题时会有帮助，它就值得优先保留：

- 当时真实报价和真实成交是什么？
- 这个 strike 的 IV、spread、OI、Gamma 当时如何变化？
- 标的先动，还是期权先动？
- 这笔 flow 是在买方向、买波动，还是在移仓？
- 做这个期权策略时，真实 hedge 条件和 carry 成本如何？
- 如果未来要重做特征工程，我还能不能从原始数据重建出来？

如果答案是“需要”，那它更应该以原始事件或半原始事件的形式进入 catalog。

## 建议的结论

对于 `nautilus_catalog_capture` 的第一阶段，最正确的目标不是“先做多少页面指标”，而是：

1. 把期权研究真正需要的原始数据家族采全
2. 把 `custom_data`、`index_prices`、`funding_rates`、`option_chains` 补进标准采集面
3. 把期权 universe 选择从“手写 instrument_id”升级成“按 underlying/expiry/liquidity 发现”
4. 把派生计算放到下游研究层

这样既能支撑 ML 和策略开发，也能自然延展到将来做类似 `Derivatives Monkey` 的应用。
