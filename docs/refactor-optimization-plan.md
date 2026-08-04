# 重构与优化计划（开源数据录制系统）

**Status:** active  
**Audience:** maintainers and contributors  
**Last updated:** 2026-08-04  

本文是把本仓库推进为 **可开源的专业数据录制系统** 的执行计划。  
依据既有架构审阅、开源产品审阅，以及 **Nautilus Trader 单 binary 哲学**；  
**不改变**「专职 CaptureActor + 声明式 Plan + Nautilus 原生 catalog」的核心设计。

---

## 1. 目标与非目标

### 1.1 目标

| ID | 目标 | 成功标准 |
|----|------|----------|
| G1 | **录制目录与 Nautilus Trader Rust catalog 对齐**，可直接用于 Rust 回测 | 仅写 Rust `ParquetDataCatalog` 布局；无 Python legacy mirror |
| G2 | 开源交付可安装、可信任 | clone + pin 依赖后 CI 与文档一致 |
| G3 | 主路径配置简洁自然 | TOML 配置 + 单 CLI；subscribe / request 不混 |
| G4 | **一个产品 binary** + 瘦编译图 | 对齐 `nautilus` CLI：无产品侧多 bin；examples 不进默认构建 |
| G5 | 运维可观测 | request/subscribe 指标清晰；soak 契约稳定 |

### 1.2 非目标（本计划明确不做）

- 不 fork Nautilus Trader  
- 不把 offline derive / ML 特征写进 capture 热路径  
- 不把本项目做成数据商或查询引擎  
- 不在本计划内重写 venue HTTP/WS（继续用 Nautilus adapter client）  
- **不为演示再增加 cargo `[[bin]]` / `[[example]]` 产品入口**  

---

## 2. 硬约束：Catalog 布局与 Rust 回测对齐

### 2.1 原则（**已落地**）

```text
真相源 = Nautilus Trader Rust ParquetDataCatalog 目录布局
本项目写入 = 仅该布局（rust_canonical_only）
Python legacy mirror = 已移除（配置旧值会 validate 失败）
```

```text
capture → file://catalog_uri  (Rust canonical only)
       → nautilus_trader Rust ParquetDataCatalog / Backtest
```

### 2.2 目录契约

```text
{catalog_uri}/
  data/
    instruments/…  quotes/…  trades/…  mark_prices/…
    index_prices/… funding_rate_update/… option_greeks/…
    order_book_deltas/… bars/…  custom/<TypeName>/…
  metadata/   # universe resolution, forward_prices, …
```

### 2.3 Track L 状态

| ID | 任务 | 状态 |
|----|------|------|
| L1 | 默认 `rust_canonical_only` | **done** |
| L2 | examples 全量改为 canonical | **done** |
| L3 | 拒绝 mirror 配置 | **done**（报错迁移） |
| L4 | how-to rust backtest | **done** |
| L5 | 无网 fixture / unit 证明写路径 | **done**（`catalog_layout` quotes + custom 写盘测试） |
| L6 | custom 路径审计 | **done**（`data/custom/{TypeName}`；subscribe/request 同 sink） |
| L7 | `metadata/capture_run.json` | **done** |

---

## 3. 硬约束：单产品 Binary（对齐 Nautilus Trader）

### 3.1 Nautilus 做法（参考）

| 点 | Nautilus Trader |
|----|----------------|
| 产品入口 | **一个** `nautilus` binary（`nautilus-cli`） |
| 能力扩展 | clap **subcommands** + **optional features**（如 `defi`） |
| 库 | adapter/model/live 均为 **`rlib`**，无产品 bin |
| 演示 | `[[example]]` + `required-features = ["examples"]`，默认不编 |
| 产物 | `profile.dev`：`debug=false`、`strip=debuginfo`；deps `opt-level=1` |

### 3.2 本仓库目标形态

```text
crates/
  catalog-capture-core/              # rlib only
  catalog-capture-runtime-adapter/   # rlib only
  catalog-capture-cli/               # 唯一产品 [[bin]]

examples/*.toml                      # 配置样例（不是 cargo binary）
tests/                               # 验证
dev/legacy-examples/                 # 已降级的旧 cargo examples（不编）
```

**用户只记住：**

```bash
cargo build -p catalog-capture-cli
cargo run -p catalog-capture-cli -- run --config examples/...
```

### 3.3 Track P — Product binary & build simplicity

| ID | 任务 | 状态 |
|----|------|------|
| P1 | 文档/Makefile：**只编 CLI**，禁止默认 `--examples` | **done** |
| P2 | 移除 runtime-adapter `[[example]]`；迁到 `dev/legacy-examples/` | **done** |
| P3 | Workspace **dev profile**（strip debuginfo；deps opt-level=1） | **done** |
| P4 | 去掉 hyperliquid **`python` feature**（capture 不需要） | **done** |
| P5 | `make clean` / `clean-debug` | **done** |
| P6 | CLI **venue cargo features**（`venue-*` / `all-venues`；默认 all-venues） | **done** |
| P7 | CI：默认 feature 的 CLI + `--lib`/`--bins` tests；slim `venue-deribit` check | **done** |
| P8 | 可选：bin 重命名为 `catalog-capture` | open |

### 3.4 原则（可贴 CONTRIBUTING）

1. 产品 binary 有且仅有一个：`catalog-capture-cli`  
2. 库 crate 禁止新增 `[[bin]]`  
3. 演示用 **TOML + CLI**，不新增 cargo examples  
4. 重依赖必须 optional feature（venue 等）  
5. dev profile 控制产物体积（对齐 nautilus_trader）  
6. `target/` 永不提交；本地可用 `make clean-debug`  

---

## 4. 工作轨道总览

```text
Track L  Layout / Rust backtest          ← 大部分 done
Track P  Product binary & build graph    ← 本轮重点（Nautilus 单 bin）
Track O  Open-source delivery
Track C  Config & code structure
Track B  Venue features（原 Track B，与 P6 合并推进）
Track R  Runtime observability
Track D  Docs IA
```

---

## 4. Track O — 开源交付

| ID | 任务 | 验收 |
|----|------|------|
| O1 | 统一 toolchain **1.97.1**（README、Makefile、CI、CONTRIBUTING、installation） | **done** |
| O2 | CI pin `nautilus_trader` **固定 git rev**（禁止默认 `develop`） | **done**（`a7159b484e…`） |
| O3 | `scripts/bootstrap-deps.sh`：优先本地 sibling/`NAUTILUS_TRADER_PATH`；缺失则 clone upstream **develop**；可选 `--pin-ci` | **done**（`make bootstrap-deps`） |
| O4 | README 首屏重写：定位、边界、多 venue、Rust catalog、3 条 happy path | **done** |
| O5 | 删除或 stub `catalog-capture-plugin-adapter` 空 crate | **done**（已删除空目录） |
| O6 | CHANGELOG `## 0.1.0` + git tag | **done**（本迭代 commit + `v0.1.0`） |
| O7 | 强化 unofficial / LGPL / Nautilus 商标说明（README + NOTICE） | **done** |
| O8 | 凭证：从环境变量注入 API key（可选）；默认 public；文档说明 | **done** |

---

## 5. Track C — 配置与代码结构

| ID | 任务 | 验收 |
|----|------|------|
| C1 | 拆分 `cli/src/config.rs` 为 `config/` 模块（venues、plan、custom subscribe/request、universe、hip4、validate） | **done**（生产代码模块均 <400 行；测试仍集中在 `tests.rs`） |
| C2 | custom 类型 registry：subscribe vs request 白名单单点维护 | **done**（`custom_data/`；parse/validate/register 共用） |
| C3 | 保持 `[[capture.custom_data]]` vs `[[capture.custom_data_requests]]` 严格分离 | **done**（双向拒绝 + unit/config 测试） |
| C4 | examples 目录：`minimal/` `research/` `production/` `experimental/` | README 只推 minimal + 一条 research |
| C5 | core 公共 API：标注 stable surface；内部 `pub(crate)` 收敛 | 文档一小节 |

---

## 6. Track B / P6 — Venue features 与 CI 瘦身

| ID | 任务 | 状态 |
|----|------|------|
| B1 / P6 | CLI `features`：`venue-deribit` `venue-binance` … | **done** |
| B2 / P7 | CI：默认 feature + `cargo test --workspace --lib --bins` + slim deribit check | **done** |
| B3 | （可选）Docker/GHCR | open |

原则（与 Nautilus 一致）：

- **一个 binary 名字不变**；变的是编进去的 venue 集合  
- 不靠增加 bin 减负  
- 不靠砍录制语义减负，靠 **optional features** 减负  

**已知残留成本：** `catalog-capture-runtime-adapter` 仍始终链接 `nautilus-hyperliquid`
（HIP-4 dynamic universe）。CLI feature 只瘦 **CLI 直接 adapter client** 图；
若要彻底去掉 hyperliquid 编译成本，需后续把 HIP-4 也做成 adapter optional feature。  

---

## 7. Track R — 运行时与运维

| ID | 任务 | 验收 |
|----|------|------|
| R1 | request 指标：polls / rows / skipped_inflight / timeouts | **done**（`/metrics` + `/metrics.json`） |
| R2 | soak 表增加 request 路径通过标准 | **done**（smoke_and_soak 验收表 + watch list） |
| R3 | per-family flush 默认建议表（文档 + 可选 overrides） | quotes vs greeks vs book_summary |
| R4 | unattended 默认 profile 使用 `rust_canonical_only` | operator 示例对齐 L2 |

---

## 8. Track D — 文档信息架构

### 8.1 目标结构（Divio，入口 ≤ 一屏）

| 类型 | 保留 |
|------|------|
| Getting started | installation, quickstart |
| How-to | smoke_and_soak, unattended, **rust_backtest_from_catalog** |
| Concepts | architecture（一页）, **本计划**, custom-data-contract, segment-lifecycle, flush-rotation |
| Reference | cli.md + TOML 字段 |
| Archive | rfc, implementation-plan, stepwise-roadmap 历史进度、options-ml-plan 等标 `historical` |

### 8.2 任务

| ID | 任务 |
|----|------|
| D1 | `docs/index.md` 链到本计划与 rust backtest how-to | **done** |
| D2 | 历史 plan/roadmap 顶部加 banner：指向 ROADMAP + 本计划 | **done** |
| D3 | ROADMAP 改为「现状 + 下一步」不再列已完成 Phase 2 项为 TODO | **done** |

---

## 9. 分阶段里程碑

### M0 — 布局与 Rust 回测契约（优先）

**主题：** G1  

- L1–L5  
- D1 中 rust backtest 文档骨架  
- 至少 1 个 minimal 示例默认 `rust_canonical_only`  
- fixture：Rust catalog 读回 quotes（或现有 synthetic 扩展）  

**退出标准：** 文档与示例明确「默认 Rust 布局」；自动化证明 canonical 可读。

### M1 — 开源可装 v0.1

**主题：** G2  

- O1–O8  
- L2 全量示例默认调整  
- tag `v0.1.0`  

**退出标准：** 干净机器按 README 能 validate + 跑 unit tests；CI 与本地 toolchain/rev 一致。

### M2 — 可贡献

**主题：** G3 + G4（结构）  

- C1–C5  
- D2–D3  
- R1–R2  

### M3 — 可扩展

**主题：** G4 + G5  

- B1–B3  
- L6–L8  
- R3–R4  

---

## 10. 已落地配置（布局）

```toml
[output]
catalog_uri = "file:///path/to/catalog"
compression = "snappy"
layout_compatibility = "rust_canonical_only"  # 唯一合法值；可省略
```

`rust_canonical_with_python_legacy_mirror` → **validate 失败**（已移除实现）。
---

## 11. 测试与验收矩阵

| 层级 | 内容 | CI |
|------|------|-----|
| Unit | plan、layout、request job、budget | PR 必跑 |
| Layout | `rust_canonical_only` 写入路径断言 | PR 必跑 |
| Rust readback | fixture → Rust ParquetDataCatalog | PR 必跑（无网） |
| CLI validate | 扫描 examples | PR 建议 |
| Live smoke | 单 venue 短跑 | manual / nightly |
| Rust backtest smoke | 录制 catalog → 最小 backtest | manual → 后进 nightly |
| Soak | dropped_items、request metrics | manual |

---

## 12. 风险与缓解

| 风险 | 缓解 |
|------|------|
| Nautilus layout 随版本变化 | pin rev；L7 写入 nautilus_git_rev；升级 adapter 单独 PR |
| 误以为需要多个 cargo examples | Track P：统一 CLI + TOML；legacy 在 `dev/legacy-examples/` |
| `target/` 膨胀 | Track P：dev profile strip debuginfo；`make clean-debug` |
| venue feature 矩阵测试爆炸 | 默认集 + nightly all-features |
| 文档与代码再漂移 | M1 起「改默认必须改 README/examples」检查清单 |

---

## 13. 当前优先级排序

### 已完成（基线）

1. ~~L1–L4 / Python mirror 移除~~ **done**  
2. ~~P1–P7 / B1–B2 单 binary + venue features + CI~~ **done**  
3. ~~O1–O5 / O7 开源装得上 + README + 合规文案~~ **done**（O3 bootstrap；O6 tag 待发）  
4. ~~C1–C3 config 拆分 + custom registry + 双向误配测试~~ **done**  

### 下一步（建议，post 0.1.0）

1. **C4** — examples 分层（minimal / research / operator）  
2. **C5** — core public API 文档  
3. **R3** — per-family flush 建议表  
4. 可选：P8 bin 改名、B3 Docker、HIP-4 optional feature  

~~M1 开源可装~~ **done** · ~~L5/L6 写路径~~ **done**

---

## 14. 相关文档

- [Architecture](architecture.md)  
- [Flush and rotation](flush-rotation-policy.md)  
- [Custom data contract](custom-data-contract.md)  
- [Smoke and soak](how_to/smoke_and_soak.md)  
- [Live validation](live-validation.md)  
- [ROADMAP](../ROADMAP.md)  
- [Rust backtest from catalog](how_to/rust_backtest_from_catalog.md)（L4 **done**）  

---

## 15. 变更记录

| 日期 | 说明 |
|------|------|
| 2026-08-04 | 初版：开源重构计划；强制 Rust canonical 布局为默认与回测契约 |
| 2026-08-04 | **已落地 Track L（严格）**：移除 Python legacy mirror 实现；仅接受 `rust_canonical_only` |
| 2026-08-04 | **Track P**：对齐 Nautilus 单 binary；移除 cargo examples；dev profile；去 hyperliquid python feature |
| 2026-08-04 | **Track O/C 推进**：bootstrap-deps；venue features；config/ + custom_data registry；README 首屏；删除空 plugin crate；C3 误配测试；合规 NOTICE/TRADEMARK |
