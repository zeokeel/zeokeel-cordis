# Cordis（Rust 内核）

**纯 Rust 的可插拔内核，实现 Cordis「时空可组合」编程范式。**
不依赖框架、不依赖运行时：上层宿主（桌面应用、服务端、Agent 运行时、测试框架）只要依赖这个 crate，
就能把自己的插件安全地插进同一棵 Context 树里——插件注册的一切（服务、监听器、清理逻辑、子插件）
都会在它的 fiber 卸载时按逆序、恰好一次地回收。

[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](#环境要求)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-311%20passed-brightgreen.svg)](#质量与测试)

---

## 目录

- [这是什么](#这是什么)
- [设计思想](#设计思想)
- [论文来源](#论文来源)
- [核心概念](#核心概念)
- [快速开始](#快速开始)
- [常用用法](#常用用法)
- [架构与模块](#架构与模块)
- [文档索引](#文档索引)
- [质量与测试](#质量与测试)
- [隔离与故障边界](#隔离与故障边界)
- [版本状态](#版本状态)
- [已知限制](#已知限制)
- [许可证](#许可证)

---

## 这是什么

Cordis 的核心只有四件事：

1. **Context（上下文）**：一棵不可变的树。每个节点都拥有自己的作用域——服务查找、事件派发、
   隔离（isolate）都沿这棵树进行。
2. **插件（Plugin / AsyncPlugin）**：一段可加载、可卸载的初始化逻辑，运行在一条 fiber 上。
3. **Fiber（纤程）**：插件的生命周期载体，状态机为
   `Pending → Loading → Active → Failed / Unloading → Disposed`。
4. **Effect（可逆副作用）**：插件注册的每个副作用都对应一个清理函数；卸载时按注册的逆序、恰好一次执行。

内核**只做核心**：没有模块加载器、没有定时器、没有权限系统、没有 UI 插槽、没有动态 ABI。
这些属于宿主（embedder）。内核的承诺是：**任何时刻都能安全地加载、卸载、重启、替换插件，
并且系统始终处于可预期的一致状态。**

### 适用场景

- 桌面 / 服务端应用的插件系统：插件热插拔、热重启、热替换；
- Agent / 工具运行时：工具（插件）按需挂载，依赖服务自动激活 / 停用；
- 需要「卸载即完全回滚」的长期运行进程；
- 想用纯 Rust、无 GC、无反射、无动态库的系统。

---

## 设计思想

> 详细推导、术语映射和设计不变量见 [`docs/DESIGN.md`](docs/DESIGN.md)。

Cordis 把「动态组合」拆成两个正交的维度：

### 1. 时间可组合性（Temporal Composability）——可逆 Effect

一个组件在**时间上**可以被完整移除：它做过的每一件事都有对应的逆操作，运行时持有这些逆操作。
卸载组件时，逆操作按注册的逆序执行，于是系统回到组件加载前的状态。

内核中的落地：

- 所有副作用都通过 `Effect` 列表登记（服务提供、事件监听、清理回调、子插件……）；
- 每条 effect 对应一个单次、幂等的 `Disposer`（或异步的 `AsyncDisposer`）；
- 卸载 / 重启 / 激活失败回滚时，按**逆注册顺序**逐个执行；
- 同步与异步清理共享同一个顺序，保证确定性；
- 取消一次异步卸载不会破坏这个顺序：未开始的清理会被放回列表，下次重试继续；
- 清理里的 panic 被逐个隔离（记日志、继续执行其余清理），不会中断整个卸载。

### 2. 空间可组合性（Spatial Composability）——响应式 Coeffect

一个组件在**空间上**可以被精确描述：它声明「我需要哪些外部上下文」（coeffect），
运行时把上下文的每次变化按这份声明分类，自动决定组件的**激活 / 停用 / 重算**。

内核中的落地：

- 插件通过 `inject()` 声明依赖的服务名列表（coeffect specification）；
- 依赖未就绪时插件处于 `Pending`，就绪后自动激活；
- 依赖变化（provider 注册 / 撤销 / 替换）会触发依赖方重新求值：该卸载的卸载，该重启的重启；
- 服务用 `ServiceKey<T>` 做**类型化、唯一**的身份，杜绝「同名不同类型」的错配；
- provider 身份带 epoch：同一个 key 换了提供者，依赖方会感知并重算。

### 3. 统一 Context 与观测等价（Observational Equivalence）

effect 上下文（「我改了什么」）与 coeffect 上下文（「我依赖什么」）被统一到同一个 `Context` 类型里，
所有 effect/coeffect 都经过它中介。其工程含义是：

- **隔离作用域**：`extend()` 派生新的作用域；`isolate()` 让同名的服务/事件在子树内不可见，互不干扰；
- **组件之间观测等价**：只要两个组件的隔离映射不同，它们对彼此的服务与事件就是不可见的，
  它们可以任意交错执行而不会互相破坏；
- 事件派发同样带作用域：默认只派发给「同一作用域」的监听器，`global` 监听器除外。

### 4. 组件与生命周期：事务式激活

插件激活是**事务性**的：`apply` 中注册的任何东西都归属这条 fiber；`apply` 返回 `Err` 或 panic 时，
已经注册的 effect 会按逆序回滚，fiber 进入 `Failed`，不会留下半初始化的状态。
生命周期操作（`dispose` / `restart` / `update`）是互斥的：并发调用返回 `LifecycleBusy` 而不是数据竞争；
同线程的重入调用被允许。子插件是父 fiber 的 effect：父卸载时整棵子树被级联回收。

---

## 论文来源

本项目的设计直接来自下面这篇论文：

> **A Programming Paradigm for Spatiotemporal Composability**
> Yifan Shi（北京大学 / DeepSeek-AI）、Wei Zhang（北京大学）、Tianyi Cui（DeepSeek-AI）
> arXiv:2608.25512 [cs.PL]，2026-08-26，92 页
> <https://arxiv.org/abs/2608.25512>

论文的核心概念与本项目的对应关系：

| 论文概念 | 内核中的体现 |
| --- | --- |
| 时间可组合性 / revertible effects | fiber 的 effect 列表、`Disposer`、逆序清理、激活回滚 |
| 空间可组合性 / reactive coeffects | `Plugin::inject`、`Pending` 门控、依赖刷新、provider epoch |
| context paradigm / 统一上下文 | `Context` 类型 + 作用域（`extend`/`isolate`） |
| 组件（component）与动态组合演算 | 插件 + fiber 状态机、子插件所有权、生命周期互斥 |
| observational equivalence（观测等价） | 隔离作用域：不同 isolate 映射的组件互相不可见 |
| Cordis 元框架（core library + loader） | 本项目实现的是 **core library**；loader/HMR 属于宿主层 |

引用格式：

```bibtex
@misc{shi2026cordis,
  title         = {A Programming Paradigm for Spatiotemporal Composability},
  author        = {Shi, Yifan and Zhang, Wei and Cui, Tianyi},
  year          = {2026},
  eprint        = {2608.25512},
  archivePrefix = {arXiv},
  primaryClass  = {cs.PL},
  url           = {https://arxiv.org/abs/2608.25512},
}
```

本 crate 是论文思想的**独立纯 Rust 实现**，与 Cordis 官方 TypeScript 实现保持语义对齐
（差异见 [`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) §5）；论文版权归原作者，代码以 MIT 发布。

---

## 核心概念

| 概念 | 一句话说明 | 入口 |
| --- | --- | --- |
| `Context` | 作用域上下文；服务、事件、配置都从它出发 | `Context::root()`、`extend`、`isolate`、`intercept` |
| `Plugin` / `AsyncPlugin` | 插件实现：`apply` / `apply_async` | `ctx.plugin`、`ctx.plugin_typed`、`ctx.plugin_async` |
| `Fiber` | 插件生命周期句柄：状态、配置、重启、替换、取消 | `Fiber::{state, restart, update, dispose, cancel}` |
| `ServiceKey<T>` | 类型化的服务身份 | `ctx.provide`、`ctx.get`、`ctx.require` |
| `inject` | 声明依赖；依赖就绪才激活 | `Plugin::inject` |
| `Effect` / `Disposer` | 可逆副作用与清理句柄 | `ctx.effect`、`ctx.effect_async` |
| 事件 | 跨插件通信（emit / bail / parallel / serial / waterfall） | `ctx.on`、`ctx.emit`、`ctx.bail`、`ctx.waterfall` |
| `Registry` | 插件 runtime 映射与全局诊断 | `ctx.registry()` |
| 静默 / 取消 | 等待所有生命周期操作结束 / 协作式取消长任务 | `when_idle`、`Fiber::cancel`、`ctx.checkpoint` |

---

## 快速开始

### 环境要求

- Rust **1.85+**（edition 2021）
- 依赖：`serde_json`、`tracing`、`futures-util`（同步内核）；异步宿主可选 `tokio`

### 添加依赖

```toml
[dependencies]
cordis = { git = "https://github.com/zeokeel/Cordis", tag = "v0.3.0" }
serde_json = "1"
```

### 最小可运行示例

```rust
use std::sync::Arc;

use cordis::{cleanup, Context, Plugin, Result};
use serde_json::Value;

struct Greeter;

impl Plugin for Greeter {
    fn name(&self) -> &'static str {
        "greeter"
    }

    fn apply(&self, ctx: Context, config: Value) -> Result<()> {
        let who = config.get("name").and_then(Value::as_str).unwrap_or("world").to_string();
        println!("hello, {who}");
        // 注册一个清理逻辑：fiber 卸载时按逆序执行，恰好一次。
        ctx.effect(move || vec![cleanup(move || println!("bye, {who}"))])?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(Greeter), serde_json::json!({ "name": "Cordis" }))?;
    assert!(fiber.is_active());

    fiber.dispose()?;   // 卸载插件：打印 "bye, Cordis"
    root.dispose_all()?; // 关闭整棵上下文树（推荐在宿主退出时调用）
    Ok(())
}
```

运行：`cargo run --example hello`（示例见 [`examples/`](examples/)）。

### 异步插件

```rust
use std::sync::Arc;

use cordis::{block_on, cleanup_async, AsyncPlugin, BoxFuture, Context, Result};
use serde_json::Value;

struct Ticker;

impl AsyncPlugin for Ticker {
    fn name(&self) -> &'static str {
        "ticker"
    }

    fn apply_async<'a>(&'a self, ctx: Context, _config: Value) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ctx.effect_async(|| vec![cleanup_async(|| async { /* 关闭后台任务 */ })])?;
            Ok(())
        })
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = block_on(root.plugin_async(Arc::new(Ticker), Value::Null))?;
    block_on(fiber.dispose_async())?;
    root.dispose_all()
}
```

异步插件必须通过 `*_async` 入口使用（`plugin_async`、`dispose_async`、`restart_async`、`update_async`）；
同步 API 用在异步插件上会返回 `CordisError::AsyncPluginNeedsAsync`。

---

## 常用用法

完整教程（含每一步的解释与可运行代码）见 [`docs/GUIDE.md`](docs/GUIDE.md)。

### 提供服务与依赖注入

```rust
static CLOCK: ServiceKey<Clock> = ServiceKey::new("clock");

// 提供方：注册即被当前 fiber 拥有，卸载时自动撤销
ctx.provide(CLOCK, Arc::new(Clock::new()))?;

// 使用方：声明依赖，依赖就绪才激活
impl Plugin for Uptime {
    fn inject(&self) -> &'static [&'static str] { &["clock"] }
    fn apply(&self, ctx: Context, _: Value) -> Result<()> {
        let clock = ctx.require(CLOCK)?; // 缺失时返回 UnknownService
        Ok(())
    }
}
```

`ServiceKey<dyn Trait>`（trait object 服务）用 `provide_any` / `get_any` / `require_any` / `set_any`。

### 事件与中间件

```rust
ctx.on("before-request", |args| EventResult::Continue)?;       // 监听
ctx.bail("before-request", &args)                              // 首个 bail 生效
ctx.waterfall("before-request", &args, Some(terminal));        // 中间件链
```

同步事件用 `emit` / `bail`；异步监听器用 `parallel_async` / `serial_async` 派发，
`on_async` 注册；两条 lane 按注册顺序各自派发，不会被静默降级。

### 子插件与隔离

```rust
let child = ctx.plugin(Arc::new(SubPlugin), Value::Null)?; // 子 fiber 是父 fiber 的 effect
let isolated = ctx.isolate("fs");                          // 同名服务/事件在本子树内独立
let labeled  = ctx.isolate_labeled("fs", "my-namespace");  // 相同 label 的子树共享作用域
```

### 热重启、热替换与关闭

```rust
fiber.update(new_config)?;          // 校验配置并重启
fiber.restart()?;                   // 用当前配置重启
root.registry().replace(old_key, Arc::new(NewPlugin))?;  // 原地替换实现，保留 fiber 身份
root.dispose_all()?;                // 关闭整棵树
```

---

## 架构与模块

```
Context 树（不可变、可派生）
 ├─ 服务存储（每个 isolate 作用域一份）
 ├─ 事件总线（同步 lane / 异步 lane / waterfall / 内部 typed hook）
 ├─ 插件 Registry（PluginKey → PluginRuntime → fibers）
 └─ Fiber 树
     ├─ 状态机：Pending → Loading → Active → Failed / Unloading → Disposed
     ├─ Effect 列表（同步 + 异步，逆序清理）
     └─ 子 Fiber（父 fiber 的 effect，级联回收）
```

| 模块 | 职责 |
| --- | --- |
| `src/context.rs` | Context 树、作用域派生/隔离、服务 API、插件入口、事件快捷方法 |
| `src/service.rs` | `ServiceKey`、可用性检查、服务条目 |
| `src/fiber.rs` | fiber 状态机、生命周期互斥、effect 所有权、重启/更新/替换/取消 |
| `src/effect.rs`、`src/async_effects.rs` | 同步/异步清理句柄与取消安全 |
| `src/events.rs`、`src/internal.rs`、`src/waterfall.rs` | 事件、typed 内部钩子、中间件链 |
| `src/registry.rs` | 插件 trait、runtime 映射、依赖刷新、静默、`replace`、`dispose_all` |
| `src/config.rs` | 配置 schema 与校验 |
| `src/logger.rs` | 日志服务与导出器 |
| `src/reflect.rs` | 访问器与 Context 元信息 |
| `src/exec.rs` | 无外部运行时依赖的极简 executor（`block_on`、`yield_now`、阻塞执行器钩子） |

设计细节、Rust 特有取舍与不变量清单见 [`docs/DESIGN.md`](docs/DESIGN.md)。

---

## 文档索引

| 文档 | 语言 | 内容 |
| --- | --- | --- |
| [`docs/GUIDE.md`](docs/GUIDE.md) | 中文 | 完整使用指南：从写到测试，含常见错误与 FAQ |
| [`docs/DESIGN.md`](docs/DESIGN.md) | 中文 | 设计思想、论文概念映射、不变量与 Rust 设计决策 |
| [`docs/API.md`](docs/API.md) | 中文 | API 速查表（按模块分组） |
| [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md) | 中文 | 故障隔离承诺、实测证据与 L1–L4 分层建议 |
| [`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) | English | 规范性语义与编号一致性用例（CORE-001…CORE-051，测试一一对应） |
| [`docs/CORDIS-RUST.md`](docs/CORDIS-RUST.md) | English | Rust 移植笔记：与 TS 实现的差异、panic/隔离策略 |
| [`docs/CORDIS-RUST-GAPS.md`](docs/CORDIS-RUST-GAPS.md) | 中文 | 缺陷清单与逐轮修复/对抗记录 |

生成离线 API 文档：`cargo doc --open`。

---

## 质量与测试

- **311 个测试、31 个测试套件**全部通过：`cargo test`
- 代码风格与静态检查：`cargo fmt --check && cargo clippy --all-targets -- -D warnings`
- `spec/CORDIS-CORE.md` 中的每个 `CORE-0xx` 用例都在 `tests/` 中有对应测试；
- 除常规单测外，还有 8 轮独立对抗性测试套件（panic / abort / hang / 并发 / 泄漏 / 语义 parity），
  共 170+ 条攻击用例长期保留在 `tests/redteam*.rs`；
- `#![forbid(unsafe_code)]`：crate 内零 `unsafe`。

---

## 隔离与故障边界

内核的隔离承诺是**明确且可测**的（详见 [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md)）：

**可以遏制（进程内 best-effort）**

- 插件 `apply` / `apply_async` 的 panic → 激活失败、回滚，内核继续运行；
- 清理函数、事件监听器、内部 hook、配置 schema、访问器、日志导出器的 panic → 记录后隔离；
- 异步卸载被取消 → 未开始的清理可恢复，下次继续；
- 生命周期并发调用 → `LifecycleBusy`，不会数据竞争。

**无法遏制（Rust 进程内物理限制）**

- `panic="abort"` 构建、栈溢出、OOM / 分配器 abort、`process::abort/exit`；
- `unsafe` 造成的 UB（本 crate 无 unsafe，但宿主/依赖可能有）；
- 死锁、无限循环、线程被外部 kill；
- 插件越过宿主边界直接操作系统资源。

对**不可信插件**，请使用宿主层的进程隔离（L3）或 WASM（L4）；内核不提供沙箱、动态 ABI 或抢占式超时。

---

## 版本状态

当前 **v0.3.0**（CORE-001…CORE-051）：

- 上下文与作用域：`extend` / `isolate` / `isolate_labeled` / `intercept`；
- 服务：类型化 `ServiceKey`（含 `?Sized` 的 `*_any`）、可用性检查、provider epoch、可逆 `set`；
- fiber：事务式激活与回滚、生命周期互斥（`LifecycleBusy`）、取消安全卸载、子 fiber 所有权、
  原地替换（`Registry::replace`）、关闭（`dispose_all`）；
- effect：同步/异步单列表逆序清理、清理 panic 隔离、异步清理 join；
- 事件：同步/异步 lane、waterfall 中间件、typed internal hooks、作用域过滤；
- 观测：静默检测（`is_idle` / `when_idle`）、协作取消（`cancel` / `checkpoint`）。

v0.2.0 引入异步插件与阻塞执行器钩子；v0.1.0 为初始内核。

---

## 已知限制

以下限制经过实测或显式设计，宿主不应依赖内核未提供的保证：

- **未 dispose 的 fiber 会保留整棵图**：正常 dispose 后完全回收（10 000 轮 boot+dispose+drop 实测 0 泄漏）；
  但从不 dispose 的 fiber 仍持有其 context/plugin 图（实测约 9 KB/boot，随负载变化）。
  **退出时务必调用 `dispose_all()`。**
- **在异步线程上做同步销毁需要宿主执行器**：`dispose`/`restart`/`update` 是阻塞边界；默认 executor
  会 park 当前线程，在单线程 tokio runtime 上可能死锁。请安装
  `exec::set_blocking_executor`（首个安装生效）或改用 `*_async` API。
- **生命周期调用是互斥而非排队**：跨线程并发调用返回 `LifecycleBusy`，宿主需自行重试或串行化。
- **`update` 先校验后跑 `internal/update` waterfall**：hook 若替换出非法配置，会在下一次激活时失败
  （此时 `update` 已返回 Ok）；hook 作者应返回 schema 合法配置。
- **`ctx.effect` 有所有权窗口**：effect 体在挂到 fiber 之前执行；体内注册的东西归 fiber 所有，
  不归正在创建的这条 effect 所有。
- **取消卸载只丢失「正在 await 的那一个」清理 future**：未开始的清理会被恢复并在下次 dispose 时
  逆序执行；在飞的那个无法恢复，会记录日志。
- **事件作用域过滤是 isolate 映射相等**，不是「自己的子树」；JSON 负载无类型化事件表。
- **无抢占、无 watchdog、无资源配额**：静默与取消都是协作式的，内核不会强杀插件。
- **无动态插件加载 / 稳定 ABI**：插件是静态链接的 Rust trait object（`Arc<dyn Plugin>`）；
  要「换一个插件二进制」请用进程隔离或 WASM。

---

## 许可证

MIT，见 [LICENSE](LICENSE)。论文《A Programming Paradigm for Spatiotemporal Composability》
版权归原作者（arXiv 许可），本项目仅为该范式的独立实现。
