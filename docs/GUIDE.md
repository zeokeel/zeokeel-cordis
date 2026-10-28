# Cordis（Rust 内核）使用指南

从零开始：安装、写插件、注册服务、处理依赖、管理生命周期、做测试。本文所有示例都对应
[`examples/`](../examples/) 下的可运行代码，建议边读边跑。

- 版本：v0.4.2（Rust 1.85+，edition 2021）
- 规范：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)（英文，编号用例）
- 设计：[`DESIGN.md`](DESIGN.md)；API 速查：`cargo doc --open`

---

## 目录

1. [安装](#1-安装)
2. [Context 树与第一个插件](#2-context-树与第一个插件)
3. [插件配置与 schema](#3-插件配置与-schema)
4. [服务](#4-服务)
5. [依赖注入与自动激活](#5-依赖注入与自动激活)
6. [Effect 与清理](#6-effect-与清理)
7. [事件与中间件](#7-事件与中间件)
8. [异步插件与 executor](#8-异步插件与-executor)
9. [子插件与所有权树](#9-子插件与所有权树)
10. [作用域与隔离](#10-作用域与隔离)
11. [生命周期：重启、更新、替换](#11-生命周期重启更新替换)
12. [关闭与静默](#12-关闭与静默)
13. [协作取消](#13-协作取消)
14. [日志](#14-日志)
15. [诊断与内部钩子](#15-诊断与内部钩子)
16. [测试建议](#16-测试建议)
17. [错误速查](#17-错误速查)
18. [FAQ 与常见坑](#18-faq-与常见坑)
19. [可选 L2 防线（supervision）](#19-可选-l2-防线supervision-feature配额心跳能力自省)

---

## 1. 安装

```bash
cargo add zeokeel-cordis
```

```toml
[dependencies]
zeokeel-cordis = "0.4.2"
serde_json = "1"   # 插件配置用 serde_json::Value
```

> crates.io 包名 `zeokeel-cordis` 与库目标名 `zeokeel_cordis` 一致：代码里统一 `use zeokeel_cordis::...`；
> 需要跟仓库最新代码时可用 git 依赖：
> `zeokeel-cordis = { git = "https://github.com/zeokeel/zeokeel-cordis", tag = "v0.4.2" }`。

- MSRV：Rust **1.85+**
- 同步宿主不需要异步运行时；异步宿主可选 tokio，但内核本身不依赖 tokio
- 内核 `#![forbid(unsafe_code)]`，纯 Rust，静态链接

运行示例：

```bash
cargo run --example hello
cargo run --example services
cargo run --example events
cargo run --example async_plugin
```

---

## 2. Context 树与第一个插件

`Context` 是一切的入口。宿主通常只创建一个根 Context，然后通过插件逐步搭建系统：

```rust
use std::sync::Arc;
use zeokeel_cordis::{cleanup, Context, Plugin, Result};
use serde_json::Value;

struct Greeter;

impl Plugin for Greeter {
    fn name(&self) -> &'static str { "greeter" }

    fn apply(&self, ctx: Context, config: Value) -> Result<()> {
        let who = config.get("name").and_then(Value::as_str).unwrap_or("world").to_string();
        println!("hello, {who}");
        ctx.effect(move || vec![cleanup(move || println!("bye, {who}"))])?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(Greeter), serde_json::json!({ "name": "Cordis" }))?;
    assert!(fiber.is_active());
    fiber.dispose()?;
    root.dispose_all()?; // 宿主退出时回收整棵树
    Ok(())
}
```

要点：

- `Context::root()` 创建根上下文（自带根 fiber 与内置 Logger 服务）；
- `ctx.plugin(...)` 返回 `Fiber`；`fiber.is_active()` 表示激活成功；
- `apply` 里通过 `ctx` 注册的一切归这条 fiber 所有，`fiber.dispose()` 时逆序清理；
- `root.dispose_all()` 回收整棵树（包括子 fiber），是推荐的关闭方式。

### Context 的克隆与派生

```rust
let root = Context::root();
let a = root.extend();   // 子作用域，继承父的可见性
let b = root.clone();    // 同一个作用域的句柄（共享 isolate 映射）
```

`Context` 是 `Clone + Send + Sync` 的轻量句柄；派生（`extend`/`isolate`）会创建新的作用域节点。

---

## 3. 插件配置与 schema

插件配置是 `serde_json::Value`。如果插件实现了 `config_schema()`，内核会在激活前、以及每次
`Fiber::update` 时做 schema 校验：

```rust
use std::sync::Arc;
use zeokeel_cordis::{ConfigSchema, Context, JsonConfigSchema, JsonType, Plugin, Result};
use serde_json::Value;

struct HttpPlugin;

impl Plugin for HttpPlugin {
    fn name(&self) -> &'static str { "http" }

    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>> {
        // 用 JsonConfigSchema 描述字段类型/必填/默认值，见 src/config.rs
        Some(Arc::new(
            JsonConfigSchema::new()
                .required("port")
                .property("port", JsonType::Number)
                .default_value("host", serde_json::json!("127.0.0.1"))
                // v0.4.2：null 默认值不声明类型。
                // 未声明类型 → 缺省注入 null（该字段不做类型检查）
                .default_value("proxy", Value::Null)
                // 已显式声明类型 → 缺省不注入（可选字段保持 absent；有值时按类型校验）
                .property("tls", JsonType::Object)
                .default_value("tls", Value::Null),
        ))
    }

    fn apply(&self, _ctx: Context, config: Value) -> Result<()> {
        let port = config["port"].as_u64().unwrap_or(8080);
        println!("listen on {port}");
        Ok(())
    }
}
```

配置相关 API：

| API | 用途 |
| --- | --- |
| `Plugin::config_schema` / `AsyncPlugin::config_schema` | 声明配置 schema |
| `fiber.config()` | 解析后的配置 |
| `fiber.raw_config()` | 调用方传入的原始配置（重载时重新解析一次） |
| `fiber.update(new_config)` / `update_async` | 校验 + 重启 |
| `ctx.find_schema(name)` / `ctx.resolve_config(...)` | 查找/合并 schema 与配置 |

---

## 4. 服务

服务是内核里唯一的「全局状态」：类型化、按作用域隔离、由提供者 fiber 拥有。

### 4.1 定义与提供

```rust
use std::sync::Arc;
use zeokeel_cordis::{Context, Plugin, Result, ServiceKey};
use serde_json::Value;

struct Clock { started: std::time::Instant }

// 常量声明服务身份（名字全局唯一，类型由 ServiceKey<T> 保证）
static CLOCK: ServiceKey<Clock> = ServiceKey::new("clock");

struct ClockPlugin;

impl Plugin for ClockPlugin {
    fn name(&self) -> &'static str { "clock" }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        // 注册即被当前 fiber 拥有：fiber 卸载时服务自动撤销
        ctx.provide(CLOCK, Arc::new(Clock { started: std::time::Instant::now() }))?;
        Ok(())
    }
}
```

### 4.2 获取

| API | 行为 |
| --- | --- |
| `ctx.get(KEY) -> Option<Arc<T>>` | 不存在返回 `None` |
| `ctx.require(KEY) -> Result<Arc<T>>` | 不存在返回 `UnknownService` |
| `ctx.service_available(name) -> bool` | 按依赖名查询是否可用 |
| `ctx.get_any/require_any/…` | trait object 服务（`ServiceKey<dyn Trait>`） |
| `ctx.set_any(KEY, Arc<T>)` | 类型化地修改**自己提供的**服务；别人的服务返回 `ServiceWriteForbidden` |
| `ctx.set(name, ServiceValue)` / `ctx.set_revertible(name, value)` | 按名字修改/可逆写入（返回 `Disposer`，可直接挂成 effect） |
| `ctx.provide_checked` / `provide_gated` | 带可用性检查的服务（见 §5.3） |

### 4.3 trait object 服务

```rust
use zeokeel_cordis::{ServiceKey, Context, Result};

trait Storage: Send + Sync {
    fn put(&self, key: &str, value: Vec<u8>);
}

static STORAGE: ServiceKey<dyn Storage> = ServiceKey::new("storage");

fn use_storage(ctx: &Context) -> Result<()> {
    if let Some(storage) = ctx.get_any(STORAGE) {
        storage.put("k", vec![1, 2, 3]);
    }
    Ok(())
}
```

trait object 无法直接转成 `Arc<dyn Any>`，所以用 `provide_any/get_any/require_any/set_any`；
具体类型服务用普通 `provide/get/require/set` 即可。

---

## 5. 依赖注入与自动激活

### 5.1 声明依赖

```rust
impl Plugin for Uptime {
    fn name(&self) -> &'static str { "uptime" }

    // 依赖名必须与 ServiceKey::new 的名字一致
    fn inject(&self) -> &'static [&'static str] { &["clock"] }

    fn apply(&self, ctx: Context, _config: Value) -> Result<()> {
        let clock = ctx.require(CLOCK)?; // 激活时依赖一定已就绪
        println!("uptime: {:?}", clock.started.elapsed());
        Ok(())
    }
}
```

行为：

- 依赖不满足时 fiber 停在 **Pending**，`apply` 不会执行；
- 提供者出现后自动 `Loading → Active`；
- **注入门是严格查询（v0.4.2）**：提供者 fiber 必须是 `Active`（`apply` 已返回）才算满足依赖；
  accessor / `internal/get` 虚拟服务只对 `ctx.get` 可见，永远不满足 `inject`；
- 提供者撤销/替换时，依赖方会自动重新求值（卸载回 Pending，或重启）；
- 依赖闭环是**有界固定点刷新**：最多 64 轮，每轮按 registry 快照顺序推进；正序依赖链
  （提供者先注册）一轮即可全部激活，逆序链约每轮前进一层——依赖链极深时可能需要多次
  `ctx.refresh()` 才能完全稳定，且带重入保护不会无限递归。

### 5.2 依赖变化

```rust
let provider = root.plugin(Arc::new(ClockPlugin), Value::Null)?;
let consumer = root.plugin(Arc::new(Uptime), Value::Null)?; // 自动 Active

provider.dispose()?;                 // 服务撤销
assert!(matches!(consumer.state(), FiberState::Pending)); // 依赖方回到 Pending

let provider = root.plugin(Arc::new(ClockPlugin), Value::Null)?; // 重新提供
// consumer 自动重新激活
```

### 5.3 动态可用性（AvailabilityCheck）

`provide_gated` / `provide_checked` 在「服务存在」之上再加一层「当前是否可用」：

```rust
use zeokeel_cordis::ServiceGate;

// 方式一：闭包式可用性检查（每次重新判定）
ctx.provide_checked(KEY, Arc::new(value), || online.load(Ordering::SeqCst))?;

// 方式二：ServiceGate：翻转时自动重算所有依赖方
let gate = ServiceGate::new(true);
ctx.provide_gated(KEY, Arc::new(value), &gate)?;
gate.set(false); // 依赖方会收到「不可用」并重新求值
```

依赖方会在可用性翻转时重新评估（离线 → 卸载/停用，恢复 → 重新激活）。
适合网关、连接池、设备就绪等「存在但暂时不可用」的场景。

---

## 6. Effect 与清理

### 6.1 注册 effect

```rust
// 同步：返回 cleanup 列表
ctx.effect(|| vec![cleanup(|| println!("closed"))])?;

// 带标签（诊断用）
ctx.effect_labeled("my-listener", || vec![cleanup(|| {})])?;

// 异步清理
ctx.effect_async(|| vec![cleanup_async(|| async { /* await 关闭 */ })])?;

// 由异步 body 产生清理列表（body 在注册时被驱动完成）
ctx.effect_future(|| async {
    Ok(vec![cleanup_async(|| async { /* 关闭后台任务 */ })])
}, "worker")?;

// 在异步上下文里注册（不经过 block_on）
ctx.effect_future_async(|| async { Ok(vec![]) }, "worker").await?;

// 由 stream 驱动的效果（每个 item 一个清理）
ctx.effect_stream(stream, "consumer")?;
```

### 6.2 顺序与幂等

- 清理按**注册的逆序**执行；
- 同步与异步清理共享同一条顺序（不会出现「异步的提前跑」）；
- 每个 `Disposer` 单次、幂等，重复 dispose 安全；
- 清理函数 panic 会被记录并继续执行后面的清理，收尾完成后**首个 panic 重抛**给
  `dispose()`/`dispose_async()` 的调用方（CORE-030）。由 refresh/retire 路径触发的
  卸载会把这类 panic 记录后**遏制**，避免 provider 卡在 `Unloading`（v0.4.2）。

### 6.3 所有权窗口

`ctx.effect` 的 body 会在 effect 挂到 fiber **之前**执行。body 里通过 `ctx` 注册的东西归 fiber 所有，
不归正在创建的这个 effect 所有；在 body 里调用 dispose 会执行清理并拒绝本次注册（返回 Disposed）。
需要「完全成组」的清理时，请把所有清理写进同一个 `effect` 的返回值里。

---

## 7. 事件与中间件

### 7.1 同步事件

```rust
use zeokeel_cordis::EventResult;

ctx.on("before-request", |args| {
    println!("got {}", args[0]);
    EventResult::Continue
})?;

ctx.emit("before-request", &[serde_json::json!("hello")]);

// bail：第一个返回 Bail 的监听器生效
let verdict = ctx.bail("before-request", &args);
```

- `on` 注册的监听器归当前 fiber 所有，fiber 卸载时自动移除；
- `once` 注册一次性监听器；
- `EventOptions { prepend, global, once }` 控制顺序/作用域；
- `EventResult::Bail(value)` 中 `null`/`false` 视为「不 bail」，其余值 bail（与 Cordis 一致）。

### 7.2 异步事件

```rust
ctx.on_async("fetch", |args| async move {
    EventResult::Continue
})?;

// 派发（必须 await）
ctx.parallel_async("fetch", &args).await;      // 并发跑完所有监听器
let v = ctx.serial_async("fetch", &args).await; // 按注册顺序，遇到 bail 停
```

同步 `emit`/`bail` 不会轮询异步监听器，异步 API 也不会执行同步 lane 的语义——两条 lane 不混用。

### 7.3 waterfall 中间件

```rust
use zeokeel_cordis::waterfall_terminal;

ctx.on_waterfall("make-response", |_args, next| {
    // next.next()：继续执行剩余中间件（最后是终端行为）
    let value = next.next();
    // 返回值会在「返回途上」被上层改写（中间件包装模式）
    json!({ "wrapped": value })
})?;

// 终端行为由调用方提供；也可以用 None 使用默认终端
let out = ctx.waterfall(
    "make-response",
    &[json!(1)],
    Some(waterfall_terminal(|_args| json!("base"))),
);
```

- `next.remaining()` 查询剩余中间件数量，`next.terminal()` 跳过剩余直接执行终端；
- 不调用 `next` 就等于否决整条链（包括终端行为）——waterfall lane **没有 `Bail` 语义**，
  `EventResult::Bail` 只属于 `emit`/`bail`/`serial_async` 这条监听器路线；
- 中间件 panic 会中止整条 waterfall 并把 panic 重抛给调用方（fail-closed，不静默放行）；
  **sync 与 async lane 一致**（v0.4.2 起 `waterfall_async` 也 re-raise，不再吞成 `Null`）。

waterfall 是显式 `next` 的中间件链：不调用 `next` 就等于否决后续（包括终端行为）。
异步版本用 `on_waterfall_async` + `waterfall_async`。

### 7.4 事件作用域

监听器注册时记录当前 isolate 映射；派发时只通知**映射相等**的监听器（`global` 除外）。
要跨隔离边界通信，使用 `global` 监听器，或 `isolate_labeled` 共享同一 label。

---

## 8. 异步插件与 executor

```rust
use std::sync::Arc;
use zeokeel_cordis::{block_on, cleanup_async, AsyncPlugin, BoxFuture, Context, Result};
use serde_json::Value;

struct Ticker;

impl AsyncPlugin for Ticker {
    fn name(&self) -> &'static str { "ticker" }

    fn apply_async<'a>(&'a self, ctx: Context, _config: Value) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ctx.effect_async(|| vec![cleanup_async(|| async {
                // await 关闭后台任务
            })])?;
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

要点：

- 异步插件必须用 `plugin_async` / `plugin_async_typed` 激活；
- 生命周期操作使用 `*_async` 变体（`dispose_async`、`restart_async`、`update_async`、`refresh_async`）；
- 在 tokio runtime 内不要调用同步的 `block_on`（它会 park 当前线程，单线程 runtime 会死锁），
  直接用 `.await` 或安装宿主执行器：

```rust
use zeokeel_cordis::exec::{set_blocking_executor, BlockingExecutor};
use std::sync::Arc;

struct MyExecutor;
impl BlockingExecutor for MyExecutor {
    fn block_on(&self, future: BoxFuture<'static, ()>) { /* 宿主自己的实现 */ }
}
set_blocking_executor(Arc::new(MyExecutor)); // 首次安装生效
```

- `exec::block_on` / `yield_now`：内核自带的极简 executor，适合测试与非异步宿主；
- `AsyncDisposer::dispose_blocking`（在同步代码里等待异步清理）v0.4.2 起会真正 **join**
  在飞清理：即使另一个调用者已经接管了 disposer，也会等到清理完成才返回；被取消的 teardown
  会被重试（S-1），而在同一个 disposer 的 cleanup 内调用它自己的 `dispose_blocking()` 会
  立即返回（自 join 短路，S-2，避免自等死锁）。
- **取消 async 生命周期**（drop `dispose_async`/`refresh_async` 的 future）v0.4.2 起会把
  unload **做完**而不是恢复旧状态：同步 cleanup 立即执行、异步 cleanup 转入 deferred 由
  下一次 dispose 收尾，fiber 落到 `Pending`/`Disposed`（A-9）；取消
  `AsyncDisposer::dispose` 则恢复未启动 cleanup、由下次 dispose 重试（S-1）——两种粒度不同。

---

## 9. 子插件与所有权树

```rust
let child = ctx.plugin(Arc::new(SubPlugin), Value::Null)?;

// 子 fiber 是父 fiber 的 effect：
// - 父 dispose → 子级联 dispose（整棵子树）
// - 父激活失败 → 子回滚
// - 父 restart/update → 子随父重建
```

规则：

- 子插件加载失败会作为父激活的失败返回（父进入 Failed 并回滚）；
- 父与子的 cleanup 顺序是「先子后父」（子 effect 后注册）；
- 如果父卸载时子正在做生命周期操作，子会被打上 `dispose_requested`，由持有 lifecycle 的操作
  在结束时完成销毁（不会产生孤儿 fiber）；
- 需要手动管理子插件时，保存 `Fiber` 并在合适时机 `dispose()`——但别忘了它同时仍归父所有。

---

## 10. 作用域与隔离

```rust
let root = Context::root();
let plain = root.extend();              // 共享父作用域
let scoped = root.isolate("db");        // 名为 db 的服务/事件在子树内独立
let labeled = root.isolate_labeled("db", "tenant-a"); // 相同 label 的子树共享
let intercepted = root.intercept("logger", json!({ "level": "debug" })); // 覆盖 logger 配置
```

规则：

| 操作 | 服务可见性 | 事件可见性 |
| --- | --- | --- |
| `extend` | 继承父 | 继承父 |
| `isolate(name)` | `name` 对应的服务在子树内新 scope | 事件派发按 isolate 映射过滤 |
| `isolate_labeled(name, label)` | 同 label 的子作用域共享 scope | 同 label 之间事件互通 |
| `intercept` | 不影响服务，只覆盖 `Context::resolve_config` 的配置解析链 | 无影响 |

`intercept` 只作用于走 `Context::resolve_config` 的配置：内置 `logger`（`logger` 配置名）
与实现了 `Service::resolve_config` 的服务；**插件配置不走这条链**（插件配置来自
`ctx.plugin` 传入值 + `internal/config` waterfall + schema），因此用
`root.intercept("http", …)` 并不会覆盖某个 `http` 插件的配置（v0.4.2 明确）。

注意：事件作用域过滤是「**映射相等**」，不是「自己的子树」。如果需要子树内广播，
使用该子树自己的 `ctx.emit`（派发上下文 = 该子树作用域）。

---

## 11. 生命周期：重启、更新、替换

```rust
// 重启（当前配置）
fiber.restart()?;

// 更新配置：校验后重启
fiber.update(json!({ "port": 9001 }))?;

// 原地替换实现：保留 fiber 身份（id/uid/raw config），runtime 迁到新 key
let fibers = root.registry().replace(
    PluginKey::Named("old-plugin"),
    Arc::new(NewPlugin),
)?;
```

- `restart` / `update` / `dispose` 是**阻塞边界**：在异步宿主里优先用 `*_async` 变体；
- 生命周期操作互斥，跨线程并发会得到 `LifecycleBusy`；`Fiber::replace_plugin` 也占用同一把
  生命周期槽（v0.4.2）；
- `update` 的顺序是：`internal/config` → schema 校验 → `internal/update`，只有 waterfall
  终端才会保存新 raw config 并重启（v0.4.2）：
  - 校验失败返回 `Validation`，不重启、不改 raw config；
  - hook 返回的替换配置会先校验，再成为下一轮激活的 raw config（重新解析一次，不叠加）；
  - hook 不调用 `next` 视为**否决**：`update` 返回 `Ok(())`，不重启、raw config 不变；
  - 公开入口：`fiber.update(config)`（等价 `no_save = false`）与
    `fiber.update_with_no_save(config, no_save)`；异步版 `update_async` /
    `update_async_with_no_save`。`no_save` 只是沿 `internal/update` 链传给 hook 的
    **持久化提示**（Cordis `update(config, noSave)`）：内核不会因为 `no_save` 跳过内存
    raw config 更新——成功的（未被否决的）update 始终保留新 raw config 并重启（与 TS 一致）；
- `Registry::replace` 失败时会把已切换的 fiber 用旧 plugin 重新加载回滚，整体保持一致；
  回滚本身失败返回 `CordisError::Rollback`（v0.4.2）；
- reload 期间旧实现的 cleanup panic **不会**再穿出 `replace`/`replace_plugin`：被遏制为
  `Err(CordisError::PluginFailed("… reload panicked: …"))` 并触发上述回滚（v0.4.2）；
- disposed fiber 不能重启（Disposed 是终态）。

---

## 12. 关闭与静默

### 12.1 关闭

```rust
root.dispose_all()?; // 逐个 dispose 所有 fiber + 根 fiber，收集首个错误但继续尝试
```

- 即使某个清理 panic，也会尝试完所有 fiber 再抛出（CORE-051）；若同时还有其它 fiber 返回
  `Err`，**panic 优先**：错误只记日志、首个 panic 重抛（v0.4.2）；
- 关闭后 registry 为空、`is_idle()` 为 true；
- **未 dispose 的 fiber 会保留整棵图**，宿主退出时务必调用 `dispose_all`；
- **极深依赖链请分层/分批 dispose**：依赖链 unload 走递归（栈深度≈链层数），2 MiB worker
  栈上 debug 约 480 层 / release 约 2200 层就会栈溢出 abort（不可捕获；0.4.1 同型，
  0.4.2 未修）。数百~数千层的依赖图请拆成多棵浅树或逐批 `dispose()`，不要依赖单次顶层
  `dispose_all` 完成全部回收。

### 12.2 静默检测

```rust
// 是否有生命周期操作在飞
let idle: bool = root.registry().is_idle();

// 等整棵树静默（无 Loading/Unloading、无 pending_ops）
root.when_idle().await; // 配合 `block_on` 用于同步代码
```

适合：关闭前等待、测试断言、supervisor 的健康检查。v0.4.2 起 `ctx.plugin*` 的
**注册→激活窗口**（含 `internal/plugin` create hook 到激活完成）都计入 in-flight，
`is_idle()`/`when_idle` 不会在插件还在创建/激活时提前返回。

> **注意（v0.4.2 可观察变化）**：不要在该注册窗口（典型是 `internal/plugin` create hook）
> 内**同步等待** `when_idle()`——guard 由发起注册的调用者持有，同步等待会**自我死锁**；
> 0.4.1 在该窗口会立即返回。同理 create hook 内 `fiber.await_settled()` 会立即 `Ok(())`
> （fiber 仍是 `Pending`），`stalled_fibers()` 也看不到该窗口。需要等待请从 hook 外部进行。

---

## 13. 协作取消

Rust 进程内无法安全强杀线程，所以取消是**协作式**的：

```rust
// 宿主/父组件：请求取消
fiber.cancel();

// 插件：在长任务里定期检查
for chunk in work {
    ctx.checkpoint()?; // 已取消 → Err(CordisError::Cancelled)
    process(chunk);
}
```

- `cancel` 只影响目标 fiber，兄弟/父 fiber 不受影响；
- `checkpoint` 在未取消时无成本返回 `Ok(())`；
- 插件可以选择返回 `Err(Cancelled)`，走正常的失败/回滚路径（fiber → `Failed`，服务/effect 回收）；
- 内核不会打断插件线程，也不会超时强杀。

---

## 14. 日志

```rust
use std::sync::Arc;

use zeokeel_cordis::TracingExporter;

let logger = ctx.logger();                 // 当前 fiber 命名的 logger
logger.info("hello", Value::Null);          // 级别：error/warn/info/debug；第二参是结构化字段
logger.warn("slow", serde_json::json!({ "ms": 120 }));
let service = ctx.logger_service();        // 全局日志服务（可 set_level）
let named = ctx.logger_named("http");      // 命名 logger

// 安装导出器后日志才会离开 ring buffer（例如转到 tracing）
let disposer = ctx.log_exporter(Arc::new(TracingExporter))?;
```

- 根上下文自带 Logger 服务，但**默认没有安装任何 exporter**：记录只写入内建 ring buffer
  （容量 1000，可用 `LoggerService::records()` 读取），不会自动打印到终端、也不会进入
  `tracing`（内核自身的 `tracing` 诊断日志是另一条独立通道）；
- 需要输出时用 `ctx.log_exporter(Arc::new(TracingExporter))` 安装导出器（`TracingExporter`
  把记录转发给 `tracing`，由宿主自己的 subscriber 决定落盘/打印）；也可以实现
  `LogExporter` 自定义导出；
- `LoggerService::set_level` 可在运行时调级；
- 导出器 panic 会被隔离（记录后继续）。

---

## 15. 诊断与内部钩子

```rust
// 观察插件生命周期（PluginHook = Arc<dyn Fn(&Fiber) + Send + Sync>）
ctx.on_internal_plugin(Arc::new(|fiber: &Fiber| {
    println!("load {} state={:?}", fiber.name(), fiber.state());
}))?;

// 配置/更新是 waterfall 型钩子：拿 next 决定是否继续
ctx.on_internal_config(Arc::new(|fiber: &Fiber, config: Value, next: &ConfigNext| {
    let resolved = next.next(config);
    resolved
}))?;

// internal/update：hook 不调 next 则否决整个 update；no_save 是持久化提示
// （来自 update_with_no_save / update_async_with_no_save，update 等价 no_save=false；
//  内核成功 update 始终保留新 raw config，不会因 no_save 跳过存储）
ctx.on_internal_update(Arc::new(|_fiber: &Fiber, config: Value, no_save: bool, next: &UpdateNext| {
    next.next(config, no_save);
}))?;

// 服务/配置/更新/get/set/监听器/派发 都有对应 typed hook
ctx.on_internal_service(...)?;
ctx.on_internal_config(...)?;
ctx.on_internal_get(...)?;
ctx.on_internal_set(...)?;
ctx.on_internal_listener(...)?;
ctx.on_internal_dispatch(...)?;
ctx.on_internal_status(...)?;

// 查询
root.registry().size();
root.registry().fibers();
root.registry().counter();
root.registry().keys();
for fiber in root.registry().fibers() {
    println!("{} {:?} effects={:?}", fiber.name(), fiber.state(), fiber.get_effects());
}
```

> `keys()`/`values()`/`size()`/`fiber_count()`/`fibers()` 各自只锁一个结构：并发生命周期
> 操作进行中，跨调用的结果可能**瞬时不一致**（例如 `keys()` 还列着 `size()` 已不计的
> runtime）。它们是**诊断快照**，不是事务视图；需要一致视图时先 `when_idle()`。

内部钩子是「中间件式」的 typed 通道（不是字符串事件），适合宿主集成：审计、每插件限流、
配置来源注入、加载拦截等。

> **v0.4.2 注意（注册窗口）**：`ctx.plugin*` 的注册→激活窗口计入 in-flight，而
> `internal/plugin` create hook 正是在该窗口内执行。**不要在 create hook 内同步等待**
> `when_idle()`：guard 由发起注册的调用者持有，会自我死锁（0.4.1 会立即返回）；
> `is_idle()` 不阻塞，但在该窗口只返回 `false`。
> 同理，create hook 里拿到的 fiber 仍是 `Pending`：`fiber.await_settled()` 立即返回
> `Ok(())`，`stalled_fibers()` 也看不到该窗口；需要等待请从注册调用之外进行。

---

## 16. 测试建议

```rust
#[test]
fn plugin_activation_and_cleanup() {
    let root = Context::root();
    let cleanup_ran = Arc::new(AtomicBool::new(false));
    // ... 加载插件、断言 Active
    root.dispose_all().unwrap();
    assert!(cleanup_ran.load(Ordering::SeqCst));
}
```

- 同步测试直接用 `zeokeel_cordis::block_on(...)`；tokio 测试里用 `.await`，不要嵌套 `block_on`；
- 每个测试结束调用 `root.dispose_all()`，避免 fiber 图泄漏；
- 慢测试建议加 watchdog（另起线程 + 超时），避免死锁把 CI 挂死；
- 内核仓库的 `tests/redteam*.rs` 提供了大量攻击性断言写法可参考。

---

## 17. 错误速查

| 错误 | 含义 | 处理 |
| --- | --- | --- |
| `InactiveEffect` | 在非 active 的 context 上注册 effect | 检查 fiber 状态/激活顺序 |
| `DuplicateService(name)` | 同一作用域重复提供同名服务 | 用 `isolate` 或先撤销旧服务 |
| `UnknownService(name)` | `require` 的服务不存在 | 检查 `inject`/提供者 |
| `InvalidPlugin(msg)` | 插件非法（名称/层级/替换目标） | 见消息 |
| `PluginFailed(msg)` | 激活失败（`apply` 返回 Err），或 replace/reload 的 panic 被遏制（v0.4.2；msg 含 `reload panicked`） | 看 `fiber.error()` / msg |
| `Disposed` | fiber/上下文已销毁 | 不要复用已 dispose 的句柄 |
| `Validation(err)` | 配置 schema 校验失败 | 修正配置或 schema |
| `ServiceWriteForbidden(name)` | 试图 `set` 别人提供的服务 | 通过提供者 API 修改 |
| `ServiceTypeMismatch(name)` | `set`/`set_any` 写入的值类型与已注册服务不一致（v0.4.2）| 用正确类型写入；要换类型需先撤销旧服务 |
| `AccessorReadOnly(name)` | 对只读计算访问器调 `set` | 改用可写 key |
| `AsyncPluginNeedsAsync(name)` | 对异步插件用了同步生命周期 API | 改用 `*_async` |
| `Cancelled` | 协作 checkpoint 观察到取消 | 正常退出/清理 |
| `LifecycleBusy` | 同 fiber 生命周期操作被其他线程占用 | 重试或串行化 |
| `Rollback(msg)` | `Registry::replace` 的回滚未完全成功（v0.4.2）| 消息含原始错误与回滚错误；必要时手动重建 |

---

## 18. FAQ 与常见坑

**Q：为什么 `ctx.set` 改不了别人提供的服务？**
A：服务归提供者 fiber 所有，`set` 只允许修改当前 fiber 自己提供的服务；跨组件修改应通过服务自身 API。

**Q：为什么 `ctx.set` 返回 `ServiceTypeMismatch`？**
A：v0.4.2 起 `set`/`set_any` 会用 `TypeId` 校验类型，写入值必须与已注册服务的类型一致；
类型不同不会覆盖成功，原服务保持不变。要更换类型请先撤销旧服务再 `provide`。

**Q：为什么日志一条都看不到？**
A：根上下文只注册了 Logger 服务、**默认没有 exporter**，记录只进 ring buffer。
用 `ctx.log_exporter(Arc::new(TracingExporter))` 安装导出器后才会输出到 `tracing`
（再由宿主的 subscriber 决定打印/落盘）。

**Q：为什么 `fiber.dispose()` 在 tokio 里会卡住/死锁？**
A：同步生命周期是阻塞边界。改用 `dispose_async().await`，或在非 async 线程调用，
或安装 `exec::set_blocking_executor`。

**Q：插件为什么一直 Pending？**
A：`inject` 声明的依赖没有满足（服务名拼写、作用域隔离、provider 还没加载）。v0.4.2 起注入门
是严格查询：provider fiber 必须已经 `Active`；accessor 与 `internal/get` 虚拟服务只对
`ctx.get` 可见，不参与 `inject`。

**Q：为什么子插件没随父卸载？**
A：只有通过 `ctx.plugin(...)` 加载的子 fiber 才是父的 effect；手动构造的 fiber 需要自己 dispose。

**Q：卸载后收到的事件/服务还在？**
A：检查是否用了 `global` 监听器、或服务是否注册在更外层的作用域；监听器/服务归注册 fiber 所有。

**Q：能在插件里 spawn 线程吗？**
A：可以，但线程的生命周期需要插件自己用 effect 管理（`ctx.effect` 里 join/停止），内核不跟踪线程。

**Q：能动态加载 .so 插件吗？**
A：不能，内核没有 ABI/动态加载（见「已知限制」）。需要动态插件请用进程隔离或 WASM。

---

## 19. 可选 L2 防线（`supervision` feature：配额、心跳、能力自省）

```rust
use std::time::Duration;
use zeokeel_cordis::{Context, FiberQuota, IsolationCapabilities};

// 1) 启动时自省：确认这个构建能遏制什么
println!("{}", IsolationCapabilities::detect().summary());

// 2) 配额：单个插件最多注册多少 effect / listener / 子插件（默认无限制）
root.set_quota(
    FiberQuota::unlimited()
        .max_effects(256)
        .max_listeners(64)
        .max_child_fibers(32),
);
// 插件超限时激活失败：CordisError::QuotaExceeded("effects" | "listeners" | "child_fibers")

// 3) 心跳：长任务里定期上报，避免 supervisor 误判为卡死
ctx.heartbeat();

// 4) watchdog：宿主侧观测（不提供强杀/抢占）
for stalled in root.stalled_fibers(Duration::from_secs(5)) {
    eprintln!("{} 已静默 {:?}", stalled.name(), stalled.idle_for);
    stalled.fiber.cancel(); // 协作取消；插件需调用 ctx.checkpoint()
}
```

要点：

- 配额在**注册时**检查，超限走正常失败/回滚路径，不会留下半注册资源；
- 心跳是**协作式**的：插件不调用 `heartbeat` 也不会被内核打断，只会被 supervisor 观察到；
- watchdog 只报告「有 in-flight 操作且静默」的 fiber（含根 fiber），不做任何强制动作；
- 需要"杀进程/杀线程/超时中断"时，用 L3 进程隔离 / L4 WASM，见 `KERNEL-ISOLATION.md`。

## 延伸阅读

- 设计思想：[`DESIGN.md`](DESIGN.md)
- API 速查：`cargo doc --open`
- 规范（英文）：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)
- 隔离承诺：[`KERNEL-ISOLATION.md`](KERNEL-ISOLATION.md)
- 可运行示例：[`examples/`](../examples/)
