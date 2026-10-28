# Cordis（Rust 内核）使用指南

从零开始：安装、写插件、注册服务、处理依赖、管理生命周期、做测试。本文所有示例都对应
[`examples/`](../examples/) 下的可运行代码，建议边读边跑。

- 版本：v0.3.0（Rust 1.85+，edition 2021）
- 规范：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)（英文，编号用例）
- 设计：[`DESIGN.md`](DESIGN.md)、API 速查：[`API.md`](API.md)

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

---

## 1. 安装

```toml
[dependencies]
cordis = { git = "https://github.com/zeokeel/Cordis", tag = "v0.3.0" }
serde_json = "1"   # 插件配置用 serde_json::Value
```

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
use cordis::{cleanup, Context, Plugin, Result};
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
use cordis::{ConfigSchema, Context, Plugin, Result};
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
                .default_value("host", serde_json::json!("127.0.0.1")),
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
use cordis::{Context, Plugin, Result, ServiceKey};
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
use cordis::{ServiceKey, Context, Result};

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
- 提供者撤销/替换时，依赖方会自动重新求值（卸载回 Pending，或重启）；
- 依赖闭环是固定点刷新，带重入保护，不会无限递归。

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
use cordis::ServiceGate;

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
- 清理函数 panic 会被隔离（记日志，继续执行后面的清理）。

### 6.3 所有权窗口

`ctx.effect` 的 body 会在 effect 挂到 fiber **之前**执行。body 里通过 `ctx` 注册的东西归 fiber 所有，
不归正在创建的这个 effect 所有；在 body 里调用 dispose 会执行清理并拒绝本次注册（返回 Disposed）。
需要「完全成组」的清理时，请把所有清理写进同一个 `effect` 的返回值里。

---

## 7. 事件与中间件

### 7.1 同步事件

```rust
use cordis::EventResult;

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
use cordis::waterfall_terminal;

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
- 不调用 `next` 就等于否决整条链（包括终端行为）；
- 中间件 panic 会中止整条 waterfall 并向上抛出（fail-closed，不静默放行）。

waterfall 是显式 `next` 的中间件链：不调用 `next` 就等于否决后续（包括终端行为）。
异步版本用 `on_waterfall_async` + `waterfall_async`。

### 7.4 事件作用域

监听器注册时记录当前 isolate 映射；派发时只通知**映射相等**的监听器（`global` 除外）。
要跨隔离边界通信，使用 `global` 监听器，或 `isolate_labeled` 共享同一 label。

---

## 8. 异步插件与 executor

```rust
use std::sync::Arc;
use cordis::{block_on, cleanup_async, AsyncPlugin, BoxFuture, Context, Result};
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
use cordis::exec::{set_blocking_executor, BlockingExecutor};
use std::sync::Arc;

struct MyExecutor;
impl BlockingExecutor for MyExecutor {
    fn block_on(&self, future: BoxFuture<'static, ()>) { /* 宿主自己的实现 */ }
}
set_blocking_executor(Arc::new(MyExecutor)); // 首次安装生效
```

- `exec::block_on` / `yield_now`：内核自带的极简 executor，适合测试与非异步宿主。

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
let intercepted = root.intercept("http", json!({ "port": 9000 })); // 覆盖配置
```

规则：

| 操作 | 服务可见性 | 事件可见性 |
| --- | --- | --- |
| `extend` | 继承父 | 继承父 |
| `isolate(name)` | `name` 对应的服务在子树内新 scope | 事件派发按 isolate 映射过滤 |
| `isolate_labeled(name, label)` | 同 label 的子作用域共享 scope | 同 label 之间事件互通 |
| `intercept` | 不影响服务，覆盖配置解析链 | 无影响 |

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
- 生命周期操作互斥，跨线程并发会得到 `LifecycleBusy`；
- `update` 的配置会被校验，然后作为下一轮激活的 raw config（重新解析一次，不叠加）；
- `Registry::replace` 是原子的：任一 fiber 拒绝 reload 时整体回滚；
- disposed fiber 不能重启（Disposed 是终态）。

---

## 12. 关闭与静默

### 12.1 关闭

```rust
root.dispose_all()?; // 逐个 dispose 所有 fiber + 根 fiber，收集首个错误但继续尝试
```

- 即使某个清理 panic，也会尝试完所有 fiber 再抛出（CORE-051）；
- 关闭后 registry 为空、`is_idle()` 为 true；
- **未 dispose 的 fiber 会保留整棵图**（约 9 KB/boot 实测），宿主退出时务必调用 `dispose_all`。

### 12.2 静默检测

```rust
// 是否有生命周期操作在飞
let idle: bool = root.registry().is_idle();

// 等整棵树静默（无 Loading/Unloading、无 pending_ops）
root.when_idle().await; // 配合 `block_on` 用于同步代码
```

适合：关闭前等待、测试断言、supervisor 的健康检查。

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
let logger = ctx.logger();                 // 当前 fiber 命名的 logger
logger.info("hello", Value::Null);          // 级别：error/warn/info/debug；第二参是结构化字段
logger.warn("slow", serde_json::json!({ "ms": 120 }));
let service = ctx.logger_service();        // 全局日志服务（可 set_level）
let named = ctx.logger_named("http");      // 命名 logger

// 导出日志（例如转到 tracing）
let disposer = ctx.log_exporter(Arc::new(TracingExporter))?;
```

- 根上下文自带 Logger 服务；日志通过 tracing 输出；
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

内部钩子是「中间件式」的 typed 通道（不是字符串事件），适合宿主集成：审计、每插件限流、
配置来源注入、加载拦截等。

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

- 同步测试直接用 `cordis::block_on(...)`；tokio 测试里用 `.await`，不要嵌套 `block_on`；
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
| `PluginFailed(msg)` | 激活失败（`apply` 返回 Err） | 看 `fiber.error()` |
| `Disposed` | fiber/上下文已销毁 | 不要复用已 dispose 的句柄 |
| `Validation(err)` | 配置 schema 校验失败 | 修正配置或 schema |
| `ServiceWriteForbidden(name)` | 试图 `set` 别人提供的服务 | 通过提供者 API 修改 |
| `AccessorReadOnly(name)` | 对只读计算访问器调 `set` | 改用可写 key |
| `AsyncPluginNeedsAsync(name)` | 对异步插件用了同步生命周期 API | 改用 `*_async` |
| `Cancelled` | 协作 checkpoint 观察到取消 | 正常退出/清理 |
| `LifecycleBusy` | 同 fiber 生命周期操作被其他线程占用 | 重试或串行化 |

---

## 18. FAQ 与常见坑

**Q：为什么 `ctx.set` 改不了别人提供的服务？**
A：服务归提供者 fiber 所有，`set` 只允许修改当前 fiber 自己提供的服务；跨组件修改应通过服务自身 API。

**Q：为什么 `fiber.dispose()` 在 tokio 里会卡住/死锁？**
A：同步生命周期是阻塞边界。改用 `dispose_async().await`，或在非 async 线程调用，
或安装 `exec::set_blocking_executor`。

**Q：插件为什么一直 Pending？**
A：`inject` 声明的依赖没有满足（服务名拼写、作用域隔离、provider 还没加载）。

**Q：为什么子插件没随父卸载？**
A：只有通过 `ctx.plugin(...)` 加载的子 fiber 才是父的 effect；手动构造的 fiber 需要自己 dispose。

**Q：卸载后收到的事件/服务还在？**
A：检查是否用了 `global` 监听器、或服务是否注册在更外层的作用域；监听器/服务归注册 fiber 所有。

**Q：能在插件里 spawn 线程吗？**
A：可以，但线程的生命周期需要插件自己用 effect 管理（`ctx.effect` 里 join/停止），内核不跟踪线程。

**Q：能动态加载 .so 插件吗？**
A：不能，内核没有 ABI/动态加载（见「已知限制」）。需要动态插件请用进程隔离或 WASM。

---

## 延伸阅读

- 设计思想：[`DESIGN.md`](DESIGN.md)
- API 速查：[`API.md`](API.md)
- 规范（英文）：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)
- 隔离承诺：[`KERNEL-ISOLATION.md`](KERNEL-ISOLATION.md)
- 可运行示例：[`examples/`](../examples/)
