# API 速查（中文）

按模块分组的常用 API 速查。**完整签名与文档注释以 rustdoc 为准**：`cargo doc --open`。

- 版本：v0.3.0
- 规范：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)
- 使用教程：[`GUIDE.md`](GUIDE.md)

---

## 模块总览

| 模块 | 主要内容 |
| --- | --- |
| `cordis::context` | `Context`、作用域、服务 API、插件入口、事件快捷方法 |
| `cordis::service` | `ServiceKey`、`ServiceGate`、`AvailabilityCheck`、`ServiceValue` |
| `cordis::fiber` | `Fiber`、`FiberState`、`EffectMeta`、`Settled` |
| `cordis::effect` / `async_effects` | `Disposer`、`AsyncDisposer`、`Cleanup`、`AsyncCleanup`、`AsyncEffect` |
| `cordis::events` | `Events`、`EventOptions`、`EventResult` |
| `cordis::waterfall` | `WaterfallNext`、`AsyncWaterfallNext`、`waterfall_terminal` 等 |
| `cordis::internal` | typed 内部钩子（plugin/status/service/config/update/get/set/listener/dispatch） |
| `cordis::registry` | `Plugin`、`AsyncPlugin`、`PluginKey`、`PluginKind`、`PluginRuntime`、`Registry` |
| `cordis::config` | `ConfigSchema`、`JsonConfigSchema`、`JsonType`、`ValidationError` |
| `cordis::logger` | `Logger`、`LoggerService`、`LogExporter`、`TracingExporter`、`LogLevel` |
| `cordis::reflect` | `Accessor`、`ContextMeta` |
| `cordis::exec` | `block_on`、`yield_now`、`boxed`、`BlockingExecutor`、`set_blocking_executor` |
| `cordis::l2` | `IsolationCapabilities`、`FiberQuota`、`StalledFiber`（L2 运行时防线） |
| `cordis::error` | `CordisError`、`Result<T>` |

---

## Context

### 构造与作用域

| API | 说明 |
| --- | --- |
| `Context::root() -> Context` | 创建根上下文（自带根 fiber 与 Logger 服务） |
| `ctx.clone()` | 同作用域句柄（`Clone + Send + Sync`） |
| `ctx.extend() -> Context` | 派生子作用域（继承父可见性） |
| `ctx.isolate(name: &'static str) -> Context` | 子树内独立的一份 `name` 服务/事件 |
| `ctx.isolate_labeled(name, label) -> Context` | 相同 label 共享作用域 id |
| `ctx.intercept(name, config) -> Context` | 覆盖子树内的配置解析链 |
| `ctx.fiber() -> Fiber` | 当前上下文所属 fiber |
| `ctx.fiber_id() -> u64` | 当前 fiber id |
| `ctx.registry() -> Arc<Registry>` | 全局 registry |
| `ctx.events() -> Arc<Events>` | 事件总线 |

### 服务

| API | 说明 |
| --- | --- |
| `ctx.provide(key, value) -> Result<Disposer>` | 提供具体类型服务（`Arc<T>`） |
| `ctx.provide_checked(key, value, check)` | 带可用性闭包 |
| `ctx.provide_gated(key, value, &ServiceGate)` | 带 `ServiceGate`，翻转时自动重算依赖方 |
| `ctx.provide_any(key, value)` | `ServiceKey<dyn Trait>` 服务 |
| `ctx.get(key) -> Option<Arc<T>>` | 查询 |
| `ctx.require(key) -> Result<Arc<T>>` | 查询，缺失报 `UnknownService` |
| `ctx.get_any/require_any(key)` | trait object 查询 |
| `ctx.set_any(key, Arc<T>)` | 类型化地修改**自己提供的**服务 |
| `ctx.set(name, ServiceValue)` / `ctx.set_revertible(name, value)` | 按名字写入 / 可逆写入（返回 `Disposer`） |
| `ctx.service_available(name) -> bool` | 按依赖名查询可用性 |

### Effect

| API | 说明 |
| --- | --- |
| `ctx.effect(|| vec![cleanup(|| …)])` | 同步 effect |
| `ctx.effect_labeled(label, …)` | 带标签（诊断） |
| `ctx.effect_async(|| vec![cleanup_async(|| async { … })])` | 异步 effect |
| `ctx.effect_future(|| async { Ok(vec![]) }, label)` | 异步 body 产生清理列表 |
| `ctx.effect_future_async(...)` | await 版，可在 async 上下文使用 |
| `ctx.effect_stream(stream, label)` | 按 stream item 注册清理 |
| `ctx.attach_effect(disposer, label)` | 把已有 `Disposer` 挂到当前 fiber |

### 插件

| API | 说明 |
| --- | --- |
| `ctx.plugin(Arc<dyn Plugin>, config) -> Result<Fiber>` | 同步加载（子 fiber 是父 fiber 的 effect） |
| `ctx.plugin_typed(Arc<P>, config)` | 以 `TypeId` 作为 runtime key |
| `ctx.plugin_async(Arc<dyn AsyncPlugin>, config).await` | 异步加载 |
| `ctx.plugin_async_typed(Arc<P>, config).await` | 异步 + TypeId |

### 事件

| API | 说明 |
| --- | --- |
| `ctx.on(name, listener) -> Result<Disposer>` | 同步监听器（当前 fiber 所有） |
| `ctx.on_with(name, prepend, global, listener)` | 控制顺序/全局 |
| `ctx.on_with_options(name, EventOptions, listener)` | 完整选项 |
| `ctx.once(name, listener)` | 一次性监听器 |
| `ctx.on_async(name, listener)` | 异步监听器 |
| `ctx.on_waterfall(name, listener)` | 同步 waterfall 中间件 |
| `ctx.on_waterfall_async(name, listener)` | 异步 waterfall 中间件 |
| `ctx.emit(name, &[Value])` | 派发（同步 lane） |
| `ctx.bail(name, &[Value]) -> Option<Value>` | 派发并在首个 bail 停止 |
| `ctx.parallel_async(name, &args).await` | 并发等待所有监听器（同步 + 异步） |
| `ctx.serial_async(name, &args).await` | 按注册顺序等待，遇到 bail 停止 |
| `ctx.waterfall(name, &args, terminal) -> Value` | 同步中间件链 |
| `ctx.waterfall_async(name, &args, terminal).await` | 异步中间件链 |

### 生命周期/关闭/诊断

| API | 说明 |
| --- | --- |
| `ctx.checkpoint() -> Result<()>` | 协作取消检查点（取消时 `Cancelled`） |
| `ctx.when_idle().await` | 等待整棵树静默 |
| `ctx.dispose_all() -> Result<()>` | 关闭整棵树（含根 fiber） |
| `ctx.refresh()` | 主动刷新依赖 |
| `ctx.logger() / logger_named(name) / logger_service()` | 日志 |
| `ctx.log_exporter(Arc<dyn LogExporter>) -> Result<Disposer>` | 安装日志导出器 |
| `ctx.resolve_config(name, base, head) -> Value` | 合并配置来源 |
| `ctx.find_schema(name) -> Option<Arc<dyn ConfigSchema>>` | 查找配置 schema |
| `ctx.plugin_name() -> String` | 当前插件名 |
| `ctx.on_internal_*(hook) -> Result<Disposer>` | 9 类 typed 内部钩子 |

---

## Plugin / AsyncPlugin

```rust
pub trait Plugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn key(&self) -> PluginKey;                        // 默认 Named(name)
    fn inject(&self) -> &'static [&'static str];       // 依赖服务名
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>>;
    fn apply(&self, ctx: Context, config: Value) -> Result<()>;
}

pub trait AsyncPlugin: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn key(&self) -> PluginKey;
    fn inject(&self) -> &'static [&'static str];
    fn config_schema(&self) -> Option<Arc<dyn ConfigSchema>>;
    fn apply_async<'a>(&'a self, ctx: Context, config: Value) -> BoxFuture<'a, Result<()>>;
}
```

`PluginKey`：`Named(&'static str)` 或 `Type(TypeId)`（`plugin_typed` 使用）。

---

## Fiber

| API | 说明 |
| --- | --- |
| `fiber.id()` / `fiber.uid()` | 运行时 id / 稳定 uid |
| `fiber.name()` | 插件名 |
| `fiber.state() -> FiberState` | `Pending/Loading/Active/Failed/Unloading/Disposed` |
| `fiber.is_active()` / `assert_active()` | 状态便捷判断 |
| `fiber.parent() -> Option<Context>` | 父上下文 |
| `fiber.context() -> Option<Context>` | 插件运行上下文 |
| `fiber.config()` / `fiber.raw_config()` | 解析后/原始配置 |
| `fiber.get_effects() -> Vec<EffectMeta>` | effect 列表（诊断） |
| `fiber.settled() -> Settled` | 等待未完成操作结束 |
| `fiber.dispose()` / `dispose_async().await` | 卸载 |
| `fiber.restart()` / `restart_async().await` | 重启 |
| `fiber.update(config)` / `update_async(config).await` | 校验配置并重启 |
| `fiber.replace_plugin(Arc<dyn Plugin>)` / `replace_plugin_async` | 单 fiber 原地替换 |
| `fiber.cancel()` / `is_cancelled()` | 协作取消 |
| `fiber.error() -> Option<String>` | 失败原因 |

---

## Registry

| API | 说明 |
| --- | --- |
| `registry.size()` / `fiber_count()` / `is_empty()` | 统计 |
| `registry.fibers() -> Vec<Fiber>` | 所有 fiber 快照 |
| `registry.get(key) / has(key) / delete(key)` | 按 runtime key 查询 |
| `registry.keys() / values() / entries() / for_each(f)` | 遍历 |
| `registry.counter()` | fiber id 计数器 |
| `registry.replace(key, Arc<dyn Plugin>) -> Result<Vec<Fiber>>` | 原地替换实现（原子） |
| `registry.replace_async(key, Arc<dyn AsyncPlugin>).await` | 异步替换 |
| `registry.is_idle() / when_idle().await` | 静默检测 |
| `registry.dispose_all() -> Result<()>` | 关闭所有 fiber + 根 fiber |

---

## 事件类型

```rust
pub struct EventOptions { pub prepend: bool, pub global: bool, pub once: bool }
pub enum EventResult { Continue, Bail(Value) }   // Bail(null/false) 视为不 bail
```

waterfall：

| 类型/函数 | 说明 |
| --- | --- |
| `WaterfallNext::{args, remaining, next, terminal}` | 同步中间件续延 |
| `AsyncWaterfallNext::{args, remaining, next, terminal}` | 异步续延（`next()` 返回 future） |
| `waterfall_listener(f)` / `waterfall_terminal(f)` | 包装 sync 中间件/终端 |
| `async_waterfall_listener(f)` / `async_waterfall_terminal(f)` | 异步版本 |
| `null_waterfall_terminal()` / `null_async_waterfall_terminal()` | 默认终端（返回 null） |

---

## 配置

| API | 说明 |
| --- | --- |
| `trait ConfigSchema { fn validate(&self, value: &Value) -> Result<(), ValidationError>; … }` | 自定义 schema |
| `JsonConfigSchema::new().required(name).property(name, JsonType).default_value(name, v).deny_unknown()` | 内置 JSON schema |
| `JsonType::{String, Number, Bool, Object, Array, Null, Any}` | 字段类型 |
| `ValidationError::{new, message, is_empty}` | 校验错误 |

---

## 日志

| API | 说明 |
| --- | --- |
| `LoggerService::new(level)` / `with_buffer_size` | 创建日志服务 |
| `service.set_level(level)` / `level()` | 运行时调级 |
| `Logger::{error, warn, info, debug}(message, fields: Value)` | 记录日志 |
| `Logger::{name, level, service, child}` | 元信息/子 logger |
| `LogExporter` trait；`TracingExporter` | 日志导出（panic 被隔离） |

---

## Executor

| API | 说明 |
| --- | --- |
| `block_on(future) -> Output` | 极简 executor（park 当前线程） |
| `yield_now().await` | 让出一次 |
| `boxed(future) -> BoxFuture` | 装箱 future |
| `BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>` | 类型别名 |
| `BlockingExecutor` trait + `set_blocking_executor` | 宿主自定义阻塞执行器（首个安装生效） |

---

## 错误

```rust
pub enum CordisError {
    InactiveEffect, DuplicateService(String), UnknownService(String),
    InvalidPlugin(String), PluginFailed(String), Disposed,
    Validation(ValidationError), ServiceWriteForbidden(String), AccessorReadOnly(String),
    AsyncPluginNeedsAsync(String), Cancelled, LifecycleBusy,
}   // #[non_exhaustive]：新增变体不破坏下游
```

| 错误 | 常见原因 |
| --- | --- |
| `InactiveEffect` | 上下文已卸载/未激活时注册 effect |
| `DuplicateService` | 同作用域重复 provide 同名服务 |
| `UnknownService` | `require` 的服务不存在 |
| `Disposed` | 句柄已 dispose 后继续使用 |
| `AsyncPluginNeedsAsync` | 异步插件用了同步生命周期 API |
| `LifecycleBusy` | 同 fiber 生命周期操作被其他线程占用 |
| `Cancelled` | 协作检查点发现已取消 |

---

## L2 运行时防线

| API | 说明 |
| --- | --- |
| `IsolationCapabilities::detect()` | 返回 `{ panic_strategy, panic_containment, unsafe_forbidden, process_isolation, wasm_isolation }` |
| `caps.summary()` | 可直接打日志的一行摘要 |
| `FiberQuota::unlimited()` / `.max_effects(n)` / `.max_listeners(n)` / `.max_child_fibers(n)` | per-fiber 资源上限（默认无限制）|
| `Context::set_quota(quota)` / `Registry::set_quota(quota)` | 为整棵树设置配额 |
| `Context::quota()` / `Registry::quota()` | 查询当前配额 |
| `Fiber::heartbeat()` / `Context::heartbeat()` | 上报活动（长任务防误报） |
| `Fiber::last_activity()` / `Fiber::idle_for()` | 活动时间戳 / 静默时长 |
| `Registry::stalled_fibers(timeout)` / `Context::stalled_fibers(timeout)` | 返回 `Vec<StalledFiber>`（含 fiber 与 idle_for） |
| `CordisError::QuotaExceeded(kind)` | 配额超限（effects / listeners / child_fibers） |

---

## 类型别名速览

| 名称 | 定义 |
| --- | --- |
| `Result<T>` | `std::result::Result<T, CordisError>` |
| `Cleanup` | `Box<dyn FnOnce() + Send + 'static>` |
| `Disposer` | 单次幂等清理句柄 |
| `AsyncCleanup` | `Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send + 'static>` |
| `AsyncDisposer` | 异步清理句柄 |
| `AsyncEffect` | `Vec<AsyncCleanup>` |
| `ServiceValue` | 类型擦除后的服务值 |
| `AvailabilityCheck` | `Arc<dyn Fn() -> bool + Send + Sync>` |
| `EffectMeta` | effect 诊断信息（label/kind/panicked 等） |
