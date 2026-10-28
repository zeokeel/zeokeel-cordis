# 设计思想：时空可组合的 Rust 内核

本文解释 Cordis（Rust 内核）为什么这样设计：它要解决什么问题、如何把论文里的两个可组合性维度
落成 Rust 代码、有哪些必须保持的不变量，以及在 Rust 生态里做了哪些取舍。
论文原文：*A Programming Paradigm for Spatiotemporal Composability*（arXiv:2608.25512），
引用信息见 [README](../README.md#论文来源)。

---

## 1. 要解决的问题：动态组合

现代软件越来越像「插件系统」：桌面应用的扩展、编辑器语言服务、Agent 的工具箱、自演化运行时……
它们共同的难点不是「加载代码」，而是**在系统持续运行的同时安全地增删改组件**：

- 卸载一个组件时，它注册的服务、监听器、定时器、后台任务是否**全部**撤掉？
- 组件之间声明的依赖变化时，谁负责激活、谁负责停用？
- 两个互不相关的组件能交错执行而不互相干扰吗？
- 插件出错时，错误会被限制在插件边界内，还是把宿主一起拖垮？

Cordis 用两个正交的可组合性维度来回答这些问题。

---

## 2. 两个正交维度

### 2.1 时间可组合性：可逆 Effect（revertible effect）

**定义**：一个组件在时间上可组合，当且仅当它可以被完整移除——它的每一次上下文变换都携带一个逆操作，
运行时持有这些逆操作；移除组件时逆操作全部执行，系统回到组件引入之前的状态。

内核中的映射：

| 论文概念 | 实现 |
| --- | --- |
| context transformation | `ctx.effect` / `ctx.effect_async` / `effect_future` / `effect_stream` 注册的副作用 |
| inverse（逆操作） | `Disposer` / `AsyncDisposer` 中的清理函数 |
| 运行时持有逆操作 | 每条 fiber 维护一个 **有序 effect 列表**（同步与异步共享同一顺序） |
| 组件移除 | `Fiber::dispose` / 父级联卸载：按**逆注册顺序**执行清理 |
| 回滚 | 激活失败 / panic 时执行已注册清理（逆序），fiber 进入 `Failed` |

实现细节（对应 CORE-020、CORE-030、CORE-040、CORE-046）：

- **单次、幂等**：同一个 `Disposer` 只会执行一次，重复 dispose 是安全的；
- **同步/异步统一顺序**：`EffectKind::Sync` 与 `Async` 共用一条列表，卸载顺序确定；
- **panic 隔离**：清理函数 panic 会被逐个捕获（记日志，继续执行剩余清理），
  不会让「清理阶段的错误」变成内核崩溃；
- **取消安全**：drop 一次 `dispose_async()` 的 future，会把**尚未开始**的清理放回 fiber，
  下次 dispose 按逆序继续（至多丢失当时正在 await 的那一个清理 future，见已知限制）；
- **所有权**：effect 属于注册它的 fiber；在 `ctx.effect` 体内注册的东西归 fiber 本身。

### 2.2 空间可组合性：响应式 Coeffect（reactive coeffect）

**定义**：一个组件在空间上可组合，当且仅当它的外部依赖可以被精确声明；运行时把上下文的每次变化
按这份声明分类，自动驱动组件的激活与停用。

内核中的映射：

| 论文概念 | 实现 |
| --- | --- |
| coeffect specification | `Plugin::inject() -> &'static [&'static str]` |
| context change | 服务 provide / withdraw / set、fiber 状态变化、provider 替换 |
| classify | `dependencies_satisfied(fiber)`：依赖服务是否在当前作用域可用 |
| activate / deactivate | 依赖就绪 → `Loading → Active`；依赖消失 → 卸载为 `Pending` |
| 反应式重算 | `refresh_dependents()`：从变化点出发刷新所有 fiber 的依赖判定 |

实现细节（对应 CORE-015、CORE-025、CORE-040）：

- 依赖是**声明式字符串**：与 `ServiceKey` 的 `name()` 对应，运行时不做反射；
- 依赖不满足时 fiber 停在 `Pending`，不会执行 `apply`，也不会泄漏 effect；
- provider 变化触发**依赖闭包的 fixed point 刷新**（带重入保护与迭代上限，防止抖动）；
- **provider epoch**：同一个 key 换了提供者（或提供者重启）时，依赖方会重新评估/重启，
  避免「旧 Arc 缓存」造成的陈旧依赖；
- 可选 `AvailabilityCheck`（`provide_checked` / `provide_gated`）：在「存在」之外再加一层
  「当前是否可用」的动态判定，依赖方会在可用性翻转时响应。

### 2.3 两个维度的正交性

时间维度关心「**撤销**」：组件离开时系统回到之前的状态。
空间维度关心「**依赖**」：组件在系统里处于什么位置、被什么驱动。
两者互不替代：只有可逆 effect 而没有依赖声明，组件卸载是干净的，但依赖变化不会自动响应；
只有依赖声明而没有可逆 effect，组件能被调度，但卸载会留下垃圾。
Cordis 把两者统一到同一个 `Context` 里，因此**卸载一个组件 = 撤销它的一切 = 让依赖它的组件自动重算**。

---

## 3. 统一 Context 与观测等价

`Context` 既是 effect 的载体（子 fiber、effect 列表），也是 coeffect 的载体（服务查找、依赖判定）。
所有操作都必须经过它，于是有了作用域：

| 机制 | 语义 |
| --- | --- |
| `ctx.extend()` | 派生一个子作用域；服务/事件仍与父可见（继承 isolate 映射） |
| `ctx.isolate(name)` | 让名为 `name` 的服务/事件在子树内拥有**独立的一份**（新 scope id） |
| `ctx.isolate_labeled(name, label)` | 相同 `label` 的多个子树共享同一作用域 id（跨子树通信） |
| `ctx.intercept(name, config)` | 在子树内覆盖某个服务的配置（`resolve_config` 从根到叶合并） |

**观测等价（observational equivalence）的工程含义**：如果两个组件所处的 isolate 映射不同，
那么它们对彼此的服务与事件就是不可见的——它们可以任意交错执行而不会互相破坏。
这就是内核隔离的最小单位：不是线程，不是进程，而是**作用域**。

事件派发遵循同一原则：监听器注册时记录当前 isolate 映射；`ctx.emit` 只派发给映射相等（或
`global`）的监听器。服务查找同理：不同 scope id 的同名服务互不可见。

---

## 4. 组件、生命周期与事务

### 4.1 状态机

```
Pending ──(依赖满足)──► Loading ──(apply 成功)──► Active
   ▲                       │                        │
   │                       ├─(apply Err/panic)──► Failed
   │                       │(生效期间被 dispose)     │
   └───────────────────────┴──────────────────────► Unloading ──► Disposed
```

- `Disposed` 是终态：disposed fiber 不能被重启；
- 激活失败会回滚已注册的 effect 并进入 `Failed`，不会留下半初始化；
- 卸载期间 state 为 `Unloading`，别的生命周期操作会看到互斥错误或等待（见下）。

### 4.2 激活是事务

`apply` 中通过 `ctx` 注册的每一项都记录到 fiber 的 effect 列表。事务提交点是 `apply` 返回 `Ok`：

- 返回 `Err` / panic → 逆序回滚，fiber `Failed`，错误被记录（`Fiber::error` 可查）；
- 子 fiber（`ctx.plugin`）也是 effect，父激活失败时子 fiber 一并回滚。

### 4.3 所有权树

- 每条 fiber 有父 fiber（根 fiber 的父是自身）；
- `ctx.plugin` 注册的子 fiber 是父 fiber 的 effect：父 dispose → **级联** dispose 整棵子树；
- 子 fiber 的清理与父的清理共享逆序语义：先子后父（子 effect 在父 effect 之后注册）；
- 若父卸载时子 fiber 正在做生命周期操作：子会被打上 `dispose_requested`，由**持有 lifecycle 的
  那次操作结束时**完成 dispose，保证最终回收而不是留下孤儿（CORE-047）。

### 4.4 生命周期互斥

`dispose` / `restart` / `update` 对同一条 fiber 互斥：

- 跨线程并发调用返回 `CordisError::LifecycleBusy`（安全错误优先于数据竞争）；
- 同线程重入被允许（如清理中再次调用 dispose）；
- 激活与 dispose 竞争时，dispose 是最终裁决：激活结果会被丢弃并回滚。

### 4.5 替换与重载

- `Fiber::restart` / `update`：走正常卸载 + 重新激活管线，配置重新解析/校验；
- `Registry::replace(old_key, new_plugin)`：保持 fiber 身份（id/uid/raw config），
  把 runtime 迁移到新 key 并逐个 reload；任一 fiber 拒绝（如 Busy）时整体回滚（CORE-048）；
- 这些能力是给宿主实现热更新/HMR 的原语；内核本身不含文件监听和模块加载器。

---

## 5. 不变量

以下不变量是设计承诺，`spec/CORDIS-CORE.md` 中的 `CORE-0xx` 用例逐一验证：

| # | 不变量 | 用例 |
| --- | --- | --- |
| I1 | effect 逆序、单次清理；fiber 卸载后不残留服务/监听器 | CORE-020、CORE-046 |
| I2 | 激活失败必须回滚已注册的一切 | CORE-026、CORE-027 |
| I3 | panic 不跨越内核边界：apply/cleanup/listener/hook/schema/exporter 逐个隔离 | CORE-028、CORE-030 |
| I4 | 依赖变化驱动激活/停用与重算 | CORE-015、CORE-025、CORE-040 |
| I5 | 服务身份由 `(scope, name)` 唯一确定，类型错误必须报错而不是 UB | CORE-005、CORE-019 |
| I6 | `ctx.set` 只允许修改自己的服务；撤销按注册逆序 | CORE-041 |
| I7 | 生命周期互斥；Disposed 是终态 | CORE-044 |
| I8 | 子 fiber 属于父 fiber，父卸载级联回收，不留孤儿 | CORE-047 |
| I9 | 替换/重载保持 fiber 身份，失败原子回滚 | CORE-048 |
| I10 | 静默（quiescence）可在无生命周期操作在飞时立即返回，且不漏唤醒 | CORE-049 |
| I11 | 取消是协作式且 per-fiber，不误伤兄弟 fiber | CORE-050 |
| I12 | 关闭路径 `dispose_all` 即使遇到 panic 也尝试所有 fiber，最后再抛出 | CORE-051 |

---

## 6. Rust 特有的设计决策

### 6.1 类型化服务身份，而不是 JS 的 Proxy

TypeScript 的 Cordis 用 `ctx.serviceName` 属性访问，依赖运行时 Proxy。Rust 没有 Proxy，因此：

- 服务用 `ServiceKey<T>` 显式声明；`T: ?Sized + Send + Sync + 'static` 同时支持具体类型与 trait object；
- 具体类型服务直接 `Arc<T>`，trait object 服务用 `provide_any/get_any/require_any/set_any`
  包装（`Arc<dyn Trait>` 无法安全地向上转成 `Arc<dyn Any>`）；
- 好处：**编译期类型安全**、零反射、零全局类型名冲突；代价：写法略啰嗦（`ServiceKey` 常量）。

### 6.2 同步与异步是两条明确通道

Rust 没有 `Promise` 的隐式调度，所以内核不「偷偷」把同步调用变成异步：

- 同步插件：`Plugin::apply`；异步插件：`AsyncPlugin::apply_async`（返回 `BoxFuture`）；
- 异步插件的所有生命周期操作必须用 `*_async` 变体，混用返回 `AsyncPluginNeedsAsync`；
- 事件也分 lane：`emit/bail`（同步）与 `parallel_async/serial_async`（异步），互不降级；
- 内核自带极简 executor（`block_on`/`yield_now`）用于同步-异步桥接，宿主可安装
  `BlockingExecutor` 避免在异步线程上 park 死锁。

### 6.3 panic 遏制的边界是明确声明的

Rust 的 `panic=unwind` 提供了一种**进程内**的故障遏制手段，但仅限 unwinding panic。
因此内核对每类用户代码（apply、cleanup、listener、internal hook、schema、exporter、accessor）
做 `catch_unwind` + 记日志，并**明确列出无法遏制的情况**（abort、栈溢出、OOM、UB、死循环、外部 kill），
详见 [KERNEL-ISOLATION.md](KERNEL-ISOLATION.md)。这不是「沙箱」，而是「插件错误不拖垮内核」的
best-effort 工程承诺。

### 6.4 静态链接、无 ABI、无 unsafe

- 插件就是 Rust 值 `Arc<dyn Plugin>`，随宿主一起编译；没有 `cdylib` ABI、没有动态加载；
- crate 级 `#![forbid(unsafe_code)]`，所有权/生命周期由 Rust 编译器保证；
- 需要动态插件/不可信插件时，用宿主层的进程隔离（L3）或 WASM（L4）。

### 6.5 并发模型

- `Context`/`Fiber` 句柄是 `Clone + Send + Sync`，可跨线程传递；
- 内核内部用 `Mutex` + `Atomic` 保护状态，锁粒度按数据结构划分，避免长临界区；
- 生命周期互斥、静默等待、idle waiter 都提供了并发安全语义；
- 不提供抢占式取消：插件需在长任务中调用 `ctx.checkpoint()` 协作退出。

---

## 7. 范围边界：内核 vs 宿主

内核只提供「组合原语」，以下全部属于宿主（embedder）：

| 能力 | 归属 |
| --- | --- |
| 模块加载器 / 热更新文件监听 / 配置协调 | 宿主（Cordis 的 loader 层） |
| 权限、沙箱、资源配额、watchdog、心跳 | 宿主（或进程/WASM 隔离层） |
| 定时器、事件循环、UI 插槽、Agent 工具契约 | 宿主 |
| 动态插件二进制加载 / 跨语言插件 | 宿主（进程隔离 / WASM） |
| 上下文、服务、fiber、effect、事件、registry | **本内核** |

这样划分的好处：内核可以保持小而稳定，任何宿主都能复用同一套组合语义；
代价是宿主需要自己实现调度与隔离层——而这本就应该由宿主根据自身威胁模型决定。

---

## 8. 延伸阅读

- 论文：<https://arxiv.org/abs/2608.25512>
- 规范：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md)（英文，CORE-001…051）
- 使用指南：[`GUIDE.md`](GUIDE.md)
- API 速查：[`API.md`](API.md)
- 隔离承诺：[`KERNEL-ISOLATION.md`](KERNEL-ISOLATION.md)
- Rust 移植笔记：[`CORDIS-RUST.md`](CORDIS-RUST.md)（英文）
