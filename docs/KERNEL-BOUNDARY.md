# 内核边界：内核 vs 插件层 vs 宿主

> 适用版本：**v0.4.4**（v0.4.3 为语义基线）。本文是工程约束文档，不是语义规范；
> 语义以 [`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md) 为准（本文不新增 CORE 用例）。
>
> 配套护栏：`tests/kernel_boundary.rs`（v3：**10 个测试**——4 个真实断言 + 6 个防绕过自测）
> + CI 的 `kernel boundary` 步骤。
> **任何 PR 都必须通过这两处**，见 §6。
>
> 相关文档：[`DESIGN.md`](DESIGN.md) §7（设计侧范围边界）、
> [`KERNEL-ISOLATION.md`](KERNEL-ISOLATION.md)（**故障**隔离，与本文的**职责**边界正交）、
> [`CONTRIBUTING.md`](../CONTRIBUTING.md)（PR 判据与清单）、[`README.md`](../README.md)。

---

## 0. 一句话

本仓库是**内核**，不是业务框架：内核只提供与业务无关的**组合原语**（context、service、
fiber、effect、events、registry、config schema、exec 桥、logger 设施、L2 观测原语）。
业务逻辑、I/O、传输、持久化、领域配置和领域策略属于**插件层**或**宿主（embedder）**，
通过扩展点接入，**不修改内核**。

判断任何改动时先回答一句话：**"这条能力需要知道某个业务/传输/存储/环境吗？"**
需要 → 不属于内核；不需要且任何宿主都用得上 → 才考虑内核。

---

## 1. 三层模型

```
业务/宿主 crate ──► 插件（impl Plugin / AsyncPlugin）──► zeokeel-cordis（内核）
        │                                                      │
        └──────────────► serde_json / tracing / futures-util ◄─┘
```

| 层 | 是什么 | 典型内容 | 代码位置 |
| --- | --- | --- | --- |
| **内核（kernel）** | 与业务无关的组合原语与生命周期机制 | Context/作用域、Service 注入、Fiber 生命周期、Effect 所有权、Events/waterfall、Registry/reflect、配置 schema 机制、exec 调度桥、logger 诊断设施、L2 隔离观测原语 | `src/**`（本 crate） |
| **插件层（plugin layer）** | 用内核 trait 实现的业务能力 | HTTP 处理器、数据库访问、缓存、消息队列消费者、业务实体与领域逻辑、业务配置 schema、业务指标/审计、重试/限流/鉴权策略 | 宿主/业务仓库、独立 crate、`examples/` |
| **宿主（embedder）** | 进程、运行时与运行环境的所有者 | 加载器/热更新、tokio 等运行时、定时器/事件循环、配置来源（文件/env/远程）、权限与审计、资源策略、进程隔离（L3）/WASM 沙箱（L4）、UI/产品契约 | 宿主二进制 |

**依赖方向是单向的**：插件层与宿主可以依赖内核，内核**绝不**依赖它们。
插件类型、业务类型、传输/存储类型不得出现在内核的公共 API 中。

---

## 2. 内核职责（只负责这些）

内核按模块划分职责；每个模块都有明确的"只做/不做"边界。

| 模块（`src/`） | 只负责 | **不**负责 |
| --- | --- | --- |
| `context.rs` / `service.rs` | 作用域容器、类型化服务注册/解析、隔离 scope、严格注入门、revertible `set`、reflect accessor 机制 | 服务里存什么业务对象、服务发现/注册中心、网络传输 |
| `fiber.rs` / `effect.rs` / `async_effects.rs` | 插件生命周期状态机、effect 逆序清理、回滚、取消/重载、卸载级联、deferred cleanup 收尾 | 业务任务调度、线程池、定时器、后台队列 |
| `events.rs` / `waterfall.rs` | 同步/异步发布订阅、`once`/`prepend`/`global`、waterfall 中间件管线、panic 策略 | 业务事件类型表、消息中间件、跨进程总线 |
| `registry.rs` | 运行时注册表、`Plugin`/`AsyncPlugin` trait、替换/重载、quiescence、插件元数据遏制 | 模块加载器/HMR、.so/dylib 动态加载、权限审批 |
| `config.rs` | `ConfigSchema` 机制、JSON 类型校验、默认值/合并、`internal/config` waterfall 协议 | **领域配置 schema**（数据库 URL、业务开关、超时策略等字段） |
| `exec.rs` | `block_on`（测试/同步边界）、`BlockingExecutor` seam、`BoxFuture`、`yield_now` | 拥有/选择异步运行时（tokio 等）、线程池管理 |
| `logger.rs` | 日志服务设施：级别、ring buffer、`LogExporter` trait、`TracingExporter` 桥 | 日志后端选型、业务指标命名、OTel 语义约定 |
| `l2.rs`（`supervision` feature） | 隔离能力自省、per-fiber 配额**机制**、心跳、`stalled_fibers` 观测 | 配额阈值/处置**策略**、watchdog 自动重启、抢占 |
| `internal.rs` | typed `internal/*` hook 通道（中间件式扩展点） | 内建鉴权/限流/审计逻辑（由插件/宿主实现） |
| 全局承诺 | `#![forbid(unsafe_code)]`、panic 遏制策略、静态链接、MSRV 1.85 | 进程内不可遏制边界（见 KERNEL-ISOLATION） |

内核**提供机制，不提供策略**：配额只提供计数与拒绝，不决定阈值；日志只提供记录与导出
接口，不定义指标；隔离只提供能力自省，不做权限判断。策略全部由宿主/插件决定。

---

## 3. 绝不属于内核（Never in the kernel）

以下内容**不得**进入 `src/**`、`Cargo.toml` 的 `[dependencies]` 或内核公共 API。
它们应做成插件、example、独立的 plugin crate，或由宿主实现。

| 类别 | 具体内容（示例） | 应归属 |
| --- | --- | --- |
| 业务领域逻辑 | 订单/支付/结算/用户/租户/库存/内容/工单等领域实体与规则 | 插件层 |
| 领域配置 schema | 某个插件的业务字段、默认值与校验（DB URL、超时、业务开关） | 插件层（用内核的 `ConfigSchema` 机制实现） |
| 网络/传输 | TCP/UDP/HTTP/WebSocket/gRPC 服务器与客户端、代理、路由 | 插件层 + 宿主运行时 |
| 持久化/存储 | 数据库/ORM/SQL、对象存储、文件系统布局、迁移 | 插件层 |
| 缓存/中间件 | Redis/Memcached、Kafka/RabbitMQ/NATS 客户端 | 插件层 |
| 进程/环境 | `std::process::Command`、环境变量读取、信号、daemon、线程/进程池、运行时（tokio/async-std/smol） | 宿主 |
| 时钟/随机 | `chrono`/`time`/`rand` 等作为内核运行时依赖；业务时间语义 | 插件/宿主（通过 effect/扩展点） |
| 业务观测 | 指标名称/标签、trace span 业务属性、告警规则 | 插件/宿主（内核只提供 logger 设施） |
| 业务策略 | 重试/退避、限流/熔断、鉴权/授权、路由/负载均衡、多租户策略、调度优先级、计费 | 插件/宿主 |
| 加载与隔离 | 模块加载器、HMR 文件监听、权限审批、进程/WASM 隔离 | 宿主（见 KERNEL-ISOLATION） |
| 产品契约 | UI/Agent 工具契约、API 版本、业务协议 | 宿主/插件 |

**快速判定**：如果一条能力需要知道具体业务实体、具体传输/存储/配置来源或具体运行时，
它就不属于内核。内核的公共 API 里出现 `http`、`database`、`tenant`、`payment`、
`tokio` 这类词即视为越界（见 §6 的 CI 护栏）。

---

## 4. 依赖方向（可执行规则）

1. **运行时依赖白名单**：`Cargo.toml` 的 `[dependencies]` 只允许
   **`serde_json`、`tracing`、`futures-util`** 三项，且这三项必须存在。
   - `serde_json`：Cordis `Value`（配置/事件负载）序列化；
   - `tracing`：结构化诊断；
   - `futures-util`：future 组合工具。
2. **禁止的依赖类别**：异步运行时（tokio/async-std/smol/futures-executor）、HTTP/网络
   （reqwest/hyper/axum/actix-web）、数据库/ORM（sqlx/diesel/rusqlite）、缓存/中间件
   （redis/mongodb）、序列化格式/工具（serde_yaml/toml）、系统/环境（libc/nix/winapi/
   windows-sys/tempfile/walkdir）、时间/随机/ID（chrono/time/uuid/rand）、CLI（clap）等。
   `[dev-dependencies]` 可用于测试，但**不得**成为运行时依赖。清单的表语法/`package =`
   重命名、build 脚本、`src/bin`、`[lib] path` 替换与 `[features]` 的 `dep:` 依赖由
   §6 的 v3 护栏按真实包名解析后拦截。
3. **源码禁用 API**：`src/**` 不得出现 `std::fs`/`std::net`/`std::process`/`std::env`/
   `std::os`/`std::path`/`std::io`、`TcpStream`/`TcpListener`/`UdpSocket`/`UnixStream`、
   `File::open`/`OpenOptions`/`Command::new`、`thread::spawn`/`thread::Builder`、
   `tokio::`/`async_std::`/`smol::`/`reqwest::`/`hyper::`/`sqlx::`，以及
   `env!`/`option_env!`/`include!`/`include_str!`/`include_bytes!`
   （注释用词法器剥离、字符串保留；`use` 分组展开 + 空白归一化后再匹配）。
4. **箭头方向**：
   ```text
   业务 crate(s) ──► 插件/embedder crate ──► zeokeel-cordis
   zeokeel-cordis ──► serde_json / tracing / futures-util
   ```
   内核不得反向依赖插件层；不得通过 feature 开关把业务依赖"可选地"加进内核；
   不得在公共 API 签名中出现业务/IO 类型。
5. **交付方式**：业务能力应做成 **example、插件 crate 或独立 crate**。
   例：
   - 需要 HTTP 服务 → 宿主 crate 依赖 `axum` + `zeokeel-cordis`，实现 `Plugin` 注册
     service/effect；**不要**给内核加 `reqwest` feature；
   - 需要持久化 → 插件持有连接池，在 `ctx.effect` 的 cleanup 中关闭；内核只提供生命周期；
   - 需要新配置 → 插件用 `JsonConfigSchema` 定义自己的字段；内核只提供校验/合并机制。

---

## 5. 扩展点（业务需求 → 正确入口）

业务需求应通过以下扩展点实现，而不是修改内核。每个扩展点都有文档化的契约。

| 需求 | 扩展点 | 入口 / 契约 |
| --- | --- | --- |
| 业务组件与资源生命周期 | `Plugin` / `AsyncPlugin` + effect | `apply` / `apply_async`；`ctx.effect*`、`provide*`、`add_*`；清理逆序、失败回滚（CORE-001/028/029/044） |
| 依赖其它插件提供的业务服务 | `inject` + `ServiceKey` | 名称列表 + 严格 provider-`Active` 注入门（CORE-005/006/016） |
| 业务事件/解耦广播 | `Events` | `on`/`on_with`/`once`/`emit`/`bail`/`parallel`/`serial`/`waterfall`；事件命名与负载由插件定义（CORE-017…020） |
| 横切拦截/审计/限流/配置来源 | `internal/*` typed hooks | 共 9 个中间件式通道：`internal/plugin`、`internal/status`、`internal/service`、`internal/config`、`internal/update`、`internal/get`、`internal/set`、`internal/listener`、`internal/dispatch`；只能按契约 veto/改写对应对象（事件广播用 `Events`/waterfall） |
| 策略性包装/改写 | waterfall 中间件 | 同步/异步 waterfall 链；panic fail-closed（CORE-020） |
| 资源配额/watchdog/能力自省 | L2 `supervision`（默认关闭） | `FiberQuota`、`set_quota`、`heartbeat`、`stalled_fibers`、`IsolationCapabilities`；**策略**由宿主决定（见 KERNEL-ISOLATION §5.6） |
| 日志/指标导出 | `LogExporter` | `ctx.log_exporter(Arc::new(...))`；内核默认不安装 exporter，`TracingExporter` 是通用桥；指标命名属插件/宿主 |
| 同步 API 用于异步宿主 | `BlockingExecutor` | `set_blocking_executor`；由宿主提供运行时桥，内核不选型、不拥有运行时 |
| 配置来源（env/文件/远程） | 宿主 + `resolve_config` | 宿主解析后交给 `ctx.plugin(..., config)` / `resolve_config`；内核只做 schema 校验与合并 |
| 热更新/加载器/权限/沙箱 | 宿主 | 内核提供替换/重载原语；loader/HMR/权限/进程或 WASM 隔离在宿主（KERNEL-ISOLATION） |
| 需要新的**通用**组合原语 | 内核（例外路径） | 先通过 §7 三条判据，再补 spec/tests/docs/CHANGELOG；公共 API 增长受 `PUBLIC_ITEM_CAP` 绊线限制 |

---

## 6. 边界护栏（可执行，v3 防绕过）

`tests/kernel_boundary.rs` 共 **10 个测试**：4 个**真实断言**（检查本仓库）+ 6 个**自测**
（证明护栏真的能抓绕过）。CI 的 `kernel boundary` 步骤会全部执行。

### 6.1 真实断言

| 测试 | v3 断言细节 |
| --- | --- |
| `runtime_dependencies_stay_generic_and_minimal` | 解析 `[dependencies]` 平铺、`[dependencies.X]` 表、`[target.'cfg(...)'.dependencies(.X)]`（含 `[target.'cfg(...)'.dependencies]` **平铺 section**），并把 `{ package = "..." }` 与**单引号** `{ package = 'tokio' }` 重命名解析为**真实包名**；运行时依赖（含 `[build-dependencies]` 合并计入）必须 ⊆ `{serde_json, tracing, futures-util}`，且禁用依赖表逐一断言不存在；禁止 `[build-dependencies]`、`build = ...`/`build.rs`、`src/bin`；`[lib] path` 只能是 `src/lib.rs`；`[features]` 只允许 `default`/`supervision`，且不得 `dep:` 引用依赖（也不得引用禁用 crate） |
| `sources_do_not_reference_io_net_process_env` | 递归扫描 `src/**`；**注释用词法器剥离（字符串字面量保留）**；`use` 分组展开（`use std::{fs, net}`）+ **全空白归一化**（`std :: fs` 也能抓）；禁止：`std::fs`/`std::net`/`std::process`/`std::env`/`std::os`/`std::path`/`std::io`、`TcpStream`/`TcpListener`/`UdpSocket`/`UnixStream`/`File::open`/`OpenOptions`/`Command::new`、`thread::spawn`/`thread::Builder`、`tokio::`/`async_std::`/`smol::`/`reqwest::`/`hyper::`/`sqlx::`、`env!`/`option_env!`/`include!`/`include_str!`/`include_bytes!` |
| `sources_do_not_contain_business_vocabulary` | 递归扫描 `src/**`：① 代码 + 字符串按**标识符分词**（snake_case 与 CamelCase/PascalCase 拆分后小写）匹配 HARD 列表（payment/invoice/tenant/customer/checkout/warehouse/database/redis/kafka/rabbitmq/http/server/client）——`payment_gateway`、`PaymentService` 都会命中；② **公开文档**（`///`、`//!`、`/**...*/`、`/*!...*/`、`#[doc = "..."]`）单独用 `FORBIDDEN_DOC_WORDS` 扫描（同 HARD 列表但**去掉 http/server/client**——内核文档需要描述 embedder 边界，允许这些词）；③ 文件名 stem 检查；普通注释与尾部 `#[cfg(test)]` 模块在代码词扫描前拆出 |
| `kernel_keeps_forbid_unsafe_and_a_bounded_public_surface` | 注释剥离后 `src/lib.rs` 中**真实存在** `#![forbid(unsafe_code)]`；公共项计数（**含 `pub use`/`pub mod`**）≤ **420**（`PUBLIC_ITEM_CAP`，tripwire） |

### 6.2 自测（防绕过证明，6 个）

| 测试 | 覆盖的绕过 |
| --- | --- |
| `guard_parses_table_dependencies_and_package_renames` | 表语法 `[target.'cfg(unix)'.dependencies.tokio]`、`package = "tokio"` 重命名都必须被抓到（含 runtime 与 dev 的区分） |
| `guard_sees_flat_target_sections_and_single_quoted_renames` | `[target.'cfg(unix)'.dependencies]` **平铺 section** 的依赖必须被解析；单引号 `serde_json = { package = 'tokio', ... }` 必须解析为真实包名 `tokio` |
| `guard_expands_grouped_use_and_spaced_paths` | `use std::{fs, net, process::Command};` 展开为 `std::fs`/`std::net`/`std::process::Command`；`use std :: process :: Command;` 归一化后仍命中 |
| `guard_tokenizes_snake_case_and_camel_case_identifiers` | `payment_gateway`、`PaymentService` 等复合标识符必须拆词命中（纯子串检查会漏） |
| `guard_collects_doc_comments_and_doc_attributes` | `///`、`//!`、`/** ... */`、`#[doc = "..."]` 的文本都要被收集（公开文档是契约的一部分） |
| `guard_comment_lexer_keeps_strings_and_drops_forged_comments` | 注释里伪造 `#![forbid(unsafe_code)]` **无效**；字符串 `"https://example.com/payment"` 不被 `//` 误截断（字符串里的业务词仍然可见） |

### 6.3 口径与取舍（为什么这样设计）

- **普通注释不扫、公开文档要扫、字符串要扫**：普通注释（`//`、`/* ... */`）只是实现
  说明，允许合法提到宿主侧概念；**公开文档**（`///`、`//!`、块文档、`#[doc = "..."]`）
  是 rustdoc **契约**，会面向使用者，出现业务领域词说明 API 在泄漏领域逻辑，所以用
  `FORBIDDEN_DOC_WORDS` 单独扫描——该列表特意**不含 http/server/client**，因为内核文档
  需要描述 embedder 边界（例如"HTTP 由插件/宿主负责"）。**字符串字面量**是生产行为与
  诊断输出（日志、错误消息、格式化文本），必须保留并参与代码词表扫描。词法器同时保证：
  注释不能伪造 `#![forbid(unsafe_code)]`，且 `"https://..."` 中的 `//` 不会误剥字符串。
- **业务词按标识符分词**：代码与字符串先按 snake_case（`_`）以及 CamelCase/PascalCase
  边界拆成小写单词，再与词表逐词比较；因此 `payment_gateway`、`PaymentService` 与独立词
  `payment` 同等命中，避免"复合标识符躲过子串检查"。
- **`std::thread` 只禁 `spawn`/`Builder`，保留 `park`/`current`/`Thread`/`panicking`**：
  内核的同步等待边界（`exec::block_on` / `BlockingExecutor` seam）需要 `thread::park`、
  `thread::current`、`Thread` 等**协作/身份原语**，panic 路径使用 `thread::panicking`，
  注册归属使用 `ThreadId`；而 `thread::spawn`/`thread::Builder` 意味着**创建/管理 OS
  线程**（线程池、运行时职责），属于宿主或插件。
- **`std::io` 也在禁用之列**：内核不直接做 I/O（读写、缓冲、流）；业务 I/O 通过插件与
  effect 完成，`std::io` 类型不应出现在 `src/**` 生产代码（测试/示例不受此限）。
- **依赖清单按真实包名解析**：`package = "..."` 重命名、`[dependencies.X]` 表、
  `[target.*.dependencies]` 都可能掩盖真实 crate；build 脚本、`[build-dependencies]`、
  `src/bin` 会注入宿主假设，一律禁止；`[lib] path` 锁死为 `src/lib.rs` 防止库入口被
  替换；`[features]` 只允许 `default`/`supervision` 且不能 `dep:` 拉依赖，防止
  "可选 feature"变成引入业务/IO 依赖的后门。
- **dev-dependencies 不查**：它们只用于测试、不随内核发布；内核测试可以用 tokio 等
  构造并发/取消场景。**examples 不查**：Cargo 不会把 examples 编译进库产物，业务示例
  （HTTP/数据库演示）正是扩展点的用法展示。
- **公共 API 上限 420**：不是硬性架构上限，而是"便利 API/业务 API 泄漏"的**绊线**；
  有意新增内核特性时必须有意识调高并写 CHANGELOG，而不是顺手加 API。

### 6.4 CI 强制

- `.github/workflows/ci.yml` 的 `rust` 作业有专门步骤：
  `cargo test --locked --test kernel_boundary`（**10 个测试** = 4 个真实断言 + 6 个自测）；
- `cargo test --locked`（rust 作业与 MSRV 作业）会同样执行该集成测试；
- **PR 必须通过**：不得 `#[ignore]`、不得删除/弱化断言来"绕过"；修改
  `tests/kernel_boundary.rs`、词表/匹配规则或 `PUBLIC_ITEM_CAP` 需要评审并配
  CHANGELOG 说明。

### 6.5 违规处理

- 新增业务依赖/业务名词 → 拒绝并改为插件/example/独立 crate；
- 需要新的通用能力 → 走 §7 判据 + spec/tests/docs/CHANGELOG；公共 API 增长受 cap 约束；
- 护栏本身误报（例如内核协议词被业务词命中）→ 修改**词表/匹配规则**，不改业务词边界原则；
- 修改护栏必须保留自测（`guard_*`）：每次加严/放宽匹配规则都要补对应自测用例。

---

## 7. 提交到 `src/` 的三条判据

任何想进 `src/**` 或内核公共 API 的改动，必须**同时**满足：

1. **通用性**：不假设具体业务、产品、厂商、平台或部署形态；任何宿主都能用。
2. **无业务名词**：公共类型/函数/错误/日志中不出现业务领域词汇（订单、支付、租户、
   HTTP、数据库、缓存、环境变量等），也不出现具体传输/存储/运行时品牌。
3. **可被任何宿主使用**：不绑定特定运行时、传输、存储或配置来源；运行时/IO/策略通过
   扩展点注入（`BlockingExecutor`、`LogExporter`、`Plugin`/effect、L2 观测等）。

**任一条不满足** → 业务能力应做成 **example / 插件 / 独立 crate**，或由宿主实现；
不要为了"方便"把它加进内核（public-item cap 与 CI 边界测试会拦截）。

---

## 8. 常见误判（FAQ）

- **"把 HTTP server 放进内核，宿主更省事"** → 不。HTTP 属插件/宿主；内核只给生命周期、
  依赖注入与事件。宿主 crate 依赖 `axum` + 内核即可。
- **"logger 就是业务指标"** → 内核只提供诊断设施（level、ring buffer、`LogExporter`）；
  指标命名、标签、后端与告警属插件/宿主。`TracingExporter` 是通用桥，不是指标约定。
- **"config schema 属于内核还是业务？"** → 内核提供 `JsonConfigSchema` 机制与校验/合并；
  **具体字段**（DB URL、业务开关、领域默认值）属插件。
- **"supervision 的配额/watchdog 是策略"** → 内核只提供计数、观测与能力自省（默认关闭）；
  阈值、告警与处置策略由宿主决定（KERNEL-ISOLATION §5.6）。
- **"internal hooks 可以内置鉴权/限流"** → hooks 只是 typed 通道；鉴权/限流逻辑由插件/宿主
  实现，内核不依赖 auth/限流 crate。
- **"tokio 只是可选 feature，不算越界"** → 仍不允许进入 `[dependencies]`；用
  `BlockingExecutor` seam 或宿主 crate 桥接，内核保持运行时无关。
- **"动态加载 .so / 热更新文件监听放内核"** → 宿主/loader 层；内核提供替换/重载原语，
  不提供加载器（KERNEL-ISOLATION §5）。
- **"examples 里有业务代码，是不是内核职责？"** → examples 是演示与用法，不构成内核职责，
  也不被内核依赖；业务代码应放独立 crate/插件。

---

## 9. 延伸阅读

- 设计侧边界：[`DESIGN.md`](DESIGN.md) §7「范围边界：内核 vs 宿主」
- 故障隔离（正交概念）：[`KERNEL-ISOLATION.md`](KERNEL-ISOLATION.md)
- PR 判据与清单：[`CONTRIBUTING.md`](../CONTRIBUTING.md)
- 规范范围：[`spec/CORDIS-CORE.md`](../spec/CORDIS-CORE.md) §1 Scope
- 使用指南：[`GUIDE.md`](GUIDE.md)
