# 贡献指南

感谢参与 Cordis（Rust 内核）。本项目遵循 **SemVer 的 `0.y` 规则**：minor 版本可以包含
破坏性 API 收敛（必须同步文档、migration 与 CHANGELOG），patch 只做 bugfix/安全/性能与
文档修正。公共 API 的取舍原则见
[`docs/KERNEL-BOUNDARY.md`](docs/KERNEL-BOUNDARY.md) §8「API 极简性 / 公开 API 策略」。

参与前请阅读 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) 与 [SECURITY.md](SECURITY.md)；
安全漏洞**不要**发公开 issue。

## 环境要求

- Rust **1.85+**（edition 2021；`Cargo.toml` 的 `rust-version = "1.85"`）
- Linux/macOS/Windows 均可；CI 在 `ubuntu-24.04` 上验证，并由独立的 **MSRV 作业**用
  Rust 1.85.0 实跑 test 与 doc（见下）

## 构建与测试

CI 与发布流程的 build/test/clippy/doc/package/publish 类命令都带 **`--locked`**
（`cargo fmt` 与 `cargo audit` 不需要；仓库提交 `Cargo.lock`，见下节）。本地等价的完整门禁：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --locked --features supervision -- -D warnings
cargo test --locked                          # 默认构建（内核核心）
cargo test --locked --features supervision   # 可选 L2 监督 API
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked --all-features
cargo build --examples --locked
cargo run --locked --quiet --example hello   # services / events / async_plugin 同理
cargo package --locked                       # 真实打包并 verify 暂存的 crate
cargo publish --dry-run --locked             # 仅在准备发布时需要，不会上传
cargo build --release --locked               # panic=unwind + overflow-checks 的严格构建
```

`RUSTFLAGS="-C panic=abort" cargo check --lib --locked` **必须失败**（`src/lib.rs` 的
`compile_error!` 守卫；CI 会反证）。

CI 的扩展验证（本地可按需复跑）：

```bash
# 覆盖率门禁：fail-under-lines 86，实测基线 86.57%
cargo llvm-cov --locked --all-features --fail-under-lines 85.5
# Miri（nightly；单测允许脚手架泄漏，miri_leaks 场景开启泄漏检查）
MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" cargo +nightly miri test --lib
MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test --test miri_leaks
# 供应链 / workflow / 依赖审计（actionlint 用 pinned docker image）
cargo deny check
docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:1.7.12
cargo audit
# MSRV clippy（1.85；允许两种版本级 lint 差异：mutex_integer / needless_lifetimes）
cargo +1.85.0 clippy --all-targets --locked -- -D warnings \
  -A clippy::mutex_integer -A clippy::needless_lifetimes
cargo +1.85.0 clippy --all-targets --locked --features supervision -- -D warnings \
  -A clippy::mutex_integer -A clippy::needless_lifetimes
# 跨平台：CI 在 windows-latest / macos-latest 上跑 default + supervision + kernel boundary
```

CI 同时跑默认与 `supervision` 两种 feature 配置；提交前请至少本地跑一遍这两种配置的
test + clippy，以及 `fmt`、两种 doc 配置。**MSRV 作业（Rust 1.85.0）只跑 test（两种
feature 配置）与 `doc --all-features`，不跑 clippy**——lint 集合随 stable 版本漂移，
MSRV 只需要保证 build/test/doc 通过。

## Cargo.lock 策略

**本仓库提交 `Cargo.lock`**（0.4.3 起不再列入 `.gitignore`，随 0.4.3 提交）：
CI 与发布命令统一使用 `--locked`，保证相同依赖解析可复现、并避免发布流水线重解析依赖。它只约束
**本仓库**的构建；作为库发布到 crates.io 后，下游项目使用自己的 lockfile，不受影响。
依赖变更（含 `cargo update`）请把更新后的 `Cargo.lock` 一并提交。

## 修改语义时

内核的规范是契约，不是描述：

1. 在 [`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) 增加/修改编号用例（`CORE-0xx`）；
2. 在 `tests/` 增加能复现该行为的测试；
3. 同步更新 `docs/` 中受影响的说明；
4. 在 [CHANGELOG.md](CHANGELOG.md) 的 `Unreleased` 下记录；
5. 涉及 panic/隔离行为的，同步 `docs/KERNEL-ISOLATION.md`。

不要引入破坏性变更却不升版本；行为变化 = breaking change。破坏性 API 收敛按 SemVer 走
minor（0.y），并必须同时提供 CHANGELOG 的 **Changed + migration**、文档更新与契约测试
（`tests/api_surface.rs`）。

## 提交规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 风格：

```
feat(core): ...     新能力（冻结期不适用）
fix(core): ...      bug 修复
docs: ...           文档
test: ...           测试
build: ...          Cargo/CI/构建配置
ci: ...             CI
refactor: ...       重构（不改语义）
chore: ...          杂项
```

- 提交作者使用维护者身份 `zeokeel-dev <zeokeel-dev@users.noreply.github.com>`；
- 提交信息用英文或中文均可，主题行 ≤ 72 字符；
- 一个 PR 一个主题，避免夹带大面积格式化。

## 代码约定

- **零 `unsafe`**：crate 级 `#![forbid(unsafe_code)]`，请不要绕过；
- 错误统一用 `CordisError`；公开 API 增加错误变体时注意 `#[non_exhaustive]`；
- 不在内核锁内执行用户代码；新代码遵循现有锁纪律与 effect 所有权模型；
- 公开 API 必须有 rustdoc（CI 以 `-D warnings` 检查失效链接）；
- 不引入与内核职责无关的能力（loader、HMR、权限、沙箱、进程/WASM 隔离属宿主）。

## 内核边界

本仓库是**内核**，不是业务框架：内核只提供与业务无关的组合原语（context、service、
fiber、effect、events、registry、config schema 机制、exec 桥、logger 设施、L2 观测
原语）；业务逻辑、I/O、传输、持久化、领域配置与领域策略属于**插件层**或**宿主**。
完整约定（三层模型、绝不属于内核清单、扩展点、依赖方向）见
[`docs/KERNEL-BOUNDARY.md`](docs/KERNEL-BOUNDARY.md)。

### 提交到 `src/` 的三条判据

任何进入 `src/**` 或内核公共 API 的改动必须**同时**满足：

1. **通用性**：不假设具体业务、产品、厂商、平台或部署形态；任何宿主都能用；
2. **无业务名词**：公共类型/函数/错误/日志中不出现业务领域词汇（订单、支付、租户、
   HTTP、数据库、缓存、环境变量等），也不出现具体传输/存储/运行时品牌；
3. **可被任何宿主使用**：不绑定特定运行时、传输、存储或配置来源；运行时/IO/策略通过
   扩展点注入（`BlockingExecutor`、`LogExporter`、`Plugin`/effect、L2 观测等）。

**任一条不满足** → 业务能力应做成 example、插件或独立 crate，或由宿主实现；不要为了
"方便"加进内核。

### API 增长与收敛判据

- **公共 API 增长**必须同时满足：①通用（任何宿主可用）；②不泄漏内部表示；③有契约测试
  与 rustdoc（`tests/api_surface.rs` + `cargo doc -D warnings`）；④通过
  `tests/kernel_boundary.rs` 的公共面上限（`PUBLIC_ITEM_CAP = 420`）。PR 必须说明通用性，
  并在 CHANGELOG 记录。
- **内部类型不是公共契约**：v0.5.0 起 `fiber::FiberRef`、`fiber::FiberData`、
  `registry::PluginKind`、`registry::IdleOrRecoverableWait`、`Registry::counter()` 均为
  `pub(crate)`；不要新增可触达的内部状态类型，状态请用 `Fiber`/`Context`/`Registry`/
  `PluginRuntime` 的公开方法。
- **破坏性收敛**：优先 deprecation；直接移除必须走 minor 版本，并在 CHANGELOG 中给出
  **migration 清单**与替代 API；优先通过扩展点（`Plugin`/effect、hooks、events/L2、
  `Clock`、`LifecycleListener`、`LogExporter`、`BlockingExecutor`）而不是新增内核 API。
- 涉及公共 API 的 PR 建议用 `cargo public-api diff`/`cargo semver-checks` 做对照（可选）。

### 运行时依赖与护栏（`tests/kernel_boundary.rs` v3）

- 运行时依赖必须 ⊆ **`serde_json`、`tracing`、`futures-util`**。护栏会解析
  `[dependencies]` 平铺、`[dependencies.X]` 表、`[target.'cfg(...)'.dependencies(.X)]`
  （含 `[target.'cfg(...)'.dependencies]` 平铺 section），并把 `package = "..."` /
  单引号 `package = '...'` 重命名解析成**真实包名**；禁止 `[build-dependencies]`、
  `build =`/`build.rs`、`src/bin`，`[lib] path` 只能是 `src/lib.rs`，
  `[features]` 只允许 `default`/`supervision` 且不得 `dep:` 引用依赖；
- `src/**`（递归）不得引用：`std::fs`/`std::net`/`std::process`/`std::env`/`std::os`/
  `std::path`/`std::io`、套接字/`File::open`/`OpenOptions`/`Command::new`、
  `thread::spawn`/`thread::Builder`（`thread::park`/`current`/`Thread`/`panicking`
  保留给 executor 与 panic 路径）。`tokio::`/`async_std::`/`smol::`/`reqwest::`/
  `hyper::`/`sqlx::`、`env!`/`option_env!`/`include!`/`include_str!`/`include_bytes!`；
  检查前会做注释词法器剥离（字符串保留）、`use` 分组展开与空白归一化；
- 业务领域词按**标识符分词**匹配：代码 + 字符串里的 `payment_gateway`、`PaymentService`
  等 snake_case/CamelCase 复合词都会命中 HARD 列表；另对**公开文档**（`///`/`//!`/
  `/**...*/`/`#[doc="..."]`）用 `FORBIDDEN_DOC_WORDS` 扫描（同列表去掉 http/server/client，
  因为内核文档需要描述 embedder 边界）；文件名 stem 同样检查；
- `#![forbid(unsafe_code)]`（注释剥离后必须真实存在）与公共 API 上限 **420**
  （含 `pub use`/`pub mod`，绊线）由护栏断言；调高 cap 或修改词表/匹配规则需评审并配
  CHANGELOG 说明；
- `[dev-dependencies]` 与 `examples/` **不检查**（测试专用 / 不进入库产物），但不得把
  dev 依赖变成运行时依赖；
- CI 的 `kernel boundary` 步骤（`cargo test --locked --test kernel_boundary`，**11 个测试**
  = 4 个真实断言 + 7 个防绕过自测）以及全量 `cargo test --locked` 都会执行；
  **PR 必须通过，不得 `#[ignore]` 或弱化断言来绕过**。

## Pull Request 清单

- [ ] `cargo test --locked --test kernel_boundary` 通过（内核边界护栏）
- [ ] `cargo test --locked` 与 `cargo test --locked --features supervision` 通过
- [ ] `cargo fmt --all -- --check` 通过
- [ ] 两种 feature 配置的 clippy 均为 `-D warnings` 干净（`--locked`）
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked`（以及 `--all-features`）通过
- [ ] 依赖有变更时已提交更新后的 `Cargo.lock`
- [ ] 语义变更已同步 spec + tests + docs + CHANGELOG
- [ ] 变更满足内核边界三条判据（见 [`docs/KERNEL-BOUNDARY.md`](docs/KERNEL-BOUNDARY.md) 与上节「内核边界」）
- [ ] 公共 API 有增长/收敛时：按 §8「API 极简性」说明通用性，更新 CHANGELOG（破坏性项附 migration）并补 `tests/api_surface.rs` 契约用例

## 发布流程（维护者）

发布是 **tag 驱动**的：把 `vX.Y.Z` tag 推到远端后，`.github/workflows/release.yml`
自动完成门禁校验与 crates.io 发布；**不要在本地直接 `cargo publish`**。

0. **先 push `main` 并等 CI 全绿**（含 cross-platform / coverage / Miri / cargo-deny /
   actionlint / MSRV-clippy）：发布 workflow 只重复核心硬门禁，其余平台与工具门禁以
   `main` 的 CI 结果为准，tag 前不得跳过；
1. 更新 `Cargo.toml` 的 `version` 与 `CHANGELOG.md`（把 `Unreleased` 移到对应版本段），
   提交为 `chore(release): X.Y.Z`，并确认 `Cargo.lock` 已随提交更新；
2. 本地跑一遍与发布流水线等价的门禁（全部带 `--locked`，命令见上节「构建与测试」）：
   fmt、两种 feature 配置的 clippy/test、两种 doc 配置、examples 构建与冒烟、
   `cargo package --locked`（真实打包并 verify）、`cargo publish --dry-run --locked`
   （不上传）；
3. 打 tag 并推送：`git tag -a vX.Y.Z -m "Cordis vX.Y.Z"` + `git push origin main --tags`；
4. `release.yml` 会依次执行：
   - 校验发布来源必须是 tag、且 **tag 版本 == `Cargo.toml` version**（不一致直接失败）；
   - 重跑 fmt / clippy（两种 feature 配置）/ test（两种 feature 配置）/ doc（默认与
     `--all-features`）/ examples 构建 / `cargo package` / `cargo publish --dry-run` /
     `cargo publish`；并执行 `cargo deny`、unfinished-markers 检查与**可复现 SBOM** 双次
     生成比对；
   - 发布成功后把 `target/package/zeokeel-cordis-X.Y.Z.crate` 的 **SHA-256** 上传到该 tag
     对应的 GitHub Release（不存在则创建；此步失败不影响 crates.io 上的发布结果）；
5. GitHub Release 正文使用 `CHANGELOG.md` 对应版本段；若 Release 由 workflow 自动创建，
   则使用自动生成的 release notes，事后可手动补充。

**前提**：仓库需配置 secret `CARGO_REGISTRY_TOKEN`；tag 必须由维护者推送，`release.yml`
不接受从分支手工触发发布（`workflow_dispatch` 也只允许在 tag 上执行）。

## 问题反馈

- Bug / 功能请求：使用 issue 模板；
- 安全问题：按 [SECURITY.md](SECURITY.md) 私有报告；
- 行为准则问题：见 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
