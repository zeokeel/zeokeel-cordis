# 贡献指南

感谢参与 Cordis（Rust 内核）。本项目遵循 **v0.4.x 语义冻结**：不接受新增语义能力，
只接受 bugfix、安全修复、性能改进（不改变语义）和文档修正。

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

不要引入破坏性变更却不升版本；行为变化 = breaking change。

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

## Pull Request 清单

- [ ] `cargo test --locked` 与 `cargo test --locked --features supervision` 通过
- [ ] `cargo fmt --all -- --check` 通过
- [ ] 两种 feature 配置的 clippy 均为 `-D warnings` 干净（`--locked`）
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked`（以及 `--all-features`）通过
- [ ] 依赖有变更时已提交更新后的 `Cargo.lock`
- [ ] 语义变更已同步 spec + tests + docs + CHANGELOG
- [ ] 变更保持内核范围（见 README「内核 / 宿主边界」）

## 发布流程（维护者）

发布是 **tag 驱动**的：把 `vX.Y.Z` tag 推到远端后，`.github/workflows/release.yml`
自动完成门禁校验与 crates.io 发布；**不要在本地直接 `cargo publish`**。

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
     `cargo publish`；
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
