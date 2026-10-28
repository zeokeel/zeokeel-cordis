# 贡献指南

感谢参与 Cordis（Rust 内核）。本项目遵循 **v0.3.x 功能冻结**：不接受新增语义能力，
只接受 bugfix、安全修复、性能改进（不改变语义）和文档修正。

参与前请阅读 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) 与 [SECURITY.md](SECURITY.md)；
安全漏洞**不要**发公开 issue。

## 环境要求

- Rust **1.85+**（edition 2021）
- Linux/macOS/Windows 均可；CI 在 `ubuntu-24.04` 上验证

## 构建与测试

```bash
cargo build
cargo test                         # 默认构建（内核核心）
cargo test --features supervision  # 可选 L2 监督 API
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features supervision -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo build --release              # panic=unwind + overflow-checks 的严格构建
```

CI 会同时跑以上两种 feature 配置；提交前请至少本地跑一遍默认与 `supervision` 配置的
test + clippy。

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

- [ ] `cargo test` 与 `cargo test --features supervision` 通过
- [ ] `cargo fmt --all -- --check` 通过
- [ ] 两种 feature 配置的 clippy 均为 `-D warnings` 干净
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` 通过
- [ ] 语义变更已同步 spec + tests + docs + CHANGELOG
- [ ] 变更保持内核范围（见 README「内核 / 宿主边界」）

## 发布流程（维护者）

1. 更新 `Cargo.toml` 版本与 `CHANGELOG.md`（把 `Unreleased` 移到对应版本）；
2. 全量验证（上面所有命令）；
3. 提交并打 tag：`git tag -a vX.Y.Z -m "Cordis vX.Y.Z"`；
4. `git push origin main --tags`；
5. 在 GitHub 创建 Release，附 CHANGELOG 对应段落。

## 问题反馈

- Bug / 功能请求：使用 issue 模板；
- 安全问题：按 [SECURITY.md](SECURITY.md) 私有报告；
- 行为准则问题：见 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
