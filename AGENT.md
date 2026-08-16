# AGENT.md（双 AI 协作约定）

## §6 交接面：docs/handoff.md（唯一固定交接文档）

- 实现方每轮改完：在 `docs/handoff.md` 的"待审计"表新增/更新条目（项目、提交/tag、
  改动摘要、**审计要点**——明确告诉对方该核对什么）；
- 审计方：看到新条目即开始审计，在条目下追加"审计结论"（复验取证 + 新发现编号），
  闭环后把条目移入"已闭环"表；
- **双方只通过 handoff.md 交接工作状态**，不依赖会话外的任何通知；
- 审计发现缺陷 → 实现方修复 → 更新同一条目的状态 → 审计方复验 → 直到闭环。

### §6.1 审计调用（实现方自驱，2026-08-16 起）

实现方在登记条目后可**直接调用审计 agent**（独立上下文）执行审计：审计 agent
阅读 handoff.md 待审计条目 → 独立复验（构建/测试/race/代码审查）→ 把"审计结论"
写回 handoff.md 并以审计身份（`audit <audit@hx.local>`）提交。实现方根据其结论
修复、更新条目、再次调用，直到闭环。审计 agent 指令需自包含（仓库路径、条目号、
复验命令、报告方式）。

## 其他约定（沿用 influx-sync AGENT.md 惯例）

- 所有工作成果/审计结论一律写入 docs/ 并提交；
- 构建：`CGO_ENABLED=0 go build -trimpath -ldflags "-s -w"`；
- fork（vm-sync-fork）与 vm-sync 同步移植后必须显式 `go build -mod=vendor ./app/vm-sync/`
  验证根包（无测试根包 `go test` 输出 `?` 会掩盖编译错误，R9 教训）；
- 版本/tag：vm-sync `v0.x.y`；fork `feature-isolation-sync-v0.x.y`。
