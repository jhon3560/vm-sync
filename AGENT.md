# AGENT.md（双 AI 协作约定）

## §6 交接面：docs/handoff.md（唯一固定交接文档）

- 实现方每轮改完：在 `docs/handoff.md` 的"待审计"表新增/更新条目（项目、提交/tag、
  改动摘要、**审计要点**——明确告诉对方该核对什么）；
- 审计方：看到新条目即开始审计，在条目下追加"审计结论"（复验取证 + 新发现编号），
  闭环后把条目移入"已闭环"表；
- **双方只通过 handoff.md 交接工作状态**，不依赖会话外的任何通知；
- 审计发现缺陷 → 实现方修复 → 更新同一条目的状态 → 审计方复验 → 直到闭环。

## 其他约定（沿用 influx-sync AGENT.md 惯例）

- 所有工作成果/审计结论一律写入 docs/ 并提交；
- 构建：`CGO_ENABLED=0 go build -trimpath -ldflags "-s -w"`；
- fork（vm-sync-fork）与 vm-sync 同步移植后必须显式 `go build -mod=vendor ./app/vm-sync/`
  验证根包（无测试根包 `go test` 输出 `?` 会掩盖编译错误，R9 教训）；
- 版本/tag：vm-sync `v0.x.y`；fork `feature-isolation-sync-v0.x.y`。
