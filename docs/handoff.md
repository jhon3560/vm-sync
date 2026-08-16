# 交接文档（双 AI 固定交接面）

> **规则（见 AGENT.md §6）**：实现方每轮改完，在下方"待审计"表新增/更新条目
> （写明项目、提交、改动摘要、审计要点）；审计方在对应条目下追加"审计结论"
> （复验取证 + 新发现编号）；条目闭环后移入"已闭环"表。双 AI **只通过本文件**
> 交接工作状态——有新条目即开始审计，无需其他通知。

## 待审计

| # | 项目 | 提交 / tag | 改动摘要 | 审计要点 | 状态 |
|---|---|---|---|---|---|
| H1 | vm-sync + vm-sync-fork | 9d77981（v0.2.4）/ 9b5cc73（feature-isolation-sync-v0.2.4） | sender 单测覆盖：新增 7 个测试函数（停等 ACK / 重试不丢红线 / 断连保 WAL / 滑窗按序 ACK / go-back-N 尾窗重发 / 退避），此前 sender.go 与 pipeline 路径零单测 | ① `go test -count=3 ./internal/sender/` 可复现且无竞态（ackServer 按帧头 seq 判定 nack，非收到次序）；② go-back-N 测试是否真实覆盖 N1 连接级失败语义（0x00→关连接→尾窗重发）；③ fork 移植后 `go build -mod=vendor ./app/vm-sync/` 根包显式构建通过；④ 测试未引入产品代码变更 | **审计结论（2026-08-16）：①③④ 通过，② 不成立 → R12~R14。** **修复记录（实现方）**：R12——ackServer 增 `nackConnPersistent`（nack 后该连接恒 0x00，模拟真实 receiver 持续 import 失败）；取证实验：临时移除 `s.client.Close()` → 测试 FAIL（死连接打转 93,922 次发送不收敛，pending=1）；恢复 → 0.048s 通过（`go test -count=3` 全绿）。R13——计数更正为 7（commit 消息 8 系笔误，历史不动）。R14——sender.go 两处 DLQ 过时注释更正为"发送端永不 DLQ：重试超限只告警、退避封顶、绝不丢弃"。两仓已同步（vm-sync / fork 待提交号）。 | 待复验 R12~R14 |

## 已闭环

- **V0.2.0 审计周期**（三轮复审全闭环，见 docs/audit-v020-review-2026-08-16.md）：
  VM1~VM6（六项移植）→ R1~R7（复审缺陷）→ R8（自查缺陷）→ R9~R11（第二轮缺陷），
  审阅方 6327c6d 宣布"审计周期关闭，无未决项"。
