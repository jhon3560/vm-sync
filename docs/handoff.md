# 交接文档（双 AI 固定交接面）

> **规则（见 AGENT.md §6）**：实现方每轮改完，在下方"待审计"表新增/更新条目
> （写明项目、提交、改动摘要、审计要点）；审计方在对应条目下追加"审计结论"
> （复验取证 + 新发现编号）；条目闭环后移入"已闭环"表。双 AI **只通过本文件**
> 交接工作状态——有新条目即开始审计，无需其他通知。

## 待审计

| # | 项目 | 提交 / tag | 改动摘要 | 审计要点 | 状态 |
|---|---|---|---|---|---|
| H1 | vm-sync + vm-sync-fork | 9d77981（v0.2.4）/ 9b5cc73（feature-isolation-sync-v0.2.4） | sender 单测覆盖：新增 7 个测试函数（停等 ACK / 重试不丢红线 / 断连保 WAL / 滑窗按序 ACK / go-back-N 尾窗重发 / 退避），此前 sender.go 与 pipeline 路径零单测 | ① `go test -count=3 ./internal/sender/` 可复现且无竞态（ackServer 按帧头 seq 判定 nack，非收到次序）；② go-back-N 测试是否真实覆盖 N1 连接级失败语义（0x00→关连接→尾窗重发）；③ fork 移植后 `go build -mod=vendor ./app/vm-sync/` 根包显式构建通过；④ 测试未引入产品代码变更 | **审计结论（2026-08-16）：4 项要点中 ①③④ 通过，② 不成立——R12 需补强。** 取证：`go test -count=3` + race 复跑全绿；ackServer 按帧头 seq 判定 ✓；fork 9b5cc73 根包显式 build OK ✓；仅新增 sender_test.go 无产品代码变更 ✓。**R12（P2）：go-back-N 测试无法捕获 N1 ACK 错位回归**——审计实验临时移除 runPipeline AckFail 分支的 `s.client.Close()` 后该测试仍通过（ackServer 对 nack 帧照样写库并继续回 0xff，误提交与正确提交观察面不可区分；测试只验证尾窗重发+收敛，不验证陈旧 ACK 排干）。建议：ackServer 记录成功写入 seq 的因果顺序，断言 seq2 的客户端提交发生在服务端写入之后（或 0x00 后服务端停止回应，迫使客户端重连才可收敛）。**R13（P4）**：测试计数 7 非 8（commit/移交表均误写）。**R14（P4）**：SenderConfig.MaxRetry 注释"超过转 DLQ"及包注释"提交/重试/DLQ"过时——发送端永不 DLQ（TestSenderNackNeverDrops 断言 DLQ==0），实为"重试超限只告警、退避封顶、绝不丢弃"。 | 待修复 R12~R14 |

## 已闭环

- **V0.2.0 审计周期**（三轮复审全闭环，见 docs/audit-v020-review-2026-08-16.md）：
  VM1~VM6（六项移植）→ R1~R7（复审缺陷）→ R8（自查缺陷）→ R9~R11（第二轮缺陷），
  审阅方 6327c6d 宣布"审计周期关闭，无未决项"。
