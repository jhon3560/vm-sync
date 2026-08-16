# vm-sync V0.2.0 审计复审（审阅方，2026-08-16）

> 审阅对象：commit 35defff（V0.2.0）+ docs/audit-v020-2026-08.md。
> 方式：独立复验每项修复的证据与测试，并自行跑通 `go build / vet / test / test -race`。
> 结论先行：**VM1~VM6 六项移植全部属实、修复有效、回归测试可复现（全量 test/vet/race 全绿）。**
> 另发现 2 个新缺陷（R1 死配置超时 / R2 欠满判定粒度）+ 若干文档问题（R3~R7），详见下文。

## 1. 逐项核验表

| # | 声称 | 核验结果 | 取证 |
|---|---|---|---|
| VM1 | backfill=all 曾被 duration 校验误拒（v0.1.0 死代码） | ✅ 属实 | v0.1.0 config.go:193 `"sync.backfill"` 确在通用校验表内；现版移出并补负值拒绝；TestBackfillAllValidates 通过 |
| VM2 | export 512MB 静默截断 → +1 探测显式报错 | ✅ 属实 | client.go LimitReader(+1) + `len(body)>max` 判断；TestExportRespTooLarge（注入 1KB 上限）通过 |
| VM3 | 空窗-only 翻倍 → 欠满（空+稀疏）翻倍、真空 1h/稀疏 MaxWindow 双档 | ✅ 属实 | v0.1.0 仅有 emptyStreak；现版 windowSize() 双档封顶；手工推演 TestPollerSparseWindowGrowth：窗口序列 5s→10s→20s→30s×9，12 轮游标 ≈305s ≥300s（修复前恒 5s/轮=60s），测试通过 |
| VM4 | export 与处理串行 → 单窗口轮次预取流水线 + window_target 解耦 | ✅ 属实 | poller.go prefetchSlot + launchPrefetch；失配丢弃重查兜底（cursor 比对）；3 个相关测试通过；race 全绿（预取 goroutine 只读共享态，slot 仅 Run 循环单 goroutine 访问） |
| VM5 | export 失败复位窗口增长、下轮基础窗口自愈 | ✅ 属实 | 错误路径 streak=0/streakAllEmpty=true；TestPollerExportErrorResetsStreak 通过 |
| VM6 | e2e 假目标单次 Body.Read 截断 >4KB 帧 | ✅ 属实 | e2e_test.go 改 io.ReadAll；e2e 全链路（含 390/390 零丢失）通过 |
| 声明"go test + vet 通过" | — | ✅ 自行复跑通过；另补跑 `go test -race ./internal/...` 亦通过 |

审计文档自带"遗留 1"（fork 平铺副本未同步）**已闭环**：vm-sync-fork bfe5725
（19:41，晚于本文档 1 分钟）已同步 VM1~VM6 全部代码 + 7 个回归测试 + FORK.md 更新，
fork 侧 `go test ./app/vm-sync/...` 通过（审阅方实测）。文档应更新该条状态。

## 2. 残余缺陷（含取证记录）

### R1（P1，部署级）`source.timeout` 是死配置——请求超时实际不可控

- 取证：`internal/vm/client.go` NewClient 解析 `cfg.Timeout` 存入 `Client.timeout`
  字段后**全库无任何读取点**（`grep -rn "\.timeout" internal/` 仅命中 config.go 的
  yaml key 字符串）；ExportRange/ImportWrite/ExportHasData 均直接用调用方 ctx，
  而 poller 的 ctx 来自 `context.WithCancel(context.Background())`（cmd/sender/main.go），
  无 deadline。实际超时只有 http.Client 全局 `Timeout: 15*time.Minute` 兜底。
- 后果：源 VM 挂起（连上不回包）时轮询停滞最长 15 分钟，用户配置 `timeout: 10s`
  以为安全却完全无效。**VM4 预取使问题更尖锐**：pollOnce 新增裸接收
  `r := <-slot.ch`（无 ctx 保护），消费轮会同步阻塞到预取查询结束——
  15 分钟无数据、WAL 空转、实时性归零。
- 建议：ExportRange/ExportHasData 内部 `context.WithTimeout(ctx, c.timeout)`；
  或至少 pollOnce 收 slot 时 `select { case r := <-slot.ch: ... case <-ctx.Done(): }`。

### R2（P2，效率/震荡）欠满判定按"行数"，但 export 单行可含大量样本

- 取证：`lineCount()` 只数 `'\n'`；VM export 每行 = 一个 series 的
  `values[]/timestamps[]` 数组（客户端注释自认"每行可含多样本"）。少 series ×
  高样本率库（如 10 条序列、10 万样本/s）行数恒 << window_target(20000) →
  被误判"稀疏"→ 窗口翻倍至 MaxWindow(30s) → 单轮 export 体积放大；极端时触碰
  N15 512MB 上限 → 报错 → VM5 复位 → 重新翻倍 → **周期性震荡**（每几轮一次失败
  export，浪费带宽与查询，但自愈且不丢数据）。
- 建议：欠满判定改为样本数（统计 `"values":[` 出现次数）或响应字节数
  （`len(raw)`，可与 N15 的 +1 读取合并统计）；或 N15 报错后本轮将窗口直接减半
  而非复位 streak（避免震荡路径）。

### R3（P3，文档）机制描述滞后于代码（AGENT.md §4 要求同步更新）

- architecture.md:53、plan.md:49/60、README.md:49 仍写"空窗翻倍跳过"，VM3 后实际
  语义是"欠满（空+稀疏）翻倍"；operations.md 未记载新增的
  `prefetch discarded (cursor mismatch)` Debug 日志。

### R4（P3，文档）"新增 8 个回归测试"计数不准

- 实际新增 7 个测试函数：config 2（TestBackfillAllValidates / TestWindowTargetValidates）、
  vm 1（TestExportRespTooLarge）、sender 4（SparseWindowGrowth / PrefetchPipeline /
  PrefetchDiscardOnMismatch / ExportErrorResetsStreak）；另修改 1 个既有断言 +
  e2e 假目标。commit message 与审计文档表格（列 7 行）自相矛盾。

### R5（P3，措辞）N15 报错提示 "reduce max_window" 覆盖不全

- 当窗口已收缩到基础 window（streak=0）仍超 512MB 时，调 max_window 无济于事；
  此时只能调小 sync.window 或说明该数据密度超出链路能力。建议报错同时给出当前
  window/max_window 值。

### R6（P4，nit）预取带来的内存峰值与守卫不一致

- 预取结果（≤512MB）在 buffered channel 中与当前轮 raw（≤512MB）并存，峰值 ~1GB
  （另加 splitFrames 拷贝），记录在案可接受；
- launchPrefetch 未应用 pollOnce 同款的 `underfillStreak==0 && >MaxWindow` 守卫，
  仅当误配置 Window > MaxWindow 时两者窗口不一致（不影响数据正确性）。

### R7（P4，nit）WatermarkDuration() 默认值不一致

- config.go:344 `WatermarkDuration()` 默认 10s，PollerConfig 的 watermark 默认 1s；
  backfill=0 首次启动游标 = now-10s（多爬 9s，不丢数据，但语义与"水位 1s"冲突）。

## 3. 修复建议优先级

1. **R1**（死配置超时，真机源库卡死场景直接停摆）——建议本批修；
2. **R2**（判定粒度，高样本率少序列库震荡）——建议本批修或列入下批；
3. R3/R4/R5（文档与措辞，顺手修）；R6/R7 可记入后续。
4. 审计文档"遗留 1"更新为"已闭环（bfe5725）"。

修复完成后请提交新 commit 并在本文档追加"修复记录"节，审阅方将复验后进入下一轮。
