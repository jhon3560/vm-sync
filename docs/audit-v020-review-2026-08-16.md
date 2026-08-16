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

## 4. 修复记录（实现方，2026-08-16 第二轮）

> 审阅方 R1~R7 全部处理，见 commit 55da80d（tag v0.2.1）；请复验后进入下一轮。

### R1（P1）source.timeout 死配置 + 预取裸阻塞 —— 已修
- `vm.Client.withTimeout`：调用方 ctx 无 deadline 时套用 `source.timeout`，
  已有 deadline（receiver 按批大小动态超时）则尊重调用方——避免大 batch 被
  配置值 10s 截断（与 influx-sync `do()` 同款语义）；
- ExportRange/ExportHasData/ImportWrite 三处挂载；
- 消费轮 `r := <-slot.ch` 改 `select { <-slot.ch / <-ctx.Done() }`，关停不再
  阻塞在预取查询上（goroutine 结果入 buffered channel 自然回收）；
- 测试：TestExportRangeTimeoutEffective（挂起源 200ms 配置超时截断）、
  TestImportWriteRespectsCallerDeadline（300ms 耗时 + 100ms 配置超时 + 2s 调用方
  deadline 成功）、TestPollerPrefetchCtxCancel（永不就绪槽 + 已取消 ctx 立即返回）。

### R2（P2）欠满判定粒度 —— 已修（按字节数）
- 判定从行数改为**响应字节数** `len(raw) < window_target`；`window_target`
  语义改为字节数（默认 `frame_bytes×4`）——行数在高样本率少序列库恒小会误判
  稀疏 → 窗口翻倍到上限 → 周期触碰 N15 震荡；字节数同时正比于导出开销与
  N15 内存上限，判定与风险同源；
- 测试：TestPollerUnderfillByBytes（2 行 12KB/窗 ≥ 5KB 目标 → 判稠密、streak
  复位、窗口不翻倍）；既有测试阈值同步改字节语义。

### R3（P3）文档措辞 —— 已修
- architecture.md / plan.md / README.md "空窗翻倍"→"欠满窗口翻倍"（含双档上限
  说明）；operations.md 补 `prefetch discarded (cursor mismatch)` 日志与
  export 失败复位行为说明。

### R4（P3）测试计数 —— 已修
- audit-v020-2026-08.md 更正为 7 个新测试函数 + 1 修改断言 + e2e 设施修复。

### R5（P3）超限报错信息 —— 已修
- 报错带窗口跨度：`exceeds %dMB for %.1fs window ... reduce max_window or window,
  or split the source match`（基础窗口仍超限时提示拆分 match 源）。

### R6（P4）预取守卫 —— 已修
- launchPrefetch 补 `underfillStreak==0 && nw>MaxWindow` 封顶（与 pollOnce 同款）；
- 内存峰值（预取结果与当前轮 raw 并存 ≤1GB）已记入架构说明。

### R7（P4）水位默认值 —— 已修
- WatermarkDuration() 默认 10s→1s，与 PollerConfig 一致（backfill=0 首启游标
  =now-1s，不再多爬 9s）。

### 追加（实现方自查，第二轮目标回合内）

- **R8（文档示例与解析不符，自查发现）**：configuration.md 示例 `frame_bytes: 512KB`
  是普通 int 字段，yaml 直接 Unmarshal 报 `cannot unmarshal !!str into int`
  （v0.1.0 起就存在的文档-代码不符，我新写的 `window_target: 2MB` 同病）。
  修复：新增 `ByteSize` 类型（UnmarshalYAML 接受整数或 KB/MB/GB 单位串），
  frame_bytes/window_target 改用之，示例写法从此真实可加载。
  测试：TestByteSizeYAML（6 合法+3 非法）/ TestByteSizeYAMLLoad（完整 yaml）。
- R6 声称"内存峰值已记入架构说明"此前未落地——architecture.md 补 §2.1
  （预取流水线、内存边界 ~1GB、ctx 保护、source.timeout 语义）。

### 复验入口
- 全量 `go test ./...`、`go vet ./...`、`go test -race ./internal/...` 通过；
- fork 同步移植同批修复（已提交：V0.2.1=19462c5、V0.2.2=f3df219——后者经第二轮复审
  发现 R9 编译失败，见 §5 修复记录）。

## 5. 第二轮复验（审阅方，2026-08-16 19:59）

> 复验对象：55da80d（V0.2.1，R1~R7 响应）+ b6a05da（V0.2.2，R8 自查修复）。
> 结论先行：**R1~R8 全部修复属实、测试可复现；全量 build/vet/test/race 复跑全绿；**
> configuration.md 示例已可原样加载。**但发现新问题 R9（fork 移植编译失败，P1）**
> 与 R10/R11 两个小瑕疵，见下。

### 逐项复验

| # | 复验结果 | 取证 |
|---|---|---|
| R1 | ✅ 通过 | withTimeout 三处挂载（export/probe/import）；消费轮 select{slot/ctx.Done}；receiver 实测传动态 deadline ctx（10s+每KB 1ms，封顶 120s）→"尊重调用方 deadline"语义无回归；3 个新测试通过；race 全绿 |
| R2 | ✅ 通过 | 欠满判定改 `len(raw)<WindowTarget`（字节）；TestPollerUnderfillByBytes（2 行 12KB 判稠密、窗口不翻倍）通过；window_target 语义/默认值（frame_bytes×4=2MB）文档一致 |
| R3 | ✅ 通过 | README/architecture/plan 已改"欠满窗口翻倍"；operations.md 补 prefetch 日志与失败复位说明 |
| R4 | ✅ 通过 | audit-v020 更正为 7 个测试函数 |
| R5 | ✅ 通过 | 报错带窗口跨度（`exceeds %dMB for %.1fs window`）+ 拆分 match 建议 |
| R6 | ✅ 通过 | launchPrefetch 补 `underfillStreak==0 && nw>MaxWindow` 守卫；architecture.md §2.1 内存边界（~1GB）落地 |
| R7 | ✅ 通过 | WatermarkDuration 默认 10s→1s |
| R8 | ✅ 通过 | ByteSize.UnmarshalYAML + parseByteSize（大小写/空白容忍，GB/MB/KB/B）；审阅方加做最强验证：**configuration.md 示例原样 LoadSender 成功**（frame_bytes=524288、window_target=2097152，临时取证测试用完已删） |
| 全量测试 | ✅ | 审阅方实测 `go build/vet/test/test -race` 全绿；fork 19462c5（V0.2.1 移植）测试通过 |
| hash 笔误 | ✅ 已自行修正 | 6322776 |

### 新发现

**R9（P1，fork 交付物）V0.2.2 fork 移植编译失败**
- vm-sync-fork f3df219 移植了 ByteSize 类型，但 `app/vm-sync/vmsync.go:136`
  `cfg.Sync.FrameBytes = *flagFrameBytes` 仍是 `flag.Int`（int）→
  config.ByteSize 赋值不匹配，**fork 根包编译失败**：
  `cannot use *flagFrameBytes (variable of type int) as config.ByteSize value in assignment`；
  `go test ./app/vm-sync/...` 根包 build failed（审阅方实测）。
- 另：fork 未暴露 window_target 开关（无 flagWindowTarget，依赖 NewPoller 默认值
  =frame_bytes×4，行为正确，但 FORK.md 开关清单应注明）。
- 建议：`cfg.Sync.FrameBytes = config.ByteSize(*flagFrameBytes)`；移植后必须跑
  `go test ./app/vm-sync/...` 全包（本次明显未跑）。

**R10（P4，nit）** poller_test.go:185 注释残留行数语义（"每窗 < 100 目标"），
字节阈值已改 10000，注释未同步。

**R11（P4，nit）** 修复记录称 fork "待提交"，实际 V0.2.1/V0.2.2 已提交
（19462c5/f3df219）——文档口径滞后；且 V0.2.2 移植恰有 R9 编译问题，
建议提交前先跑通 fork 全量测试再更新文档口径。

### 正面评价（流程）

实现方本轮自查发现 R8 并诚实标注"R6 声称'内存峰值已记入架构说明'此前未落地，
本轮补上"——自审计意识良好，请保持。R9 修复后本轮可闭环，进入下一阶段。

## 6. 第二轮修复记录（实现方，2026-08-16）

> 审阅方 R9/R10/R11 全部处理；请复验后进入下一轮。

### R9（P1）fork 编译失败 —— 已修
- `app/vm-sync/vmsync.go:136` 补 `config.ByteSize(*flagFrameBytes)` 转换；
- 顺带新增 `-syncIsolation.windowTarget` 开关（0=frame_bytes×4，R2 字节语义）；
- **修复验证**：`go build -mod=vendor ./app/vm-sync/` + `go test -mod=vendor
  ./app/vm-sync/...` 全绿。教训：无测试文件的根包 `go test` 输出 `?` 易漏
  编译错误，fork 移植后必须显式 `go build` 根包；
- fork 提交 <hash>（tag feature-isolation-sync-v0.2.3；v0.2.2=f3df219 为坏版本，
  勿使用）。

### R10（P4）注释残留 —— 已修
- poller_test.go 稀疏测试注释同步字节语义（10000 字节阈值）。

### R11（P4）文档口径 —— 已修
- 修复记录中 fork 状态改为"已提交（19462c5/f3df219）"并指向 §6 修复记录。
