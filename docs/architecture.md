# vm-sync 架构与数据流

## 1. 总体架构

```
[I 区] 源 VictoriaMetrics
        │  /api/v1/export 窗口轮询（毫秒游标）
        ▼
[sender] ──┬─ Poller：export [cursor, now-watermark) → 按行/字节分块 → zstd/gzip 编码 → WAL → 推进游标
           └─ Sender：WAL.Peek → 停等发送（帧 → 单字节 ACK）→ Commit / 重试 / DLQ
        │  ISFP over TCP（经隔离装置映射）
        ▼
[receiver] ──┬─ Server：逐连接逐帧读 → 并发 handler → 按序 ACK
             ├─ 去重（last_seq 连续前缀 + 缺口闭合）→ 解压 → 原样 POST /api/v1/import
             ├─ import 失败：4xx 永久 → DLQ 隔离 + 0xff（解卡）；瞬时 → 0x00（重发）
        ▼
[III 区] 目标 VictoriaMetrics
```

## 2. 数据面：零转换透传

VictoriaMetrics 的 `/api/v1/export`（查询）与 `/api/v1/import`（写入）使用**完全对称**
的 JSON lines 格式：

```json
{"metric":{"__name__":"cpu","job":"a"},"values":[1.5,2.5],"timestamps":[1786800000000,1786800001000]}
```

- sender 只做**分块**（按 `frame_lines`/`frame_bytes` 双阈值，单行绝不拆分），不解析、
  不重建任何样本——标签、值、毫秒时间戳逐位保留；
- receiver 解压后把原文整体 POST 给 import（与 influx-sync 的 WriteRaw 同思路，
  且比 LP 透传更彻底：连行都不用解析）；
- 目标 VM 对同 series+ts 覆盖写（last-wins）→ 重发/重爬幂等，计数不重复。

## 3. 可靠性机制（与 influx-sync 同款）

| 机制 | 说明 |
|---|---|
| 顺序铁律 | 先 WAL 落盘成功、后推进游标——违反会漏数据 |
| 帧序 | seq 连续（WAL 内部分配），停等逐帧确认；0xff=已落库才 Commit |
| 去重 | receiver last_seq 连续前缀 + 发送方权威缺口闭合（重启后恢复连续推进） |
| 断点续传 | WAL 游标 + checkpoint（1s 节流）+ 重启扫描重建；last_seq 每秒持久化 |
| 毒丸 | import 4xx → DLQ 落盘隔离 + 0xff 解卡（主链路不阻塞）；瞬时失败 0x00 重试，永不丢弃 |
| 反压 | WAL 盘占用三级水位（60%/80% 迟滞） |
| WAL 健壮性 | 撕裂尾截断恢复、中段坏帧跳帧重同步、flock 防多实例 |

## 4. 历史回填（backfill，与 influx-sync V1.7 同语义）

- `all`（默认）：探测库内最早数据（export 二分，谓词"[0,X) 有无数据"），全量同步；
- `0`：仅实时（游标 = now - watermark）；`30d`：有界回填（d 单位）；
- 改配置 + 重启即生效；只在配置值变化时**一次性回拨游标**（checkpoint 记录 backfill 值）；
  存量部署升级只记录不回拨（防升级即全库重发）；
- "库只有 10 天数据、配 30d/all"→ 从 10 天前开始搬；真空区/稀疏区由欠满窗口翻倍跳过兜底（V0.2/N14：空窗或字节数 < window_target 均翻倍；真空区上限 1h、稀疏区封顶 max_window）
  （5s→…→1h，命中数据复位）。

## 5. 实时性模型（与 influx-sync 的关键差异）

VictoriaMetrics 单机版**没有** SUBSCRIPTION 式"写后推送"，V0.1 无快路径：

- 实时性靠低水位轮询：`interval: 500ms` + `watermark: 1s`（默认）→ e2e ≈ 1.5~2.5s；
- VM 写入即查询可见（无 Influx 的可见性顾虑），watermark 仅防时钟抖动，可配到更低；
- export 查询走索引、无聚合开销，500ms 轮询成本可忽略。

**快路径（V2 计划）**：VM 生态的标准"订阅"替代是 **vmagent 多目标 remoteWrite 双写**
（写入方 → vmagent → [源 VM, vm-sync 快路径端点]），sender 实现 remote_write 接收端
（snappy protobuf 解码 → JSON lines → 帧），复用 influx-sync 去重集，e2e 可达 0~1s。

## 6. 关键设计约束

- 隔离装置：TCP 单通、响应单字节 0xff/0x00 → 停等协议（滑窗需装置验证后开启）；
- 时间戳：毫秒精度（VM 存储精度），游标/回拨/指标均以毫秒为单位；
- 协议 Version=1、帧布局与 influx-sync 一致（zstd 帧需两端同版本，同包部署满足）。
