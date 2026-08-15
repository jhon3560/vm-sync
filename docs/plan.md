# vm-sync 规划文档（VictoriaMetrics 跨隔离装置单向同步）

> 状态：v0.1.0 已按本规划交付（tag v0.1.0）；§2.3 实时性方案在 v0.1.0 交付后修正——
> VM 本体无推送机制，但快路径可经 **vmagent 双写** 实现（V2 计划）。

## 1. 目标与范围

- 安全区 I 的 VictoriaMetrics（源）→ 正向隔离装置（TCP 映射，响应单字节）→ 安全区 III 的
  VictoriaMetrics（目标），单向、有序、At-Least-Once、断点续传；
- 历史回填（默认全量，随时开关、免清数据）+ 实时同步；
- 交付物：源码 + 测试（含 e2e）+ race 全绿 + 完整文档套件 + 静态二进制 + 安装包
  （upgrade.sh / systemd / SHA256SUMS）。

## 2. 架构决策

### 2.1 复用 influx-sync 的可靠性骨架（不重造轮子）

ISFP 协议（帧/CRC/seq、gzip+zstd）、TCP 停等/滑窗 + 流水线按序 ACK、分段 WAL
（group commit/checkpoint 节流/撕裂尾恢复/中段坏帧跳帧/backfill 回拨策略）、Receiver
去重（last_seq 连续前缀/缺口闭合/DLQ）、反压三级水位、监控/日志/配置/打包体系——
全部与 influx-sync 同款。

### 2.2 数据面：VM export → import 零转换透传（核心亮点）

- 源查询：`GET /api/v1/export?match[]=…&start=…&end=…` —— 原始样本 JSON lines，
  标签/毫秒时间戳逐位保留；
- 目标写入：`POST /api/v1/import` —— 接受完全相同的格式；
- 链路 **不做任何数据转换**：export 输出 → 按行/字节分块 → gzip/zstd → 帧 → 隔离链路
  → receiver 解压 → 原样 POST import；
- 幂等：VM 同 series+ts 覆盖写（last-wins）→ 重发/重爬安全、计数不重复。

### 2.3 实时性：VM 本体无推送；快路径经 vmagent 双写实现（V2 计划）

- InfluxDB 的 SUBSCRIPTION 是"写后推送"；**VM 单机版无此机制**，但 VM 生态的标准
  替代是 **vmagent 多目标 remoteWrite 双写**：写入方指向 vmagent，`-remoteWrite.url`
  同时配"源 VM"与"vm-sync 快路径端点"，每笔数据自动复制（且带磁盘队列+重试，
  比 Influx 订阅的 fire-and-forget 更可靠）；
- **V0.1 交付不含快路径**，实时性靠低水位轮询（`interval: 500ms` + `watermark: 1s`
  → e2e ≈ 1.5~2.5s）；export 走索引、500ms 轮询成本可忽略；watermark 可更低
  （VM 写入即查询可见，水位仅防时钟抖动）；
- **V2（快路径）**：sender 新增 remote_write 接收端点（snappy protobuf 解码 →
  export/import JSON lines → 帧 → 链路），复用 influx-sync 的去重集（快路径转发
  登记 → 轮询跳过），届时 e2e 0~1s 与 influx-sync 对齐；部署需写入入口经过 vmagent。

### 2.4 历史回填：与 influx-sync V1.7 完全同语义

`backfill: all`（默认全量）/ `0`（仅实时）/ `30d`（有界，d 单位）；改配置+重启即生效
（免清数据目录）；配置变化才一次性回拨游标；存量升级只记录不回拨；export 二分探测
最早数据（谓词"[0,X) 有无数据"）+ 空窗翻倍跳过兜底。

## 3. 模块划分（module vm-sync）

```
cmd/sender        发送端入口（I 区）
cmd/receiver      接收端入口（III 区）
internal/protocol ISFP 帧编解码（gzip/zstd）
internal/transport TCP 停等客户端 / 流水线服务端
internal/wal      分段 WAL + backfill 回拨策略
internal/vm       VictoriaMetrics 客户端：ExportRange / ImportWrite / ProbeOldestData
internal/sender   Poller（export 窗口 + 空窗跳过 + 分块）+ Sender（停等/滑窗）
internal/receiver 帧处理（去重/DLQ/import 写库）
internal/monitor  Prometheus 指标（vm_sync_* 前缀）
internal/config   配置加载（d 时长单位、backfill all/0/Nd）
bench/linkproxy   字节计数代理（带宽测试用）
docs/             规划/架构/配置/部署/运维
```

## 4. 测试与验证

- 单元：vm 客户端（export/import 往返逐位一致、探测收敛/空库、typed 错误）、分块
  （行数+字节双阈值、超限行不拆）、backfill 回拨五态、d 单位；
- e2e（httptest 假 VM）：全量回填零丢失 + 标签保真、断连恢复补传、重启续传去重、
  配置变化回拨重爬幂等、毒丸 import 4xx 进 DLQ 解卡；
- `go test -count=1 ./...` + `go test -race` 全绿。

## 5. 明确不做（一期范围外）

- 不做 InfluxDB 源支持（那是 influx-sync）；快路径（remote_write 接收端）列为 V2；
- 不做 VM 集群 HA 感知、租户（accountID/projectID）过滤（有需求扩展 match 参数即可）；
- 不改变协议 Version（=1）。
