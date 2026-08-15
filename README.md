# vm-sync

VictoriaMetrics 跨正向隔离同步系统（ISFP 协议，v0.1.0）。

**功能**：安全区 I 的 VictoriaMetrics → 正向隔离装置（TCP 映射）→ 安全区 III 的
VictoriaMetrics，单向、有序、At-Least-Once、断点续传同步。

## 能力概览

| 能力 | 说明 |
|---|---|
| 数据获取 | `/api/v1/export` 窗口轮询（原始样本、标签/毫秒时间戳逐位保留），游标 + WAL 缓冲 |
| 数据面 | **零转换透传**：export 输出与 import 输入格式完全对称，原文进帧、原文写库 |
| 传输 | ISFP：20B 头 + gzip/zstd(JSON lines) + CRC32；停等协议（隔离装置单字节 ACK）；滑窗（实验项） |
| 可靠性 | WAL group commit + 停等 ACK + last_seq 连续前缀去重 + 断点续传 + DLQ 毒丸隔离 + 反压三级水位 + WAL 撕裂尾自恢复 + 缺口闭合 |
| 历史回填 | `backfill: all`（默认全量）/ `0`（仅实时）/ `30d`（有界，d 单位）；改配置+重启即生效，**免清数据目录** |
| 压缩 | zstd（默认，约为 gzip 链路带宽的 1/2~1/3）/ gzip（兼容旧对端） |
| 监控 | /metrics（Prometheus：游标/端到端延迟/积压/重试/DLQ/反压） |

## 与 influx-sync 的差异

| 维度 | influx-sync | vm-sync |
|---|---|---|
| 源/目标 | InfluxDB 1.x | VictoriaMetrics |
| 查询 | `SELECT *` + schema 发现 | `/api/v1/export`（无 schema 概念） |
| 写库 | Line Protocol | `/api/v1/import`（与 export 对称，零转换） |
| 实时快路径 | SUBSCRIPTION 推送透传（0~1s） | V0.1 无（VM 本体无推送机制）；V2 计划经 vmagent 双写 remoteWrite 实现（见 docs/plan.md §2.3） |
| 时间精度 | 纳秒 | 毫秒（VM 存储精度） |
| 可靠性骨架 | 同款（ISFP/WAL/去重/DLQ/反压/打包体系） | 同款 |

## 快速开始

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver
go test ./...
go test -race ./internal/...
```

## 目录

```
cmd/sender        发送端入口（I 区）
cmd/receiver      接收端入口（III 区）
internal/protocol ISFP 帧编解码（gzip/zstd）
internal/transport TCP 停等客户端 / 流水线服务端
internal/wal      分段 WAL（group commit + 撕裂尾恢复 + backfill 回拨策略）
internal/vm       VictoriaMetrics 客户端（export/import/最早数据探测）
internal/sender   Poller（export 窗口 + 空窗跳过 + 分块）
internal/receiver 帧处理（去重/DLQ/import 写库）
internal/monitor  Prometheus 指标
internal/config   配置加载（d 时长单位、backfill all/0/Nd）
bench/linkproxy   字节计数代理（带宽测试用）
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/plan.md](docs/plan.md) | 规划文档（架构决策/交付清单） |
| [docs/architecture.md](docs/architecture.md) | 架构与数据流、与 influx-sync 差异 |
| [docs/configuration.md](docs/configuration.md) | 参数详解 + backfill 三模式白话说明 |
| [docs/deployment.md](docs/deployment.md) | 安装/升级/防火墙/上架清单 |
| [docs/operations.md](docs/operations.md) | 日志/指标/排障 |
