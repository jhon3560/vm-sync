# vm-sync

**VictoriaMetrics 跨正向隔离装置单向同步**：安全区 I 的 VictoriaMetrics → 正向隔离装置
（TCP 映射，响应单字节）→ 安全区 III 的 VictoriaMetrics。单向、有序、At-Least-Once、
断点续传。ISFP 帧协议，v0.3.0。

数据面**零转换透传**：`/api/v1/export` 的原始样本 JSON lines 逐位保留，解压后原样
`POST /api/v1/import`——标签、毫秒时间戳不经过任何解析重建。

## 功能特性

| 能力 | 说明 |
|---|---|
| 数据面 | export→import 零转换透传；同 series+ts 覆盖写幂等（重发/重爬不重复计数） |
| 传输 | ISFP 帧（20B 头 + zstd/gzip + CRC32）；停等协议适配装置单字节 ACK；滑窗 go-back-N（实验项） |
| 可靠性 | 先 WAL 后游标铁律；分段 WAL（group commit/撕裂尾恢复/坏帧跳帧）；接收端 last_seq 连续前缀 + **内容去重窗口**（发送端 WAL 重建重导不丢）；DLQ 毒丸隔离；反压三级水位；优雅退出 |
| 历史回填 | `backfill: all`（默认全量）/ `0`（仅实时）/ `30d`（有界，d 单位）；改配置重启即生效、免清数据目录；探测失败/空库不锁死策略 |
| 实时性 | e2e ≈1.5~2.5s（500ms 轮询 + 1s 水位 + 实时区 force_flush 导出可见性保证） |
| 窗口自适应 | 欠满窗口翻倍（真空区 1h/稀疏区封顶 max_window）；单行超限窗口收缩（高密度序列不卡死）；查询预取流水线隐藏 export 延迟 |
| 监控 | Prometheus /metrics：游标/回填进度/端到端延迟/积压/重试/DLQ/反压 |
| 部署 | 独立进程（cmd/sender + cmd/receiver）或内嵌 VM 二进制（见 vm-sync-fork） |

## 性能（实测，详见 [docs/performance.md](docs/performance.md)）

| 指标 | 值 | 备注 |
|---|---|---|
| 历史回填 | ≈1.5 小时数据/分钟（1,787 样本/s） | 默认配置 20 序列×1/s 场景；调大 max_window 线性提速 |
| 链路压缩 | ≈221:1（zstd 帧） | 113.4MB JSON → 0.51MB 链路字节 |
| 实时 e2e | 1.5~2.5s（设计值） | V2 快路径（vmagent 双写）可达 0~1s |
| 零丢失 | 全量回填逐帧 ACK 全成功 | 测试套件 e2e + race 全绿 |

## 架构

```
[I 区] 源 VictoriaMetrics
        │  /api/v1/export 窗口轮询（毫秒游标；实时区先 /internal/force_flush）
        ▼
[sender] ──┬─ Poller：export [cursor, end) → 分块（行/字节双阈值）→ zstd/gzip → WAL → 游标
           ├─ 窗口自适应：欠满翻倍 / 超限收缩 / 下窗口查询预取
           └─ Sender：WAL.Peek → 停等发送 → 单字节 ACK → Commit / 重试（永不丢弃）
        │  ISFP over TCP（经隔离装置映射）
        ▼
[receiver] ──┬─ 逐帧校验 → last_seq 连续前缀 + 内容去重窗口 → 解压
             └─ 原样 POST /api/v1/import；4xx 毒丸 DLQ 隔离 + 0xff 解卡
        ▼
[III 区] 目标 VictoriaMetrics
```

详细设计见 [docs/architecture.md](docs/architecture.md)。

## 快速开始

```bash
# 构建（纯静态二进制）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver

# 测试
go test ./...            # 全量单测 + e2e
go test -race ./...      # 竞态检测
go vet ./...
```

**I 区（源侧）** `sender.yaml`：

```yaml
source:
  url: http://127.0.0.1:8428        # 源 VictoriaMetrics
sync:
  backfill: all                     # all=全量 / 0=仅实时 / 30d=有界
  interval: 500ms
  watermark: 1s
  max_window: 30s
  frame_lines: 5000
  frame_bytes: 512KB
  window_target: 2MB
  export_lag: 15s
wal:
  path: /opt/vm-sync/data/wal
tcp:
  addr: 192.172.200.131:28101       # 对端（经隔离装置映射的虚地址）
  compression: zstd
```

**III 区（目标侧）** `receiver.yaml`：

```yaml
target:
  url: http://127.0.0.1:8428        # 目标 VictoriaMetrics
tcp:
  listen: :28101
```

```bash
./bin/sender -config sender.yaml
./bin/receiver -config receiver.yaml
```

完整参数见 [docs/configuration.md](docs/configuration.md)，部署见
[docs/deployment.md](docs/deployment.md)，运维排障见 [docs/operations.md](docs/operations.md)，
性能数据见 [docs/performance.md](docs/performance.md)，下一步开发方向见
[docs/roadmap.md](docs/roadmap.md)。

## 目录

```
cmd/sender        发送端入口（I 区）
cmd/receiver      接收端入口（III 区）
internal/protocol ISFP 帧编解码（gzip/zstd）
internal/transport TCP 停等客户端 / 流水线服务端（zstd 帧 frameIdx 递增）
internal/wal      分段 WAL + backfill 回拨策略（checkpoint 记录）
internal/vm       VictoriaMetrics 客户端（export/import/二分探测/force_flush）
internal/sender   Poller（窗口自适应/预取）+ Sender（停等/滑窗）
internal/receiver 帧处理（内容去重/DLQ/import 写库）
internal/monitor  Prometheus 指标（vm_sync_*）
internal/config   配置加载（d 单位、KB/MB 单位、backfill 三模式）
bench/linkproxy   字节计数代理（带宽测试）
docs/             架构/配置/部署/运维/性能/路线图/规划/审计记录
```

## 相关项目

- **VictoriaMetrics 内嵌版**：同一套组件内嵌进 VM 二进制（`-syncIsolation.*` 开关），
  无需独立进程——基于上游 VictoriaMetrics 的 fork，仅新增 app/vm-sync + 入口
  Start/Stop，不触碰 VM 核心存储（本仓库的维护分支）。
- 可靠性骨架与 influx-sync 同源（ISFP/WAL/去重/DLQ/反压体系）。

## 许可证

[Apache License 2.0](LICENSE)
