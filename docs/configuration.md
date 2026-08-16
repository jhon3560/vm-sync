# 参数配置详解

## 1. Sender 配置（cmd/sender，-config 指定）

```yaml
source:
  url: http://192.172.12.174:8428   # 源 VictoriaMetrics（必填）
  timeout: 10s                      # HTTP 超时
  match: ['{__name__=~".+"}']       # export 过滤（Prometheus 选择器，空=全部序列）
  token: ""                         # 可选 Bearer 认证

sync:
  interval: 500ms    # 轮询周期（实时性关键：VM 无订阅推送，靠快轮询）
  window: 5s         # 查询窗口大小
  watermark: 1s      # 水位延迟：窗口终点 = now-watermark
                     # VM 写入即查询可见，水位仅防时钟抖动，默认 1s（e2e ≈1.5~2.5s）
  max_window: 30s    # 单轮窗口上限（防时间跳变/积压）
  frame_lines: 5000  # 每帧最多 export 行数（每行可含多样本）
  frame_bytes: 512KB # 每帧压缩前字节上限（超限自动分块，单行不拆分）
  window_target: 2MB    # V0.2 窗口增长目标字节数（默认=frame_bytes×4）：欠满判定阈值，
                        # 按 export 响应字节数判定（行数在高样本率少序列库会误判稀疏→窗口震荡），
                        # 与帧大小解耦——稀疏库窗口仍按大数据量目标增长，不被帧行数锁死
  backfill: all      # 回填：all=全量(默认) / 0=仅实时 / 30d=有界（d 单位，1d=24h）

wal:
  path: /opt/vm-sync/data/wal   # WAL 目录（必填；勿与其他实例共用）
  segment_size: 64MB

tcp:
  addr: 192.172.200.131:28101  # receiver 地址（必填；经隔离装置映射的虚地址）
  timeout: 10s
  dial_timeout: 10s
  compression: zstd     # zstd（默认，链路带宽约为 gzip 的 1/2~1/3）/ gzip（兼容旧对端）
                        # zstd 需两端同版本（同一安装包满足）；混合版本升级期设 gzip

sender:
  max_retry: 10         # 连续失败告警阈值（不丢弃）
  backoff_base: 1s
  backoff_max: 60s
  heartbeat_interval: 30s
  pipeline_window: 1    # 滑窗实验项：默认 1=停等；>1 需确认装置支持

monitor:
  addr: :28080          # /metrics 端口
  username: ""
  password: ""

log:
  level: info
  file: /opt/vm-sync/logs/sender.log
  max_mb: 100
  max_backups: 10
```

## 2. Receiver 配置（cmd/receiver）

```yaml
target:
  url: http://127.0.0.1:8428   # 目标 VictoriaMetrics（必填；本机）
  timeout: 30s
  token: ""

tcp:
  listen: :28101                # 监听地址（必填）
  read_timeout: 60s             # > sender 心跳间隔 30s
  max_inflight: 8               # 并发写库流水线窗口
  max_conns: 0                  # 最大连接数（0=不限制）

dedup:
  last_seq_file: /opt/vm-sync/data/last_seq

dlq:
  dir: /opt/vm-sync/data/dlq    # 毒丸死信目录（空=禁用 DLQ，退回重试）

monitor:  # 同 sender
  addr: :28080
log:      # 同 sender
  file: /opt/vm-sync/logs/receiver.log
```

## 3. backfill 三种模式（白话说明）

| 配置 | 含义 | 典型用法 |
|---|---|---|
| `backfill: all`（默认） | 从库内**最早的数据**开始全部同步，直到追平实时 | 新装，想把整个库搬过去 |
| `backfill: 30d` | 只补**最近 30 天**（`d` 单位，可写 `12h`/`1d12h`） | 只要最近一段时间 |
| `backfill: 0` | 只同步"现在"往后的新数据 | 纯实时，不碰历史 |

**记住三条**：

1. **改配置 + 重启即生效**：`0 → 30d` 就补 30 天；`30d → 0` 就停在新进度继续追；
   **不需要清任何数据目录**。
2. **只在"值变化"时回拨一次游标**：配置不变重启绝不重发历史；想再补一次，把值改一下
   （如 `30d → 0` 重启、再改回 `30d` 重启）即可重新触发。
3. **库内数据比回拨边界晚也不怕**：启动时探测库内最早数据（export 二分，秒级），
   真空区直接跳过——"库里只有 10 天数据、配了 30d/all"就从 10 天前开始搬，不空爬。

**升级提醒**：已在运行的旧部署升级后，默认 `all` **不会**触发全库重发（旧进度被识别为
"存量部署"，只记录配置值、游标不动）；想补历史，按第 1 条改配置即可。

**回填期间的目标库**：新旧数据并存——`sync_e2e_delay_seconds` 显示 ~1~2s（实时新数据
已到），但总条数在慢慢涨；回填进度看 `sync_delay_seconds`。

## 4. 参数选择建议

- **实时性**：watermark=1s + interval=500ms（e2e ≈1.5~2.5s）；VM 写入即查询可见，
  watermark 可更低（谨慎：跨机时钟抖动）；要亚秒级暂无机制（VM 无订阅推送）。
- **吞吐**：frame_lines/frame_bytes 大→帧少、停等 RTT 摊销好（单帧压缩后 ≤1MB 协议上限，
  超限自动分块不卡死）；吞吐 = 帧样本数 / RTT。
- **压缩**：zstd 默认；**字典训练不需要**（JSON lines 规整，帧内自学习已收敛）。
- **回填大库**：先 `30d` 摸底再 `all`；回填时长 = 数据量 ÷ 链路吞吐。
