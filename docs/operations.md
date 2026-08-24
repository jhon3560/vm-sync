# 运维手册

## 1. 日志关键行

| 日志 | 含义 |
|---|---|
| `poll window start=… end=… frames=N` | 每轮 export 查询与分块 |
| `export failed, keep cursor` | 源查询失败（保持游标，下轮重试；V0.2 起失败同时复位窗口增长，下轮回基础窗口自愈） |
| `source force_flush failed; falling back to conservative export lag`（Warn，最多 1 条/分钟） | 实时区强制可见性 flush 失败（远端源/未授权）→ 回退到 export_lag 保守余量收口，实时延迟变大但数据不漏 |
| `prefetch discarded (cursor mismatch), re-export`（Debug） | N16 预取槽与当前游标不符（上一轮失败未推进）→ 丢弃同步重查，正常防御路径非错误 |
| `ack success seq=N` / `retry frame seq=N` | 发送进度/重试 |
| `frame written seq=N` | 落库成功 |
| `poison packet isolated to DLQ` | import 4xx 毒丸隔离（含 http_status） |
| `backpressure: poller paused/degraded` | 反压触发 |
| `backfill policy changed: cursor rewound` | 配置变化回拨（一次） |

## 2. 监控指标（/metrics，Prometheus 格式）

| 指标 | 含义 |
|---|---|
| `sync_delay_seconds` | now - 游标（**回填进度看它**：持续收敛=回填推进中） |
| `sync_e2e_delay_seconds` | now - 目标库最后写入样本时间（实时新鲜度） |
| `wal_pending` / `wal_size_bytes` | 积压帧数/字节（持续增长=对端断连或处理瓶颈） |
| `send_total` / `ack_success` / `ack_fail` / `retry_total` | 链路健康 |
| `dlq_total` / `vm_sync_poison_packet_count` | 毒丸隔离计数（非零需查 DLQ 文件） |
| `write_ok` / `write_fail` / `dup_total` / `recv_inflight` | 接收端落库与去重 |
| `vm_sync_wal_disk_usage_ratio` / `vm_sync_backpressure_status` | 反压依据 |

## 3. 排障

| 现象 | 排查 |
|---|---|
| 目标库无新数据 | ①源库有无新写入 ②sender 日志 poll/ack ③wal_pending 是否增长 ④receiver 日志/进程 ⑤防火墙/虚地址连通 |
| wal_pending 持续增长 | 对端不可达（receiver 停机/隔离装置/网络）→ 恢复后自动追平 |
| sync_delay_seconds 持续大 | ①回填进行中（正常，等收敛）②写入量 > 链路能力 ③源库 export 慢 |
| 毒丸计数增长 | 看 receiver 日志 http_status + DLQ 文件（4xx=目标库拒收，查目标 VM 配置/权限） |
| 重启后重复数据 | 目标 VM 同 series+ts 覆盖（last-wins），计数不重复；无需处理 |
| 重启后链路不动 | 确认 last_seq 与 WAL 完好（数据目录勿删）；seq 跳跃会接受处理，不死锁 |

## 4. 备份与恢复

- **备份**：conf/*.yaml、data/（WAL+last_seq+DLQ，随服务停机快照）；
  VM 数据由 VM 自身备份工具负责（vmbackup）；
- **恢复**：停服务 → 还原 conf 与 data → 起服务；WAL 未确认帧自动重发追平。

## 5. 例行检查

- 每日：/metrics 扫一遍（delay/pending/retry/dlq）；
- 每周：磁盘（/opt/vm-sync/data）、日志轮转；
- 每月：DLQ 目录清理（分析后）、重启演练（验证断点续传）。
