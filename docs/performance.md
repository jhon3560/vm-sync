# vm-sync 性能实测与调优

> 实测环境：WSL2 单机（笔记本），同机双 VictoriaMetrics 实例（内嵌发送/接收端），
> 链路经 `bench/linkproxy` 字节计数代理（localhost 回环，不代表真机隔离装置）。
> 真机部署时链路 RTT 与装置吞吐是实际瓶颈，本数据用于验证软件自身速率上限。

## 1. 历史回填吞吐（实测）

**场景**：20 条序列 × 1 样本/s × 14 小时 = 1,008,000 样本（113.4MB JSON），
`backfill: all` 全量回填，默认配置（`interval: 500ms`、`max_window: 30s`、zstd 帧压缩）。

| 指标 | 实测值 |
|---|---|
| 总样本 | 1,008,000（14h 数据跨度） |
| 回填耗时 | 564s（≈9.4 分钟） |
| 吞吐 | ≈1,787 样本/s |
| 数据速度 | ≈1.5 小时数据/分钟（≈0.06 天/分钟） |
| 链路行为 | 1,360+ 帧逐帧 ACK 全部成功，接收端逐帧写库 |

### 速率模型（为什么是这个数）

回填每轮推进一个窗口的数据（间隔 = `interval`），因此：

```
回填速度(数据秒/墙钟秒) ≈ min( max_window / interval, 数据密度上限 )
```

默认 `30s / 0.5s = 60×` → 理论上限 ≈1 小时数据/分钟；实测 ≈1.5 小时/分钟
（预取流水线隐藏查询延迟后略高于简单模型）。**调大 `max_window` 可线性提速**
（如 5min → 上限 ≈10 小时数据/分钟），代价是单轮 export 响应体积增大
（512MB 上限保护 + 超限显式报错，见 N15）；`max_window` 建议按
"窗口内数据量 ≈ 数百 MB"反推。

## 2. 链路压缩（实测）

| 项 | 值 |
|---|---|
| export JSON 原文 | 113.4MB |
| 链路正向字节（zstd 帧） | ≈0.51MB |
| 有效压缩比 | **≈221:1**（JSON 冗余 + zstd 叠加） |
| 帧级 zstd | ≈33:1（12.3KB JSON/30s 窗 → ≈375B/帧） |

低基数遥测数据的标签/时间戳高度规律，zstd 收益显著；带宽受限的真机隔离装置上
这是回填速度的决定因素之一。

## 3. 实时 e2e 延迟（设计值）

- 设计值 **1.5~2.5s**：`interval 500ms` + `watermark 1s` + 实时区
  `/internal/force_flush`（V0.3/R24，使 pending rows/新序列立即可见）。
- 背景实测（VM 本体可见性）：pending rows 旧序列 ≈3.3~3.9s 可见；**新序列名**要等
  索引 flushCallback 10s 节拍（最坏 ≈10.5s）——不 flush 直接收口会永久漏发，故
  实时区一律先 flush（失败才回退 `export_lag` 余量）。
- 快路径（V2 计划，vmagent 双写 remoteWrite）：e2e 0~1s。

## 4. 可靠性验证

- 测试套件：全量 `go test ./...` + `go test -race ./...` 全绿（零丢失回填、标签/
  时间戳保真、断连补传、重启续传去重、回拨重爬幂等、毒丸 DLQ 解卡、滑窗
  go-back-N 判别力等）；
- 每项修复带回归测试并做过"revert 修复→测试必失败"突变验证（见
  docs/audit-v020-*.md 审计记录）。

## 5. 调优速查

| 目标 | 参数 | 建议 |
|---|---|---|
| 加快回填 | `sync.max_window` | 调大（线性提速）；按窗口数据量 < 512MB 反推上限 |
| 回填起步更快 | `sync.window` | 基础窗口调大（稀疏区翻倍封顶 max_window） |
| 降低实时延迟 | `sync.interval` / `sync.watermark` | 调小（下限受源 VM export 延迟约束） |
| 带宽受限链路 | `tcp.compression` | 保持 zstd；gzip 仅混合版本升级期 |
| 高样本率少序列库 | `sync.window_target` | 字节阈值已防误判；极密库按需调大 |
| 远端源（无 force_flush 权限） | `sync.export_lag` | 只允许调大（默认 15s，覆盖新序列 ≈10.5s 可见延迟） |
| 帧大小 | `sync.frame_lines` / `sync.frame_bytes` | 大帧提吞吐、增内存；隔离装置单帧上限需实测 |

## 6. 测试复现

```bash
# 回填吞吐（需真实 VictoriaMetrics；本仓库提供 bench/linkproxy 计数代理）
# 1) 源 VM 导入 N 样本；2) 发送端经 linkproxy 连接收端；3) 观察 sync_delay_seconds
#    归零耗时与 linkproxy_tx_bytes_total。
go build -o /tmp/linkproxy ./bench/linkproxy
```
