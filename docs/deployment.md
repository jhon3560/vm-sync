# 部署手册

> v0.1.0。安装包：`vm-sync-v0.1.0-linux-amd64.tar.gz`。

## 1. 安装布局

```
/opt/vm-sync/
├── bin/
│   ├── sender            # 静态二进制（CGO_ENABLED=0，兼容麒麟 V10 glibc 2.28）
│   ├── receiver
│   └── *.bak             # 升级前备份
├── conf/
│   ├── sender.yaml
│   └── receiver.yaml
├── data/                 # WAL / last_seq / DLQ（按实例分子目录）
└── logs/                 # 日志（内置轮转 100MB×10）
```

专用系统用户：`influxsync` 同款约定（`useradd -r -M -s /sbin/nologin vmsync`），
`chown -R vmsync:vmsync /opt/vm-sync`。

## 2. 首次安装

```bash
tar xzf vm-sync-v0.1.0-linux-amd64.tar.gz -C /tmp
cd /tmp/vm-sync-v0.1.0
mkdir -p /opt/vm-sync/{bin,conf,data,logs}
install -m 0755 bin/sender bin/receiver /opt/vm-sync/bin/
# 编辑 conf/sender.example.yaml → /opt/vm-sync/conf/sender.yaml（按拓扑修改）
# 编辑 conf/receiver.example.yaml → /opt/vm-sync/conf/receiver.yaml
# systemd：cp systemd/vm-sync-*.service /etc/systemd/system/ && systemctl enable --now vm-sync-sender vm-sync-receiver
```

## 3. 升级

```bash
tar xzf vm-sync-v<新版本>-linux-amd64.tar.gz -C /tmp
cd /tmp/vm-sync-v<新版本> && ./upgrade.sh   # 自动备份旧二进制并重启服务
```

- 协议/WAL 格式向后兼容；**zstd 帧需两端同版本**（同包部署满足），滚动升级窗口期
  把 `tcp.compression` 设回 `gzip`；
- 升级默认 backfill 值 `all` **不会**触发全库重发（存量进度自动识别）。

## 4. 防火墙

```bash
# 103（sender 出站到隔离装置虚地址，通常默认放行）
# 171（ISFP 入站）
firewall-cmd --permanent --add-port=28101/tcp
firewall-cmd --reload
```

## 5. 上架检查清单

1. `nc -z <虚地址> 28101` 通；`ip route` 默认路由走生产网关；
2. 二进制溯源 `go version -m bin/sender | grep vcs` 与版本一致；
3. 启动 sender → 日志 `sender started` + `/metrics` 可查；`sync_delay_seconds` 收敛；
4. 写入测试样本 → 目标 VM 查询验证（标签/时间戳逐位一致）；
5. `sync_e2e_delay_seconds` ≈ watermark + 处理时间（~1.5~2.5s）；
6. backfill 按需配置（all/0/Nd，见 configuration.md §3）。
