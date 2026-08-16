// Package config 加载 YAML 配置并支持环境变量覆盖（VMSYNC_ 前缀）。
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"vm-sync/internal/monitor"
	"vm-sync/internal/protocol"
	"vm-sync/internal/receiver"
	"vm-sync/internal/sender"
	"vm-sync/internal/transport"
	"vm-sync/internal/vm"
	"vm-sync/internal/wal"
)

// LogConfig 日志配置。
type LogConfig struct {
	Level      string `yaml:"level"` // debug/info/warn/error
	File       string `yaml:"file"`  // 日志文件（空=stderr）
	MaxMB      int    `yaml:"max_mb"`
	MaxBackups int    `yaml:"max_backups"`
}

// MonitorConfig 指标服务。
type MonitorConfig struct {
	Addr     string `yaml:"addr"`     // 如 :8080
	Username string `yaml:"username"` // 监控端口 Basic Auth 用户名（空=不启用认证）
	Password string `yaml:"password"`
}

// Auth 返回监控端口认证配置（nil=不启用）。
func (c *MonitorConfig) Auth() *monitor.Auth {
	if c.Username == "" {
		return nil
	}
	return &monitor.Auth{Username: c.Username, Password: c.Password}
}

// SenderConfig Sender 完整配置。
type SenderConfig struct {
	Source vm.Config `yaml:"source"`
	Sync   struct {
		Interval   string `yaml:"interval"`    // 轮询周期（默认 500ms）
		Window     string `yaml:"window"`      // 查询窗口（默认 5s）
		Watermark  string `yaml:"watermark"`   // 水位延迟（默认 1s；VM 写入即查询可见，仅防时钟抖动）
		MaxWindow  string `yaml:"max_window"`  // 单轮窗口上限（防时间跳变，默认 30s）
		FrameLines int    `yaml:"frame_lines"` // 每帧最多 export 行数（每行可含多样本），默认 5000
		FrameBytes int    `yaml:"frame_bytes"` // 每帧压缩前字节上限，默认 512KB
		// WindowTarget 窗口增长目标字节数（V0.2/N14+R2，默认=frame_bytes×4）：欠满判定
		// 阈值，按 export 响应字节数判定——行数在高样本率少序列库会误判稀疏导致
		// 窗口震荡，字节数与帧大小解耦且直接对应导出开销（N15 内存上限同源）。
		WindowTarget int    `yaml:"window_target"`
		Backfill     string `yaml:"backfill"` // 回填：all=全量(默认) / 0=仅实时 / 30d=有界（d=天）
	} `yaml:"sync"`
	WAL struct {
		Path        string `yaml:"path"`
		SegmentSize string `yaml:"segment_size"` // 如 64MB
	} `yaml:"wal"`
	TCP struct {
		Addr        string `yaml:"addr"`
		Timeout     string `yaml:"timeout"`
		DialTimeout string `yaml:"dial_timeout"`
		Compression string `yaml:"compression"` // 帧压缩算法：zstd（默认，V1.6）/ gzip（兼容旧接收端）
	} `yaml:"tcp"`
	Sender struct {
		MaxRetry          int    `yaml:"max_retry"`
		BackoffBase       string `yaml:"backoff_base"`
		BackoffMax        string `yaml:"backoff_max"`
		HeartbeatInterval string `yaml:"heartbeat_interval"`
		Pipeline          int    `yaml:"pipeline_window"` // A1 滑窗（实验项）：默认 1=停等；>1 需确认装置支持
	} `yaml:"sender"`
	Monitor MonitorConfig `yaml:"monitor"`
	Log     LogConfig     `yaml:"log"`
}

// ReceiverConfig Receiver 完整配置。
type ReceiverConfig struct {
	Target vm.Config `yaml:"target"`
	TCP    struct {
		Listen      string `yaml:"listen"`
		ReadTimeout string `yaml:"read_timeout"`
		MaxInflight int    `yaml:"max_inflight"` // A2 并发写库流水线窗口（默认 8）
		MaxConns    int    `yaml:"max_conns"`    // 最大并发连接（0=不限制）
	} `yaml:"tcp"`
	Dedup struct {
		Cap         int    `yaml:"cap"`
		LastSeqFile string `yaml:"last_seq_file"`
	} `yaml:"dedup"`
	DLQ struct {
		Dir string `yaml:"dir"` // 毒丸死信目录（空=禁用 DLQ，退回 0x00 重试）
	} `yaml:"dlq"`
	Relay struct {
		Addr    string `yaml:"addr"`    // 中继目标地址（如 "192.168.137.x:28103"）；空=不启用中继
		WALDir  string `yaml:"wal_dir"` // 转发 WAL 目录（重启不丢，必须配置）
		Timeout string `yaml:"timeout"` // 转发读写超时
		DLQDir  string `yaml:"dlq_dir"` // 转发失败转存目录（C2）；空=默认 <wal_dir>/../relay_dlq
	} `yaml:"relay"`
	RelayWAL *wal.WAL      `yaml:"-"` // 转发 WAL 句柄（程序内注入，非配置文件）
	Monitor  MonitorConfig `yaml:"monitor"`
	Log      LogConfig     `yaml:"log"`
}

// LoadSender 加载 Sender 配置并应用环境变量覆盖。
func LoadSender(path string) (*SenderConfig, error) {
	cfg := &SenderConfig{}
	if err := loadFile(path, cfg); err != nil {
		return nil, err
	}
	applySenderEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadReceiver 加载 Receiver 配置并应用环境变量覆盖。
func LoadReceiver(path string) (*ReceiverConfig, error) {
	cfg := &ReceiverConfig{}
	if err := loadFile(path, cfg); err != nil {
		return nil, err
	}
	applyReceiverEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadFile(path string, out interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("config: parse %s: %w", path, err)
	}
	return nil
}

// applySenderEnv 环境变量覆盖（VMSYNC_ 前缀）。
func applySenderEnv(c *SenderConfig) {
	if v := os.Getenv("VMSYNC_SOURCE_URL"); v != "" {
		c.Source.URL = v
	}
	if v := os.Getenv("VMSYNC_TCP_ADDR"); v != "" {
		c.TCP.Addr = v
	}
	if v := os.Getenv("VMSYNC_WAL_PATH"); v != "" {
		c.WAL.Path = v
	}
	if v := os.Getenv("VMSYNC_MONITOR_ADDR"); v != "" {
		c.Monitor.Addr = v
	}
	if v := os.Getenv("VMSYNC_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// applyReceiverEnv 环境变量覆盖。
func applyReceiverEnv(c *ReceiverConfig) {
	if v := os.Getenv("VMSYNC_TARGET_URL"); v != "" {
		c.Target.URL = v
	}
	if v := os.Getenv("VMSYNC_TCP_LISTEN"); v != "" {
		c.TCP.Listen = v
	}
	if v := os.Getenv("VMSYNC_MONITOR_ADDR"); v != "" {
		c.Monitor.Addr = v
	}
	if v := os.Getenv("VMSYNC_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// Validate 校验必填项与取值范围（dur 解析失败/负值不再静默回退默认值）。
func (c *SenderConfig) Validate() error {
	if c.Source.URL == "" {
		return fmt.Errorf("config: source.url required")
	}
	if c.TCP.Addr == "" {
		return fmt.Errorf("config: tcp.addr required")
	}
	if c.WAL.Path == "" {
		return fmt.Errorf("config: wal.path required")
	}
	for name, s := range map[string]string{
		"sync.interval":             c.Sync.Interval,
		"sync.window":               c.Sync.Window,
		"sync.watermark":            c.Sync.Watermark,
		"sync.max_window":           c.Sync.MaxWindow,
		"tcp.timeout":               c.TCP.Timeout,
		"tcp.dial_timeout":          c.TCP.DialTimeout,
		"sender.backoff_base":       c.Sender.BackoffBase,
		"sender.backoff_max":        c.Sender.BackoffMax,
		"sender.heartbeat_interval": c.Sender.HeartbeatInterval,
	} {
		if err := validateDur(name, s); err != nil {
			return err
		}
	}
	if c.Sender.Pipeline < 0 {
		return fmt.Errorf("config: sender.pipeline_window must be >= 0")
	}
	if c.Sync.WindowTarget < 0 {
		return fmt.Errorf("config: sync.window_target must be >= 0")
	}
	if cp := c.TCP.Compression; cp != "" && cp != "zstd" && cp != "gzip" {
		return fmt.Errorf("config: tcp.compression must be zstd/gzip, got %q", cp)
	}
	if b := strings.TrimSpace(c.Sync.Backfill); b != "" && b != "all" && b != "0" {
		d, err := parseDurationExt(b)
		if err != nil {
			return fmt.Errorf("config: sync.backfill must be all/0/时长(如 30d), got %q", b)
		}
		if d < 0 {
			return fmt.Errorf("config: sync.backfill: negative duration %q not allowed", b)
		}
	}
	return nil
}

// CompressionFrameType 返回数据帧类型（= 压缩算法标识）。
// 默认 zstd（TypeDataZstd，V1.6）；配置 gzip 兼容旧接收端（混合版本升级期使用）。
func (c *SenderConfig) CompressionFrameType() uint8 {
	if c.TCP.Compression == "gzip" {
		return protocol.TypeData
	}
	return protocol.TypeDataZstd
}

// validateDur 校验时长配置：空合法（用默认值），非空必须可解析且非负。
// 支持扩展单位 d（天）=24h（V1.7）。
func validateDur(name, s string) error {
	if s == "" {
		return nil
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return fmt.Errorf("config: %s: bad duration %q: %w", name, s, err)
	}
	if d < 0 {
		return fmt.Errorf("config: %s: negative duration %q not allowed", name, s)
	}
	return nil
}

// Validate 校验必填项与取值范围。
func (c *ReceiverConfig) Validate() error {
	if c.Target.URL == "" {
		return fmt.Errorf("config: target.url required")
	}
	if c.TCP.Listen == "" {
		return fmt.Errorf("config: tcp.listen required")
	}
	if c.TCP.MaxInflight < 0 {
		return fmt.Errorf("config: tcp.max_inflight must be >= 0")
	}
	if c.TCP.MaxConns < 0 {
		return fmt.Errorf("config: tcp.max_conns must be >= 0")
	}
	for name, s := range map[string]string{
		"tcp.read_timeout": c.TCP.ReadTimeout,
		"relay.timeout":    c.Relay.Timeout,
	} {
		if err := validateDur(name, s); err != nil {
			return err
		}
	}
	return nil
}

// --- 转换为各模块配置 ---

func dur(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := parseDurationExt(s)
	if err != nil {
		return def
	}
	return d
}

// parseDurationExt 解析扩展时长：在 time.ParseDuration 基础上支持 d（天）=24h。
// 例：30d、1d12h、0.5d、12h30m。负值允许解析（由 validateDur 拒绝）。
func parseDurationExt(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	var sb strings.Builder
	i := 0
	for i < len(s) {
		j := i
		for j < len(s) && (s[j] == '-' || s[j] == '+' || s[j] == '.' || (s[j] >= '0' && s[j] <= '9')) {
			j++
		}
		if j == i {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		num := s[i:j]
		k := j
		for k < len(s) && !(s[k] == '-' || s[k] == '+' || s[k] == '.' || (s[k] >= '0' && s[k] <= '9')) {
			k++
		}
		unit := s[j:k]
		if unit == "d" {
			f, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid day value %q in %q", num, s)
			}
			sb.WriteString(strconv.FormatFloat(f*24, 'f', -1, 64))
			sb.WriteString("h")
		} else {
			sb.WriteString(num)
			sb.WriteString(unit)
		}
		i = k
	}
	return time.ParseDuration(sb.String())
}

// PollerConfig 转换。
func (c *SenderConfig) PollerConfig() sender.PollerConfig {
	return sender.PollerConfig{
		Interval:     dur(c.Sync.Interval, 500*time.Millisecond),
		Window:       dur(c.Sync.Window, 5*time.Second),
		Watermark:    dur(c.Sync.Watermark, time.Second),
		MaxWindow:    dur(c.Sync.MaxWindow, 30*time.Second),
		FrameLines:   c.Sync.FrameLines,
		FrameBytes:   c.Sync.FrameBytes,
		WindowTarget: c.Sync.WindowTarget,
		Compression:  c.CompressionFrameType(),
	}
}

// WatermarkDuration 返回水位延迟。
// R7：默认 1s——与 PollerConfig 的 watermark 默认一致（曾为 10s，backfill=0
// 首次游标多爬 9s，与"水位 1s"语义冲突；多爬不丢数据仅语义不符）。
func (c *SenderConfig) WatermarkDuration() time.Duration {
	return dur(c.Sync.Watermark, time.Second)
}

// FastPathConfig 转换（A4 快路径）。
// BackfillDuration 返回首次启动回填时长。
// BackfillMode 回填模式（V1.7）。
type BackfillMode int

const (
	BackfillNone     BackfillMode = iota // 0：仅实时
	BackfillAll                          // all：全量同步（默认）
	BackfillDuration                     // Nd：有界回填
)

// BackfillSpec 解析后的回填配置。
type BackfillSpec struct {
	Mode BackfillMode
	Dur  time.Duration // 仅 BackfillDuration 有效
}

// BackfillSpec 解析 sync.backfill：默认（空）与 "all" 均按全量处理；"0" 为仅实时；
// 其余为时长（支持 d）。解析失败回退全量（Validate 已先行拦截）。
func (c *SenderConfig) BackfillSpec() BackfillSpec {
	v := strings.TrimSpace(c.Sync.Backfill)
	if v == "" || v == "all" {
		return BackfillSpec{Mode: BackfillAll}
	}
	if v == "0" || v == "0s" {
		return BackfillSpec{Mode: BackfillNone}
	}
	d, err := parseDurationExt(v)
	if err != nil || d <= 0 {
		return BackfillSpec{Mode: BackfillAll}
	}
	return BackfillSpec{Mode: BackfillDuration, Dur: d}
}

// SenderConfig 转换（发送循环）。
func (c *SenderConfig) SenderLoopConfig() sender.SenderConfig {
	return sender.SenderConfig{
		MaxRetry:          c.Sender.MaxRetry,
		BackoffBase:       dur(c.Sender.BackoffBase, time.Second),
		BackoffMax:        dur(c.Sender.BackoffMax, 60*time.Second),
		HeartbeatInterval: dur(c.Sender.HeartbeatInterval, 30*time.Second),
		Pipeline:          c.Sender.Pipeline,
	}
}

// ClientConfig 转换（TCP 客户端）。
func (c *SenderConfig) ClientConfig() transport.ClientConfig {
	return transport.ClientConfig{
		Addr:        c.TCP.Addr,
		Timeout:     dur(c.TCP.Timeout, 10*time.Second),
		DialTimeout: dur(c.TCP.DialTimeout, 10*time.Second),
	}
}

// SegmentSize 返回 WAL 段大小字节。
func (c *SenderConfig) SegmentSize() int64 {
	if c.WAL.SegmentSize == "" {
		return wal.DefaultSegmentSize
	}
	d, err := parseSize(c.WAL.SegmentSize)
	if err != nil {
		return wal.DefaultSegmentSize
	}
	return d
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	n, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return 0, err
	}
	switch strings.ToUpper(strings.TrimSpace(s[i:])) {
	case "", "B":
		return n, nil
	case "KB":
		return n * 1024, nil
	case "MB":
		return n * 1024 * 1024, nil
	case "GB":
		return n * 1024 * 1024 * 1024, nil
	}
	return 0, fmt.Errorf("unknown unit %q", s[i:])
}

// ServerConfig 转换（TCP 服务器）。
func (c *ReceiverConfig) ServerConfig() transport.ServerConfig {
	return transport.ServerConfig{
		Listen:      c.TCP.Listen,
		ReadTimeout: dur(c.TCP.ReadTimeout, 60*time.Second),
		MaxInflight: c.TCP.MaxInflight,
		MaxConns:    c.TCP.MaxConns,
	}
}

// ReceiverConfig 转换（Receiver 模块）。
// N2：last_seq 按序推进（seqTracker）在 Receiver 内恒开，与 TCP MaxInflight
// 的实际生效值无关——默认配置（max_inflight 空→服务端按 8 生效）不再出现
// "流水线服务端 + 非按序推进"的危险错配。
func (c *ReceiverConfig) ReceiverConfig() receiver.Config {
	return receiver.Config{
		LastSeqFile: c.Dedup.LastSeqFile,
		DLQDir:      c.DLQ.Dir,
		RelayWAL:    c.RelayWAL, // 由 main 注入（需要 wal 包），nil=不启用中继
		RelayDLQDir: c.Relay.DLQDir,
	}
}

// RelayTimeout 返回中继转发超时（C5：配置文件解析后真正生效）。
func (c *ReceiverConfig) RelayTimeout() time.Duration {
	return dur(c.Relay.Timeout, 10*time.Second)
}
