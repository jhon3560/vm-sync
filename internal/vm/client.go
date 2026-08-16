// Package vm 实现 VictoriaMetrics HTTP 客户端：
//   - ExportRange：/api/v1/export 拉取 [start,end) 原始样本（JSON lines，逐位保留）
//   - ImportWrite：/api/v1/import 原样写入（与 export 格式完全对称 → 零转换透传）
//   - ProbeOldestData：export 二分探测库内最早数据时间（backfill 用）
//
// 时间戳语义：VictoriaMetrics 存储精度为毫秒，本包对外统一使用毫秒 int64；
// export/import 的 start/end 参数用秒（可带小数）表达，输出时间戳为毫秒。
package vm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config VictoriaMetrics 连接配置。
type Config struct {
	URL     string   `yaml:"url"`     // 如 http://127.0.0.1:8428
	Timeout string   `yaml:"timeout"` // HTTP 超时，如 10s
	Match   []string `yaml:"match"`   // export 过滤（Prometheus 选择器），空=全部序列
	Token   string   `yaml:"token"`   // 可选 Bearer 认证令牌
}

// Client VictoriaMetrics HTTP 客户端。
type Client struct {
	cfg     Config
	http    *http.Client
	timeout time.Duration
}

// ImportHTTPError import 写入的非 2xx 响应（typed，供 DLQ 分类）。
type ImportHTTPError struct {
	StatusCode int
	Body       string
}

func (e *ImportHTTPError) Error() string {
	return fmt.Sprintf("vm: import http %d: %s", e.StatusCode, truncate(e.Body, 512))
}

// NewClient 创建客户端。timeout 为空时默认 10s。
func NewClient(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("vm: url required")
	}
	d := 10 * time.Second
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("vm: bad timeout %q: %w", cfg.Timeout, err)
		}
		d = parsed
	}
	if len(cfg.Match) == 0 {
		cfg.Match = []string{`{__name__=~".+"}`} // export 端点必须带 match[]；匹配所有具名序列
	}
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: 15 * time.Minute, // 兜底；具体请求按 ctx 截止
		},
		timeout: d,
	}, nil
}

// withTimeout R1：让 source.timeout 真正生效——调用方 ctx 无 deadline 时套用
// c.timeout；已有 deadline（如 receiver 按批大小动态超时）则尊重调用方，避免
// 大 batch 被配置值 10s 截断（influx-sync do() 同款语义）。
func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.timeout)
}

// secFloat 将毫秒时间戳格式化为秒（6 位小数，保留毫秒精度）。
func secFloat(ms int64) string {
	return strconv.FormatFloat(float64(ms)/1000, 'f', 6, 64)
}

// maxExportRespBytes 单次 export 响应上限（N15：静默截断会产出半截 JSON lines，
// 落库后静默丢尾部数据——必须显式报错并让上层缩小窗口重试）。var 以便测试注入。
var maxExportRespBytes = 512 << 20

// ExportRange 拉取 [start, end)（毫秒）内全部原始样本，返回 JSON lines 原文
// （格式与 /api/v1/import 完全对称，可原样写入目标）。上限 512MB 防异常，
// 超限显式报错（N15，提示调低 max_window）。
func (c *Client) ExportRange(ctx context.Context, start, end int64) ([]byte, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	q := url.Values{}
	for _, m := range c.cfg.Match {
		q.Add("match[]", m)
	}
	q.Set("start", secFloat(start))
	q.Set("end", secFloat(end))
	u := fmt.Sprintf("%s/api/v1/export?%s", c.cfg.URL, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("vm: build export: %w", err)
	}
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vm: export [%d,%d): %w", start, end, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxExportRespBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("vm: read export: %w", err)
	}
	if len(body) > maxExportRespBytes {
		return nil, fmt.Errorf("vm: export response exceeds %dMB for %.1fs window (data too dense; reduce max_window or window, or split the source match)",
			maxExportRespBytes>>20, float64(end-start)/1000)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("vm: export http %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	return body, nil
}

// ExportHasData 探测 [start, end)（毫秒）内是否有任何样本（读到首行即返回）。
func (c *Client) ExportHasData(ctx context.Context, start, end int64) (bool, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	q := url.Values{}
	for _, m := range c.cfg.Match {
		q.Add("match[]", m)
	}
	q.Set("start", secFloat(start))
	q.Set("end", secFloat(end))
	u := fmt.Sprintf("%s/api/v1/export?%s", c.cfg.URL, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, fmt.Errorf("vm: build export: %w", err)
	}
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("vm: export probe: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("vm: export http %d: %s", resp.StatusCode, truncate(string(body), 512))
	}
	var buf [1]byte
	if _, err := io.ReadFull(resp.Body, buf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil // 空窗口
		}
		return false, fmt.Errorf("vm: export probe read: %w", err)
	}
	return true, nil
}

// ProbeOldestData 二分探测库内最早数据时间（毫秒）；库为空返回 0。
//
// 谓词 P(X) = "区间 [0, X) 内存在任何样本"——X ≤ 最早数据时恒假、X > 最早数据时恒真，
// 在"最早数据"处单调翻转。直接在 [0, now] 上二分（约 41 次探测），无需指数扩展。
func (c *Client) ProbeOldestData(ctx context.Context) (int64, error) {
	now := time.Now().UnixMilli()
	has, err := c.ExportHasData(ctx, 0, now)
	if err != nil {
		return 0, err
	}
	if !has {
		return 0, nil // 空库
	}
	lo, hi := int64(0), now
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		has, err := c.ExportHasData(ctx, 0, mid)
		if err != nil {
			return 0, err
		}
		if has {
			hi = mid // [0,mid) 已有数据 → 最早数据 ≤ mid
		} else {
			lo = mid
		}
	}
	// 收敛时 hi=lo+1：lo 为"最后一个无数据上界"，即最早数据时间本身
	// （返回 hi 会偏大 1ms，使游标窗口把最早样本排除在外）
	return lo, nil
}

// ImportWrite 将 export 格式的 JSON lines 原样写入 /api/v1/import。
// 失败返回 *ImportHTTPError（4xx 毒丸 / 5xx 可重试由调用方分类）。
func (c *Client) ImportWrite(ctx context.Context, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	u := fmt.Sprintf("%s/api/v1/import", c.cfg.URL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("vm: build import: %w", err)
	}
	req.Header.Set("Content-Type", "application/stream+json")
	c.setAuth(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("vm: import %d bytes: %w", len(raw), err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &ImportHTTPError{StatusCode: resp.StatusCode, Body: string(msg)}
	}
	return nil
}

// LastSampleTimestamp 返回 JSON lines 中最后一个样本的时间戳（毫秒，A5 e2e 延迟指标用）。
// 每行形如 {"metric":{...},"values":[...],"timestamps":[t1,t2,...]}——取全部行的最大尾时间戳。
func LastSampleTimestamp(raw []byte) int64 {
	var maxTs int64
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		if ts := lastTsInLine(line); ts > maxTs {
			maxTs = ts
		}
	}
	return maxTs
}

// lastTsInLine 提取单行 JSON 中 timestamps 数组的最后一个元素（毫秒）。
// 解析策略：定位最后一个 `"timestamps":[`，从其后扫描数字直到 `]`，取最后一个数。
func lastTsInLine(line []byte) int64 {
	idx := bytes.LastIndex(line, []byte(`"timestamps":[`))
	if idx < 0 {
		return 0
	}
	rest := line[idx+len(`"timestamps":[`):]
	end := bytes.IndexByte(rest, ']')
	if end < 0 {
		return 0
	}
	rest = rest[:end]
	// 最后一个逗号分隔的数字
	last := bytes.LastIndexByte(rest, ',')
	var tok []byte
	if last >= 0 {
		tok = rest[last+1:]
	} else {
		tok = rest
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(string(tok)), 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

func (c *Client) setAuth(req *http.Request) {
	if c.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
