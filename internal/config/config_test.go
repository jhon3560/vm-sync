package config

import (
	"os"
	"strings"
	"testing"
)

func validSender() *SenderConfig {
	c := &SenderConfig{}
	c.Source.URL = "http://127.0.0.1:8428"
	c.TCP.Addr = "1.2.3.4:28101"
	c.WAL.Path = "/tmp/w"
	return c
}

// TestBackfillAllValidates 回归（influx-sync V1.7.4 同款缺陷）：sync.backfill
// 曾被误入通用 duration 校验表，文档默认值 "all" 无法通过 Validate——
// 专门处理 all/0 的代码成死代码（"0" 碰巧能解析为 0s 才幸免）。
func TestBackfillAllValidates(t *testing.T) {
	for _, v := range []string{"all", "0", "30d", "1d12h", ""} {
		c := validSender()
		c.Sync.Backfill = v
		if err := c.Validate(); err != nil {
			t.Fatalf("backfill %q must pass Validate, got %v", v, err)
		}
	}
	for _, v := range []string{"-5d", "xyz"} {
		c := validSender()
		c.Sync.Backfill = v
		if err := c.Validate(); err == nil {
			t.Fatalf("backfill %q must be rejected", v)
		}
	}
}

// TestWindowTargetValidates N14 配置：window_target 负值拒绝。
func TestWindowTargetValidates(t *testing.T) {
	c := validSender()
	c.Sync.WindowTarget = -1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "window_target") {
		t.Fatalf("negative window_target must be rejected, got %v", err)
	}
}

// TestByteSizeYAML 文档示例 "512KB"/"2MB" 写法必须可解析（修复前 int 字段
// 直接 Unmarshal 报 cannot unmarshal !!str）。
func TestByteSizeYAML(t *testing.T) {
	cases := map[string]int{
		"512KB":    512 << 10,
		"2MB":      2 << 20,
		"1gb":      1 << 30,
		"524288":   524288,
		"1024B":    1024,
		"  64 kb ": 64 << 10,
	}
	for in, want := range cases {
		got, err := parseByteSize(in)
		if err != nil || int(got) != want {
			t.Fatalf("parseByteSize(%q)=%d,%v want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "abc", "12XB"} {
		if _, err := parseByteSize(bad); err == nil {
			t.Fatalf("parseByteSize(%q) must fail", bad)
		}
	}
}

// TestByteSizeYAMLLoad 完整 yaml 加载：frame_bytes/window_target 带单位写法。
func TestByteSizeYAMLLoad(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/s.yaml"
	if err := os.WriteFile(path, []byte(`
source:
  url: http://127.0.0.1:8428
sync:
  frame_bytes: 512KB
  window_target: 2MB
tcp:
  addr: 1.2.3.4:28101
wal:
  path: /tmp/w
`), 0644); err != nil {
		t.Fatal(err)
	}
	c, err := LoadSender(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Sync.FrameBytes != 512<<10 || c.Sync.WindowTarget != 2<<20 {
		t.Fatalf("frame_bytes=%d window_target=%d", c.Sync.FrameBytes, c.Sync.WindowTarget)
	}
}

// TestValidateBackfill R20：入口级回填值校验（YAML 路径已在 Validate 拦截；
// 本函数供内嵌 flag 路径复用）——乱值/负值必须拒绝，合法值通过。
func TestValidateBackfill(t *testing.T) {
	for _, ok := range []string{"", "all", "0", "0s", "30d", "1d12h"} {
		if err := ValidateBackfill(ok); err != nil {
			t.Fatalf("backfill %q must validate: %v", ok, err)
		}
	}
	for _, bad := range []string{"30dd", "-30d", "xyz"} {
		if err := ValidateBackfill(bad); err == nil {
			t.Fatalf("backfill %q must be rejected", bad)
		}
	}
}
