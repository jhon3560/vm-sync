package config

import (
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
