package gateway

import (
	"strings"
	"testing"
	"time"
)

// TestOptionsCoverage checks the supported OSS subset and rejected operations.
func TestOptionsCoverage(t *testing.T) {
	valid := []string{transform, "image/resize,w_240,h_640,m_lfit,limit_1/format,webp", "image/format,jpeg/quality,Q_100", "image/format,png", "image/resize,h_640"}
	for _, value := range valid {
		if _, err := parseOptions(value, 4096); err != nil {
			t.Errorf("valid operation rejected %s: %v", value, err)
		}
	}
	invalid := []string{"", "image", "image/", "image/resize", "image/resize,w_0", "image/resize,w_-1", "image/resize,w_4097", "image/resize,w_1,w_2", "image/resize,m_lfit", "image/resize,w_640,m_fill", "image/resize,w_1,limit_0", "image/quality,q_85", "image/quality,Q_0", "image/quality,Q_101", "image/quality,Q_85,Q_90", "image/format,svg", "image/format,webp/format,png", "image/watermark,x", "image/resize,w_1/resize,w_2", strings.Repeat("x", 257)}
	for _, value := range invalid {
		if _, err := parseOptions(value, 4096); err == nil {
			t.Errorf("invalid operation accepted: %s", value)
		}
	}
	a, _ := parseOptions(transform, 4096)
	b, _ := parseOptions("image/format,webp/quality,Q_85/resize,w_640", 4096)
	if a.path() != b.path() {
		t.Fatal("equivalent operations were not canonicalized")
	}
}

// TestByteLimitedCache checks byte eviction, replacement, disabled caching, and oversized entries.
func TestByteLimitedCache(t *testing.T) {
	c := newCache(400)
	value := &result{data: make([]byte, 100)}
	c.put("a", value)
	c.put("b", value)
	if c.get("a") != nil || c.get("b") == nil {
		t.Fatal("cache did not evict by byte usage")
	}
	c.put("b", &result{data: []byte("updated")})
	if c.get("b").data[0] != 'u' {
		t.Fatal("cache replacement failed")
	}
	c.put("large", &result{data: make([]byte, 401)})
	if c.get("large") != nil || c.used > c.capacity {
		t.Fatal("oversized result exceeded cache capacity")
	}
	disabled := newCache(0)
	disabled.put("a", value)
	if disabled.get("a") != nil {
		t.Fatal("cache disabling failed")
	}
	large := newCache(1 << 20)
	for i := 0; i < 2048; i++ {
		large.put(string(rune(i)), value)
	}
	if len(large.entries) != 1024 {
		t.Fatal("small object entry count was not bounded")
	}
}

// TestConfigurationRejectsUnsafeBackends checks explicitly configured backends without embedded credentials.
func TestConfigurationRejectsUnsafeBackends(t *testing.T) {
	base := Config{Source: "http://localhost:8333/bucket", Imgproxy: "http://localhost:8080", Concurrency: 1, MaxDimension: 4096, CacheBytes: 0, MaxSourceBytes: 1024, MaxResultBytes: 1024, Timeout: time.Second}
	for _, source := range []string{"", "file:///etc/passwd", "http://secret:password@localhost", "http://localhost?signature=secret", "http://localhost#fragment"} {
		c := base
		c.Source = source
		if _, err := New(c); err == nil {
			t.Errorf("invalid backend accepted: %s", source)
		}
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Key = "zz"; c.Salt = "aa" }, func(c *Config) { c.Key = "aa" },
		func(c *Config) { c.Concurrency = 0 }, func(c *Config) { c.CacheBytes = -1 },
		func(c *Config) { c.MaxSourceBytes = 0 }, func(c *Config) { c.Timeout = 0 },
	} {
		c := base
		mutate(&c)
		if _, err := New(c); err == nil {
			t.Fatal("invalid resource or signing configuration accepted")
		}
	}
}
