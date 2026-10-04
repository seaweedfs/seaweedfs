package gateway

import (
	"strings"
	"testing"
	"time"
)

// TestOptionsCoverage 验证受支持的 OSS 子集及明确拒绝的操作。
func TestOptionsCoverage(t *testing.T) {
	valid := []string{transform, "image/resize,w_240,h_640,m_lfit,limit_1/format,webp", "image/format,jpeg/quality,Q_100", "image/format,png", "image/resize,h_640"}
	for _, value := range valid {
		if _, err := parseOptions(value, 4096); err != nil {
			t.Errorf("有效操作被拒绝 %s: %v", value, err)
		}
	}
	invalid := []string{"", "image", "image/", "image/resize", "image/resize,w_0", "image/resize,w_-1", "image/resize,w_4097", "image/resize,w_1,w_2", "image/resize,m_lfit", "image/resize,w_640,m_fill", "image/resize,w_1,limit_0", "image/quality,q_85", "image/quality,Q_0", "image/quality,Q_101", "image/quality,Q_85,Q_90", "image/format,svg", "image/format,webp/format,png", "image/watermark,x", "image/resize,w_1/resize,w_2", strings.Repeat("x", 257)}
	for _, value := range invalid {
		if _, err := parseOptions(value, 4096); err == nil {
			t.Errorf("非法操作被接受: %s", value)
		}
	}
	a, _ := parseOptions(transform, 4096)
	b, _ := parseOptions("image/format,webp/quality,Q_85/resize,w_640", 4096)
	if a.path() != b.path() {
		t.Fatal("等价操作未规范化")
	}
}

// TestByteLimitedCache 验证按字节淘汰、重写、禁用以及超大对象跳过。
func TestByteLimitedCache(t *testing.T) {
	c := newCache(400)
	value := &result{data: make([]byte, 100)}
	c.put("a", value)
	c.put("b", value)
	if c.get("a") != nil || c.get("b") == nil {
		t.Fatal("缓存未按字节淘汰")
	}
	c.put("b", &result{data: []byte("updated")})
	if c.get("b").data[0] != 'u' {
		t.Fatal("重写缓存失败")
	}
	c.put("large", &result{data: make([]byte, 401)})
	if c.get("large") != nil || c.used > c.capacity {
		t.Fatal("超大对象突破容量限制")
	}
	disabled := newCache(0)
	disabled.put("a", value)
	if disabled.get("a") != nil {
		t.Fatal("缓存禁用失败")
	}
	large := newCache(1 << 20)
	for i := 0; i < 2048; i++ {
		large.put(string(rune(i)), value)
	}
	if len(large.entries) != 1024 {
		t.Fatal("小对象条数未受限")
	}
}

// TestConfigurationRejectsUnsafeBackends 验证后端必须显式配置且不可携带凭据。
func TestConfigurationRejectsUnsafeBackends(t *testing.T) {
	base := Config{Source: "http://localhost:8333/bucket", Imgproxy: "http://localhost:8080", Concurrency: 1, MaxDimension: 4096, CacheBytes: 0, MaxSourceBytes: 1024, MaxResultBytes: 1024, Timeout: time.Second}
	for _, source := range []string{"", "file:///etc/passwd", "http://secret:password@localhost", "http://localhost?signature=secret", "http://localhost#fragment"} {
		c := base
		c.Source = source
		if _, err := New(c); err == nil {
			t.Errorf("非法后端被接受: %s", source)
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
			t.Fatal("非法资源或签名配置被接受")
		}
	}
}
