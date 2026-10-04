package gateway

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const transform = "image/resize,w_640/quality,Q_85/format,webp"

// fixture 模拟实际 S3 权限、版本和外部编码服务，以验证请求而非内部实现。
type fixture struct {
	origin, processor *httptest.Server
	gateway           *Gateway
	mu                sync.Mutex
	status            int
	etag, version     string
	sourceBytes       int64
	processStatus     int
	processType       string
	data              []byte
	sources           []string
	heads             atomic.Int32
	encodes           atomic.Int32
	started, release  chan struct{}
}

// newFixture 创建独立 HTTP 后端，任何意外凭据转发都会令测试失败。
func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{status: 200, etag: "\"source-v1\"", sourceBytes: 100, processStatus: 200,
		processType: "image/webp", data: []byte("RIFF transformed webp bytes")}
	f.origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.URL.Query().Get("x-oss-process") != "" {
			t.Error("原图请求带有凭据或处理参数")
		}
		if r.Method == http.MethodHead {
			f.heads.Add(1)
			w.Header().Set("ETag", f.etag)
			w.Header().Set("Content-Length", fmt.Sprint(f.sourceBytes))
			w.Header().Set("Last-Modified", "Sun, 04 Oct 2026 10:00:00 GMT")
			if f.version != "" {
				w.Header().Set("x-amz-version-id", f.version)
			}
			w.WriteHeader(f.status)
			return
		}
		if f.status != 200 {
			w.WriteHeader(f.status)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("ETag", f.etag)
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader([]byte("original-image")))
	}))
	f.processor = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.encodes.Add(1)
		encoded := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], ".webp")
		source, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			t.Errorf("编码服务收到无效源地址: %v", err)
		}
		f.mu.Lock()
		f.sources = append(f.sources, string(source))
		status, contentType, data := f.processStatus, f.processType, append([]byte(nil), f.data...)
		f.mu.Unlock()
		if f.started != nil {
			select {
			case f.started <- struct{}{}:
			default:
			}
		}
		if f.release != nil {
			select {
			case <-f.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(data)
	}))
	var err error
	f.gateway, err = New(Config{Source: f.origin.URL + "/bucket", Imgproxy: f.processor.URL,
		Concurrency: 16, MaxDimension: 4096, CacheBytes: 4096, MaxSourceBytes: 1024, MaxResultBytes: 1024, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.origin.Close(); f.processor.Close(); f.gateway.client.CloseIdleConnections() })
	return f
}

// request 将条件或范围请求直接发送给待测 HTTP 入口。
func (f *fixture) request(method, path string, headers http.Header) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://untrusted-host"+path, nil)
	if headers != nil {
		r.Header = headers
	}
	w := httptest.NewRecorder()
	f.gateway.ServeHTTP(w, r)
	return w
}

// imagePath 保留参数中的真实斜线语义，并由 URL 编码处理逗号等字符。
func imagePath() string { return "/image.png?x-oss-process=" + url.QueryEscape(transform) }

// TestCacheRechecksAccess 验证缓存命中、撤销公开权限及删除原图后的响应。
func TestCacheRechecksAccess(t *testing.T) {
	f := newFixture(t)
	first := f.request("GET", imagePath(), nil)
	if first.Code != 200 || first.Header().Get("ETag") == "\"source-v1\"" || first.Header().Get("Content-Type") != "image/webp" {
		t.Fatalf("首次派生图响应错误: %d %v", first.Code, first.Header())
	}
	second := f.request("GET", imagePath(), nil)
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) || f.encodes.Load() != 1 || f.heads.Load() != 3 {
		t.Fatal("缓存命中仍应重新鉴权，且不应重复编码")
	}
	f.mu.Lock()
	f.status = 403
	f.mu.Unlock()
	denied := f.request("GET", imagePath(), http.Header{"If-None-Match": {first.Header().Get("ETag")}})
	if denied.Code != 403 || denied.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("条件请求绕过了撤销后的权限")
	}
	f.mu.Lock()
	f.status = 404
	f.mu.Unlock()
	if f.request("HEAD", imagePath(), nil).Code != 404 {
		t.Fatal("删除后的对象仍可读取缓存")
	}
}

// TestRepresentationHeaders 验证 HEAD、304 和 Range 作用于派生字节。
func TestRepresentationHeaders(t *testing.T) {
	f := newFixture(t)
	full := f.request("GET", imagePath(), nil)
	etag := full.Header().Get("ETag")
	head := f.request("HEAD", imagePath(), nil)
	if head.Code != 200 || head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(len(f.data)) {
		t.Fatal("HEAD 使用了原图长度或输出了正文")
	}
	conditional := f.request("GET", imagePath(), http.Header{"If-None-Match": {etag}})
	if conditional.Code != 304 || conditional.Body.Len() != 0 {
		t.Fatal("派生 ETag 未产生 304")
	}
	if f.request("GET", imagePath(), http.Header{"If-None-Match": {"\"source-v1\""}}).Code != 200 {
		t.Fatal("误用了原图 ETag")
	}
	ranged := f.request("GET", imagePath(), http.Header{"Range": {"bytes=0-3"}})
	if ranged.Code != 206 || ranged.Body.String() != "RIFF" || ranged.Header().Get("Content-Range") != fmt.Sprintf("bytes 0-3/%d", len(f.data)) {
		t.Fatal("Range 未作用于派生字节")
	}
	invalid := f.request("GET", imagePath(), http.Header{"Range": {"bytes=999-"}})
	if invalid.Code != 416 || invalid.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("未拒绝不可满足的派生范围")
	}
	failed := f.request("GET", imagePath(), http.Header{"If-Match": {"\"wrong\""}})
	if failed.Code != 412 || failed.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("前置条件失败响应被缓存")
	}
}

// TestChangedSource 验证覆盖和显式版本读取不会复用旧派生图。
func TestChangedSource(t *testing.T) {
	f := newFixture(t)
	f.request("GET", imagePath(), nil)
	f.mu.Lock()
	f.etag = "\"source-v2\""
	f.data = []byte("new result")
	f.mu.Unlock()
	if f.request("GET", imagePath(), nil).Body.String() != "new result" || f.encodes.Load() != 2 {
		t.Fatal("原图覆盖后仍返回旧缓存")
	}
	if f.request("GET", imagePath()+"&versionId=v1", nil).Code != 200 {
		t.Fatal("显式版本读取失败")
	}
	f.mu.Lock()
	last := f.sources[len(f.sources)-1]
	f.mu.Unlock()
	if last != f.origin.URL+"/bucket/image.png?versionId=v1" {
		t.Fatalf("版本未传给源: %s", last)
	}
}

// TestSourceChangesDuringEncoding 验证正在处理的旧版本不会填充缓存。
func TestSourceChangesDuringEncoding(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.request("GET", imagePath(), nil) }()
	<-f.started
	f.mu.Lock()
	f.etag = "\"replaced\""
	f.mu.Unlock()
	close(f.release)
	if (<-done).Code != 409 {
		t.Fatal("原图被替换时仍缓存了处理结果")
	}
	if f.request("GET", imagePath(), nil).Code != 200 || f.encodes.Load() != 2 {
		t.Fatal("重试未重新编码")
	}
}

// TestRejectUnsafeRequests 验证私有签名、任意 URL、重复参数及写操作不触达后端。
func TestRejectUnsafeRequests(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		method, path string
		header       http.Header
		status       int
	}{
		{"PUT", imagePath(), nil, 405},
		{"GET", imagePath(), http.Header{"Authorization": {"AWS4-HMAC-SHA256 secret"}}, 403},
		{"GET", imagePath(), http.Header{"X-Amz-Security-Token": {"secret"}}, 403},
		{"GET", imagePath() + "&X-Amz-Signature=secret", nil, 400},
		{"GET", imagePath() + "&source=http://private-target", nil, 400},
		{"GET", imagePath() + "&x-oss-process=image", nil, 400},
		{"GET", "/image.png?x-oss-process=%", nil, 400},
		{"GET", "/image.png?x-oss-process=", nil, 400},
	}
	for _, test := range tests {
		if response := f.request(test.method, test.path, test.header); response.Code != test.status {
			t.Errorf("请求 %s 返回 %d，预期 %d", test.path, response.Code, test.status)
		}
	}
	if f.encodes.Load() != 0 || f.heads.Load() != 0 {
		t.Fatal("非法请求触达了后端")
	}
}

// TestLimitsAndBackendFailures 验证资源上限、异常格式、缺失原图及编码错误不入缓存。
func TestLimitsAndBackendFailures(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fixture)
		status int
	}{
		{"原图过大", func(f *fixture) { f.sourceBytes = 1025 }, 413},
		{"结果过大", func(f *fixture) { f.data = make([]byte, 1025) }, 413},
		{"空结果", func(f *fixture) { f.data = nil }, 413},
		{"错误格式", func(f *fixture) { f.processType = "text/html" }, 502},
		{"编码失败", func(f *fixture) { f.processStatus = 500 }, 502},
		{"原图拒绝", func(f *fixture) { f.status = 403 }, 403},
		{"原图缺失", func(f *fixture) { f.status = 404 }, 404},
		{"原图异常", func(f *fixture) { f.status = 500 }, 502},
		{"重定向", func(f *fixture) { f.status = 302 }, 502},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			test.change(f)
			for i := 0; i < 2; i++ {
				w := f.request("GET", imagePath(), nil)
				if w.Code != test.status || w.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("错误响应: %d %v", w.Code, w.Header())
				}
			}
			if f.gateway.cache.order.Len() != 0 {
				t.Fatal("错误结果进入缓存")
			}
		})
	}
}

// TestConcurrentMisses 验证相同图片的并发首次访问只调用一次编码服务。
func TestConcurrentMisses(t *testing.T) {
	f := newFixture(t)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			if w := f.request("GET", imagePath(), nil); w.Code != 200 {
				t.Errorf("并发请求失败: %d", w.Code)
			}
		}()
	}
	<-f.started
	close(f.release)
	wg.Wait()
	if f.encodes.Load() != 1 {
		t.Fatalf("并发首次请求编码次数为 %d", f.encodes.Load())
	}
}

// TestConcurrencyAndCancellation 验证拥塞立即拒绝，取消请求能终止后端等待。
func TestConcurrencyAndCancellation(t *testing.T) {
	f := newFixture(t)
	f.gateway.requests = make(chan struct{}, 1)
	f.started, f.release = make(chan struct{}, 1), make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", imagePath(), nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { f.gateway.ServeHTTP(httptest.NewRecorder(), r); close(done) }()
	<-f.started
	if f.request("GET", imagePath(), nil).Code != 429 {
		t.Fatal("拥塞未返回 429")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("取消后仍等待编码")
	}
	close(f.release)
}

// TestOriginalRead 验证未请求处理时仍然按原图 ETag、HEAD 和 Range 返回。
func TestOriginalRead(t *testing.T) {
	f := newFixture(t)
	get := f.request("GET", "/image.png", nil)
	if get.Code != 200 || get.Body.String() != "original-image" || get.Header().Get("Content-Type") != "image/png" {
		t.Fatal("原图读取错误")
	}
	ranged := f.request("GET", "/image.png", http.Header{"Range": {"bytes=0-2"}})
	if ranged.Code != 206 || ranged.Body.String() != "ori" {
		t.Fatal("原图范围读取错误")
	}
	if f.request("GET", "/image.png", http.Header{"If-None-Match": {"\"source-v1\""}}).Code != 304 {
		t.Fatal("原图条件读取错误")
	}
	if f.request("HEAD", "/image.png", nil).Body.Len() != 0 || f.encodes.Load() != 0 {
		t.Fatal("原图请求意外编码")
	}
}

// TestBrowserCookiesAreNotForwarded 验证浏览器附带站点 Cookie 时仍按匿名权限取图。
func TestBrowserCookiesAreNotForwarded(t *testing.T) {
	f := newFixture(t)
	if f.request("GET", imagePath(), http.Header{"Cookie": {"session=secret"}}).Code != 200 {
		t.Fatal("站点 Cookie 干扰了公开图片读取")
	}
	f.mu.Lock()
	f.status = 403
	f.mu.Unlock()
	if f.request("GET", imagePath(), http.Header{"Cookie": {"session=secret"}}).Code != 403 {
		t.Fatal("Cookie 提升了原图读取权限")
	}
}

// TestNoETagDisablesCache 验证没有内容校验信息时每次重新处理。
func TestNoETagDisablesCache(t *testing.T) {
	f := newFixture(t)
	f.etag = ""
	f.request("GET", imagePath(), nil)
	f.request("GET", imagePath(), nil)
	if f.encodes.Load() != 2 {
		t.Fatal("缺少 ETag 时错误复用了结果")
	}
}

// TestFixedSourceAndSigning 验证路径转义、固定主机及 imgproxy HMAC 签名。
func TestFixedSourceAndSigning(t *testing.T) {
	f := newFixture(t)
	f.gateway.key, f.gateway.salt = []byte("secret"), []byte("salt")
	r := httptest.NewRequest("GET", "http://attacker/a%20b/%23.png?versionId=v%2B1", nil)
	source := f.gateway.sourceURL(r, r.URL.Query())
	if source.String() != f.origin.URL+"/bucket/a%20b/%23.png?versionId=v%2B1" {
		t.Fatal("固定源地址或对象转义被改变")
	}
	u := f.gateway.processorURL(source, options{width: 640, quality: 85, format: "webp"})
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte("salt/" + parts[1]))
	if parts[0] != base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatal("imgproxy 签名不符合协议")
	}
}

// TestRealHTTPHead 验证真实 HTTP 服务会保留派生长度且不发送 HEAD 正文。
func TestRealHTTPHead(t *testing.T) {
	f := newFixture(t)
	s := httptest.NewServer(f.gateway)
	defer s.Close()
	resp, err := http.Head(s.URL + imagePath())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || resp.ContentLength != int64(len(f.data)) || len(body) != 0 {
		t.Fatal("真实 HEAD 响应不符合 HTTP 语义")
	}
}
