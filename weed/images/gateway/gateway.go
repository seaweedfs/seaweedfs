// Package gateway 为公开 S3 图片提供独立的按需处理入口。
package gateway

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

type Config struct {
	Source, Imgproxy, Key, Salt                string
	Concurrency, MaxDimension                  int
	CacheBytes, MaxSourceBytes, MaxResultBytes int64
	Timeout                                    time.Duration
}

type Gateway struct {
	source, processor *url.URL
	key, salt         []byte
	config            Config
	client            *http.Client
	requests          chan struct{}
	cache             *cache
	flights           singleflight.Group
}

type failure struct {
	status  int
	message string
}

type responseWriter struct{ http.ResponseWriter }

// WriteHeader 也为 ServeContent 的 412、416 等协议错误禁止下游缓存。
func (w responseWriter) WriteHeader(status int) {
	if status >= 400 {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.ResponseWriter.WriteHeader(status)
}

// Error 只暴露固定错误，不包含源地址或签名材料。
func (f *failure) Error() string { return f.message }

// New 校验固定后端地址和资源限额；不允许重定向到其他数据源。
func New(c Config) (*Gateway, error) {
	if c.Concurrency < 1 || c.Concurrency > 1024 || c.MaxDimension < 1 || c.MaxDimension > 16384 ||
		c.CacheBytes < 0 || c.CacheBytes > 1<<40 || c.MaxSourceBytes < 1 || c.MaxSourceBytes > 1<<30 ||
		c.MaxResultBytes < 1 || c.MaxResultBytes > 1<<30 || c.Timeout <= 0 {
		return nil, fmt.Errorf("资源限额无效")
	}
	parse := func(value string) (*url.URL, error) {
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return nil, fmt.Errorf("后端必须是无凭据、查询参数或片段的 HTTP(S) 地址")
		}
		return u, nil
	}
	source, err := parse(c.Source)
	if err != nil {
		return nil, err
	}
	processor, err := parse(c.Imgproxy)
	if err != nil {
		return nil, err
	}
	key, keyErr := hex.DecodeString(c.Key)
	salt, saltErr := hex.DecodeString(c.Salt)
	if keyErr != nil || saltErr != nil || (len(key) == 0) != (len(salt) == 0) {
		return nil, fmt.Errorf("imgproxy 签名密钥和盐必须同时提供十六进制值")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	return &Gateway{
		source: source, processor: processor, key: key, salt: salt, config: c,
		client:   &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		requests: make(chan struct{}, c.Concurrency), cache: newCache(c.CacheBytes),
	}, nil
}

// sourceURL 仅拼接固定源和对象路径，客户端 Host 及签名不能改变取图目标。
func (g *Gateway) sourceURL(r *http.Request, query url.Values) *url.URL {
	u := *g.source
	u.Path = strings.TrimSuffix(u.Path, "/") + r.URL.Path
	u.RawPath = strings.TrimSuffix(g.source.EscapedPath(), "/") + r.URL.EscapedPath()
	if id := query.Get("versionId"); id != "" {
		u.RawQuery = url.Values{"versionId": {id}}.Encode()
	}
	return &u
}

// ServeHTTP 在缓存和条件响应之前检查匿名原图权限，私有对象不能因旧缓存泄漏。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w = responseWriter{w}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		g.writeError(w, r, &failure{405, "只允许 GET 或 HEAD"})
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		g.writeError(w, r, &failure{400, "无效的查询参数"})
		return
	}
	for name, values := range query {
		if (name != "x-oss-process" && name != "versionId") || len(values) != 1 || len(values[0]) > 1024 {
			g.writeError(w, r, &failure{400, "不支持或重复的查询参数"})
			return
		}
	}
	if r.Header.Get("Authorization") != "" {
		g.writeError(w, r, &failure{403, "此入口只处理匿名公开图片"})
		return
	}
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			g.writeError(w, r, &failure{403, "此入口不接受 S3 凭据或加密参数"})
			return
		}
	}
	var o options
	_, processing := query["x-oss-process"]
	if processing {
		o, err = parseOptions(query.Get("x-oss-process"), g.config.MaxDimension)
		if err != nil {
			g.writeError(w, r, &failure{400, err.Error()})
			return
		}
	}
	select {
	case g.requests <- struct{}{}:
		defer func() { <-g.requests }()
	default:
		g.writeError(w, r, &failure{429, "图片请求并发已达上限"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), g.config.Timeout)
	defer cancel()
	source := g.sourceURL(r, query)
	if !processing {
		g.serveOriginal(w, r.WithContext(ctx), source)
		return
	}
	metadata, err := g.head(ctx, source)
	if err != nil {
		g.writeError(w, r, err)
		return
	}
	// 只有带版本校验信息的源才能复用派生结果，权限仍然每次查询。
	cacheKey := source.String() + "\n" + revision(metadata) + "\n" + o.path()
	cacheable := metadata.Get("ETag") != ""
	var image *result
	if cacheable {
		image = g.cache.get(cacheKey)
	}
	if image == nil {
		flight := g.flights.DoChan(cacheKey, func() (interface{}, error) {
			if cacheable {
				if hit := g.cache.get(cacheKey); hit != nil {
					return hit, nil
				}
			}
			processed, processErr := g.process(ctx, source, o)
			if processErr != nil {
				return nil, processErr
			}
			// 编码期间原图被替换或撤销权限时，不能把结果保存到旧版本缓存。
			current, checkErr := g.head(ctx, source)
			if checkErr != nil {
				return nil, checkErr
			}
			if revision(current) != revision(metadata) {
				return nil, &failure{409, "处理期间原图已改变，请重试"}
			}
			if cacheable {
				g.cache.put(cacheKey, processed)
			}
			return processed, nil
		})
		select {
		case outcome := <-flight:
			if outcome.Err != nil {
				g.writeError(w, r, outcome.Err)
				return
			}
			image = outcome.Val.(*result)
		case <-ctx.Done():
			g.writeError(w, r, &failure{504, "图片处理超时或请求已取消"})
			return
		}
	}
	// 原图的 ETag、长度及范围不能用于派生图，按输出字节重新计算。
	w.Header().Set("Content-Type", image.contentType)
	w.Header().Set("ETag", image.etag)
	modified, _ := http.ParseTime(metadata.Get("Last-Modified"))
	http.ServeContent(w, r, "", modified, bytes.NewReader(image.data))
}

// revision 将 S3 对象的版本与内容校验信息一起加入缓存键。
func revision(h http.Header) string {
	return h.Get("ETag") + "\n" + h.Get("Last-Modified") + "\n" + h.Get("Content-Length") + "\n" + h.Get("x-amz-version-id")
}

// read 发出无凭据的后端请求，并在所有路径保留调用方的超时和取消。
func (g *Gateway) read(ctx context.Context, method string, u *url.URL, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, &failure{502, "无法创建后端请求"}
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	resp, err := g.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &failure{504, "图片后端请求超时"}
		}
		return nil, &failure{502, "图片后端请求失败"}
	}
	return resp, nil
}

// head 检查原对象是否可匿名读取及文件大小，不向后端转发客户端条件头。
func (g *Gateway) head(ctx context.Context, source *url.URL) (http.Header, error) {
	resp, err := g.read(ctx, http.MethodHead, source, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 || resp.StatusCode == 404 {
		return nil, &failure{resp.StatusCode, "原图不可访问"}
	}
	if resp.StatusCode != 200 {
		return nil, &failure{502, "原图检查失败"}
	}
	if resp.ContentLength < 0 || resp.ContentLength > g.config.MaxSourceBytes {
		return nil, &failure{413, "原图超过大小限制或缺少长度"}
	}
	return resp.Header, nil
}

// processorURL 生成固定操作的 imgproxy 地址，可使用与编码服务相同的签名密钥。
func (g *Gateway) processorURL(source *url.URL, o options) *url.URL {
	path := o.path() + "/" + base64.RawURLEncoding.EncodeToString([]byte(source.String())) + "." + o.format
	signature := "insecure"
	if len(g.key) != 0 {
		mac := hmac.New(sha256.New, g.key)
		mac.Write(g.salt)
		mac.Write([]byte(path))
		signature = base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	}
	u := *g.processor
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + signature + path
	u.RawPath = ""
	return &u
}

// process 读取有大小上限的编码结果，拒绝非图片及错误响应，错误内容不入缓存。
func (g *Gateway) process(ctx context.Context, source *url.URL, o options) (*result, error) {
	resp, err := g.read(ctx, http.MethodGet, g.processorURL(source, o), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, &failure{502, "图片编码服务返回错误"}
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	expected := map[string]string{"jpg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[o.format]
	if err != nil || contentType != expected {
		return nil, &failure{502, "图片编码服务返回错误格式"}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, g.config.MaxResultBytes+1))
	if err != nil {
		return nil, &failure{502, "读取处理结果失败"}
	}
	if len(data) == 0 || int64(len(data)) > g.config.MaxResultBytes {
		return nil, &failure{413, "处理结果为空或超过大小限制"}
	}
	digest := sha256.Sum256(data)
	return &result{data: data, contentType: contentType, etag: fmt.Sprintf("\"%x\"", digest)}, nil
}

// serveOriginal 保留公开原图的条件和范围语义，仍然不转发凭据。
func (g *Gateway) serveOriginal(w http.ResponseWriter, r *http.Request, source *url.URL) {
	headers := make(http.Header)
	for _, key := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		if value := r.Header.Get(key); value != "" {
			headers.Set(key, value)
		}
	}
	resp, err := g.read(r.Context(), r.Method, source, headers)
	if err != nil {
		g.writeError(w, r, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 && resp.StatusCode != 304 {
		status := resp.StatusCode
		if status < 400 || status >= 500 {
			status = 502
		}
		g.writeError(w, r, &failure{status, "原图读取失败"})
		return
	}
	if resp.ContentLength > g.config.MaxSourceBytes {
		g.writeError(w, r, &failure{413, "原图超过大小限制"})
		return
	}
	var data []byte
	if r.Method != http.MethodHead && resp.StatusCode != 304 {
		data, err = io.ReadAll(io.LimitReader(resp.Body, g.config.MaxSourceBytes+1))
		if err != nil || int64(len(data)) > g.config.MaxSourceBytes {
			g.writeError(w, r, &failure{502, "原图读取失败或超过大小限制"})
			return
		}
	}
	for _, key := range []string{"Content-Type", "Content-Length", "ETag", "Last-Modified", "Accept-Ranges", "Content-Range", "x-amz-version-id"} {
		if value := resp.Header.Get(key); value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if len(data) != 0 {
		_, _ = w.Write(data)
	}
}

// writeError 始终禁止缓存失败响应，避免 CDN 长期保存权限或临时错误。
func (g *Gateway) writeError(w http.ResponseWriter, r *http.Request, err error) {
	f, ok := err.(*failure)
	if !ok {
		f = &failure{502, "图片处理失败"}
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Del("ETag")
	w.Header().Del("Content-Length")
	http.Error(w, f.message, f.status)
}
