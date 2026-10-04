// Package gateway provides an optional on-demand gateway for public S3 images.
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
	"strconv"
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
	work              chan struct{}
	cache             *cache
	flights           singleflight.Group
}

type failure struct {
	status  int
	message string
}

type responseWriter struct {
	http.ResponseWriter
	timeout time.Duration
	started bool
}

// beginWrite starts a separate client write budget after backend processing.
func (w *responseWriter) beginWrite() {
	if !w.started {
		w.started = true
		_ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(w.timeout))
	}
}

// Write sets the deadline before the first body write.
func (w *responseWriter) Write(data []byte) (int, error) {
	w.beginWrite()
	return w.ResponseWriter.Write(data)
}

// WriteHeader prevents downstream caching of protocol errors, including 412 and 416.
func (w *responseWriter) WriteHeader(status int) {
	w.beginWrite()
	if status >= 400 {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.ResponseWriter.WriteHeader(status)
}

// Error returns a fixed message without source URLs or signing material.
func (f *failure) Error() string { return f.message }

// New validates fixed backends and resource limits; redirects are disabled.
func New(c Config) (*Gateway, error) {
	if c.Concurrency < 1 || c.Concurrency > 1024 || c.MaxDimension < 1 || c.MaxDimension > 16384 ||
		c.CacheBytes < 0 || c.CacheBytes > 1<<40 || c.MaxSourceBytes < 1 || c.MaxSourceBytes > 1<<30 ||
		c.MaxResultBytes < 1 || c.MaxResultBytes > 1<<30 || c.Timeout <= 0 {
		return nil, fmt.Errorf("invalid resource limits")
	}
	parse := func(value string) (*url.URL, error) {
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
			u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return nil, fmt.Errorf("backends must be HTTP(S) URLs without credentials, queries, or fragments")
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
	// imgproxy signs its root-relative processing path; reject ambiguous proxy prefixes.
	if processor.Path != "" && processor.Path != "/" || processor.RawPath != "" {
		return nil, fmt.Errorf("imgproxy URL must not contain a path prefix")
	}
	key, keyErr := hex.DecodeString(c.Key)
	salt, saltErr := hex.DecodeString(c.Salt)
	if keyErr != nil || saltErr != nil || (len(key) == 0) != (len(salt) == 0) {
		return nil, fmt.Errorf("imgproxy key and salt must both be provided as hexadecimal values")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableCompression = true
	return &Gateway{
		source: source, processor: processor, key: key, salt: salt, config: c,
		client:   &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		requests: make(chan struct{}, c.Concurrency), cache: newCache(c.CacheBytes),
		work: make(chan struct{}, c.Concurrency),
	}, nil
}

// sourceURL joins the fixed source and object path, ignoring client hosts and credentials.
func (g *Gateway) sourceURL(r *http.Request, query url.Values) *url.URL {
	u := *g.source
	u.Path = strings.TrimSuffix(u.Path, "/") + r.URL.Path
	u.RawPath = strings.TrimSuffix(g.source.EscapedPath(), "/") + r.URL.EscapedPath()
	if id := query.Get("versionId"); id != "" {
		u.RawQuery = url.Values{"versionId": {id}}.Encode()
	}
	return &u
}

// ServeHTTP checks anonymous source access before cached or conditional responses.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w = &responseWriter{ResponseWriter: w, timeout: g.config.Timeout}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		g.writeError(w, r, &failure{405, "only GET and HEAD are allowed"})
		return
	}
	if !safeObjectPath(r.URL.Path) {
		g.writeError(w, r, &failure{400, "object path contains unsafe normalization segments"})
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		g.writeError(w, r, &failure{400, "invalid query parameters"})
		return
	}
	for name, values := range query {
		if (name != "x-oss-process" && name != "versionId") || len(values) != 1 || len(values[0]) > 1024 {
			g.writeError(w, r, &failure{400, "unsupported or repeated query parameters"})
			return
		}
	}
	if r.Header.Get("Authorization") != "" {
		g.writeError(w, r, &failure{403, "this endpoint only serves anonymously readable public images"})
		return
	}
	for name := range r.Header {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			g.writeError(w, r, &failure{403, "this endpoint does not accept S3 credentials or encryption headers"})
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
		g.writeError(w, r, &failure{429, "image request concurrency limit reached"})
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
	// Only sources with validators can reuse results; access is still checked on every request.
	cacheKey := source.String() + "\n" + revision(metadata) + "\n" + o.path()
	cacheable := metadata.Get("ETag") != ""
	var image *result
	if cacheable {
		image = g.cache.get(cacheKey)
	}
	if image == nil {
		// Source metadata and shared work each have their own bounded phase.
		// A slow HEAD must not consume the time needed to await a valid encoding job.
		waitCtx, waitCancel := context.WithTimeout(r.Context(), g.config.Timeout)
		defer waitCancel()
		cancel()
		flight := g.flights.DoChan(cacheKey, func() (interface{}, error) {
			if cacheable {
				if hit := g.cache.get(cacheKey); hit != nil {
					return hit, nil
				}
			}
			// Shared work is independent of any waiter and remains bounded by its own timeout.
			// Separate tokens prevent cancelled requests from bypassing the work concurrency limit.
			select {
			case g.work <- struct{}{}:
				defer func() { <-g.work }()
			default:
				return nil, &failure{429, "image encoding concurrency limit reached"}
			}
			workCtx, workCancel := context.WithTimeout(context.Background(), g.config.Timeout)
			defer workCancel()
			processed, processErr := g.process(workCtx, source, o)
			if processErr != nil {
				return nil, processErr
			}
			// Recheck access and revision before caching results from an in-flight encoding.
			current, checkErr := g.head(workCtx, source)
			if checkErr != nil {
				return nil, checkErr
			}
			if revision(current) != revision(metadata) {
				return nil, &failure{409, "source changed during processing; retry the request"}
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
		case <-waitCtx.Done():
			g.writeError(w, r, &failure{504, "image processing timed out or request was cancelled"})
			return
		}
	}
	// Derive validators, lengths, and ranges from the output bytes.
	w.Header().Set("Content-Type", image.contentType)
	w.Header().Set("ETag", image.etag)
	// A second-resolution source date cannot distinguish overwrites; use the output ETag only.
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(image.data))
}

// safeObjectPath rejects paths that a backend proxy could normalize outside the fixed bucket prefix.
// Check repeatedly escaped dot segments and backslashes while preserving ordinary object escaping.
func safeObjectPath(value string) bool {
	for i := 0; i < 4; i++ {
		if strings.Contains(value, "\\") {
			return false
		}
		for _, part := range strings.Split(value, "/") {
			if part == "." || part == ".." {
				return false
			}
		}
		next, err := url.PathUnescape(value)
		if err != nil || next == value {
			return true
		}
		value = next
	}
	return false
}

// revision includes S3 version and content validators in the cache key.
func revision(h http.Header) string {
	return h.Get("ETag") + "\n" + h.Get("Last-Modified") + "\n" + h.Get("Content-Length") + "\n" + h.Get("x-amz-version-id")
}

// read makes credential-free requests with the supplied timeout and cancellation context.
func (g *Gateway) read(ctx context.Context, method string, u *url.URL, headers http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, &failure{502, "could not create backend request"}
	}
	if headers != nil {
		req.Header = headers.Clone()
	}
	resp, err := g.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &failure{504, "image backend request timed out"}
		}
		return nil, &failure{502, "image backend request failed"}
	}
	return resp, nil
}

// head checks anonymous access and source size without forwarding client conditions.
func (g *Gateway) head(ctx context.Context, source *url.URL) (http.Header, error) {
	resp, err := g.read(ctx, http.MethodHead, source, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 403 || resp.StatusCode == 404 {
		return nil, &failure{resp.StatusCode, "source image is not accessible"}
	}
	if resp.StatusCode != 200 {
		return nil, &failure{502, "source image check failed"}
	}
	if resp.ContentLength < 0 || resp.ContentLength > g.config.MaxSourceBytes {
		return nil, &failure{413, "source image exceeds size limit or lacks a content length"}
	}
	return resp.Header, nil
}

// processorURL builds a fixed-operation imgproxy URL, optionally signed with its key and salt.
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

// process reads bounded results, rejecting incorrect media types and unsuccessful responses.
func (g *Gateway) process(ctx context.Context, source *url.URL, o options) (*result, error) {
	resp, err := g.read(ctx, http.MethodGet, g.processorURL(source, o), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, &failure{502, "image encoder returned an error"}
	}
	contentType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	expected := map[string]string{"jpg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[o.format]
	if err != nil || contentType != expected {
		return nil, &failure{502, "image encoder returned an unexpected media type"}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, g.config.MaxResultBytes+1))
	if err != nil {
		return nil, &failure{502, "could not read processed image"}
	}
	if len(data) == 0 || int64(len(data)) > g.config.MaxResultBytes {
		return nil, &failure{413, "processed image is empty or exceeds size limit"}
	}
	digest := sha256.Sum256(data)
	return &result{data: data, contentType: contentType, etag: fmt.Sprintf("\"%x\"", digest)}, nil
}

// serveOriginal preserves public source conditions and ranges without forwarding credentials.
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
		if status == 416 {
			w.Header().Set("Content-Range", resp.Header.Get("Content-Range"))
		}
		g.writeError(w, r, &failure{status, "source image read failed"})
		return
	}
	sourceSize := resp.ContentLength
	if resp.StatusCode == 206 {
		// A 206 length describes only the selected range; enforce limits against the complete object.
		rangeValue := resp.Header.Get("Content-Range")
		_, total, ok := strings.Cut(rangeValue, "/")
		sourceSize, err = strconv.ParseInt(total, 10, 64)
		if !ok || !strings.HasPrefix(rangeValue, "bytes ") || err != nil || sourceSize < 1 {
			g.writeError(w, r, &failure{502, "source range response lacks a valid complete length"})
			return
		}
	}
	if sourceSize > g.config.MaxSourceBytes || resp.ContentLength > g.config.MaxSourceBytes {
		g.writeError(w, r, &failure{413, "source image exceeds size limit"})
		return
	}
	var data []byte
	if r.Method != http.MethodHead && resp.StatusCode != 304 {
		data, err = io.ReadAll(io.LimitReader(resp.Body, g.config.MaxSourceBytes+1))
		if err != nil || int64(len(data)) > g.config.MaxSourceBytes {
			g.writeError(w, r, &failure{502, "source read failed or exceeded size limit"})
			return
		}
	}
	for _, key := range []string{"Content-Type", "Content-Encoding", "Content-Length", "ETag", "Last-Modified", "Accept-Ranges", "Content-Range", "x-amz-version-id"} {
		if value := resp.Header.Get(key); value != "" {
			w.Header().Set(key, value)
		}
	}
	w.WriteHeader(resp.StatusCode)
	if len(data) != 0 {
		_, _ = w.Write(data)
	}
}

// writeError prevents downstream caching of access and transient errors.
func (g *Gateway) writeError(w http.ResponseWriter, r *http.Request, err error) {
	f, ok := err.(*failure)
	if !ok {
		f = &failure{502, "image processing failed"}
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Del("ETag")
	w.Header().Del("Content-Length")
	http.Error(w, f.message, f.status)
}
