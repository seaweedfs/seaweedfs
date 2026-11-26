package yandex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// defaultEndpoint is the Yandex Disk REST API root. Every method is addressed
// relative to it, e.g. "/resources".
const defaultEndpoint = "https://cloud-api.yandex.net/v1/disk"

// listPageSize is the API's maximum number of entries per page. It is a
// variable so a test can page a small folder.
var listPageSize = 1000

// statusBandwidthLimitExceeded is Yandex's throttling answer (509); Go's
// net/http has no constant for it.
const statusBandwidthLimitExceeded = 509

// operationWait bounds an asynchronous operation (a large delete or copy) that
// the API reports as 202. The caller's context still ends it earlier.
const operationWait = 30 * time.Minute

// defaultRequestTimeout bounds one metadata exchange with the API, from the
// request to the decoded answer. Object transfers on the upload and download
// links are not bounded by it: a large object may take much longer.
const defaultRequestTimeout = time.Minute

// resource is a file or folder as the API describes it.
type resource struct {
	Name             string         `json:"name"`
	Path             string         `json:"path"`
	Type             string         `json:"type"`
	Size             int64          `json:"size"`
	Created          string         `json:"created"`
	Modified         string         `json:"modified"`
	Md5              string         `json:"md5"`
	MimeType         string         `json:"mime_type"`
	CustomProperties map[string]any `json:"custom_properties"`
	Embedded         *resourceList  `json:"_embedded"`
}

type resourceList struct {
	Items  []resource `json:"items"`
	Total  uint64     `json:"total"`
	Limit  uint64     `json:"limit"`
	Offset uint64     `json:"offset"`
}

// items is the children of a folder resource, nil for a file or an answer
// without an embedded list.
func (r *resource) items() []resource {
	if r == nil || r.Embedded == nil {
		return nil
	}
	return r.Embedded.Items
}

func (r *resource) total() uint64 {
	if r == nil || r.Embedded == nil {
		return 0
	}
	return r.Embedded.Total
}

// link is a download or upload href the API hands out. The href is short-lived
// and must be requested anew for every transfer.
type link struct {
	Href        string `json:"href"`
	Method      string `json:"method"`
	Templated   bool   `json:"templated"`
	OperationID string `json:"operation_id"`
}

type operationStatus struct {
	Status string `json:"status"`
}

// apiError is a non-2xx answer. The API names the failure in "error"; a
// missing path is DiskNotFoundError, an existing folder is
// DiskPathPointsToExistentDirectoryError.
type apiError struct {
	Status      int
	Name        string
	Description string
	Message     string
}

func (e *apiError) Error() string {
	status := http.StatusText(e.Status)
	if status == "" {
		status = fmt.Sprintf("status %d", e.Status)
	}
	message := fmt.Sprintf("yandex disk api: %d %s", e.Status, status)
	if e.Name != "" {
		message += ": " + e.Name
	}
	if e.Description != "" {
		message += ": " + e.Description
	} else if e.Message != "" && e.Message != e.Name {
		message += ": " + e.Message
	}
	if e.Status == statusBandwidthLimitExceeded {
		// the assistant's transient-error classifier knows "rate limit" but
		// not the 509 status text
		message += " (rate limit exceeded)"
	}
	return message
}

// Timeout makes apiError a net.Error: the sync's retry classifier only retries
// errors that report a timeout, and a throttled or failed upload is worth a
// retry from a fresh reader.
func (e *apiError) Timeout() bool { return retryableStatus(e.Status) }

func (e *apiError) Temporary() bool { return e.Timeout() }

// apiClient speaks the Yandex Disk REST API over a cacheable HTTP client.
type apiClient struct {
	baseURL    string
	httpClient *http.Client
	tokens     *tokenSource

	// knobs a test shortens
	requestTimeout time.Duration
	maxRetries     int
	retryBase      time.Duration
	pollEvery      time.Duration
	// statTries bounds the stat that follows an upload while the service
	// finalizes it.
	statTries int
}

func newAPIClient(baseURL string, httpClient *http.Client, tokens *tokenSource) *apiClient {
	if baseURL == "" {
		baseURL = defaultEndpoint
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	c := &apiClient{
		baseURL:        strings.TrimSuffix(baseURL, "/"),
		tokens:         tokens,
		requestTimeout: defaultRequestTimeout,
		maxRetries:     3,
		retryBase:      200 * time.Millisecond,
		pollEvery:      time.Second,
		statTries:      5,
	}
	c.httpClient = c.checkingRedirects(httpClient)
	return c
}

// checkingRedirects returns a copy of the client that validates every
// redirect target with checkHref, so a transfer link that redirects cannot
// downgrade to plain http. The client's own redirect policy, if any, still
// applies; the transport is shared.
func (c *apiClient) checkingRedirects(httpClient *http.Client) *http.Client {
	checked := *httpClient
	previous := httpClient.CheckRedirect
	checked.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := c.checkHref(req.URL.String()); err != nil {
			return err
		}
		if previous != nil {
			return previous(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &checked
}

// checkEndpoint refuses an API endpoint that would carry the OAuth token in
// plain text: http is accepted only on a loopback host (a local proxy or a
// test service).
func checkEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("parse yandex endpoint %q: %w", endpoint, err)
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		host := parsed.Hostname()
		if host == "localhost" {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("yandex endpoint %q would send the oauth token in plain text; use https (http is accepted on a loopback host only)", endpoint)
	}
	return fmt.Errorf("yandex endpoint %q is not http(s)", endpoint)
}

// retryableStatus reports an answer worth repeating: throttling, a server
// failure, or 423 DiskResourceLockedError, which a folder that is still being
// deleted asynchronously answers for a moment.
func retryableStatus(status int) bool {
	switch status {
	case http.StatusLocked, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout, statusBandwidthLimitExceeded:
		return true
	}
	return false
}

func retryableMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodDelete, http.MethodPut, http.MethodPatch:
		return true
	}
	return false
}

func isNotFound(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

// isMissingParent reports a request refused because a parent folder of the
// path does not exist. The Disk API answers that with 409
// DiskPathDoesntExistsError; a 404 is accepted too.
func isMissingParent(err error) bool {
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.Status == http.StatusNotFound || apiErr.Name == "DiskPathDoesntExistsError"
}

// isExistingResource reports a copy the service refused because the
// destination exists, whatever overwrite asked for.
func isExistingResource(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict &&
		(apiErr.Name == "DiskResourceAlreadyExistsError" || apiErr.Name == "DiskPathPointsToExistentFileError")
}

func isExistingDirectory(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.Name == "DiskPathPointsToExistentDirectoryError"
}

func isExistingFile(err error) bool {
	var apiErr *apiError
	return errors.As(err, &apiErr) && apiErr.Name == "DiskPathPointsToExistentFileError"
}

func (c *apiClient) url(methodPath string, query url.Values) string {
	if len(query) == 0 {
		return c.baseURL + methodPath
	}
	return c.baseURL + methodPath + "?" + query.Encode()
}

// operationURL locates an asynchronous operation by its id.
func (c *apiClient) operationURL(operationID string) string {
	return c.baseURL + "/operations/" + operationID
}

// doJSON sends a request with an optional JSON body and returns the response
// for the caller to decode and close. Idempotent requests are retried on
// network errors, 429 and 5xx; a 401 refreshes the token once.
func (c *apiClient) doJSON(ctx context.Context, method, methodPath string, query url.Values, payload any) (*http.Response, error) {
	var body []byte
	if payload != nil {
		var err error
		if body, err = json.Marshal(payload); err != nil {
			return nil, fmt.Errorf("encode yandex request: %w", err)
		}
	}
	retries := 0
	refreshed := false
	for {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		// each attempt has its own deadline, which also covers reading the
		// answer: the caller closes the body, and closing ends the attempt
		attemptCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
		req, err := http.NewRequestWithContext(attemptCtx, method, c.url(methodPath, query), reader)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("build yandex request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		token, tokenErr := c.tokens.Token()
		if tokenErr != nil {
			cancel()
			return nil, tokenErr
		}
		req.Header.Set("Authorization", authTokenPrefix+token)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			cancel()
			if ctx.Err() != nil || !retryableMethod(method) || retries >= c.maxRetries {
				return nil, fmt.Errorf("yandex %s %s: %w", method, methodPath, err)
			}
			retries++
			if sleepErr := sleep(ctx, c.backoff(retries)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized && !refreshed {
			resp.Body.Close()
			cancel()
			c.tokens.Invalidate(token)
			refreshed = true
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}
		apiErr := decodeAPIError(resp)
		cancel()
		if retryableStatus(resp.StatusCode) && retryableMethod(method) && retries < c.maxRetries {
			retries++
			if sleepErr := sleep(ctx, c.backoff(retries)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}
		return nil, apiErr
	}
}

// cancelOnClose ends a request's context once its body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

func (c *apiClient) backoff(attempt int) time.Duration {
	delay := c.retryBase << (attempt - 1)
	if delay > 2*time.Second {
		delay = 2 * time.Second
	}
	return delay
}

func (c *apiClient) getJSON(ctx context.Context, methodPath string, query url.Values, out any) error {
	resp, err := c.doJSON(ctx, http.MethodGet, methodPath, query, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err = json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode yandex %s: %w", methodPath, err)
	}
	return nil
}

func decodeAPIError(resp *http.Response) error {
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	apiErr := &apiError{Status: resp.StatusCode}
	if len(body) > 0 {
		var parsed struct {
			Error       string `json:"error"`
			Description string `json:"description"`
			Message     string `json:"message"`
		}
		if json.Unmarshal(body, &parsed) == nil {
			apiErr.Name, apiErr.Description, apiErr.Message = parsed.Error, parsed.Description, parsed.Message
		} else {
			apiErr.Message = strings.TrimSpace(string(body))
		}
	}
	return apiErr
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// stat returns the metadata of one path.
func (c *apiClient) stat(ctx context.Context, path string) (*resource, error) {
	res := &resource{}
	if err := c.getJSON(ctx, "/resources", url.Values{"path": {path}}, res); err != nil {
		return nil, err
	}
	return res, nil
}

// list returns one page of a folder's children in name order.
func (c *apiClient) list(ctx context.Context, path string, offset uint64) (*resource, error) {
	query := url.Values{
		"path":   {path},
		"limit":  {strconv.Itoa(listPageSize)},
		"offset": {strconv.FormatUint(offset, 10)},
		"sort":   {"name"},
	}
	res := &resource{}
	if err := c.getJSON(ctx, "/resources", query, res); err != nil {
		return nil, err
	}
	return res, nil
}

// mkdir creates a folder. An existing folder is success; an existing file is
// an error, since the API answers both with 409 and only the name tells them
// apart.
func (c *apiClient) mkdir(ctx context.Context, path string) error {
	resp, err := c.doJSON(ctx, http.MethodPut, "/resources", url.Values{"path": {path}}, nil)
	if err == nil {
		resp.Body.Close()
		return nil
	}
	if isExistingDirectory(err) {
		return nil
	}
	if isExistingFile(err) {
		return fmt.Errorf("yandex path %s is an existing file: %w", path, err)
	}
	return err
}

// remove deletes a file or folder, recursively for a folder. With trash the
// object moves to the trash and keeps occupying quota.
func (c *apiClient) remove(ctx context.Context, path string, trash bool) error {
	query := url.Values{
		"path":        {path},
		"permanently": {strconv.FormatBool(!trash)},
	}
	resp, err := c.doJSON(ctx, http.MethodDelete, "/resources", query, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		return c.waitOperationResponse(ctx, resp)
	}
	resp.Body.Close()
	return nil
}

// copyResource copies a file server-side.
func (c *apiClient) copyResource(ctx context.Context, from, to string, overwrite bool) error {
	query := url.Values{
		"from":      {from},
		"path":      {to},
		"overwrite": {strconv.FormatBool(overwrite)},
	}
	resp, err := c.doJSON(ctx, http.MethodPost, "/resources/copy", query, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		return c.waitOperationResponse(ctx, resp)
	}
	resp.Body.Close()
	return nil
}

// setProperties writes resource metadata. custom_properties is a map of user
// attributes; an empty string records an attribute without a value.
func (c *apiClient) setProperties(ctx context.Context, path string, properties map[string]string) error {
	payload := map[string]any{"custom_properties": properties}
	resp, err := c.doJSON(ctx, http.MethodPatch, "/resources", url.Values{"path": {path}}, payload)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// uploadHref asks where to PUT a file's bytes.
func (c *apiClient) uploadHref(ctx context.Context, path string, overwrite bool) (*link, error) {
	query := url.Values{
		"path":      {path},
		"overwrite": {strconv.FormatBool(overwrite)},
	}
	l := &link{}
	if err := c.getJSON(ctx, "/resources/upload", query, l); err != nil {
		return nil, err
	}
	if l.Href == "" {
		return nil, fmt.Errorf("yandex upload link for %s is empty", path)
	}
	return l, nil
}

// downloadHref asks where to GET a file's bytes.
func (c *apiClient) downloadHref(ctx context.Context, path string) (*link, error) {
	l := &link{}
	if err := c.getJSON(ctx, "/resources/download", url.Values{"path": {path}}, l); err != nil {
		return nil, err
	}
	if l.Href == "" {
		return nil, fmt.Errorf("yandex download link for %s is empty", path)
	}
	return l, nil
}

// upload PUTs the object body to the href and returns the number of bytes
// copied. The href is not reused: a failed upload needs a fresh one.
func (c *apiClient) upload(ctx context.Context, href string, body io.Reader, size int64, contentType string) (int64, error) {
	if err := c.checkHref(href); err != nil {
		return 0, err
	}
	counter := &countingReader{reader: body}
	var requestBody io.Reader = counter
	if size > 0 {
		// report a short body as io.ErrUnexpectedEOF; the transport would only
		// say the lengths disagree
		requestBody = &sizedReader{reader: counter, remaining: size}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, href, requestBody)
	if err != nil {
		return 0, fmt.Errorf("build yandex upload request: %w", err)
	}
	if size > 0 {
		req.ContentLength = size
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return counter.n, fmt.Errorf("upload to yandex: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return counter.n, decodeAPIError(resp)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	return counter.n, nil
}

// openRange asks for a new download href and opens a byte range on it. A short
// body is the caller's to detect; a server that ignores the range is refused
// here, since the bytes would come from the wrong offset.
func (c *apiClient) openRange(ctx context.Context, path string, offset, size int64) (*http.Response, error) {
	download, err := c.downloadHref(ctx, path)
	if err != nil {
		return nil, err
	}
	if err = c.checkHref(download.Href); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, download.Href, nil)
	if err != nil {
		return nil, fmt.Errorf("build yandex download request: %w", err)
	}
	switch {
	case size > 0:
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+size-1))
	case offset > 0:
		// no size: the rest of the object from offset
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download from yandex: %w", err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if offset > 0 && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return nil, fmt.Errorf("yandex download for %s ignored the byte range (status %d)", path, resp.StatusCode)
		}
		return resp, nil
	}
	return nil, decodeAPIError(resp)
}

// checkHref refuses a transfer link that is not https (or that downgrades an
// https configuration), so a compromised or misconfigured endpoint cannot
// redirect object bytes or credentials to a plaintext or non-HTTP target.
func (c *apiClient) checkHref(href string) error {
	parsed, err := url.Parse(href)
	if err != nil {
		return fmt.Errorf("parse yandex transfer link: %w", err)
	}
	if parsed.Host == "" {
		return fmt.Errorf("yandex transfer link %q has no host", href)
	}
	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		base, baseErr := url.Parse(c.baseURL)
		if baseErr == nil && base.Scheme == "https" {
			return fmt.Errorf("yandex transfer link %q would downgrade https to http", href)
		}
		return nil
	}
	return fmt.Errorf("yandex transfer link %q is not http(s)", href)
}

// waitOperationResponse follows the operation a 202 answer points at.
func (c *apiClient) waitOperationResponse(ctx context.Context, resp *http.Response) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if err != nil {
		return fmt.Errorf("read yandex operation link: %w", err)
	}
	op := &link{}
	if err = json.Unmarshal(body, op); err != nil || op.Href == "" {
		// a 202 promises an operation to follow; without its link the outcome
		// is unknown, so the request is not reported done
		return fmt.Errorf("yandex answered 202 without an operation link: %q", strings.TrimSpace(string(body)))
	}
	return c.waitOperation(ctx, op.Href)
}

// waitOperation polls an asynchronous operation until it finishes.
func (c *apiClient) waitOperation(ctx context.Context, href string) error {
	if err := c.checkHref(href); err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, operationWait)
	defer cancel()
	refreshed := false
	for {
		if err := c.pollOperation(waitCtx, href, &refreshed); err != errOperationPending {
			return err
		}
		if err := sleep(waitCtx, c.pollEvery); err != nil {
			return fmt.Errorf("wait for yandex operation: %w", err)
		}
	}
}

// errOperationPending is pollOperation's answer for an operation that has not
// finished yet.
var errOperationPending = errors.New("yandex operation in progress")

// pollOperation asks once for an operation's status, within one request
// deadline. A 401 refreshes the token once per wait, as doJSON does; a token
// the API keeps rejecting ends the wait instead of polling without a pause.
// A failed poll is retried like a failed request: the operation keeps running
// on the service, and a long wait should not end on one throttled answer or
// dropped connection.
func (c *apiClient) pollOperation(waitCtx context.Context, href string, refreshed *bool) error {
	retries := 0
	for {
		pollCtx, cancel := context.WithTimeout(waitCtx, c.requestTimeout)
		err := c.pollOnce(pollCtx, href, refreshed)
		cancel()
		if err == errRetryPoll {
			continue
		}
		if retries < c.maxRetries && waitCtx.Err() == nil && retryablePollError(err) {
			retries++
			if sleepErr := sleep(waitCtx, c.backoff(retries)); sleepErr != nil {
				return fmt.Errorf("wait for yandex operation: %w", sleepErr)
			}
			continue
		}
		return err
	}
}

func retryablePollError(err error) bool {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return retryableStatus(apiErr.Status)
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr) || errors.Is(err, context.DeadlineExceeded)
}

// errRetryPoll asks pollOperation to poll again at once, after a refresh.
var errRetryPoll = errors.New("retry the yandex operation poll")

func (c *apiClient) pollOnce(ctx context.Context, href string, refreshed *bool) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, href, nil)
	if err != nil {
		return fmt.Errorf("build yandex operation request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	token, tokenErr := c.tokens.Token()
	if tokenErr != nil {
		return tokenErr
	}
	req.Header.Set("Authorization", authTokenPrefix+token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("poll yandex operation: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized && !*refreshed {
		resp.Body.Close()
		c.tokens.Invalidate(token)
		*refreshed = true
		return errRetryPoll
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeAPIError(resp)
	}
	*refreshed = false
	status := &operationStatus{}
	err = json.NewDecoder(resp.Body).Decode(status)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("decode yandex operation status: %w", err)
	}
	switch status.Status {
	case "success":
		return nil
	case "failure", "failed":
		return fmt.Errorf("yandex operation %s failed", href)
	}
	return errOperationPending
}

// statAfterWrite reads the object the service just finalized, retrying while
// it is still being committed.
func (c *apiClient) statAfterWrite(ctx context.Context, path string) (*resource, error) {
	var lastErr error
	for attempt := 0; attempt < c.statTries; attempt++ {
		if attempt > 0 {
			if err := sleep(ctx, c.pollEvery); err != nil {
				return nil, err
			}
		}
		res, err := c.stat(ctx, path)
		if err == nil {
			return res, nil
		}
		lastErr = err
		var apiErr *apiError
		if !errors.As(err, &apiErr) || !(apiErr.Status == http.StatusNotFound || retryableStatus(apiErr.Status)) {
			return nil, err
		}
	}
	return nil, lastErr
}

type sizedReader struct {
	reader    io.Reader
	remaining int64
}

func (r *sizedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF && r.remaining > 0 {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

type countingReader struct {
	reader io.Reader
	n      int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.n += int64(n)
	return n, err
}

// ErrDirectory is returned for an operation that only applies to a file.
var ErrDirectory = errors.New("yandex path is a directory")

// ErrObjectMissingWithParent is returned when the object and its folder are
// both gone: the mount's remote folder disappeared, which must not read as
// every object under it having been deleted.
var ErrObjectMissingWithParent = errors.New("yandex object and its parent folder are missing")
