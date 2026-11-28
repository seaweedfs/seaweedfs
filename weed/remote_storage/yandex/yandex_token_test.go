package yandex

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
)

func TestParseToken(t *testing.T) {
	bare, err := parseToken("  tok  ")
	if err != nil || bare.AccessToken != "tok" || bare.RefreshToken != "" {
		t.Fatalf("bare token = %+v, err %v", bare, err)
	}

	stored := `{"access_token":"a","refresh_token":"r","token_type":"bearer","expiry":"2030-01-02T03:04:05Z"}`
	token, err := parseToken(stored)
	if err != nil {
		t.Fatalf("json token: %v", err)
	}
	if token.AccessToken != "a" || token.RefreshToken != "r" || token.Expiry.Year() != 2030 {
		t.Fatalf("json token = %+v", token)
	}

	if _, err = parseToken(""); err == nil {
		t.Fatal("an empty token was accepted")
	}
	if _, err = parseToken(`{"token_type":"bearer"}`); err == nil {
		t.Fatal("a token without credentials was accepted")
	}
	if _, err = parseToken(`{"access_token":`); err == nil {
		t.Fatal("malformed json was accepted")
	}
}

func TestMarshalTokenRoundTrip(t *testing.T) {
	encoded, err := MarshalToken(&oauth2.Token{AccessToken: "a", RefreshToken: "r", TokenType: "bearer"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	parsed, err := parseToken(encoded)
	if err != nil {
		t.Fatalf("parse back: %v", err)
	}
	if parsed.AccessToken != "a" || parsed.RefreshToken != "r" {
		t.Fatalf("round trip = %+v", parsed)
	}
}

type fakeTokenServer struct {
	mu       sync.Mutex
	requests []map[string]string
	status   int
	body     string
	count    int
}

func (f *fakeTokenServer) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		values := map[string]string{}
		for key := range r.PostForm {
			values[key] = r.PostForm.Get(key)
		}
		f.mu.Lock()
		f.requests = append(f.requests, values)
		f.count++
		count := f.count
		status := f.status
		body := f.body
		f.mu.Unlock()
		if status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":  fmt.Sprintf("access-%d", count),
			"refresh_token": fmt.Sprintf("refresh-%d", count),
			"token_type":    "bearer",
			"expires_in":    3600,
		})
	}))
	t.Cleanup(server.Close)
	return server
}

func (f *fakeTokenServer) snapshot() []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]string(nil), f.requests...)
}

type tokenConf struct {
	token        string
	clientID     string
	clientSecret string
}

func (c tokenConf) conf(endpoint string) *remote_pb.RemoteConf {
	return &remote_pb.RemoteConf{
		Name:               "yd",
		Type:               "yandex",
		YandexToken:        c.token,
		YandexClientId:     c.clientID,
		YandexClientSecret: c.clientSecret,
		YandexEndpoint:     endpoint,
		YandexRoot:         "/root",
	}
}

func TestTokenRefreshOnExpiry(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	server := disk.server()
	t.Cleanup(server.Close)
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	expired := tokenConf{
		token:        `{"access_token":"stale","refresh_token":"refresh-0","token_type":"bearer","expiry":"2020-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)

	client, err := makeClient(expired.conf(server.URL+"/v1/disk"), server.Client(), tokenServer.URL)
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	yandexClient := client.(*yandexRemoteStorageClient)
	yandexClient.api.pollEvery = time.Millisecond

	if _, err = yandexClient.ReadFile(testLocation("bucket", "/a.txt"), 0, 5); err != nil {
		t.Fatalf("read: %v", err)
	}
	requests := tokens.snapshot()
	if len(requests) != 1 {
		t.Fatalf("token requests = %d, want 1", len(requests))
	}
	if requests[0]["grant_type"] != "refresh_token" || requests[0]["refresh_token"] != "refresh-0" {
		t.Fatalf("refresh request = %v", requests[0])
	}
	if requests[0]["client_id"] != "app" || requests[0]["client_secret"] != "secret" {
		t.Fatalf("client credentials missing from %v", requests[0])
	}
	for _, request := range disk.requests {
		if strings.HasPrefix(request.path, "/download/") {
			continue
		}
		if request.auth != "OAuth access-1" {
			t.Errorf("api request carried %q, want the refreshed token", request.auth)
		}
	}
}

func TestNoRefreshWhileTheTokenIsValid(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	server := disk.server()
	t.Cleanup(server.Close)
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	valid := tokenConf{
		token:        `{"access_token":"fresh","refresh_token":"refresh-0","token_type":"bearer","expiry":"2999-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)

	client, err := makeClient(valid.conf(server.URL+"/v1/disk"), server.Client(), tokenServer.URL)
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	if requests := tokens.snapshot(); len(requests) != 0 {
		t.Fatalf("the client refreshed at build time: %v", requests)
	}
	if _, err = client.StatFile(testLocation("bucket", "/a.txt")); err != nil {
		t.Fatalf("stat: %v", err)
	}
	if requests := tokens.snapshot(); len(requests) != 0 {
		t.Fatalf("the client refreshed a valid token: %v", requests)
	}
}

func TestRefreshOnUnauthorized(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	disk.unauthorizedOnce = true
	server := disk.server()
	t.Cleanup(server.Close)
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	valid := tokenConf{
		token:        `{"access_token":"first","refresh_token":"refresh-0","token_type":"bearer","expiry":"2999-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)

	client, err := makeClient(valid.conf(server.URL+"/v1/disk"), server.Client(), tokenServer.URL)
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	yandexClient := client.(*yandexRemoteStorageClient)
	yandexClient.api.pollEvery = time.Millisecond

	if _, err = yandexClient.StatFile(testLocation("bucket", "/a.txt")); err != nil {
		t.Fatalf("stat after a 401: %v", err)
	}
	if requests := tokens.snapshot(); len(requests) != 1 {
		t.Fatalf("token requests = %d, want 1", len(requests))
	}
	var lastAuth string
	for _, request := range disk.requests {
		if request.method == http.MethodGet && request.path == "/v1/disk/resources" {
			lastAuth = request.auth
		}
	}
	if lastAuth != "OAuth access-1" {
		t.Fatalf("the retried request carried %q, want the refreshed token", lastAuth)
	}
}

func TestRefreshTokenRotation(t *testing.T) {
	disk := newFakeDisk(t)
	disk.put("/root/bucket/a.txt", &fakeNode{data: []byte("hello")})
	server := disk.server()
	t.Cleanup(server.Close)
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	expired := tokenConf{
		token:        `{"access_token":"stale","refresh_token":"refresh-0","token_type":"bearer","expiry":"2020-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)

	client, err := makeClient(expired.conf(server.URL+"/v1/disk"), server.Client(), tokenServer.URL)
	if err != nil {
		t.Fatalf("make client: %v", err)
	}
	yandexClient := client.(*yandexRemoteStorageClient)

	if _, err = yandexClient.StatFile(testLocation("bucket", "/a.txt")); err != nil {
		t.Fatalf("stat: %v", err)
	}
	// the API rejected the token; the next refresh must use the rotated one
	yandexClient.api.tokens.Invalidate(yandexClient.api.tokens.tok.AccessToken)
	if _, err = yandexClient.StatFile(testLocation("bucket", "/a.txt")); err != nil {
		t.Fatalf("stat after invalidating: %v", err)
	}
	requests := tokens.snapshot()
	if len(requests) != 2 {
		t.Fatalf("token requests = %d, want 2", len(requests))
	}
	if requests[0]["refresh_token"] != "refresh-0" {
		t.Fatalf("first refresh used %v", requests[0])
	}
	if requests[1]["refresh_token"] != "refresh-1" {
		t.Fatalf("second refresh used %v, want the rotated refresh token", requests[1])
	}
}

// TestConcurrentUnauthorizedRefreshesOnce: requests that were all rejected
// with the same token refresh it once; a late 401 for a token already
// replaced does not rotate the refresh token again.
func TestConcurrentUnauthorizedRefreshesOnce(t *testing.T) {
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	valid := tokenConf{
		token:        `{"access_token":"first","refresh_token":"refresh-0","token_type":"bearer","expiry":"2999-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)
	source, err := newTokenSource(valid.conf(""), nil, tokenServer.URL)
	if err != nil {
		t.Fatalf("token source: %v", err)
	}

	source.Invalidate("first")
	refreshed, err := source.Token()
	if err != nil || refreshed == "first" {
		t.Fatalf("token after the first 401 = %q, err %v", refreshed, err)
	}
	source.Invalidate("first")
	if token, err := source.Token(); err != nil || token != refreshed {
		t.Fatalf("token after a late 401 = %q, err %v, want %q", token, err, refreshed)
	}
	if requests := tokens.snapshot(); len(requests) != 1 {
		t.Fatalf("token requests = %d, want 1", len(requests))
	}
}

func TestRefreshFailureExplainsHowToReissue(t *testing.T) {
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	expired := tokenConf{
		token:        `{"access_token":"stale","refresh_token":"refresh-0","token_type":"bearer","expiry":"2020-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	tokens := &fakeTokenServer{status: http.StatusBadRequest, body: `{"error":"invalid_grant","error_description":"Token expired"}`}
	tokenServer := tokens.server(t)

	source, err := newTokenSource(expired.conf(""), nil, tokenServer.URL)
	if err != nil {
		t.Fatalf("token source: %v", err)
	}
	if _, err = source.Token(); err == nil || !strings.Contains(err.Error(), "expired or revoked") {
		t.Fatalf("err = %v, want the reissue instruction", err)
	}
}

func TestBareTokenDoesNotRefresh(t *testing.T) {
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)
	source, err := newTokenSource(tokenConf{token: "bare", clientID: "app", clientSecret: "secret"}.conf(""), nil, tokenServer.URL)
	if err != nil {
		t.Fatalf("token source: %v", err)
	}
	token, err := source.Token()
	if err != nil || token != "bare" {
		t.Fatalf("token = %q, err %v", token, err)
	}
	source.Invalidate("bare")
	if token, err = source.Token(); err != nil || token != "bare" {
		t.Fatalf("after invalidate token = %q, err %v", token, err)
	}
	if requests := tokens.snapshot(); len(requests) != 0 {
		t.Fatalf("a bare token triggered a refresh: %v", requests)
	}
}

func TestTokenSourceIsSharedPerConfiguration(t *testing.T) {
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)

	disk := newFakeDisk(t)
	disk.mkdirAll("/root/bucket")
	server := disk.server()
	t.Cleanup(server.Close)

	conf := tokenConf{token: "bare", clientID: "", clientSecret: ""}.conf(server.URL + "/v1/disk")
	first, err := makeClient(conf, nil, "")
	if err != nil {
		t.Fatalf("make first client: %v", err)
	}
	second, err := makeClient(conf, nil, "")
	if err != nil {
		t.Fatalf("make second client: %v", err)
	}
	if first.(*yandexRemoteStorageClient).api.tokens != second.(*yandexRemoteStorageClient).api.tokens {
		t.Fatal("two clients of one configuration do not share a token source")
	}
}

func TestExchangeCode(t *testing.T) {
	tokens := &fakeTokenServer{}
	tokenServer := tokens.server(t)

	stored, err := ExchangeCode(context.Background(), "app", "secret", "the-code", tokenServer.Client(), tokenServer.URL)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	token, err := parseToken(stored)
	if err != nil {
		t.Fatalf("parse stored token: %v", err)
	}
	if token.AccessToken != "access-1" || token.RefreshToken != "refresh-1" {
		t.Fatalf("stored token = %+v", token)
	}
	requests := tokens.snapshot()
	if len(requests) != 1 || requests[0]["grant_type"] != "authorization_code" || requests[0]["code"] != "the-code" {
		t.Fatalf("exchange request = %v", requests)
	}
	if requests[0]["redirect_uri"] != RedirectURI {
		t.Fatalf("redirect_uri = %q, want %q", requests[0]["redirect_uri"], RedirectURI)
	}

	if _, err = ExchangeCode(context.Background(), "app", "secret", "", tokenServer.Client(), tokenServer.URL); err == nil {
		t.Fatal("an empty code was accepted")
	}
	if _, err = ExchangeCode(context.Background(), "", "secret", "code", tokenServer.Client(), tokenServer.URL); err == nil {
		t.Fatal("a missing client id was accepted")
	}
}

func TestAuthCodeURL(t *testing.T) {
	url := AuthCodeURL("app")
	if !strings.HasPrefix(url, "https://oauth.yandex.ru/authorize?") ||
		!strings.Contains(url, "client_id=app") ||
		!strings.Contains(url, "response_type=code") {
		t.Fatalf("authorize url = %q", url)
	}
	if !strings.Contains(url, "redirect_uri=") {
		t.Fatalf("authorize url lacks the redirect: %q", url)
	}
}

// TestTokenRefreshHasADeadline: a token endpoint that stops answering ends the
// refresh with the token request timeout; Token holds the source's lock while
// it refreshes, so an unbounded refresh would block every client.
func TestTokenRefreshHasADeadline(t *testing.T) {
	release := make(chan struct{})
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(stalled.Close)
	t.Cleanup(func() { close(release) })
	ResetTokenSources()
	t.Cleanup(ResetTokenSources)
	previous := tokenRequestTimeout
	tokenRequestTimeout = 50 * time.Millisecond
	t.Cleanup(func() { tokenRequestTimeout = previous })

	expired := tokenConf{
		token:        `{"access_token":"old","refresh_token":"refresh-0","token_type":"bearer","expiry":"2000-01-01T00:00:00Z"}`,
		clientID:     "app",
		clientSecret: "secret",
	}
	source, err := newTokenSource(expired.conf(""), nil, stalled.URL)
	if err != nil {
		t.Fatalf("token source: %v", err)
	}
	started := time.Now()
	if _, err = source.Token(); err == nil {
		t.Fatal("a refresh against a stalled endpoint succeeded")
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("the refresh took %v", elapsed)
	}

	if _, err = ExchangeCode(context.Background(), "app", "secret", "code", nil, stalled.URL); err == nil {
		t.Fatal("a code exchange against a stalled endpoint succeeded")
	}
}
