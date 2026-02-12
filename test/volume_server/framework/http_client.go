package framework

import (
	"io"
	"net/http"
	"testing"
	"time"
)

func NewHTTPClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

func DoRequest(t testing.TB, client *http.Client, req *http.Request) *http.Response {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("http request %s %s: %v", req.Method, req.URL.String(), err)
	}
	return resp
}

func ReadAllAndClose(t testing.TB, resp *http.Response) []byte {
	t.Helper()
	if resp == nil {
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return body
}
