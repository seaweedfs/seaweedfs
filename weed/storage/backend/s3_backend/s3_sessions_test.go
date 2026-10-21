package s3_backend

import (
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func backendTransport(t *testing.T, s *S3BackendStorage) *http.Transport {
	t.Helper()
	client, ok := s.conn.(*s3.S3)
	if !ok {
		t.Fatalf("unexpected client type %T", s.conn)
	}
	httpClient := client.Client.Config.HTTPClient
	if httpClient == nil {
		return nil
	}
	transport, _ := httpClient.Transport.(*http.Transport)
	return transport
}

func TestS3BackendHTTPOptionsReachTheClient(t *testing.T) {
	s, err := newS3BackendStorage(testProperties{
		"endpoint":                        "http://127.0.0.1:1",
		"disable_http2":                   "true",
		"max_connections":                 "64",
		"idle_connection_timeout_seconds": "50",
	}, "", "http-options")
	if err != nil {
		t.Fatal(err)
	}
	transport := backendTransport(t, s)
	if transport == nil {
		t.Fatal("configured HTTP options did not reach the S3 client")
	}
	if transport.ForceAttemptHTTP2 || transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 {
		t.Errorf("HTTP/2 not disabled: ForceAttemptHTTP2=%v TLSNextProto=%v", transport.ForceAttemptHTTP2, transport.TLSNextProto)
	}
	if transport.MaxConnsPerHost != 64 {
		t.Errorf("MaxConnsPerHost = %d, want 64", transport.MaxConnsPerHost)
	}
	if transport.IdleConnTimeout != 50*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 50s", transport.IdleConnTimeout)
	}

	props := s.ToProperties()
	if props["disable_http2"] != "true" || props["max_connections"] != "64" || props["idle_connection_timeout_seconds"] != "50" {
		t.Errorf("ToProperties: %v", props)
	}
}

func TestS3BackendDefaultHTTPClientUnchanged(t *testing.T) {
	s, err := newS3BackendStorage(testProperties{"endpoint": "http://127.0.0.1:2"}, "", "http-defaults")
	if err != nil {
		t.Fatal(err)
	}
	if transport := backendTransport(t, s); transport != nil && transport != http.DefaultTransport {
		t.Fatalf("no options set, yet the client got a custom transport %+v", transport)
	}
	if (httpOptions{}).newTransport() != nil {
		t.Fatal("zero options must keep the default client")
	}
}

func TestS3BackendSessionsDifferByHTTPOptions(t *testing.T) {
	a, err := createSession("", "", "us-east-1", "http://127.0.0.1:3", true, httpOptions{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := createSession("", "", "us-east-1", "http://127.0.0.1:3", true, httpOptions{disableHTTP2: true})
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("backends with different HTTP options share one client")
	}
	c, err := createSession("", "", "us-east-1", "http://127.0.0.1:3", true, httpOptions{disableHTTP2: true})
	if err != nil {
		t.Fatal(err)
	}
	if b != c {
		t.Fatal("the same endpoint and options must reuse the cached client")
	}
}

func TestParseNonNegative(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{{"", 0}, {"0", 0}, {"12", 12}, {"-1", 0}, {"x", 0}} {
		if got := parseNonNegative(c.in, "test"); got != c.want {
			t.Errorf("parseNonNegative(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestS3BackendIdleTimeoutOverflowFallsBackToDefault(t *testing.T) {
	maxSeconds := int64(math.MaxInt64 / int64(time.Second))
	for i, c := range []struct {
		in   string
		want time.Duration
	}{
		{strconv.FormatInt(maxSeconds, 10), time.Duration(maxSeconds) * time.Second},
		{strconv.FormatInt(maxSeconds+1, 10), 0},
		{"10000000000", 0},
		{"-1", 0},
		{"x", 0},
	} {
		s, err := newS3BackendStorage(testProperties{
			"endpoint":                        "http://127.0.0.1:4",
			"idle_connection_timeout_seconds": c.in,
		}, "", "idle-overflow-"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		if s.httpOptions.idleConnTimeout != c.want {
			t.Errorf("idle_connection_timeout_seconds=%s: timeout %v, want %v", c.in, s.httpOptions.idleConnTimeout, c.want)
		}
		want := strconv.FormatInt(int64(c.want/time.Second), 10)
		if got := s.ToProperties()["idle_connection_timeout_seconds"]; got != want {
			t.Errorf("idle_connection_timeout_seconds=%s: ToProperties exports %s, want %s", c.in, got, want)
		}
	}
}
