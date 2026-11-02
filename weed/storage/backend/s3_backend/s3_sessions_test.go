package s3_backend

import (
	"bufio"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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

func TestS3BackendDefaultTransportHasTimeouts(t *testing.T) {
	s, err := newS3BackendStorage(testProperties{"endpoint": "http://127.0.0.1:2"}, "", "http-defaults")
	if err != nil {
		t.Fatal(err)
	}
	transport := backendTransport(t, s)
	if transport == nil || transport == http.DefaultTransport {
		t.Fatalf("the client must not use the shared default transport, got %+v", transport)
	}
	if transport.ResponseHeaderTimeout != s3ReadTimeout {
		t.Errorf("ResponseHeaderTimeout = %v, want %v", transport.ResponseHeaderTimeout, s3ReadTimeout)
	}
	if transport.DialContext == nil {
		t.Error("no dialer with a connect timeout")
	}
	if !transport.ForceAttemptHTTP2 {
		t.Error("default options must keep HTTP/2")
	}
}

// stallingEndpoint accepts connections and holds them open. With a non-empty
// head, it answers each request with head and then sends nothing more.
func stallingEndpoint(t *testing.T, head string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			if head != "" {
				go func() {
					if _, err := http.ReadRequest(bufio.NewReader(c)); err == nil {
						c.Write([]byte(head))
					}
				}()
			}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return "http://" + ln.Addr().String()
}

func readFromStallingEndpoint(t *testing.T, head, wantErr string) {
	old := s3ReadTimeout
	s3ReadTimeout = 100 * time.Millisecond
	defer func() { s3ReadTimeout = old }()

	s, err := newS3BackendStorage(testProperties{
		"endpoint":              stallingEndpoint(t, head),
		"region":                "us-east-1",
		"bucket":                "b",
		"aws_access_key_id":     "k",
		"aws_secret_access_key": "s",
	}, "", "stalling")
	if err != nil {
		t.Fatal(err)
	}
	f := s.NewStorageFile("key", &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{{FileSize: 1024}}})

	done := make(chan error, 1)
	go func() {
		_, err := f.ReadAt(make([]byte, 8), 0)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("want an error containing %q from a stalled endpoint, got %v", wantErr, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a read from a stalled endpoint did not give up")
	}
}

func TestS3BackendReadFailsAgainstSilentEndpoint(t *testing.T) {
	readFromStallingEndpoint(t, "", "timeout")
}

func TestS3BackendReadFailsWhenBodyStalls(t *testing.T) {
	readFromStallingEndpoint(t, "HTTP/1.1 206 Partial Content\r\nContent-Length: 8\r\nContent-Range: bytes 0-7/1024\r\n\r\n", "no data for")
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
