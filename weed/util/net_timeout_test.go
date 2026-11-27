package util

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func TestMain(m *testing.M) {
	// Conn reports every read and write to the stats channels; weed/server
	// drains them in a weed process, a test binary of this package has to.
	go stats.NewServerStats().Start()
	os.Exit(m.Run())
}

const (
	testIdleTimeout = 200 * time.Millisecond
	// testQuietFor is how long the quiet handlers work without reading or
	// writing: well past the idle timeout.
	testQuietFor = 3 * testIdleTimeout
)

// countingListener counts the connections it accepts.
type countingListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// ctxState is what the test handlers answer: "ok" while the request context
// is live, else its error.
func ctxState(ctx context.Context) string {
	if err := ctx.Err(); err != nil {
		return err.Error()
	}
	return "ok"
}

// idleTimeoutTestServer serves the test handlers through a Listener whose
// activity timeout is testIdleTimeout, and /quiet and /upload work quietly
// for testQuietFor. mode is "http", "https" or "h2"; pause wraps the
// handlers in PauseIdleTimeoutWhileHandling.
type idleTimeoutTestServer struct {
	*httptest.Server
	listener *countingListener
	quietFor time.Duration
	// bodyErr gets the error of the body read on /upload when it fails.
	bodyErr chan error
	// hungUp gets the context state /wait saw once it stopped waiting.
	hungUp chan string
	// downloadErr gets the outcome of /download's writes.
	downloadErr chan error
}

func newIdleTimeoutTestServer(t *testing.T, mode string, pause bool) *idleTimeoutTestServer {
	t.Helper()
	return newIdleTimeoutTestServerWith(t, mode, pause, testIdleTimeout, testQuietFor)
}

// newIdleTimeoutTestServerWith is newIdleTimeoutTestServer with its own idle
// timeout and quiet time.
func newIdleTimeoutTestServerWith(t *testing.T, mode string, pause bool, idle, quietFor time.Duration) *idleTimeoutTestServer {
	t.Helper()
	s := &idleTimeoutTestServer{
		quietFor: quietFor,
		bodyErr:  make(chan error, 1),
		hungUp:   make(chan string, 1),

		downloadErr: make(chan error, 1),
	}
	mux := http.NewServeMux()
	quiet := func(ctx context.Context) {
		select {
		case <-time.After(s.quietFor):
		case <-ctx.Done():
		}
	}
	mux.HandleFunc("/quiet", func(w http.ResponseWriter, r *http.Request) {
		quiet(r.Context())
		fmt.Fprint(w, ctxState(r.Context()))
	})
	mux.HandleFunc("/ctx", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, ctxState(r.Context()))
	})
	mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			s.bodyErr <- err
			return
		}
		quiet(r.Context())
		fmt.Fprintf(w, "%s %d", ctxState(r.Context()), n)
	})
	mux.HandleFunc("/handling", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		tc, _ := r.Context().Value(connContextKey{}).(*Conn)
		if tc == nil {
			fmt.Fprint(w, "no conn")
			return
		}
		tc.mu.Lock()
		defer tc.mu.Unlock()
		fmt.Fprint(w, tc.handling)
	})
	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		chunk := bytes.Repeat([]byte("z"), 64<<10)
		var err error
		for i := 0; i < 1024 && err == nil; i++ { // 64 MiB
			_, err = w.Write(chunk)
		}
		s.downloadErr <- err
	})
	mux.HandleFunc("/wait", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(10 * time.Second):
		case <-r.Context().Done():
		}
		s.hungUp <- ctxState(r.Context())
	})
	var h http.Handler = mux
	if pause {
		h = PauseIdleTimeoutWhileHandling(mux)
	}

	s.Server = httptest.NewUnstartedServer(h)
	s.listener = &countingListener{Listener: s.Server.Listener}
	s.Server.Listener = &Listener{Listener: s.listener, Timeout: idle}
	s.Server.Config.ConnContext = ConnContext
	switch mode {
	case "http":
		s.Server.Start()
	case "https":
		s.Server.StartTLS()
	case "h2":
		s.Server.EnableHTTP2 = true
		s.Server.StartTLS()
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	t.Cleanup(s.Server.Close)
	return s
}

// client returns a client that sends every request over one connection.
func (s *idleTimeoutTestServer) client() *http.Client {
	tr := s.Server.Client().Transport.(*http.Transport).Clone()
	tr.MaxConnsPerHost = 1
	tr.MaxIdleConnsPerHost = 1
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func (s *idleTimeoutTestServer) send(c *http.Client, method, path string, body io.Reader) (string, error) {
	req, err := http.NewRequest(method, s.Server.URL+path, body)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("%s %s: read answer: %w", method, path, err)
	}
	return string(b), nil
}

func (s *idleTimeoutTestServer) do(t *testing.T, c *http.Client, method, path string, body io.Reader) string {
	t.Helper()
	got, err := s.send(c, method, path, body)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func (s *idleTimeoutTestServer) rawConn(t *testing.T) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", s.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestQuietHandlerKeepsItsContext(t *testing.T) {
	for _, mode := range []string{"http", "https"} {
		t.Run(mode, func(t *testing.T) {
			s := newIdleTimeoutTestServer(t, mode, true)
			c := s.client()
			if got := s.do(t, c, http.MethodGet, "/quiet", nil); got != "ok" {
				t.Fatalf("quiet request: context %s", got)
			}
			if got := s.do(t, c, http.MethodGet, "/ctx", nil); got != "ok" {
				t.Fatalf("next request on the connection: context %s", got)
			}
			if n := s.listener.accepted.Load(); n != 1 {
				t.Fatalf("requests used %d connections, want 1", n)
			}
		})
	}
}

// TestQuietHandlerWithoutPauseLosesItsContext shows what the pause prevents:
// the idle timeout ends net/http's background read, which cancels the
// context of the quiet request and of the next request on the connection.
func TestQuietHandlerWithoutPauseLosesItsContext(t *testing.T) {
	s := newIdleTimeoutTestServer(t, "http", false)
	c := s.client()
	if got := s.do(t, c, http.MethodGet, "/quiet", nil); got != context.Canceled.Error() {
		t.Fatalf("quiet request: context %s, want %v", got, context.Canceled)
	}
	if got := s.do(t, c, http.MethodGet, "/ctx", nil); got != context.Canceled.Error() {
		t.Fatalf("next request on the connection: context %s, want %v", got, context.Canceled)
	}
	if n := s.listener.accepted.Load(); n != 1 {
		t.Fatalf("requests used %d connections, want 1", n)
	}
}

func TestQuietHandlerAfterItsBodyKeepsItsContext(t *testing.T) {
	for _, mode := range []string{"http", "https"} {
		t.Run(mode, func(t *testing.T) {
			s := newIdleTimeoutTestServer(t, mode, true)
			c := s.client()
			body := bytes.Repeat([]byte("x"), 1<<20)
			if got, want := s.do(t, c, http.MethodPost, "/upload", bytes.NewReader(body)), fmt.Sprintf("ok %d", len(body)); got != want {
				t.Fatalf("upload: %q, want %q", got, want)
			}
			if got := s.do(t, c, http.MethodGet, "/ctx", nil); got != "ok" {
				t.Fatalf("next request on the connection: context %s", got)
			}
			if n := s.listener.accepted.Load(); n != 1 {
				t.Fatalf("requests used %d connections, want 1", n)
			}
		})
	}
}

// TestStalledBodyStillTimesOut checks that a client that stops sending its
// body is still cut off by the idle timeout.
func TestStalledBodyStillTimesOut(t *testing.T) {
	s := newIdleTimeoutTestServer(t, "http", true)
	c := s.rawConn(t)
	if _, err := io.WriteString(c, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n0123456789"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-s.bodyErr:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("body read failed with %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * testIdleTimeout):
		t.Fatal("body read still waiting on a stalled client")
	}
}

// TestIdleConnectionStillTimesOut checks that a kept-alive connection with
// no request on it is still closed after the idle timeout.
func TestIdleConnectionStillTimesOut(t *testing.T) {
	s := newIdleTimeoutTestServer(t, "http", true)
	c := s.rawConn(t)
	if _, err := io.WriteString(c, "GET /ctx HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "ok" {
		t.Fatalf("answer %q", b)
	}
	start := time.Now()
	c.SetReadDeadline(time.Now().Add(10 * testIdleTimeout))
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("idle connection: read got %v, want the server to close it", err)
	}
	if waited := time.Since(start); waited < testIdleTimeout/2 {
		t.Fatalf("server closed the idle connection after %v, before the idle timeout", waited)
	}
}

// TestOnlyHTTP1RequestsArePaused checks the scope of the pause: an HTTP/1.1
// request holds it while its handler runs, with or without a body, and an
// HTTP/2 request leaves the connection's timeout alone.
func TestOnlyHTTP1RequestsArePaused(t *testing.T) {
	for mode, want := range map[string]string{"http": "1", "https": "1", "h2": "0"} {
		t.Run(mode, func(t *testing.T) {
			s := newIdleTimeoutTestServer(t, mode, true)
			c := s.client()
			for _, body := range []string{"", "x"} {
				method := http.MethodGet
				if body != "" {
					method = http.MethodPost
				}
				if got := s.do(t, c, method, "/handling", strings.NewReader(body)); got != want {
					t.Fatalf("%s with body %q: %s requests paused, want %s", method, body, got, want)
				}
			}
		})
	}
}

// TestStalledDownloadStillTimesOut checks that a client that stops reading
// an answer is still cut off by the idle timeout: over HTTP/1.1 the write
// waits on the socket, over HTTP/2 on flow-control credit, which only the
// frame reader's deadline ends. Over TLS, closing the connection after the
// failed write can take up to the 5 s crypto/tls gives its close_notify.
func TestStalledDownloadStillTimesOut(t *testing.T) {
	for _, mode := range []string{"http", "https", "h2"} {
		t.Run(mode, func(t *testing.T) {
			s := newIdleTimeoutTestServer(t, mode, true)
			c := s.client()
			c.Timeout = 0 // the client must not give up first
			resp, err := c.Get(s.Server.URL + "/download")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			select {
			case err := <-s.downloadErr:
				if err == nil {
					t.Fatal("download finished although the client read none of it")
				}
			case <-time.After(8 * time.Second):
				t.Fatal("download still waiting on a client that stopped reading")
			}
		})
	}
}

// TestClientHangUpStillCancelsTheContext checks that the background read
// still sees a client close its connection while the read is paused.
func TestClientHangUpStillCancelsTheContext(t *testing.T) {
	s := newIdleTimeoutTestServer(t, "http", true)
	c := s.rawConn(t)
	if _, err := io.WriteString(c, "GET /wait HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * testIdleTimeout)
	c.Close()
	select {
	case got := <-s.hungUp:
		if got != context.Canceled.Error() {
			t.Fatalf("context %s after the client hung up, want %v", got, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not see the client hang up")
	}
}

// TestPauseUnderConcurrentRequests runs quiet requests, uploads and quick
// requests at once over several connections; run it with -race. A request
// can wait for its turn on a connection, so the idle timeout here is 1 s:
// a slow run must not find the connection closed between two requests.
func TestPauseUnderConcurrentRequests(t *testing.T) {
	for _, mode := range []string{"http", "https"} {
		t.Run(mode, func(t *testing.T) {
			s := newIdleTimeoutTestServerWith(t, mode, true, time.Second, 1500*time.Millisecond)
			var clients []*http.Client
			for i := 0; i < 4; i++ {
				clients = append(clients, s.client())
			}
			body := bytes.Repeat([]byte("y"), 64<<10)
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					c := clients[i%len(clients)]
					for j := 0; j < 2; j++ {
						var got, want string
						var err error
						switch (i + j) % 3 {
						case 0:
							want = "ok"
							got, err = s.send(c, http.MethodGet, "/quiet", nil)
						case 1:
							want = fmt.Sprintf("ok %d", len(body))
							got, err = s.send(c, http.MethodPost, "/upload", bytes.NewReader(body))
						default:
							want = "ok"
							got, err = s.send(c, http.MethodGet, "/ctx", nil)
						}
						if err != nil || got != want {
							t.Errorf("request %d.%d: %q (%v), want %q", i, j, got, err, want)
						}
					}
				}(i)
			}
			wg.Wait()
		})
	}
}

// TestPauseHoldsAWaitingRead checks a read that is already waiting with the
// activity deadline, as net/http's background read is when the handler
// begins: the pause lifts its deadline, and its end puts the read back
// under the timeout.
func TestPauseHoldsAWaitingRead(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Conn{Conn: a, Timeout: testIdleTimeout}
	readErr := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		readErr <- err
	}()
	time.Sleep(testIdleTimeout / 4)
	p := &idleTimeoutPause{conn: c}
	p.begin()
	select {
	case err := <-readErr:
		t.Fatalf("read ended while the pause held it: %v", err)
	case <-time.After(3 * testIdleTimeout):
	}
	p.end()
	select {
	case err := <-readErr:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read ended with %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * testIdleTimeout):
		t.Fatal("read still waiting after the pause ended")
	}
}

// TestConnPauseRacesWithReadsAndWrites exercises pauseIdleTimeout and
// resumeIdleTimeout against concurrent reads and writes of one Conn; run it
// with -race.
func TestConnPauseRacesWithReadsAndWrites(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Conn{Conn: a, Timeout: time.Second}
	go io.Copy(io.Discard, b)
	go func() {
		for i := 0; i < 2000; i++ {
			if _, err := b.Write([]byte{1}); err != nil {
				return
			}
		}
	}()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		buf := make([]byte, 16)
		for read := 0; read < 2000; {
			n, err := c.Read(buf)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			read += n
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if _, err := c.Write([]byte{2}); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			p := &idleTimeoutPause{conn: c}
			p.begin()
			p.end()
		}
	}()
	wg.Wait()
	c.mu.Lock()
	handling := c.handling
	c.mu.Unlock()
	if handling != 0 {
		t.Fatalf("handling = %d after every pause ended", handling)
	}
}

// TestPauseEndsOnceAndNotAfterTheHandler checks the pause bookkeeping: a
// body that ends after its handler returned (read by a goroutine the
// handler left behind) does not leave the connection paused.
func TestPauseEndsOnceAndNotAfterTheHandler(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Conn{Conn: a, Timeout: time.Second}
	p := &idleTimeoutPause{conn: c}
	body := &pauseAtBodyEnd{ReadCloser: io.NopCloser(strings.NewReader("")), pause: p}
	p.begin()
	p.begin()
	p.end()
	p.end()
	if _, err := body.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read %v, want EOF", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handling != 0 {
		t.Fatalf("handling = %d, want 0", c.handling)
	}
}

// TestPauseRemovesMultipartTempFiles checks that a multipart form the
// handler parses into temporary files is removed when the handler returns:
// net/http removes only those of a form parsed on the Request it passed in,
// not on the copy the pause hands the handler.
func TestPauseRemovesMultipartTempFiles(t *testing.T) {
	tmp := t.TempDir()
	for _, k := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(k, tmp)
	}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Conn{Conn: a, Timeout: time.Second}

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile("file", "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	fw.Write(bytes.Repeat([]byte("x"), 4096))
	mw.Close()
	r := httptest.NewRequest(http.MethodPost, "/upload", &body)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	r = r.WithContext(context.WithValue(r.Context(), connContextKey{}, c))

	h := PauseIdleTimeoutWhileHandling(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1); err != nil {
			t.Errorf("parse: %v", err)
			return
		}
		if entries, _ := os.ReadDir(tmp); len(entries) == 0 {
			t.Errorf("form parsed without a temporary file")
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), r)

	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("left behind %s", e.Name())
	}
}
