package command

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api"
)

func TestS3ShutdownDrainsActiveRequestBeforeServeReturns(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(finish)
	httpClosing := make(chan struct{})
	server.Config.RegisterOnShutdown(func() { close(httpClosing) })

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := server.Client().Get(server.URL)
		if err != nil {
			t.Error(err)
			return
		}
		response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Errorf("active request returned %s", response.Status)
		}
	}()
	select {
	case <-entered:
	case <-requestDone:
		t.Fatal("request ended before reaching the handler")
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the handler")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcStopped := make(chan struct{})
	s3opt := &S3Options{shutdownCtx: ctx}
	shutdown := s3opt.newShutdown(func() { close(grpcStopped) }, &s3api.S3ApiServer{}, []*http.Server{server.Config})

	cancel()
	select {
	case <-httpClosing:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelling shutdownCtx did not start the HTTP drain")
	}
	<-grpcStopped

	// Model the main Serve path joining shutdown once its listener closes.
	serveReturned := make(chan struct{})
	go func() { shutdown(); close(serveReturned) }()
	select {
	case <-serveReturned:
		t.Fatal("Serve exit path returned while a request was still active")
	case <-requestDone:
		t.Fatal("active request ended before it was released")
	case <-time.After(100 * time.Millisecond):
	}

	finish()
	<-requestDone
	select {
	case <-serveReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve exit path did not return after the request drained")
	}
}

func TestS3PreStopRunsBeforeMiniCancelsClients(t *testing.T) {
	savedClients, savedClusterCtx := miniClients, MiniClusterCtx
	t.Cleanup(func() { miniClients, MiniClusterCtx = savedClients, savedClusterCtx })
	clusterCtx, stopCluster := context.WithCancel(context.Background())
	t.Cleanup(stopCluster)
	MiniClusterCtx = clusterCtx
	resetMiniClients()

	s3a := &s3api.S3ApiServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s3a.StatusHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	httpClosing := make(chan struct{})
	server.Config.RegisterOnShutdown(func() { close(httpClosing) })

	get := func(path string) int {
		t.Helper()
		response, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		return response.StatusCode
	}
	if code := get("/healthz"); code != http.StatusOK {
		t.Fatalf("probe before shutdown returned %d", code)
	}

	preStopSeconds := 3600
	s3opt := &S3Options{shutdownCtx: miniClientsCtx(), preStopSeconds: &preStopSeconds}
	s3opt.newShutdown(func() {}, s3a, []*http.Server{server.Config})

	// weed mini's interrupt hook, which runs before the S3 one.
	triggerDone := make(chan struct{})
	go func() { triggerMiniClientsShutdown(5 * time.Second); close(triggerDone) }()

	deadline := time.Now().Add(5 * time.Second)
	for get("/healthz") != http.StatusServiceUnavailable {
		if time.Now().After(deadline) {
			t.Fatal("probe did not start failing before mini cancelled its clients")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code := get("/bucket/key"); code != http.StatusNoContent {
		t.Fatalf("request during the pre-stop wait returned %d", code)
	}
	select {
	case <-httpClosing:
		t.Fatal("HTTP drain started during the pre-stop wait")
	default:
	}

	stopCluster()
	select {
	case <-httpClosing:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP drain did not start once the wait ended")
	}
	select {
	case <-triggerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("mini client shutdown did not finish")
	}
}

func TestS3PreStopWaitZeroDelay(t *testing.T) {
	drained := false
	preStopWait(func() { drained = true }, 0, nil)()
	if drained {
		t.Error("probes failed with no pre-stop delay")
	}
}

func TestS3PreStopWaitElapses(t *testing.T) {
	const delay = 50 * time.Millisecond
	drained := false
	started := time.Now()
	preStopWait(func() { drained = true }, delay, nil)()
	if !drained {
		t.Error("probes did not fail during the pre-stop delay")
	}
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("wait returned after %s, want at least %s", elapsed, delay)
	}
}

func TestS3PreStopDelay(t *testing.T) {
	seconds := func(n int) *int { return &n }
	for _, tc := range []struct {
		name    string
		seconds *int
		want    time.Duration
	}{
		{"unset", nil, 0},
		{"zero", seconds(0), 0},
		{"negative", seconds(-1), 0},
		{"set", seconds(20), 20 * time.Second},
	} {
		if got := (&S3Options{preStopSeconds: tc.seconds}).preStopDelay(); got != tc.want {
			t.Errorf("%s: preStopDelay() = %s, want %s", tc.name, got, tc.want)
		}
	}
}
