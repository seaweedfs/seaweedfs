package command

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFilerShutdownJoinsServeDuringParallelDrain(t *testing.T) {
	var servers []*http.Server
	var releases []func()
	var completed []chan struct{}
	var listenersClosing []chan struct{}
	for range 2 {
		entered := make(chan struct{})
		release := make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		done := make(chan struct{})
		closing := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-release
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(server.Close)
		t.Cleanup(finish)
		server.Config.RegisterOnShutdown(func() { close(closing) })
		servers = append(servers, server.Config)
		releases = append(releases, finish)
		completed = append(completed, done)
		listenersClosing = append(listenersClosing, closing)
		go func() {
			defer close(done)
			response, err := server.Client().Get(server.URL)
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			io.Copy(io.Discard, response.Body)
			if response.StatusCode != http.StatusNoContent {
				t.Errorf("active request returned %s", response.Status)
			}
		}()
		<-entered
	}

	grpcStarted := make(chan struct{})
	grpcRelease := make(chan struct{})
	releaseGrpc := sync.OnceFunc(func() { close(grpcRelease) })
	t.Cleanup(releaseGrpc)
	grpcStopped := make(chan struct{})
	filerClosed := make(chan struct{})
	var closes atomic.Int32
	shutdown := newFilerShutdown(func() {
		close(grpcStarted)
		<-grpcRelease
		close(grpcStopped)
	}, func() {
		closes.Add(1)
		close(filerClosed)
	}, servers...)
	joined := make(chan struct{})
	go func() { shutdown(); close(joined) }()
	deadline := time.After(5 * time.Second)
	select {
	case <-grpcStarted:
	case <-deadline:
		t.Fatal("gRPC shutdown did not start")
	}
	for _, closing := range listenersClosing {
		select {
		case <-closing:
		case <-deadline:
			t.Fatal("HTTP shutdown did not start while gRPC was draining")
		}
	}
	// Model the main Serve path joining shutdown as soon as listeners close.
	serveReturned := make(chan struct{})
	go func() { shutdown(); close(serveReturned) }()

	releases[0]()
	<-completed[0]
	select {
	case <-filerClosed:
		t.Error("filer closed while another server still had an active request")
	default:
	}
	select {
	case <-serveReturned:
		t.Error("Serve exit path returned before request draining completed")
	default:
	}
	releases[1]()
	<-completed[1]
	select {
	case <-filerClosed:
		t.Error("filer closed while gRPC was still draining")
	default:
	}
	releaseGrpc()
	<-grpcStopped
	<-joined
	<-serveReturned
	if closes.Load() != 1 {
		t.Errorf("filer closed %d times, want once", closes.Load())
	}
}

func TestFilerShutdownWaitsForHTTPAfterGrpcStops(t *testing.T) {
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
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := server.Client().Get(server.URL)
		if err != nil {
			t.Error(err)
			return
		}
		response.Body.Close()
	}()
	<-entered

	httpClosing := make(chan struct{})
	server.Config.RegisterOnShutdown(func() { close(httpClosing) })
	grpcStopped := make(chan struct{})
	filerClosed := make(chan struct{})
	shutdown := newFilerShutdown(func() { close(grpcStopped) }, func() { close(filerClosed) }, server.Config)
	joined := make(chan struct{})
	go func() { shutdown(); close(joined) }()
	<-httpClosing
	<-grpcStopped
	select {
	case <-filerClosed:
		t.Error("filer closed while HTTP request was active")
	case <-time.After(100 * time.Millisecond):
	}
	finish()
	<-requestDone
	<-joined
	select {
	case <-filerClosed:
	default:
		t.Error("filer was not closed after HTTP request completed")
	}
}
