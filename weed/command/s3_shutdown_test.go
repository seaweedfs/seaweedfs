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
