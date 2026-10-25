package command

import (
	"context"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func gracefulStopGrpc(grpcS *grpc.Server, timeout time.Duration) {
	glog.V(0).Infof("Gracefully stopping gRPC server")
	stopped := make(chan struct{})
	go func() {
		grpcS.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		glog.V(0).Infof("gRPC server stopped gracefully")
	case <-time.After(timeout):
		glog.V(0).Infof("gRPC server graceful stop timed out after %s, forcing stop", timeout)
		grpcS.Stop()
	}
}

// newGracefulShutdown joins shutdown callers while gRPC and HTTP drain concurrently.
func newGracefulShutdown(stopGrpc, closeServer func(), httpServers ...*http.Server) func() {
	return sync.OnceFunc(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		var drained sync.WaitGroup
		drained.Add(1)
		go func() {
			defer drained.Done()
			stopGrpc()
		}()
		for _, server := range httpServers {
			drained.Add(1)
			go func() {
				defer drained.Done()
				if err := server.Shutdown(shutdownCtx); err != nil {
					glog.Warningf("HTTP shutdown: %v", err)
				}
			}()
		}
		drained.Wait()
		closeServer()
	})
}
