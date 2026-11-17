package pb

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func isolateLocalGrpcSockets(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Unix-socket routing is disabled on Windows")
	}
	localGrpcSocketsLock.Lock()
	prevSockets, prevHosts := localGrpcSockets, localGrpcHosts
	localGrpcSockets = make(map[int]string)
	localGrpcHosts = make(map[int]map[string]struct{})
	localGrpcSocketsLock.Unlock()
	t.Cleanup(func() {
		localGrpcSocketsLock.Lock()
		localGrpcSockets, localGrpcHosts = prevSockets, prevHosts
		localGrpcSocketsLock.Unlock()
	})
}

// localSocketDir is short because TMPDIR can push socket paths past the
// sockaddr_un limit.
func localSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "swsock")
	if err != nil {
		t.Fatalf("create socket dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func startHealthGrpcServer(t *testing.T) (*grpc.Server, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	go grpcServer.Serve(listener)
	t.Cleanup(grpcServer.Stop)
	return grpcServer, listener.Addr().(*net.TCPAddr).Port
}

func checkGrpcHealth(t *testing.T, address string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := GrpcDial(ctx, address, false, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer conn.Close()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check over %s: %v", address, err)
	}
}

func TestGrpcDialFallsBackToTcpWhenLocalSocketIsMissing(t *testing.T) {
	isolateLocalGrpcSockets(t)
	_, port := startHealthGrpcServer(t)
	RegisterLocalGrpcSocket("127.0.0.1", port, filepath.Join(localSocketDir(t), "removed.sock"))

	checkGrpcHealth(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
}
