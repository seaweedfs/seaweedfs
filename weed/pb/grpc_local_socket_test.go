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

func TestServeGrpcOnLocalSocketRecreatesRemovedSocket(t *testing.T) {
	isolateLocalGrpcSockets(t)
	prevInterval := localSocketCheckInterval
	localSocketCheckInterval = 10 * time.Millisecond
	t.Cleanup(func() { localSocketCheckInterval = prevInterval })

	grpcServer, port := startHealthGrpcServer(t)
	socketDir := localSocketDir(t)
	socketPath := filepath.Join(socketDir, "grpc.sock")
	RegisterLocalGrpcSocket("127.0.0.1", port, socketPath)
	ServeGrpcOnLocalSocket(grpcServer, port)
	waitForSocketFile(t, socketPath)

	// Rebinding fails while the folder is gone and must be retried later.
	if err := os.RemoveAll(socketDir); err != nil {
		t.Fatalf("remove socket dir: %v", err)
	}
	time.Sleep(10 * localSocketCheckInterval)
	if err := os.Mkdir(socketDir, 0o700); err != nil {
		t.Fatalf("recreate socket dir: %v", err)
	}
	waitForSocketFile(t, socketPath)

	conn, err := grpc.NewClient("unix:"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial recreated socket: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("health check over the recreated socket: %v", err)
	}

	grpcServer.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Lstat(socketPath); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s left behind after the server stopped", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForSocketFile(t *testing.T, socketPath string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Lstat(socketPath); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %s was not created", socketPath)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
