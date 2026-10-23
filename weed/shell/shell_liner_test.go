package shell

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeCommand struct {
	name string
	err  error
}

func (c *fakeCommand) Name() string                              { return c.name }
func (c *fakeCommand) Help() string                              { return "test helper" }
func (c *fakeCommand) HasTag(CommandTag) bool                    { return false }
func (c *fakeCommand) Do([]string, *CommandEnv, io.Writer) error { return c.err }

func TestProcessEachCmdReturnsErrors(t *testing.T) {

	exit, err := processEachCmd("definitely.not.a.command", nil)
	if exit {
		t.Errorf("unknown command must not request exit")
	}
	if err == nil {
		t.Errorf("unknown command must return an error")
	}
	if err != nil && !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("unexpected error for unknown command: %v", err)
	}

	exit, err = processEachCmd("exit", nil)
	if !exit {
		t.Errorf("exit must request exit")
	}
	if err != nil {
		t.Errorf("exit must not return an error, got: %v", err)
	}

	exit, err = processEachCmd("   ", nil)
	if exit || err != nil {
		t.Errorf("blank input must be a no-op, got exit=%v err=%v", exit, err)
	}
}

func TestRunShellConnectTimeout(t *testing.T) {
	masters := "127.0.0.1:1"
	filerGroup := ""

	start := time.Now()
	err := RunShell(ShellOptions{
		Masters:        &masters,
		FilerGroup:     &filerGroup,
		ConnectTimeout: 200 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "cannot connect to master") {
		t.Fatalf("expected a master connection timeout, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("RunShell should give up near the connect timeout, took %v", elapsed)
	}
}

func TestProcessEachCmdPreservesRegisteredCommandError(t *testing.T) {
	doErr := errors.New("daily_run: shard=3: recovery walk failed")
	failing := &fakeCommand{name: "test.failing.command", err: doErr}
	succeeding := &fakeCommand{name: "test.succeeding.command"}
	Commands = append(Commands, failing, succeeding)
	t.Cleanup(func() { Commands = Commands[:len(Commands)-2] })

	exit, err := processEachCmd("test.failing.command -some -args", nil)
	if exit {
		t.Errorf("a failing command must not request exit")
	}
	if !errors.Is(err, doErr) {
		t.Errorf("the command's own error must be preserved, got: %v", err)
	}

	exit, err = processEachCmd("test.succeeding.command", nil)
	if exit || err != nil {
		t.Errorf("a succeeding command must return no error, got exit=%v err=%v", exit, err)
	}
}

// fakeMaster implements just enough of master_pb.SeaweedServer for filer
// discovery: KeepConnected holds the stream open so the client marks the
// master connected, and ListClusterNodes answers from a script.
type fakeMaster struct {
	master_pb.UnimplementedSeaweedServer
	listCalls atomic.Int32
	onList    func() (*master_pb.ListClusterNodesResponse, error)
}

func (f *fakeMaster) KeepConnected(stream grpc.BidiStreamingServer[master_pb.KeepConnectedRequest, master_pb.KeepConnectedResponse]) error {
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func (f *fakeMaster) ListClusterNodes(context.Context, *master_pb.ListClusterNodesRequest) (*master_pb.ListClusterNodesResponse, error) {
	f.listCalls.Add(1)
	return f.onList()
}

func (f *fakeMaster) VolumeList(context.Context, *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	return &master_pb.VolumeListResponse{TopologyInfo: &master_pb.TopologyInfo{}}, nil
}

// startFakeMaster serves the fake master on an ephemeral gRPC port and
// returns the http-form master address (whose gRPC port is port+10000) to
// hand to ShellOptions.Masters.
func startFakeMaster(t *testing.T) (*fakeMaster, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fake master listen: %v", err)
	}
	grpcPort := listener.Addr().(*net.TCPAddr).Port
	if grpcPort < 10001 {
		listener.Close()
		t.Fatalf("fake master needs a port above 10000 for its http form, got %d", grpcPort)
	}
	fake := &fakeMaster{}
	server := grpc.NewServer()
	master_pb.RegisterSeaweedServer(server, fake)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	return fake, fmt.Sprintf("127.0.0.1:%d", grpcPort-10000)
}

func newTestCommandEnv(t *testing.T, masters string) *CommandEnv {
	t.Helper()
	filerGroup := ""
	options := &ShellOptions{
		Masters:        &masters,
		FilerGroup:     &filerGroup,
		GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	commandEnv := NewCommandEnv(options)
	ctx, cancel := context.WithCancel(context.Background())
	go commandEnv.MasterClient.KeepConnectedToMaster(ctx)
	t.Cleanup(cancel)
	return commandEnv
}

func filerNode(address string) *master_pb.ListClusterNodesResponse {
	return &master_pb.ListClusterNodesResponse{
		ClusterNodes: []*master_pb.ListClusterNodesResponse_ClusterNode{
			{Address: address},
		},
	}
}

// A filer registering mid-discovery turns an empty list into a populated one
// on a later attempt.
func TestListFilersEmptyThenPopulated(t *testing.T) {
	master, masters := startFakeMaster(t)
	master.onList = func() (*master_pb.ListClusterNodesResponse, error) {
		if master.listCalls.Load() <= 2 {
			return &master_pb.ListClusterNodesResponse{}, nil
		}
		return filerNode("127.0.0.1:9888"), nil
	}

	commandEnv := newTestCommandEnv(t, masters)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filers, err := listFilers(ctx, commandEnv, "", 10*time.Second)
	if err != nil {
		t.Fatalf("listFilers failed: %v", err)
	}
	if len(filers) != 1 || filers[0] != "127.0.0.1:9888" {
		t.Fatalf("expected the late-registered filer, got %v", filers)
	}
	if got := master.listCalls.Load(); got != 3 {
		t.Fatalf("expected 3 discovery attempts, got %d", got)
	}
}

// A master that answers but never lists a filer exhausts the discovery
// window and reports an empty, error-free result — the caller then prints
// the "no filer registered" notice and continues without a filer.
func TestListFilersEmptyUntilTimeout(t *testing.T) {
	master, masters := startFakeMaster(t)
	master.onList = func() (*master_pb.ListClusterNodesResponse, error) {
		return &master_pb.ListClusterNodesResponse{}, nil
	}
	commandEnv := newTestCommandEnv(t, masters)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	filers, err := listFilers(ctx, commandEnv, "", 1500*time.Millisecond)
	if err != nil {
		t.Fatalf("empty discovery must not return an error, got: %v", err)
	}
	if len(filers) != 0 {
		t.Fatalf("expected no filers, got %v", filers)
	}
	if got := master.listCalls.Load(); got < 2 {
		t.Fatalf("expected repeated polling, got %d attempts", got)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("discovery should give up near the timeout, took %v", elapsed)
	}
}

// A non-transient failure on the last attempt surfaces as the discovery
// error, so the diagnostic reports the RPC failure instead of a missing
// filer.
func TestListFilersReportsLastError(t *testing.T) {
	master, masters := startFakeMaster(t)
	master.onList = func() (*master_pb.ListClusterNodesResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}
	commandEnv := newTestCommandEnv(t, masters)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	filers, err := listFilers(ctx, commandEnv, "", 10*time.Second)
	if err == nil {
		t.Fatalf("expected the permission error to be reported")
	}
	if len(filers) != 0 {
		t.Fatalf("expected no filers, got %v", filers)
	}
	if got := master.listCalls.Load(); got != 1 {
		t.Fatalf("a non-transient error must stop retries, got %d attempts", got)
	}
}

// A transient RPC failure on the last attempt also surfaces as the discovery
// error rather than as a missing filer.
func TestListFilersErrorAfterEmptyList(t *testing.T) {
	master, masters := startFakeMaster(t)
	master.onList = func() (*master_pb.ListClusterNodesResponse, error) {
		if master.listCalls.Load() <= 1 {
			return &master_pb.ListClusterNodesResponse{}, nil
		}
		return nil, status.Error(codes.Unavailable, "master gone")
	}
	commandEnv := newTestCommandEnv(t, masters)

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	filers, err := listFilers(ctx, commandEnv, "", 1500*time.Millisecond)
	if err == nil {
		t.Fatalf("expected the late RPC failure to be reported")
	}
	if len(filers) != 0 {
		t.Fatalf("expected no filers, got %v", filers)
	}
}

// After discovery times out with no filer, master-only commands still run:
// the shell warns about the missing filer but proceeds to execute stdin.
func TestRunShellFilerDiscoveryTimeout(t *testing.T) {
	master, masters := startFakeMaster(t)
	master.onList = func() (*master_pb.ListClusterNodesResponse, error) {
		return &master_pb.ListClusterNodesResponse{}, nil
	}
	filerGroup := ""

	oldStdin := os.Stdin
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	os.Stdin = inReader
	t.Cleanup(func() { os.Stdin = oldStdin })
	go func() {
		fmt.Fprintln(inWriter, "cluster.ps")
		fmt.Fprintln(inWriter, "exit")
		inWriter.Close()
	}()

	oldStderr := os.Stderr
	errReader, errWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	os.Stderr = errWriter
	t.Cleanup(func() { os.Stderr = oldStderr })

	done := make(chan error, 1)
	go func() {
		done <- RunShell(ShellOptions{
			Masters:        &masters,
			FilerGroup:     &filerGroup,
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
			ConnectTimeout: 1500 * time.Millisecond,
		})
	}()

	select {
	case err = <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("RunShell did not return after discovery timeout")
	}
	errWriter.Close()
	stderr, _ := io.ReadAll(errReader)
	if err != nil {
		t.Fatalf("master-only commands must run without a filer, got: %v", err)
	}
	if !strings.Contains(string(stderr), "no filer registered") {
		t.Fatalf("expected the missing-filer notice on stderr, got: %q", stderr)
	}
}
