package s3_lifecycle

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	frontKey = "readiness-test-front-key"
	otherKey = "readiness-test-other-key"
)

type testFront struct {
	s3_lifecycle_pb.UnimplementedSeaweedS3LifecycleInternalServer
	key     string
	legacy  bool
	block   bool
	readies atomic.Int32
	deletes atomic.Int32
}

func (f *testFront) LifecycleReady(ctx context.Context, _ *s3_lifecycle_pb.LifecycleReadyRequest) (*s3_lifecycle_pb.LifecycleReadyResponse, error) {
	f.readies.Add(1)
	if f.legacy {
		return nil, status.Error(codes.Unimplemented, "method LifecycleReady not implemented")
	}
	if f.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err := security.CheckGrpcAdminToken(ctx, security.SigningKey(f.key)); err != nil {
		return nil, err
	}
	return &s3_lifecycle_pb.LifecycleReadyResponse{}, nil
}

func (f *testFront) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	f.deletes.Add(1)
	if err := security.CheckGrpcAdminToken(ctx, security.SigningKey(f.key)); err != nil {
		return nil, err
	}
	if req.Bucket == "" || req.ObjectPath == "" {
		return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_BLOCKED}, nil
	}
	return &s3_lifecycle_pb.LifecycleDeleteResponse{Outcome: s3_lifecycle_pb.LifecycleDeleteOutcome_DONE}, nil
}

type countingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	lists atomic.Int32
}

func (f *countingFiler) GetFilerConfiguration(context.Context, *filer_pb.GetFilerConfigurationRequest) (*filer_pb.GetFilerConfigurationResponse, error) {
	return &filer_pb.GetFilerConfigurationResponse{DirBuckets: "/buckets"}, nil
}

func (f *countingFiler) ListEntries(*filer_pb.ListEntriesRequest, grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	f.lists.Add(1)
	return nil
}

func serveGrpc(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func startFront(t *testing.T, f *testFront) string {
	return serveGrpc(t, func(s *grpc.Server) { s3_lifecycle_pb.RegisterSeaweedS3LifecycleInternalServer(s, f) })
}

func setWorkerKey(t *testing.T, key string) {
	t.Helper()
	v := util.GetViper()
	prev := v.GetString("jwt.filer_signing.key")
	v.Set("jwt.filer_signing.key", key)
	t.Cleanup(func() { v.Set("jwt.filer_signing.key", prev) })
}

func newTestHandler() *Handler {
	return NewHandler(grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func assertNoSecrets(t *testing.T, msg string) {
	t.Helper()
	for _, secret := range []string{frontKey, otherKey, "Bearer", "eyJ"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("error exposes %q: %s", secret, msg)
		}
	}
}

func TestSelectS3Front_SharedKeyPassesWithoutDeletes(t *testing.T) {
	setWorkerKey(t, frontKey)
	front := &testFront{key: frontKey}
	addr := startFront(t, front)

	conn, got, skipped := newTestHandler().selectS3Front(context.Background(), []string{addr})
	if conn == nil {
		t.Fatalf("expected a ready front, skipped: %v", skipped)
	}
	conn.Close()
	if got != addr || len(skipped) != 0 {
		t.Fatalf("got front %q skipped %v, want %q and none", got, skipped, addr)
	}
	if front.deletes.Load() != 0 {
		t.Fatalf("readiness check sent %d LifecycleDelete calls", front.deletes.Load())
	}
}

func TestSelectS3Front_KeyProblemsNameTheConfig(t *testing.T) {
	cases := []struct {
		name, workerKey, wantState string
	}{
		{"missing worker key", "", "is not set"},
		{"mismatched worker key", otherKey, "is set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setWorkerKey(t, tc.workerKey)
			addr := startFront(t, &testFront{key: frontKey})

			conn, _, skipped := newTestHandler().selectS3Front(context.Background(), []string{addr})
			if conn != nil {
				conn.Close()
				t.Fatal("front with a different key was selected")
			}
			if len(skipped) != 1 || status.Code(errors.Unwrap(skipped[0])) != codes.Unauthenticated {
				t.Fatalf("want one Unauthenticated skip, got %v", skipped)
			}
			msg := noReadyS3Error(skipped).Error()
			for _, want := range []string{addr, "jwt.filer_signing.key " + tc.wantState, "security.toml"} {
				if !strings.Contains(msg, want) {
					t.Fatalf("error %q does not mention %q", msg, want)
				}
			}
			assertNoSecrets(t, msg)
		})
	}
}

func TestSelectS3Front_SkipsFrontWithOtherKey(t *testing.T) {
	setWorkerKey(t, frontKey)
	bad := &testFront{key: otherKey}
	good := &testFront{key: frontKey}
	badAddr, goodAddr := startFront(t, bad), startFront(t, good)

	conn, got, skipped := newTestHandler().selectS3Front(context.Background(), []string{badAddr, goodAddr})
	if conn == nil {
		t.Fatalf("expected the second front, skipped: %v", skipped)
	}
	conn.Close()
	if got != goodAddr {
		t.Fatalf("selected %q, want %q", got, goodAddr)
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0].Error(), badAddr) || !strings.Contains(skipped[0].Error(), "rejected lifecycle credentials") {
		t.Fatalf("want the first front reported as a credential problem, got %v", skipped)
	}
	if bad.deletes.Load() != 0 || good.deletes.Load() != 0 {
		t.Fatal("readiness check sent LifecycleDelete calls")
	}
}

func TestSelectS3Front_OlderFrontUsesEmptyDelete(t *testing.T) {
	setWorkerKey(t, frontKey)
	ok := &testFront{key: frontKey, legacy: true}
	okAddr := startFront(t, ok)
	conn, _, skipped := newTestHandler().selectS3Front(context.Background(), []string{okAddr})
	if conn == nil {
		t.Fatalf("older front with the shared key was skipped: %v", skipped)
	}
	conn.Close()
	if ok.deletes.Load() != 1 {
		t.Fatalf("want one empty LifecycleDelete, got %d", ok.deletes.Load())
	}

	mismatched := &testFront{key: otherKey, legacy: true}
	conn, _, skipped = newTestHandler().selectS3Front(context.Background(), []string{startFront(t, mismatched)})
	if conn != nil {
		conn.Close()
		t.Fatal("older front with another key was selected")
	}
	if len(skipped) != 1 || !strings.Contains(skipped[0].Error(), "rejected lifecycle credentials") {
		t.Fatalf("want a credential problem, got %v", skipped)
	}
}

func TestSelectS3Front_TimeoutMovesOn(t *testing.T) {
	setWorkerKey(t, frontKey)
	prev := lifecycleReadyTimeout
	lifecycleReadyTimeout = 200 * time.Millisecond
	t.Cleanup(func() { lifecycleReadyTimeout = prev })
	stuckAddr := startFront(t, &testFront{key: frontKey, block: true})
	goodAddr := startFront(t, &testFront{key: frontKey})

	start := time.Now()
	conn, got, skipped := newTestHandler().selectS3Front(context.Background(), []string{stuckAddr, goodAddr})
	if conn == nil {
		t.Fatalf("expected the second front, skipped: %v", skipped)
	}
	conn.Close()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("readiness check took %v", elapsed)
	}
	if got != goodAddr || len(skipped) != 1 || status.Code(errors.Unwrap(skipped[0])) != codes.DeadlineExceeded {
		t.Fatalf("got %q skipped %v, want %q after a DeadlineExceeded skip", got, skipped, goodAddr)
	}
}

func TestSelectS3Front_ParentCancelStops(t *testing.T) {
	setWorkerKey(t, frontKey)
	next := &testFront{key: frontKey}
	stuckAddr := startFront(t, &testFront{key: frontKey, block: true})
	nextAddr := startFront(t, next)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	conn, _, skipped := newTestHandler().selectS3Front(ctx, []string{stuckAddr, nextAddr})
	if conn != nil {
		conn.Close()
		t.Fatal("selected a front after the parent was cancelled")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancelled selection took %v", elapsed)
	}
	if !errors.Is(noReadyS3Error(skipped), context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", skipped)
	}
	if next.readies.Load() != 0 {
		t.Fatal("tried another front after the parent was cancelled")
	}
}

func TestExecute_RejectedCredentialsFailBeforeListing(t *testing.T) {
	setWorkerKey(t, otherKey)
	filer := &countingFiler{}
	filerAddr := serveGrpc(t, func(s *grpc.Server) { filer_pb.RegisterSeaweedFilerServer(s, filer) })
	front := &testFront{key: frontKey}
	frontAddr := startFront(t, front)

	err := newTestHandler().Execute(context.Background(), &plugin_pb.ExecuteJobRequest{
		Job: &plugin_pb.JobSpec{
			JobId: "j1", JobType: jobType,
			Parameters: map[string]*plugin_pb.ConfigValue{
				"filer_grpc_address": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: filerAddr}},
			},
		},
		ClusterContext: &plugin_pb.ClusterContext{S3GrpcAddresses: []string{frontAddr}},
	}, &recordingExecSender{})
	if err == nil || !strings.Contains(err.Error(), "jwt.filer_signing.key") {
		t.Fatalf("want a signing-key error, got %v", err)
	}
	assertNoSecrets(t, err.Error())
	if filer.lists.Load() != 0 || front.deletes.Load() != 0 {
		t.Fatalf("pass started anyway: %d listings, %d deletes", filer.lists.Load(), front.deletes.Load())
	}
}
