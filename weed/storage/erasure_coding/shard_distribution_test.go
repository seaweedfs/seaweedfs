package erasure_coding

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
	"google.golang.org/grpc"
)

func intPtr(n int) *int { return &n }

func writeShardFiles(t *testing.T, names ...string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	files := make(map[string]string, len(names))
	for _, name := range names {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(name), 0644); err != nil {
			t.Fatal(err)
		}
		files[name] = p
	}
	return files
}

func shardTarget(node string, disk uint32, shards ...uint32) *worker_pb.TaskTarget {
	return &worker_pb.TaskTarget{Node: node, DiskId: disk, ShardIds: shards}
}

type flightRecorder struct {
	mu        sync.Mutex
	inFlight  int
	maxFlight int
	keyFlight map[string]int
	keyMax    int
	attempts  []string
	sent      []string
	failOn    string
	failFirst bool
	enter     chan struct{}
	goAhead   chan struct{}
	noGate    atomic.Bool
}

func newFlightRecorder() *flightRecorder {
	return &flightRecorder{
		keyFlight: make(map[string]int),
		enter:     make(chan struct{}, 32),
		goAhead:   make(chan struct{}),
	}
}

func (r *flightRecorder) release() {
	r.noGate.Store(true)
	close(r.goAhead)
}

func (r *flightRecorder) send(ctx context.Context, _ uint32, _ string, _ grpc.DialOption, destServer string, diskID uint32, _ string, shardType string) error {
	id := destServer + "/" + shardType
	key := shardTransferKey(shardJob{destNode: destServer, diskID: diskID, hasDisk: true})

	r.mu.Lock()
	r.inFlight++
	if r.inFlight > r.maxFlight {
		r.maxFlight = r.inFlight
	}
	r.keyFlight[key]++
	if r.keyFlight[key] > r.keyMax {
		r.keyMax = r.keyFlight[key]
	}
	r.attempts = append(r.attempts, id)
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.keyFlight[key]--
		r.inFlight--
		r.mu.Unlock()
	}()

	if r.failOn == id || r.failFirst {
		r.failFirst = false
		return errors.New("mock send failed")
	}
	if !r.noGate.Load() {
		select {
		case r.enter <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-r.goAhead:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	r.sent = append(r.sent, id)
	r.mu.Unlock()
	return nil
}

func waitEnters(t *testing.T, rec *flightRecorder, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-rec.enter:
		case <-deadline:
			t.Fatalf("timed out waiting for %d in-flight sends (got %d)", n, i)
		}
	}
}

func assertNoExtraEnter(t *testing.T, rec *flightRecorder, wait time.Duration) {
	t.Helper()
	select {
	case <-rec.enter:
		t.Fatal("unexpected extra in-flight send")
	case <-time.After(wait):
	}
}

func runDistribute(t *testing.T, targets []*worker_pb.TaskTarget, files map[string]string, rec *flightRecorder, max *int) (map[string][]string, error) {
	t.Helper()
	opts := &ShardDistributionOptions{
		MaxShardTransfers: max,
		send:              rec.send,
	}
	return DistributeEcShards(1, "col", targets, files, nil, nil, opts)
}

func TestDistributeEcShardsZeroIsSequential(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02", "ec03")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 0),
		shardTarget("full-01:8080", 1, 1),
		shardTarget("full-02:8080", 0, 2),
		shardTarget("full-02:8080", 1, 3),
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, intPtr(0))
		errCh <- err
	}()

	waitEnters(t, rec, 1)
	assertNoExtraEnter(t, rec, 40*time.Millisecond)
	rec.mu.Lock()
	if rec.maxFlight != 1 {
		rec.mu.Unlock()
		t.Fatalf("max in-flight=%d, want 1 for max_shard_transfers=0", rec.maxFlight)
	}
	rec.mu.Unlock()
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.maxFlight != 1 {
		t.Fatalf("max in-flight=%d after release, want 1", rec.maxFlight)
	}
	if rec.keyMax != 1 {
		t.Fatalf("per-key in-flight=%d, want 1", rec.keyMax)
	}
	if len(rec.sent) != 4 {
		t.Fatalf("sent %d shards, want 4: %v", len(rec.sent), rec.sent)
	}
}

func TestDistributeEcShardsNoCapParallelByKey(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02", "ec03")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 1, 0),
		shardTarget("full-01:8080", 2, 1),
		shardTarget("full-02:8080", 0, 2),
		shardTarget("full-02:8081", 0, 3), // same rack host, different destNode
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, nil)
		errCh <- err
	}()

	waitEnters(t, rec, 4)
	rec.mu.Lock()
	if rec.maxFlight != 4 {
		rec.mu.Unlock()
		t.Fatalf("no cap: max in-flight=%d, want 4 (one per dest disk/node)", rec.maxFlight)
	}
	if rec.keyMax != 1 {
		rec.mu.Unlock()
		t.Fatalf("per-key in-flight=%d, want 1", rec.keyMax)
	}
	rec.mu.Unlock()
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.sent) != 4 {
		t.Fatalf("sent %d, want 4: %v", len(rec.sent), rec.sent)
	}
}

func TestDistributeEcShardsSameDiskQueued(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 0, 1),
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, nil)
		errCh <- err
	}()

	waitEnters(t, rec, 1)
	assertNoExtraEnter(t, rec, 40*time.Millisecond)
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.maxFlight != 1 {
		t.Fatalf("same disk: max in-flight=%d, want 1", rec.maxFlight)
	}
	if rec.sent[0] != "full-01:8080/ec00" || rec.sent[1] != "full-01:8080/ec01" {
		t.Fatalf("same-disk order %v, want ec00 then ec01", rec.sent)
	}
}

func TestDistributeEcShardsAutoDiskSerializesServer(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 0), // disk picked by the receiver
		shardTarget("full-01:8080", 1, 1),
		shardTarget("full-02:8080", 1, 2),
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, nil)
		errCh <- err
	}()

	waitEnters(t, rec, 2)
	rec.mu.Lock()
	if rec.maxFlight != 2 {
		rec.mu.Unlock()
		t.Fatalf("auto-placement server must serialize: in-flight=%d, want 2 (one per server)", rec.maxFlight)
	}
	rec.mu.Unlock()
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.sent) != 3 {
		t.Fatalf("sent %d, want 3", len(rec.sent))
	}
}

func TestDistributeEcShardsTwoDisksTwoStreams(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 1, 0),
		shardTarget("full-01:8080", 2, 1),
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, nil)
		errCh <- err
	}()

	waitEnters(t, rec, 2)
	rec.mu.Lock()
	if rec.maxFlight != 2 {
		rec.mu.Unlock()
		t.Fatalf("two disks on one VS: in-flight=%d, want 2", rec.maxFlight)
	}
	rec.mu.Unlock()
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestDistributeEcShardsCapNeverExceedsN(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02", "ec03")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 1, 0),
		shardTarget("full-01:8080", 2, 1),
		shardTarget("full-02:8080", 1, 2),
		shardTarget("full-02:8080", 2, 3),
	}
	rec := newFlightRecorder()
	errCh := make(chan error, 1)
	go func() {
		_, err := runDistribute(t, targets, files, rec, intPtr(2))
		errCh <- err
	}()

	waitEnters(t, rec, 2)
	assertNoExtraEnter(t, rec, 40*time.Millisecond)
	rec.mu.Lock()
	if rec.maxFlight != 2 {
		rec.mu.Unlock()
		t.Fatalf("cap 2: in-flight=%d, want 2", rec.maxFlight)
	}
	rec.mu.Unlock()
	rec.release()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.maxFlight > 2 {
		t.Fatalf("cap 2: max in-flight=%d", rec.maxFlight)
	}
	if rec.keyMax != 1 {
		t.Fatalf("per-key in-flight=%d, want 1 under cap", rec.keyMax)
	}
	if len(rec.sent) != 4 {
		t.Fatalf("sent %d, want 4", len(rec.sent))
	}
}

func TestDistributeEcShardsErrorAbortSameKey(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 0, 1, 2),
	}
	rec := newFlightRecorder()
	rec.noGate.Store(true)
	rec.failOn = "full-01:8080/ec01"

	_, err := runDistribute(t, targets, files, rec, nil)
	if err == nil {
		t.Fatal("expected send error")
	}
	if !strings.Contains(err.Error(), "failed to send ec01 to full-01:8080") {
		t.Fatalf("error wrap: %v", err)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if got := strings.Join(rec.attempts, ","); got != "full-01:8080/ec00,full-01:8080/ec01" {
		t.Fatalf("attempts %v, want ec00 then ec01 (ec02 aborted)", rec.attempts)
	}
	if len(rec.sent) != 1 || rec.sent[0] != "full-01:8080/ec00" {
		t.Fatalf("successful sends %v, want only ec00", rec.sent)
	}
}

func TestDistributeEcShardsErrorAbortCancelsOtherKeys(t *testing.T) {
	files := writeShardFiles(t, "ec00", "ec01", "ec02")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 0),
		shardTarget("full-02:8080", 0, 1),
		shardTarget("full-03:8080", 0, 2),
	}
	rec := newFlightRecorder()
	rec.noGate.Store(true)
	rec.failFirst = true

	_, err := runDistribute(t, targets, files, rec, intPtr(1))
	if err == nil {
		t.Fatal("expected send error")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.attempts) != 1 {
		t.Fatalf("cap=1 abort: attempts %v, want only the failing send", rec.attempts)
	}
	if len(rec.sent) != 0 {
		t.Fatalf("successful sends %v, want none", rec.sent)
	}
}

func TestDistributeEcShardsMissingFile(t *testing.T) {
	files := writeShardFiles(t, "ec00")
	targets := []*worker_pb.TaskTarget{
		shardTarget("full-01:8080", 0, 1),
	}
	for _, max := range []*int{intPtr(0), nil} {
		_, err := DistributeEcShards(1, "col", targets, files, nil, nil, &ShardDistributionOptions{
			MaxShardTransfers: max,
			send: func(context.Context, uint32, string, grpc.DialOption, string, uint32, string, string) error {
				t.Fatal("send should not run for missing file")
				return nil
			},
		})
		if err == nil || !strings.Contains(err.Error(), "shard file ec01 not found") {
			t.Fatalf("missing file error (max=%v): %v", max, err)
		}
	}
}

func TestShardTransferKeyUsesDestNodeNotRack(t *testing.T) {
	a := shardTransferKey(shardJob{destNode: "full-01:8080", diskID: 0, hasDisk: true})
	b := shardTransferKey(shardJob{destNode: "full-01:8081", diskID: 0, hasDisk: true})
	if a == b {
		t.Fatalf("host:port keys collided: %s", a)
	}
	noDisk := shardTransferKey(shardJob{destNode: "full-01:8080", hasDisk: false})
	if noDisk != "full-01:8080" {
		t.Fatalf("no disk id key=%q, want destNode", noDisk)
	}
	if fmt.Sprint(a) == noDisk {
		t.Fatalf("disk 0 key should differ from destNode-only when hasDisk")
	}
}
