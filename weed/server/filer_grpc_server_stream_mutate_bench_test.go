package weed_server

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeFilerServer simulates a filer whose CreateEntry takes a fixed amount of
// wall-clock time (meant to model the filer-store commit latency). It implements
// only what these benchmarks need: unary CreateEntry and bidi StreamMutateEntry.
//
// The StreamMutateEntry handler mirrors the production handler's structure:
// a single goroutine per stream, processing one request at a time.
type fakeFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	serviceDelay time.Duration
	createCalls  atomic.Int64
}

func (s *fakeFilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.createCalls.Add(1)
	if s.serviceDelay > 0 {
		time.Sleep(s.serviceDelay)
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

// StreamMutateEntry mirrors the OLD serial handler: one goroutine, strictly
// one request at a time.
func (s *fakeFilerServer) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch r := req.Request.(type) {
		case *filer_pb.StreamMutateEntryRequest_CreateRequest:
			resp, createErr := s.CreateEntry(stream.Context(), r.CreateRequest)
			if createErr != nil {
				resp = &filer_pb.CreateEntryResponse{Error: createErr.Error()}
			}
			out := &filer_pb.StreamMutateEntryResponse{
				RequestId: req.RequestId,
				IsLast:    true,
				Response:  &filer_pb.StreamMutateEntryResponse_CreateResponse{CreateResponse: resp},
			}
			if resp.Error != "" {
				out.Error = resp.Error
				out.Errno = int32(syscall.EIO)
			}
			if sendErr := stream.Send(out); sendErr != nil {
				return sendErr
			}
		}
	}
}

// fakeConcurrentFilerServer mirrors the NEW concurrent handler: per-request
// goroutine, bounded by a semaphore, with a Send mutex for stream safety.
// Structurally identical to weed/server/filer_grpc_server_stream_mutate.go
// after the parallelization patch.
type fakeConcurrentFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	serviceDelay time.Duration
	concurrency  int
	createCalls  atomic.Int64
}

func (s *fakeConcurrentFilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.createCalls.Add(1)
	if s.serviceDelay > 0 {
		time.Sleep(s.serviceDelay)
	}
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *fakeConcurrentFilerServer) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	var sendMu sync.Mutex
	send := func(r *filer_pb.StreamMutateEntryResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(r)
	}
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			wg.Wait()
			return nil
		}
		if err != nil {
			wg.Wait()
			return err
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(req *filer_pb.StreamMutateEntryRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			switch r := req.Request.(type) {
			case *filer_pb.StreamMutateEntryRequest_CreateRequest:
				resp, _ := s.CreateEntry(stream.Context(), r.CreateRequest)
				_ = send(&filer_pb.StreamMutateEntryResponse{
					RequestId: req.RequestId,
					IsLast:    true,
					Response:  &filer_pb.StreamMutateEntryResponse_CreateResponse{CreateResponse: resp},
				})
			}
		}(req)
	}
}

// startFakeConcurrentFilerServer spins up the concurrent-handler variant.
func startFakeConcurrentFilerServer(t testing.TB, serviceDelay time.Duration, concurrency int) (string, *fakeConcurrentFilerServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	fake := &fakeConcurrentFilerServer{serviceDelay: serviceDelay, concurrency: concurrency}
	filer_pb.RegisterSeaweedFilerServer(srv, fake)
	go srv.Serve(lis)
	return lis.Addr().String(), fake, func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
}

// fakeSchedulerFilerServer uses the real mutateScheduler from the production
// handler to admit requests by (path, kind). Per-path operations serialize;
// cross-path operations run in parallel.
type fakeSchedulerFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	serviceDelay time.Duration
	concurrency  int
	createCalls  atomic.Int64
	maxInFlight  atomic.Int64 // sampled peak of in-flight create goroutines
	curInFlight  atomic.Int64
}

func (s *fakeSchedulerFilerServer) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.createCalls.Add(1)
	cur := s.curInFlight.Add(1)
	for {
		peak := s.maxInFlight.Load()
		if cur <= peak || s.maxInFlight.CompareAndSwap(peak, cur) {
			break
		}
	}
	if s.serviceDelay > 0 {
		time.Sleep(s.serviceDelay)
	}
	s.curInFlight.Add(-1)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *fakeSchedulerFilerServer) StreamMutateEntry(stream grpc.BidiStreamingServer[filer_pb.StreamMutateEntryRequest, filer_pb.StreamMutateEntryResponse]) error {
	var sendMu sync.Mutex
	send := func(r *filer_pb.StreamMutateEntryResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(r)
	}
	sched := newMutateScheduler(s.concurrency)
	var wg sync.WaitGroup
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			wg.Wait()
			return nil
		}
		if err != nil {
			wg.Wait()
			return err
		}
		primary, secondary, kind := classifyMutation(req)
		sched.Admit(primary, secondary, kind)
		wg.Add(1)
		go func(req *filer_pb.StreamMutateEntryRequest) {
			defer wg.Done()
			defer sched.Done(primary, secondary, kind)
			switch r := req.Request.(type) {
			case *filer_pb.StreamMutateEntryRequest_CreateRequest:
				resp, _ := s.CreateEntry(stream.Context(), r.CreateRequest)
				_ = send(&filer_pb.StreamMutateEntryResponse{
					RequestId: req.RequestId,
					IsLast:    true,
					Response:  &filer_pb.StreamMutateEntryResponse_CreateResponse{CreateResponse: resp},
				})
			}
		}(req)
	}
}

func startFakeSchedulerFilerServer(t testing.TB, serviceDelay time.Duration, concurrency int) (string, *fakeSchedulerFilerServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	fake := &fakeSchedulerFilerServer{serviceDelay: serviceDelay, concurrency: concurrency}
	filer_pb.RegisterSeaweedFilerServer(srv, fake)
	go srv.Serve(lis)
	return lis.Addr().String(), fake, func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
}

// startFakeFilerServer spins up an in-process gRPC filer with the given per-
// request service delay, returns the dial target and a shutdown func.
func startFakeFilerServer(t testing.TB, serviceDelay time.Duration) (string, *fakeFilerServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	fake := &fakeFilerServer{serviceDelay: serviceDelay}
	filer_pb.RegisterSeaweedFilerServer(srv, fake)
	go srv.Serve(lis)
	return lis.Addr().String(), fake, func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
}

// dialFakeFiler returns a client connection to the in-process filer.
func dialFakeFiler(t testing.TB, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

// opsForWorker splits `ops` across `concurrency` workers, handing the
// remainder to the first `remainder` workers so the total is always exactly
// `ops`. Returns the count for worker index g.
func opsForWorker(g, concurrency, ops int) int {
	per := ops / concurrency
	if g < ops%concurrency {
		return per + 1
	}
	return per
}

// runUnaryCreateWorkload fires `ops` CreateEntry unary RPCs across `concurrency`
// goroutines, all sharing a single client connection. Returns total duration.
func runUnaryCreateWorkload(t testing.TB, conn *grpc.ClientConn, concurrency, ops int) time.Duration {
	t.Helper()
	client := filer_pb.NewSeaweedFilerClient(conn)
	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		count := opsForWorker(g, concurrency, ops)
		go func() {
			defer wg.Done()
			for i := 0; i < count; i++ {
				_, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
					Directory: "/",
					Entry: &filer_pb.Entry{
						Name:        "f",
						IsDirectory: false,
					},
				})
				if err != nil {
					t.Errorf("unary CreateEntry: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	return time.Since(start)
}

// pathFn chooses the (directory, name) of the i'th op. The default used by the
// existing callers is samePath — all ops hit "/f". distinctPath gives every
// op its own name so cross-path parallelism can kick in.
type pathFn func(i int) (dir, name string)

func samePath(_ int) (string, string)     { return "/", "f" }
func distinctPath(i int) (string, string) { return "/", "f" + itoa(i) }

// itoa is a tight local decimal formatter — avoids a strconv import inside a
// hot benchmark loop.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

// runStreamCreateWorkload fires `ops` CreateEntry ops through a SINGLE bidi
// stream, with `concurrency` goroutines all submitting to the same stream via
// a small multiplexer that mirrors weed/mount/weedfs_stream_mutate.go's
// sendLoop+recvLoop structure. Returns total duration.
func runStreamCreateWorkload(t testing.TB, conn *grpc.ClientConn, concurrency, ops int) time.Duration {
	return runStreamCreateWorkloadAt(t, conn, concurrency, ops, samePath)
}

func runStreamCreateWorkloadAt(t testing.TB, conn *grpc.ClientConn, concurrency, ops int, path pathFn) time.Duration {
	t.Helper()
	client := filer_pb.NewSeaweedFilerClient(conn)
	stream, err := client.StreamMutateEntry(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	var nextID atomic.Uint64
	var pending sync.Map // map[uint64]chan struct{}

	// single recv goroutine, dispatching by request id
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if ch, ok := pending.LoadAndDelete(resp.RequestId); ok {
				close(ch.(chan struct{}))
			}
		}
	}()

	// dedicated send goroutine (gRPC Send is not concurrent-safe)
	sendCh := make(chan *filer_pb.StreamMutateEntryRequest, 512)
	sendErr := make(chan error, 1)
	go func() {
		for req := range sendCh {
			if err := stream.Send(req); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	var wg sync.WaitGroup
	start := time.Now()
	offset := 0
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		count := opsForWorker(g, concurrency, ops)
		startOff := offset
		offset += count
		go func(startOff, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				id := nextID.Add(1)
				done := make(chan struct{})
				pending.Store(id, done)
				dir, name := path(startOff + i)
				sendCh <- &filer_pb.StreamMutateEntryRequest{
					RequestId: id,
					Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
						CreateRequest: &filer_pb.CreateEntryRequest{
							Directory: dir,
							Entry:     &filer_pb.Entry{Name: name},
						},
					},
				}
				<-done
			}
		}(startOff, count)
	}
	wg.Wait()
	elapsed := time.Since(start)

	close(sendCh)
	_ = stream.CloseSend()
	select {
	case <-sendErr:
	case <-time.After(time.Second):
	}
	select {
	case <-recvErr:
	case <-time.After(time.Second):
	}
	return elapsed
}

// runStreamAsyncCreateWorkload fires `ops` CreateEntry ops through a single
// bidi stream WITHOUT the client waiting for responses between sends. The
// timer starts at the first send and stops when the last response is received,
// so this measures true end-to-end throughput (not just client-send rate).
//
// This isolates the question: does relaxing client-side per-request waiting
// help throughput when the server is a serial loop?
func runStreamAsyncCreateWorkload(t testing.TB, conn *grpc.ClientConn, concurrency, ops int) time.Duration {
	t.Helper()
	client := filer_pb.NewSeaweedFilerClient(conn)
	stream, err := client.StreamMutateEntry(context.Background())
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	var nextID atomic.Uint64
	var outstanding atomic.Int64
	allDone := make(chan struct{})

	// Single recv goroutine — signals allDone when the last response arrives.
	recvErr := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if outstanding.Add(-1) == 0 {
				select {
				case <-allDone:
				default:
					close(allDone)
				}
				return
			}
		}
	}()

	// Dedicated send goroutine (gRPC Send is not concurrent-safe).
	sendCh := make(chan *filer_pb.StreamMutateEntryRequest, 4096)
	sendErr := make(chan error, 1)
	go func() {
		for req := range sendCh {
			if err := stream.Send(req); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	outstanding.Store(int64(ops))

	var wg sync.WaitGroup
	start := time.Now()
	for g := 0; g < concurrency; g++ {
		wg.Add(1)
		count := opsForWorker(g, concurrency, ops)
		go func(count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				id := nextID.Add(1)
				// Fire-and-forget: push to sendCh, do not wait for response.
				sendCh <- &filer_pb.StreamMutateEntryRequest{
					RequestId: id,
					Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{
						CreateRequest: &filer_pb.CreateEntryRequest{
							Directory: "/",
							Entry:     &filer_pb.Entry{Name: "f"},
						},
					},
				}
			}
		}(count)
	}
	wg.Wait()
	// All requests queued; wait for server to drain all responses.
	<-allDone
	elapsed := time.Since(start)

	close(sendCh)
	_ = stream.CloseSend()
	select {
	case <-sendErr:
	case <-time.After(time.Second):
	}
	select {
	case <-recvErr:
	case <-time.After(time.Second):
	}
	return elapsed
}

// TestReproStreamSerializationCeiling runs a head-to-head: same concurrency,
// same op count, same per-request service delay. The only difference is the
// transport — unary (one goroutine per RPC at the server) vs single bidi stream
// (one goroutine for the whole stream at the server).
//
// With a 2ms per-request service delay, unary with N=12 should deliver ~6000/s
// (12 concurrent handlers, each 2ms), while stream delivers ~500/s (serial).
func TestReproStreamSerializationCeiling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping repro in -short mode")
	}

	const (
		concurrency  = 12
		opsPerRun    = 1200
		serviceDelay = 2 * time.Millisecond
	)

	addr, fake, stop := startFakeFilerServer(t, serviceDelay)
	defer stop()

	conn := dialFakeFiler(t, addr)
	defer conn.Close()

	// warm up one call so the gRPC HTTP/2 connection is fully established
	warmupClient := filer_pb.NewSeaweedFilerClient(conn)
	if _, err := warmupClient.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/",
		Entry:     &filer_pb.Entry{Name: "warmup"},
	}); err != nil {
		t.Fatalf("warmup: %v", err)
	}
	fake.createCalls.Store(0)

	unaryDur := runUnaryCreateWorkload(t, conn, concurrency, opsPerRun)
	unaryCalls := fake.createCalls.Load()
	fake.createCalls.Store(0)

	streamDur := runStreamCreateWorkload(t, conn, concurrency, opsPerRun)
	streamCalls := fake.createCalls.Load()

	unaryQPS := float64(unaryCalls) / unaryDur.Seconds()
	streamQPS := float64(streamCalls) / streamDur.Seconds()

	t.Logf("service delay:       %v", serviceDelay)
	t.Logf("concurrency:         %d", concurrency)
	t.Logf("ops per run:         %d", opsPerRun)
	t.Logf("unary  : %d calls in %v -> %.0f QPS", unaryCalls, unaryDur, unaryQPS)
	t.Logf("stream : %d calls in %v -> %.0f QPS", streamCalls, streamDur, streamQPS)
	t.Logf("unary / stream ratio: %.2fx", unaryQPS/streamQPS)

	// The serial stream handler cannot exceed 1 / serviceDelay regardless of
	// client concurrency. Assert that unary is meaningfully faster — if the
	// server is ever parallelized, this assertion is safe to revisit.
	if unaryQPS < streamQPS*2 {
		t.Fatalf("expected unary to be at least 2x faster than stream, got unary=%.0f stream=%.0f",
			unaryQPS, streamQPS)
	}

	// Stream QPS should cluster near the theoretical ceiling of 1/serviceDelay.
	theoreticalMax := 1.0 / serviceDelay.Seconds()
	if streamQPS > theoreticalMax*1.25 {
		t.Fatalf("stream QPS %.0f exceeds theoretical serial ceiling %.0f — handler may have changed",
			streamQPS, theoreticalMax)
	}
}

// TestServerSerialVsConcurrentHandler measures the lift from changing the
// server-side handler from serial (one goroutine per stream) to concurrent
// (per-request goroutine, bounded by a semaphore, Send protected by a mutex).
//
// Client is identical in both runs (sync stream, N concurrent submitters).
// Only the server handler differs.
func TestServerSerialVsConcurrentHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping repro in -short mode")
	}

	const (
		opsPerRun         = 2400
		serviceDelay      = 2 * time.Millisecond
		serverConcurrency = 64
	)

	// Serial server (the OLD handler).
	serialAddr, serialFake, stopSerial := startFakeFilerServer(t, serviceDelay)
	defer stopSerial()
	serialConn := dialFakeFiler(t, serialAddr)
	defer serialConn.Close()

	// Concurrent server (the NEW handler).
	concAddr, concFake, stopConc := startFakeConcurrentFilerServer(t, serviceDelay, serverConcurrency)
	defer stopConc()
	concConn := dialFakeFiler(t, concAddr)
	defer concConn.Close()

	// Warm up both connections.
	for _, c := range []*grpc.ClientConn{serialConn, concConn} {
		client := filer_pb.NewSeaweedFilerClient(c)
		if _, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/",
			Entry:     &filer_pb.Entry{Name: "warmup"},
		}); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	clientConcurrencies := []int{1, 12, 64, 256}
	theoreticalSerial := 1.0 / serviceDelay.Seconds()
	theoreticalConcurrent := float64(serverConcurrency) / serviceDelay.Seconds()

	t.Logf("service delay:               %v", serviceDelay)
	t.Logf("server-side concurrency cap: %d", serverConcurrency)
	t.Logf("theoretical serial ceiling:  %.0f QPS", theoreticalSerial)
	t.Logf("theoretical concurrent cap:  %.0f QPS", theoreticalConcurrent)
	t.Logf("ops per run:                 %d", opsPerRun)
	t.Logf("")
	t.Logf("%-16s %-14s %-14s %-10s",
		"client workers", "serial QPS", "concurrent QPS", "lift")

	for _, c := range clientConcurrencies {
		serialFake.createCalls.Store(0)
		serialDur := runStreamCreateWorkload(t, serialConn, c, opsPerRun)
		serialCalls := serialFake.createCalls.Load()
		serialQPS := float64(serialCalls) / serialDur.Seconds()

		concFake.createCalls.Store(0)
		concDur := runStreamCreateWorkload(t, concConn, c, opsPerRun)
		concCalls := concFake.createCalls.Load()
		concQPS := float64(concCalls) / concDur.Seconds()

		t.Logf("%-16d %-14.0f %-14.0f %.2fx",
			c, serialQPS, concQPS, concQPS/serialQPS)
	}
}

// TestSchedulerOrderedParallelism compares three server-side handler shapes
// under two workloads:
//
//	serial     : OLD handler — one goroutine per stream
//	sem        : NEW handler v1 — per-request goroutine + semaphore, no ordering
//	scheduler  : NEW handler v2 — per-path admission + subtree barriers
//	              (mirrors filer.sync's MetadataProcessor)
//
// Workload "distinct" puts each op on its own path; workload "same" puts every
// op on the same path.
//
// Expected shape:
//   - serial     — both workloads ~1/serviceDelay (no parallelism)
//   - sem        — both workloads lift ~serverConcurrency× (wrong: reorders
//     same-path ops, acceptable for our filer store but undesired)
//   - scheduler  — distinct workload lifts ~serverConcurrency×
//     same workload falls back to ~1/serviceDelay (correctness)
func TestSchedulerOrderedParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping repro in -short mode")
	}

	const (
		opsPerRun         = 2400
		serviceDelay      = 2 * time.Millisecond
		serverConcurrency = 64
		clientWorkers     = 12
	)

	// Serial, sem-only concurrent, scheduler-based concurrent.
	serialAddr, serialFake, stopSerial := startFakeFilerServer(t, serviceDelay)
	defer stopSerial()
	serialConn := dialFakeFiler(t, serialAddr)
	defer serialConn.Close()

	semAddr, semFake, stopSem := startFakeConcurrentFilerServer(t, serviceDelay, serverConcurrency)
	defer stopSem()
	semConn := dialFakeFiler(t, semAddr)
	defer semConn.Close()

	schedAddr, schedFake, stopSched := startFakeSchedulerFilerServer(t, serviceDelay, serverConcurrency)
	defer stopSched()
	schedConn := dialFakeFiler(t, schedAddr)
	defer schedConn.Close()

	// Warm up all three connections.
	for _, c := range []*grpc.ClientConn{serialConn, semConn, schedConn} {
		client := filer_pb.NewSeaweedFilerClient(c)
		if _, err := client.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
			Directory: "/",
			Entry:     &filer_pb.Entry{Name: "warmup"},
		}); err != nil {
			t.Fatalf("warmup: %v", err)
		}
	}

	t.Logf("service delay:       %v", serviceDelay)
	t.Logf("server concurrency:  %d", serverConcurrency)
	t.Logf("client workers:      %d", clientWorkers)
	t.Logf("ops per run:         %d", opsPerRun)
	t.Logf("serial ceiling:      %.0f QPS", 1.0/serviceDelay.Seconds())
	t.Logf("concurrent ceiling:  %.0f QPS (%d x %.0f)", float64(serverConcurrency)/serviceDelay.Seconds(), serverConcurrency, 1.0/serviceDelay.Seconds())
	t.Logf("")

	workloads := []struct {
		name string
		fn   pathFn
	}{
		{"distinct paths", distinctPath},
		{"same path    ", samePath},
	}

	t.Logf("%-16s %-14s %-14s %-14s %-14s",
		"workload", "serial QPS", "sem QPS", "sched QPS", "sched peak-conc")

	for _, w := range workloads {
		serialFake.createCalls.Store(0)
		serialDur := runStreamCreateWorkloadAt(t, serialConn, clientWorkers, opsPerRun, w.fn)
		serialQPS := float64(serialFake.createCalls.Load()) / serialDur.Seconds()

		semFake.createCalls.Store(0)
		semDur := runStreamCreateWorkloadAt(t, semConn, clientWorkers, opsPerRun, w.fn)
		semQPS := float64(semFake.createCalls.Load()) / semDur.Seconds()

		schedFake.createCalls.Store(0)
		schedFake.maxInFlight.Store(0)
		schedDur := runStreamCreateWorkloadAt(t, schedConn, clientWorkers, opsPerRun, w.fn)
		schedQPS := float64(schedFake.createCalls.Load()) / schedDur.Seconds()
		schedPeak := schedFake.maxInFlight.Load()

		t.Logf("%-16s %-14.0f %-14.0f %-14.0f %d",
			w.name, serialQPS, semQPS, schedQPS, schedPeak)
	}
}

// TestStreamSyncVsAsyncClient measures whether making the CLIENT side
// asynchronous (fire-and-forget, no per-request waits) lifts the server-set
// ceiling. It runs sync and async workloads against the same serial server
// and compares end-to-end throughput.
//
// Expectation: at steady state, async cannot exceed 1/serviceDelay because the
// server still processes one request at a time. Any "improvement" is transient
// (in-flight pipelining up to the HTTP/2 window) and disappears as ops >> window.
func TestStreamSyncVsAsyncClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping repro in -short mode")
	}

	const (
		opsPerRun    = 2400
		serviceDelay = 2 * time.Millisecond
	)

	addr, fake, stop := startFakeFilerServer(t, serviceDelay)
	defer stop()

	conn := dialFakeFiler(t, addr)
	defer conn.Close()

	warmupClient := filer_pb.NewSeaweedFilerClient(conn)
	if _, err := warmupClient.CreateEntry(context.Background(), &filer_pb.CreateEntryRequest{
		Directory: "/",
		Entry:     &filer_pb.Entry{Name: "warmup"},
	}); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// Sweep concurrency to show that async does not break the 1/serviceDelay
	// ceiling no matter how much client-side parallelism we throw at it.
	concurrencies := []int{1, 12, 64, 256}
	t.Logf("service delay:    %v (theoretical serial ceiling = %.0f QPS)",
		serviceDelay, 1.0/serviceDelay.Seconds())
	t.Logf("ops per run:      %d", opsPerRun)
	t.Logf("")
	t.Logf("%-12s %-12s %-12s %-10s", "concurrency", "sync QPS", "async QPS", "ratio")

	for _, c := range concurrencies {
		fake.createCalls.Store(0)
		syncDur := runStreamCreateWorkload(t, conn, c, opsPerRun)
		syncCalls := fake.createCalls.Load()
		syncQPS := float64(syncCalls) / syncDur.Seconds()

		fake.createCalls.Store(0)
		asyncDur := runStreamAsyncCreateWorkload(t, conn, c, opsPerRun)
		asyncCalls := fake.createCalls.Load()
		asyncQPS := float64(asyncCalls) / asyncDur.Seconds()

		t.Logf("%-12d %-12.0f %-12.0f %-10.2fx",
			c, syncQPS, asyncQPS, asyncQPS/syncQPS)
	}
}
