package dash

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/worker_pb"
)

func newTestWorkerConn(bufferSize int) *WorkerConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerConnection{
		workerID: "w-test",
		outgoing: make(chan *worker_pb.AdminMessage, bufferSize),
		ctx:      ctx,
		cancel:   cancel,
	}
}

// TestSendToWorkerDropsOnCancelledContext verifies a send bails out via the
// connection context instead of blocking for the full timeout once the
// connection is torn down and its buffer is full.
func TestSendToWorkerDropsOnCancelledContext(t *testing.T) {
	s := &WorkerGrpcServer{adminServer: &AdminServer{}}
	conn := newTestWorkerConn(1)

	// Fill the buffer so the next send cannot enqueue.
	conn.outgoing <- &worker_pb.AdminMessage{}
	conn.cancel()

	start := time.Now()
	if s.sendToWorker(conn, &worker_pb.AdminMessage{}, 5*time.Second, "heartbeat response") {
		t.Fatal("expected send to be dropped when context cancelled and buffer full")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("send blocked for %v; expected an immediate context-cancelled bail", elapsed)
	}
}

// TestHeartbeatDuringUnregisterDoesNotPanic is a regression test for the
// "panic: send on closed channel" crash: unregisterWorker (called from the
// stale-connection cleanup routine) used to close conn.outgoing while the
// WorkerStream goroutine was still sending heartbeat responses to it.
func TestHeartbeatDuringUnregisterDoesNotPanic(t *testing.T) {
	s := &WorkerGrpcServer{
		adminServer: &AdminServer{},
		connections: make(map[string]*WorkerConnection),
	}
	conn := newTestWorkerConn(100)
	s.connections[conn.workerID] = conn

	// Drain outgoing like handleOutgoingMessages would, until teardown.
	go func() {
		for {
			select {
			case <-conn.ctx.Done():
				return
			case <-conn.outgoing:
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			s.handleHeartbeat(conn, &worker_pb.WorkerHeartbeat{})
		}
	}()
	go func() {
		defer wg.Done()
		s.unregisterWorker(conn, "test")
	}()

	// Under the old close()-based teardown this would panic the sender goroutine
	// and crash the test binary.
	wg.Wait()
}
