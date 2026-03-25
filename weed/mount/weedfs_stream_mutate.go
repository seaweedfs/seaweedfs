package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcMetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// streamMutateError is returned when the server reports a structured errno.
// It is also used by helpers to distinguish application errors (don't retry
// on unary fallback) from transport errors (do retry).
type streamMutateError struct {
	msg   string
	errno syscall.Errno
}

func (e *streamMutateError) Error() string        { return e.msg }
func (e *streamMutateError) Errno() syscall.Errno { return e.errno }

// ErrStreamTransport is a sentinel error type for transport-level stream
// failures (disconnects, send errors). Callers use errors.Is to decide
// whether to fall back to unary RPCs.
var ErrStreamTransport = errors.New("stream transport error")

// streamMutateMux multiplexes filer mutation RPCs (create, update, delete,
// rename) over a single bidirectional gRPC stream. Multiple goroutines can
// call the mutation methods concurrently; requests are serialized through
// sendCh and responses are dispatched back via per-request channels.
type streamMutateMux struct {
	wfs *WFS

	mu         sync.Mutex // protects stream, cancel, grpcConn, closed, stopSend, generation
	stream     filer_pb.SeaweedFiler_StreamMutateEntryClient
	cancel     context.CancelFunc
	grpcConn   *grpc.ClientConn // dedicated connection, closed on stream teardown
	closed     bool
	disabled   bool       // permanently disabled if filer doesn't support the RPC
	stopSend   chan struct{} // closed to signal the current sendLoop to exit
	generation uint64     // incremented each time a new stream is created

	nextID atomic.Uint64

	// pending maps request_id → response channel. The recvLoop dispatches
	// each response to the correct waiter. For rename (multi-response),
	// the channel receives multiple messages until is_last=true.
	pending sync.Map // map[uint64]chan *filer_pb.StreamMutateEntryResponse

	sendCh   chan *streamMutateReq
	recvDone chan struct{} // closed when recvLoop exits
}

type streamMutateReq struct {
	req   *filer_pb.StreamMutateEntryRequest
	errCh chan error // send error feedback
	gen   uint64    // stream generation this request targets
}

func newStreamMutateMux(wfs *WFS) *streamMutateMux {
	return &streamMutateMux{
		wfs:    wfs,
		sendCh: make(chan *streamMutateReq, 512),
	}
}

// CreateEntry sends a CreateEntryRequest over the stream and waits for the response.
func (m *streamMutateMux) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	resp, err := m.doUnary(ctx, &filer_pb.StreamMutateEntryRequest{
		Request: &filer_pb.StreamMutateEntryRequest_CreateRequest{CreateRequest: req},
	})
	if err != nil {
		return nil, err
	}
	r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_CreateResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", resp.Response)
	}
	// Check nested error fields (same logic as CreateEntryWithResponse).
	cr := r.CreateResponse
	if cr.ErrorCode != filer_pb.FilerError_OK {
		if sentinel := filer_pb.FilerErrorToSentinel(cr.ErrorCode); sentinel != nil {
			return nil, fmt.Errorf("CreateEntry %s/%s: %w", req.Directory, req.Entry.Name, sentinel)
		}
		return nil, &streamMutateError{msg: cr.Error, errno: syscall.EIO}
	}
	if cr.Error != "" {
		return nil, &streamMutateError{msg: cr.Error, errno: syscall.EIO}
	}
	return cr, nil
}

// UpdateEntry sends an UpdateEntryRequest over the stream and waits for the response.
func (m *streamMutateMux) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	resp, err := m.doUnary(ctx, &filer_pb.StreamMutateEntryRequest{
		Request: &filer_pb.StreamMutateEntryRequest_UpdateRequest{UpdateRequest: req},
	})
	if err != nil {
		return nil, err
	}
	if r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_UpdateResponse); ok {
		return r.UpdateResponse, nil
	}
	return nil, fmt.Errorf("unexpected response type %T", resp.Response)
}

// DeleteEntry sends a DeleteEntryRequest over the stream and waits for the response.
func (m *streamMutateMux) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	resp, err := m.doUnary(ctx, &filer_pb.StreamMutateEntryRequest{
		Request: &filer_pb.StreamMutateEntryRequest_DeleteRequest{DeleteRequest: req},
	})
	if err != nil {
		return nil, err
	}
	r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_DeleteResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", resp.Response)
	}
	// Check nested error field.
	if r.DeleteResponse.Error != "" {
		return nil, &streamMutateError{msg: r.DeleteResponse.Error, errno: syscall.EIO}
	}
	return r.DeleteResponse, nil
}

// Rename sends a StreamRenameEntryRequest over the stream and collects all
// response events until is_last=true. The callback is invoked for each
// intermediate rename event (same as the current StreamRenameEntry recv loop).
func (m *streamMutateMux) Rename(ctx context.Context, req *filer_pb.StreamRenameEntryRequest, onEvent func(*filer_pb.StreamRenameEntryResponse) error) error {
	gen, err := m.ensureStream()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStreamTransport, err)
	}

	id := m.nextID.Add(1)
	ch := make(chan *filer_pb.StreamMutateEntryResponse, 64)
	m.pending.Store(id, ch)
	defer m.pending.Delete(id)

	sendReq := &streamMutateReq{
		req: &filer_pb.StreamMutateEntryRequest{
			RequestId: id,
			Request:   &filer_pb.StreamMutateEntryRequest_RenameRequest{RenameRequest: req},
		},
		errCh: make(chan error, 1),
		gen:   gen,
	}
	select {
	case m.sendCh <- sendReq:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := <-sendReq.errCh; err != nil {
		return fmt.Errorf("rename send: %w: %v", ErrStreamTransport, err)
	}

	// Collect rename events until is_last=true.
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				return fmt.Errorf("rename recv: %w: stream closed", ErrStreamTransport)
			}
			if r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_RenameResponse); ok {
				if r.RenameResponse != nil && r.RenameResponse.EventNotification != nil {
					if err := onEvent(r.RenameResponse); err != nil {
						return err
					}
				}
			}
			if resp.IsLast {
				if resp.Error != "" {
					return &streamMutateError{
						msg:   resp.Error,
						errno: syscall.Errno(resp.Errno),
					}
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// doUnary sends a single-response request and waits for the reply.
func (m *streamMutateMux) doUnary(ctx context.Context, req *filer_pb.StreamMutateEntryRequest) (*filer_pb.StreamMutateEntryResponse, error) {
	gen, err := m.ensureStream()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamTransport, err)
	}

	id := m.nextID.Add(1)
	req.RequestId = id
	ch := make(chan *filer_pb.StreamMutateEntryResponse, 1)
	m.pending.Store(id, ch)
	defer m.pending.Delete(id)

	sendReq := &streamMutateReq{
		req:   req,
		errCh: make(chan error, 1),
		gen:   gen,
	}
	select {
	case m.sendCh <- sendReq:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := <-sendReq.errCh; err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamTransport, err)
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("%w: stream closed", ErrStreamTransport)
		}
		if resp.Error != "" {
			return nil, &streamMutateError{
				msg:   resp.Error,
				errno: syscall.Errno(resp.Errno),
			}
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ensureStream opens the bidi stream if not already open. It returns the
// stream generation so callers can tag outgoing requests.
func (m *streamMutateMux) ensureStream() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, fmt.Errorf("stream mux is closed")
	}
	if m.disabled {
		return 0, fmt.Errorf("StreamMutateEntry not supported by filer")
	}
	if m.stream != nil {
		return m.generation, nil
	}

	// Wait for prior generation's recvLoop to fully tear down before opening
	// a new stream. This guarantees all pending waiters from the old stream
	// have been failed before we create a new generation.
	if m.recvDone != nil {
		done := m.recvDone
		m.mu.Unlock()
		<-done
		m.mu.Lock()
		// Re-check after reacquiring the lock.
		if m.closed {
			return 0, fmt.Errorf("stream mux is closed")
		}
		if m.disabled {
			return 0, fmt.Errorf("StreamMutateEntry not supported by filer")
		}
		if m.stream != nil {
			return m.generation, nil
		}
	}

	var stream filer_pb.SeaweedFiler_StreamMutateEntryClient
	err := m.openStream(&stream)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
			m.disabled = true
			glog.V(0).Infof("filer does not support StreamMutateEntry, falling back to unary RPCs")
		}
		return 0, err
	}

	m.generation++
	m.stream = stream
	m.stopSend = make(chan struct{})
	recvDone := make(chan struct{})
	m.recvDone = recvDone
	gen := m.generation
	go m.sendLoop(stream, m.stopSend, gen)
	go m.recvLoop(stream, gen, recvDone)
	return gen, nil
}

func (m *streamMutateMux) openStream(out *filer_pb.SeaweedFiler_StreamMutateEntryClient) error {
	i := atomic.LoadInt32(&m.wfs.option.filerIndex)
	n := int32(len(m.wfs.option.FilerAddresses))
	var lastErr error

	for x := int32(0); x < n; x++ {
		idx := (i + x) % n
		filerGrpcAddress := m.wfs.option.FilerAddresses[idx].ToGrpcAddress()

		ctx := context.Background()
		if m.wfs.signature != 0 {
			ctx = grpcMetadata.AppendToOutgoingContext(ctx, "sw-client-id", fmt.Sprintf("%d", m.wfs.signature))
		}
		grpcConn, err := pb.GrpcDial(ctx, filerGrpcAddress, false, m.wfs.option.GrpcDialOption)
		if err != nil {
			lastErr = fmt.Errorf("stream dial %s: %v", filerGrpcAddress, err)
			continue
		}

		client := filer_pb.NewSeaweedFilerClient(grpcConn)
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := client.StreamMutateEntry(streamCtx)
		if err != nil {
			cancel()
			grpcConn.Close()
			lastErr = err
			// Unimplemented means all filers lack it — stop rotating.
			if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
				return err
			}
			continue
		}

		atomic.StoreInt32(&m.wfs.option.filerIndex, idx)
		m.cancel = cancel
		m.grpcConn = grpcConn
		*out = stream
		return nil
	}
	return lastErr
}

func (m *streamMutateMux) sendLoop(stream filer_pb.SeaweedFiler_StreamMutateEntryClient, stop <-chan struct{}, gen uint64) {
	for {
		select {
		case req, ok := <-m.sendCh:
			if !ok {
				return // Close() was called
			}
			if req.gen != gen {
				req.errCh <- fmt.Errorf("%w: stream generation mismatch", ErrStreamTransport)
				continue
			}
			err := stream.Send(req.req)
			req.errCh <- err
			if err != nil {
				m.teardownStream(gen)
				return
			}
		case <-stop:
			return
		}
	}
}

func (m *streamMutateMux) recvLoop(stream filer_pb.SeaweedFiler_StreamMutateEntryClient, gen uint64, recvDone chan struct{}) {
	defer close(recvDone)
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				glog.V(1).Infof("stream mutate recv error (gen=%d): %v", gen, err)
			}
			m.teardownStream(gen)
			return
		}

		if ch, ok := m.pending.Load(resp.RequestId); ok {
			ch.(chan *filer_pb.StreamMutateEntryResponse) <- resp
			// For single-response ops, the caller deletes from pending after recv.
			// For rename, the caller collects until is_last.
		}
	}
}

// teardownStream cleans up the stream for the given generation. It is safe to
// call from both sendLoop and recvLoop; only the first call for a given
// generation takes effect (idempotent via generation + nil-stream check).
func (m *streamMutateMux) teardownStream(gen uint64) {
	m.mu.Lock()
	if m.generation != gen || m.stream == nil {
		m.mu.Unlock()
		return
	}
	m.stream = nil
	if m.stopSend != nil {
		close(m.stopSend)
		m.stopSend = nil
	}
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	conn := m.grpcConn
	m.grpcConn = nil
	m.mu.Unlock()

	// Fail pending outside the lock to avoid holding mu during channel closes.
	m.failAllPending()

	if conn != nil {
		conn.Close()
	}
}

// failAllPending closes all pending response channels, causing waiters to
// receive ok=false. It is idempotent: entries are deleted before channels are
// closed, so concurrent calls cannot double-close.
func (m *streamMutateMux) failAllPending() {
	var channels []chan *filer_pb.StreamMutateEntryResponse
	m.pending.Range(func(key, value any) bool {
		m.pending.Delete(key)
		channels = append(channels, value.(chan *filer_pb.StreamMutateEntryResponse))
		return true
	})
	for _, ch := range channels {
		close(ch)
	}
}

// IsAvailable returns true if the stream mux is usable (not permanently disabled).
func (m *streamMutateMux) IsAvailable() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.disabled
}

// Close shuts down the stream. Called during unmount after all flushes complete.
func (m *streamMutateMux) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	stream := m.stream
	cancel := m.cancel
	grpcConn := m.grpcConn
	if m.stopSend != nil {
		close(m.stopSend)
		m.stopSend = nil
	}
	m.mu.Unlock()

	close(m.sendCh)
	if stream != nil {
		stream.CloseSend()
		<-m.recvDone
	}
	if cancel != nil {
		cancel()
	}
	if grpcConn != nil {
		grpcConn.Close()
	}
}
