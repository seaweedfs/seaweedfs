package mount

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// streamMutateMux multiplexes filer mutation RPCs (create, update, delete,
// rename) over a single bidirectional gRPC stream. Multiple goroutines can
// call the mutation methods concurrently; requests are serialized through
// sendCh and responses are dispatched back via per-request channels.
type streamMutateMux struct {
	wfs *WFS

	mu       sync.Mutex // protects stream, cancel, closed
	stream   filer_pb.SeaweedFiler_StreamMutateEntryClient
	cancel   context.CancelFunc
	closed   bool
	disabled bool // permanently disabled if filer doesn't support the RPC

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
	if r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_CreateResponse); ok {
		return r.CreateResponse, nil
	}
	return nil, fmt.Errorf("unexpected response type %T", resp.Response)
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
	if r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_DeleteResponse); ok {
		return r.DeleteResponse, nil
	}
	return nil, fmt.Errorf("unexpected response type %T", resp.Response)
}

// Rename sends a StreamRenameEntryRequest over the stream and collects all
// response events until is_last=true. The callback is invoked for each
// intermediate rename event (same as the current StreamRenameEntry recv loop).
func (m *streamMutateMux) Rename(ctx context.Context, req *filer_pb.StreamRenameEntryRequest, onEvent func(*filer_pb.StreamRenameEntryResponse) error) error {
	if err := m.ensureStream(); err != nil {
		return err
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
	}
	select {
	case m.sendCh <- sendReq:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := <-sendReq.errCh; err != nil {
		return err
	}

	// Collect rename events until is_last=true.
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				return fmt.Errorf("stream closed during rename")
			}
			if r, ok := resp.Response.(*filer_pb.StreamMutateEntryResponse_RenameResponse); ok {
				if r.RenameResponse != nil && r.RenameResponse.EventNotification != nil {
					if err := onEvent(r.RenameResponse); err != nil {
						return err
					}
				}
			}
			if resp.IsLast {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// doUnary sends a single-response request and waits for the reply.
func (m *streamMutateMux) doUnary(ctx context.Context, req *filer_pb.StreamMutateEntryRequest) (*filer_pb.StreamMutateEntryResponse, error) {
	if err := m.ensureStream(); err != nil {
		return nil, err
	}

	id := m.nextID.Add(1)
	req.RequestId = id
	ch := make(chan *filer_pb.StreamMutateEntryResponse, 1)
	m.pending.Store(id, ch)
	defer m.pending.Delete(id)

	sendReq := &streamMutateReq{
		req:   req,
		errCh: make(chan error, 1),
	}
	select {
	case m.sendCh <- sendReq:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if err := <-sendReq.errCh; err != nil {
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("stream closed")
		}
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ensureStream opens the bidi stream if not already open.
func (m *streamMutateMux) ensureStream() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.disabled {
		return fmt.Errorf("StreamMutateEntry not supported by filer")
	}
	if m.stream != nil {
		return nil
	}

	var stream filer_pb.SeaweedFiler_StreamMutateEntryClient
	err := m.openStream(&stream)
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unimplemented {
			m.disabled = true
			glog.V(0).Infof("filer does not support StreamMutateEntry, falling back to unary RPCs")
		}
		return err
	}

	m.stream = stream
	m.recvDone = make(chan struct{})
	go m.sendLoop()
	go m.recvLoop()
	return nil
}

func (m *streamMutateMux) openStream(out *filer_pb.SeaweedFiler_StreamMutateEntryClient) error {
	i := atomic.LoadInt32(&m.wfs.option.filerIndex)
	filerGrpcAddress := m.wfs.option.FilerAddresses[i].ToGrpcAddress()
	return pb.WithGrpcClient(true, m.wfs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.StreamMutateEntry(ctx)
		if err != nil {
			cancel()
			return err
		}
		m.cancel = cancel
		*out = stream
		return nil
	}, filerGrpcAddress, false, m.wfs.option.GrpcDialOption)
}

func (m *streamMutateMux) sendLoop() {
	for req := range m.sendCh {
		err := m.stream.Send(req.req)
		req.errCh <- err
		if err != nil {
			m.failAllPending(err)
			return
		}
	}
}

func (m *streamMutateMux) recvLoop() {
	defer close(m.recvDone)
	for {
		resp, err := m.stream.Recv()
		if err != nil {
			if err != io.EOF {
				m.failAllPending(err)
			}
			m.mu.Lock()
			m.stream = nil
			if m.cancel != nil {
				m.cancel()
				m.cancel = nil
			}
			m.mu.Unlock()
			return
		}

		if ch, ok := m.pending.Load(resp.RequestId); ok {
			ch.(chan *filer_pb.StreamMutateEntryResponse) <- resp
			// For single-response ops, the caller deletes from pending after recv.
			// For rename, the caller collects until is_last.
		}
	}
}

func (m *streamMutateMux) failAllPending(err error) {
	m.pending.Range(func(key, value any) bool {
		ch := value.(chan *filer_pb.StreamMutateEntryResponse)
		close(ch) // causes waiters to get ok=false
		m.pending.Delete(key)
		return true
	})
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
	m.mu.Unlock()

	close(m.sendCh)
	if stream != nil {
		stream.CloseSend()
		<-m.recvDone
	}
	if cancel != nil {
		cancel()
	}
}
