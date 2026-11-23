package weed_server

// Streaming methods under a scoped token: the server stream handed to the
// handler is wrapped so that every request message is checked as it is
// received and every response message is filtered as it is sent. A stream
// keeps the grants of the token it was opened with and enforces expiry at
// each message boundary. Subscription handlers also end idle streams at expiry;
// other idle streams await client or transport cancellation. A client whose
// token changed reconnects.

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// stream is the stream server interceptor.
func (s *filerGrpcScope) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !strings.HasPrefix(info.FullMethod, scopeFilerMethodPrefix) {
		return handler(srv, ss)
	}
	sc, err := s.resolve(ss.Context())
	if err != nil {
		// A bearer that is no filer scope leaves a subscription to the existing
		// verifier, as before scoped tokens: it admits an mTLS peer, or any
		// caller when no subscription key is set, whatever the bearer.
		method := strings.TrimPrefix(info.FullMethod, scopeFilerMethodPrefix)
		if s.cfg.subscriptionScope != nil && (method == "SubscribeMetadata" || method == "SubscribeLocalMetadata") {
			if _, authErr := bearerFromMetadata(ss.Context()); authErr == nil {
				if _, subErr := s.cfg.subscriptionScope(ss.Context(), method); subErr == nil {
					return handler(srv, ss)
				}
			}
		}
		return err
	}
	if sc == nil {
		return handler(srv, ss)
	}
	if sc.expired() {
		return scopeExpiredError(sc)
	}
	if !sc.methodAllowed(info.FullMethod) {
		return sc.deny(info.FullMethod, "method is not in the token's allowed_methods")
	}
	if !scopeStreamMethods[strings.TrimPrefix(info.FullMethod, scopeFilerMethodPrefix)] {
		return sc.deny(info.FullMethod, "not available to a scoped token")
	}
	w := &scopedStream{ServerStream: ss, sc: sc, ctx: s.withWriteAuthority(ss.Context(), sc)}
	switch info.FullMethod {
	case filer_pb.SeaweedFiler_SubscribeMetadata_FullMethodName, filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName:
		// Preserve the verified authority for the handler, including on mTLS.
		// Its scoped stream applies the deadline, registration isolation and
		// event filter without verifying this token against a different key.
		w.ctx = context.WithValue(w.Context(), scopeSubscriptionContextKey{}, sc)
		w.onRecv = func(m any) (bool, error) {
			req, ok := m.(*filer_pb.SubscribeMetadataRequest)
			if !ok {
				return false, sc.deny(info.FullMethod, fmt.Sprintf("unexpected request %T", m))
			}
			return false, sc.checkSubscribe(info.FullMethod, req)
		}
	case filer_pb.SeaweedFiler_ListEntries_FullMethodName:
		w.ctx = context.WithValue(w.Context(), scopeListContextKey{}, true)
		var listedDir string
		w.onRecv = func(m any) (bool, error) {
			req, ok := m.(*filer_pb.ListEntriesRequest)
			if !ok {
				return false, sc.deny("ListEntries", fmt.Sprintf("unexpected request %T", m))
			}
			listedDir = req.GetDirectory()
			_, err := sc.checkPath("ListEntries", req.GetDirectory(), "", false)
			return false, err
		}
		w.onSend = func(m any) (any, bool, error) {
			resp, ok := m.(*filer_pb.ListEntriesResponse)
			if !ok {
				return m, true, nil
			}
			// filer.conf is hidden from a scoped token, as in LookupDirectoryEntry:
			// omit it from a listing of its own directory.
			if isScopeFilerConf(listedDir, resp.GetEntry().GetName()) {
				return nil, false, nil
			}
			return m, true, nil
		}
	case filer_pb.SeaweedFiler_StreamRenameEntry_FullMethodName:
		w.onRecv = func(m any) (bool, error) {
			req, ok := m.(*filer_pb.StreamRenameEntryRequest)
			if !ok {
				return false, sc.deny("StreamRenameEntry", fmt.Sprintf("unexpected request %T", m))
			}
			return false, s.checkRename(w.Context(), sc, "StreamRenameEntry", req.GetOldDirectory(), req.GetOldName(), req.GetNewDirectory(), req.GetNewName())
		}
	case filer_pb.SeaweedFiler_StreamMutateEntry_FullMethodName:
		w.onRecv = func(m any) (bool, error) {
			req, ok := m.(*filer_pb.StreamMutateEntryRequest)
			if !ok {
				return false, sc.deny("StreamMutateEntry", fmt.Sprintf("unexpected request %T", m))
			}
			if checkErr := s.checkMutate(w, sc, req); checkErr != nil {
				// A refused message is answered on the stream for its request id
				// (errno EACCES) and the stream continues with the next message.
				out := &filer_pb.StreamMutateEntryResponse{RequestId: req.GetRequestId(), IsLast: true, Error: checkErr.Error(), Errno: scopeErrnoEACCES}
				if err := w.sendRaw(out); err != nil {
					return false, err
				}
				return true, nil
			}
			return false, nil
		}
	default:
		return sc.deny(info.FullMethod, "not available to a scoped token")
	}
	return handler(srv, w)
}

// scopedStream wraps the handler's stream. onRecv checks (and may rewrite) a
// received request; returning skip=true answers the message itself and reads
// the next one. onSend filters a response: it returns the message to send
// (possibly a filtered copy) or send=false to drop it.
type scopedStream struct {
	grpc.ServerStream
	sc     *scope
	ctx    context.Context
	onRecv func(m any) (skip bool, err error)
	onSend func(m any) (out any, send bool, err error)
	sendMu sync.Mutex
}

// ListEntries uses this server-only marker to omit the configuration entry
// before counting visible rows or stamping the first response's snapshot.
type scopeListContextKey struct{}

func scopeHideListEntry(ctx context.Context, dir, name string) bool {
	scoped, _ := ctx.Value(scopeListContextKey{}).(bool)
	return scoped && isScopeFilerConf(dir, name)
}

func (w *scopedStream) Context() context.Context {
	if w.ctx != nil {
		return w.ctx
	}
	return w.ServerStream.Context()
}

func (w *scopedStream) RecvMsg(m any) error {
	for {
		if w.sc.expired() {
			return scopeExpiredError(w.sc)
		}
		if err := w.ServerStream.RecvMsg(m); err != nil {
			return err
		}
		if w.sc.expired() {
			return scopeExpiredError(w.sc)
		}
		if w.onRecv == nil {
			return nil
		}
		skip, err := w.onRecv(m)
		if err != nil {
			return err
		}
		if !skip {
			return nil
		}
		// The handler's message is reused for the next receive: clear it so
		// fields of the refused message cannot leak into a partial decode.
		if pm, ok := m.(proto.Message); ok {
			proto.Reset(pm)
		}
	}
}

func (w *scopedStream) SendMsg(m any) error {
	if w.sc.expired() {
		return scopeExpiredError(w.sc)
	}
	if w.onSend != nil {
		out, send, err := w.onSend(m)
		if err != nil {
			return err
		}
		if !send {
			return nil
		}
		m = out
	}
	return w.sendRaw(m)
}

// sendRaw sends on the underlying stream; the handler's own sends and the
// refusals written from RecvMsg share one lock, as gRPC requires.
func (w *scopedStream) sendRaw(m any) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	if w.sc.expired() {
		return scopeExpiredError(w.sc)
	}
	return w.ServerStream.SendMsg(m)
}

// checkMutate checks one StreamMutateEntry message as its unary counterpart.
func (s *filerGrpcScope) checkMutate(w *scopedStream, sc *scope, req *filer_pb.StreamMutateEntryRequest) error {
	const m = "StreamMutateEntry"
	ctx := w.Context()
	switch r := req.GetRequest().(type) {
	case *filer_pb.StreamMutateEntryRequest_CreateRequest:
		return s.checkCreate(ctx, sc, m, r.CreateRequest)
	case *filer_pb.StreamMutateEntryRequest_UpdateRequest:
		return s.checkUpdate(ctx, sc, m, r.UpdateRequest)
	case *filer_pb.StreamMutateEntryRequest_DeleteRequest:
		return sc.checkDelete(m, r.DeleteRequest)
	case *filer_pb.StreamMutateEntryRequest_RenameRequest:
		return s.checkRename(ctx, sc, m, r.RenameRequest.GetOldDirectory(), r.RenameRequest.GetOldName(), r.RenameRequest.GetNewDirectory(), r.RenameRequest.GetNewName())
	}
	return sc.deny(m, "empty mutate request")
}
