package weed_server

// Streaming methods under a scoped token: the server stream handed to the
// handler is wrapped so that every request message is checked as it is
// received and every response message is filtered as it is sent. A stream
// keeps the grants of the token it was opened with and ends at its expiry;
// a client whose token changed reconnects.

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// metadataSubscriptionMethod reports whether fullMethod is one of the two filer
// metadata-subscription streams.
func metadataSubscriptionMethod(fullMethod string) bool {
	return fullMethod == filer_pb.SeaweedFiler_SubscribeMetadata_FullMethodName ||
		fullMethod == filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName
}

// subscribeKeyAccepts reports whether a subscription call that failed scope-key
// verification carries a bearer token the subscribe-key path accepts -- a token
// signed with jwt.filer_signing.subscribe.key, or an admin token -- so the call
// is deferred to the handler, which re-runs the check and bounds the stream to
// the token's scope. It checks the token independently of the mTLS shortcut
// (CheckGrpcMetadataSubscriptionToken, not CheckGrpcMetadataSubscription): an
// mTLS peer with no valid token is not accepted here, so require mode still
// refuses it. It returns false when no subscribe key is configured, so a token
// invalid under the scope key is still refused there, keeping require mode
// fail-closed.
func (s *filerGrpcScope) subscribeKeyAccepts(ctx context.Context, fullMethod string) bool {
	subKey := s.cfg.subscribeKey()
	if len(subKey) == 0 {
		return false
	}
	short := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	_, err := security.CheckGrpcMetadataSubscriptionToken(ctx, subKey, s.cfg.adminKey(), short)
	return err == nil
}

// stream is the stream server interceptor.
func (s *filerGrpcScope) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if !strings.HasPrefix(info.FullMethod, scopeFilerMethodPrefix) {
		return handler(srv, ss)
	}
	sc, err := s.resolve(ss.Context())
	if err != nil {
		// A metadata-subscription token signed with jwt.filer_signing.subscribe.key
		// fails scope-key verification; the handler authorizes it
		// (CheckGrpcMetadataSubscription), as it did before scoped tokens existed.
		// Defer to the handler for the two subscription methods when that key
		// accepts the token; otherwise keep the refusal, so require mode stays
		// fail-closed and a token invalid under both keys is still refused.
		if metadataSubscriptionMethod(info.FullMethod) && s.subscribeKeyAccepts(ss.Context(), info.FullMethod) {
			return handler(srv, ss)
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
	w := &scopedStream{ServerStream: ss, sc: sc}
	switch info.FullMethod {
	case filer_pb.SeaweedFiler_ListEntries_FullMethodName:
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
			return false, sc.checkRename("StreamRenameEntry", req.GetOldDirectory(), req.GetOldName(), req.GetNewDirectory(), req.GetNewName())
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
	case filer_pb.SeaweedFiler_SubscribeMetadata_FullMethodName, filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName:
		m := "SubscribeMetadata"
		if info.FullMethod == filer_pb.SeaweedFiler_SubscribeLocalMetadata_FullMethodName {
			m = "SubscribeLocalMetadata"
		}
		// The interceptor has verified this grpc_key token and bounds the stream
		// here; tell the handler so its own subscribe/filer-key check does not
		// reject a token it never signed. The stream stays bounded by onRecv/onSend.
		w.ctx = context.WithValue(ss.Context(), subscriptionPreAuthKey, true)
		w.onRecv = func(msg any) (bool, error) {
			req, ok := msg.(*filer_pb.SubscribeMetadataRequest)
			if !ok {
				return false, sc.deny(m, fmt.Sprintf("unexpected request %T", msg))
			}
			return false, sc.checkSubscribe(m, req)
		}
		w.onSend = func(msg any) (any, bool, error) {
			resp, ok := msg.(*filer_pb.SubscribeMetadataResponse)
			if !ok {
				return msg, true, nil
			}
			if len(resp.GetLogFileRefs()) > 0 {
				return nil, false, sc.deny(m, "log file references are not available to a scoped token")
			}
			out := sc.filterEvent(resp)
			if out == nil {
				return nil, false, nil
			}
			return out, true, nil
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

// Context returns the wrapped stream's context, overridden when the interceptor
// needs the handler to see that it already authorized the stream (see
// subscriptionPreAuthKey).
func (w *scopedStream) Context() context.Context {
	if w.ctx != nil {
		return w.ctx
	}
	return w.ServerStream.Context()
}

// subscriptionPreAuthKey marks a context whose metadata-subscription stream a
// scoped grpc_key token already passed through the scope interceptor, which
// verified the token and bounds the stream to its prefixes. metadataSubscriptionScope
// then adds no further handler-level restriction, so a valid grpc_key
// subscription token is not rejected by the handler's subscribe/filer-key check.
type subscriptionPreAuthKeyType struct{}

var subscriptionPreAuthKey subscriptionPreAuthKeyType

func (w *scopedStream) RecvMsg(m any) error {
	for {
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
		return sc.checkRename(m, r.RenameRequest.GetOldDirectory(), r.RenameRequest.GetOldName(), r.RenameRequest.GetNewDirectory(), r.RenameRequest.GetNewName())
	}
	return sc.deny(m, "empty mutate request")
}

// checkSubscribe admits a metadata subscription whose every prefix and
// directory is allowed, and rewrites the request: inline events only
// (metadata chunks would hand the client the shared log files holding every
// path's events), and without the filer.conf prefix weed mount adds to reload
// storage rules (that file is hidden from scoped tokens, see
// LookupDirectoryEntry).
func (sc *scope) checkSubscribe(m string, req *filer_pb.SubscribeMetadataRequest) error {
	req.ClientSupportsMetadataChunks = false
	kept := req.PathPrefixes[:0]
	for _, p := range req.GetPathPrefixes() {
		if p != scopeFilerConfDir+"/"+scopeFilerConfName {
			kept = append(kept, p)
		}
	}
	req.PathPrefixes = kept
	prefixes := append([]string{}, req.GetPathPrefixes()...)
	if req.GetPathPrefix() != "" {
		prefixes = append(prefixes, req.GetPathPrefix())
	}
	if len(prefixes) == 0 && len(req.GetDirectories()) == 0 {
		return sc.deny(m, "a subscription must name at least one path prefix or directory")
	}
	for _, p := range prefixes {
		if !scopePrefixAllowed(sc.grants, p) {
			return sc.deny(m, fmt.Sprintf("subscription prefix %q is outside the token's scope", p))
		}
	}
	for _, d := range req.GetDirectories() {
		if _, err := sc.checkPath(m, d, "", false); err != nil {
			return err
		}
	}
	return nil
}

// filterEvent returns the response to deliver, or nil to drop it. A rename
// across the scope boundary is delivered as the half the holder may see: a
// delete of the old path, or a create of the new one.
func (sc *scope) filterEvent(resp *filer_pb.SubscribeMetadataResponse) *filer_pb.SubscribeMetadataResponse {
	if len(resp.GetEvents()) > 0 {
		kept := make([]*filer_pb.SubscribeMetadataResponse, 0, len(resp.Events))
		for _, ev := range resp.Events {
			if out := sc.filterEvent(ev); out != nil {
				kept = append(kept, out)
			}
		}
		if len(kept) == 0 {
			return nil
		}
		out := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		out.Events = kept
		return out
	}
	ev := resp.GetEventNotification()
	if ev == nil {
		return resp // heartbeat / idle marker
	}
	// filer.conf is hidden from a scoped token on every read path, even when the
	// subscription grant covers /etc/seaweedfs.
	newDir := resp.GetDirectory()
	if ev.GetNewParentPath() != "" {
		newDir = ev.GetNewParentPath()
	}
	oldIsConf := ev.GetOldEntry() != nil && resp.GetDirectory() == scopeFilerConfDir && ev.GetOldEntry().GetName() == scopeFilerConfName
	newIsConf := ev.GetNewEntry() != nil && newDir == scopeFilerConfDir && ev.GetNewEntry().GetName() == scopeFilerConfName
	if oldIsConf {
		// Never reveal filer.conf's content, including a rename of it out to a
		// visible name (the new side would carry its chunks).
		return nil
	}
	if newIsConf {
		if ev.GetOldEntry() == nil {
			return nil // a create or in-place update of filer.conf: nothing visible
		}
		// A rename of a visible entry into filer.conf: hide only the filer.conf
		// (new) side. The old (source) side is still delivered below as a
		// deletion when it is in scope, so the subscriber does not keep a stale
		// copy of the entry that moved.
		resp = proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		ev = resp.GetEventNotification()
		ev.NewEntry, ev.NewParentPath = nil, ""
	}
	if ev.OldEntry == nil && ev.NewEntry == nil {
		// Nothing names an entry: decide by the event's directory alone.
		if p, err := scopeFullPath(resp.GetDirectory(), ""); err == nil && scopePathAllowed(sc.grants, p, false) {
			return resp
		}
		return nil
	}
	oldOK, newOK := true, true
	if ev.OldEntry != nil {
		p, err := scopeFullPath(resp.GetDirectory(), ev.OldEntry.GetName())
		oldOK = err == nil && scopePathAllowed(sc.grants, p, false)
	}
	if ev.NewEntry != nil {
		dir := resp.GetDirectory()
		if ev.GetNewParentPath() != "" {
			dir = ev.GetNewParentPath()
		}
		p, err := scopeFullPath(dir, ev.NewEntry.GetName())
		newOK = err == nil && scopePathAllowed(sc.grants, p, false)
	}
	switch {
	case oldOK && newOK:
		return resp
	case oldOK && ev.OldEntry != nil:
		out := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		out.EventNotification.NewEntry, out.EventNotification.NewParentPath = nil, ""
		return out
	case newOK && ev.NewEntry != nil:
		out := proto.Clone(resp).(*filer_pb.SubscribeMetadataResponse)
		out.EventNotification.OldEntry = nil
		// The event's Directory is the rename source, which is outside the
		// scope (oldOK is false here). Move it to the visible new parent and
		// drop NewParentPath so the holder sees a plain create in its own path,
		// not the source directory. Mirrors the rename-out half above, which
		// keeps Directory as the (visible) source and clears NewEntry.
		if ev.GetNewParentPath() != "" {
			out.Directory = ev.GetNewParentPath()
			out.EventNotification.NewParentPath = ""
		}
		return out
	}
	return nil
}
