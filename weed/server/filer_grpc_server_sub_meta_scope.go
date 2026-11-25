package weed_server

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (fs *FilerServer) metadataSubscriptionScope(ctx context.Context, method string) (*security.MetadataSubscriptionScope, error) {
	if sc, ok := ctx.Value(scopeSubscriptionContextKey{}).(*scope); ok {
		if sc.expired() {
			return nil, scopeExpiredError(sc)
		}
		prefixes := make([]string, 0, len(sc.grants))
		for _, grant := range sc.grants {
			prefixes = append(prefixes, grant.path)
		}
		return &security.MetadataSubscriptionScope{Prefixes: prefixes, ExpiresAt: sc.exp}, nil
	}
	var adminKey, previousAdminKey security.SigningKey
	if fs.filerGuard != nil {
		adminKey, previousAdminKey = fs.filerGuard.VerificationKeys(true)
	}
	return security.CheckGrpcMetadataSubscription(ctx,
		security.SigningKey(util.GetViper().GetString("jwt.filer_signing.subscribe.key")),
		adminKey,
		previousAdminKey,
		method)
}

// scopeMetadataResponse keeps only what a scoped subscriber may see: events
// whose every path is in scope, so a rename across the boundary never shows
// the far side, and no log file refs. Returns nil when nothing is left.
func scopeMetadataResponse(resp *filer_pb.SubscribeMetadataResponse, prefixes []string) *filer_pb.SubscribeMetadataResponse {
	if resp.EventNotification == nil && len(resp.LogFileRefs) > 0 {
		return nil
	}
	var kept []*filer_pb.SubscribeMetadataResponse
	for _, e := range resp.Events {
		if metadataEventInScope(e, prefixes) {
			kept = append(kept, e)
		}
	}
	headInScope := metadataEventInScope(resp, prefixes)
	if !headInScope && len(kept) == 0 {
		return nil
	}
	if headInScope && len(kept) == len(resp.Events) && len(resp.LogFileRefs) == 0 {
		return resp
	}
	scoped := &filer_pb.SubscribeMetadataResponse{
		TsNs:        resp.TsNs,
		Events:      kept,
		FlushedTsNs: resp.FlushedTsNs,
	}
	if headInScope {
		scoped.Directory = resp.Directory
		scoped.EventNotification = resp.EventNotification
		scoped.MutationTsNs = resp.MutationTsNs
	} else {
		// the batch head keeps its position as an empty marker
		scoped.EventNotification = &filer_pb.EventNotification{}
	}
	return scoped
}

func metadataEventInScope(event *filer_pb.SubscribeMetadataResponse, prefixes []string) bool {
	if event.EventNotification == nil || filer_pb.IsEmpty(event) {
		return true
	}
	return security.MetadataPathInScope(filer_pb.MetadataEventSourceFullPath(event), prefixes) &&
		security.MetadataPathInScope(filer_pb.MetadataEventTargetFullPath(event), prefixes)
}

var errMetadataSubscriptionExpired = status.Error(codes.Unauthenticated, "metadata subscription token expired")

// scopedMetadataStream ends its context when the token expires, so an open
// subscription stops there.
type scopedMetadataStream struct {
	ctx        context.Context
	scope      *security.MetadataSubscriptionScope
	withheld   *int64
	filerScope *scope
}

// scopedClientEpoch is below every other epoch, so a scoped registration
// never replaces another one, even if its id gets taken after it was chosen.
const scopedClientEpoch = math.MinInt32

// unusedClientId gives a scoped caller a registration id of its own, so it
// cannot replace or squat on another subscriber's registration.
func (fs *FilerServer) unusedClientId() int32 {
	fs.knownListenersLock.Lock()
	defer fs.knownListenersLock.Unlock()
	for {
		id := rand.Int32N(1<<31-1) + 1
		_, found := fs.knownListeners[id]
		_, foundLocal := fs.knownListeners[-id]
		if !found && !foundLocal {
			return id
		}
	}
}

func newScopedMetadataStream(parent context.Context, subscription *security.MetadataSubscriptionScope) (scopedMetadataStream, context.CancelFunc) {
	ctx, cancel := context.WithDeadline(parent, subscription.ExpiresAt)
	filerScope, _ := parent.Value(scopeSubscriptionContextKey{}).(*scope)
	return scopedMetadataStream{ctx: ctx, scope: subscription, withheld: new(int64), filerScope: filerScope}, cancel
}

func (s scopedMetadataStream) Context() context.Context { return s.ctx }

func (s scopedMetadataStream) send(resp *filer_pb.SubscribeMetadataResponse, send func(*filer_pb.SubscribeMetadataResponse) error) error {
	if !time.Now().Before(s.scope.ExpiresAt) {
		return errMetadataSubscriptionExpired
	}
	var scoped *filer_pb.SubscribeMetadataResponse
	if s.filerScope != nil {
		scoped = s.filerScope.filterEvent(resp)
	} else {
		scoped = scopeMetadataResponse(resp, s.scope.Prefixes)
	}
	if scoped != nil {
		if err := send(scoped); err != nil {
			return err
		}
		*s.withheld = 0
		return nil
	}
	if resp.EventNotification == nil {
		return nil
	}
	// withheld events still move the subscriber's resume position
	*s.withheld += int64(1 + len(resp.Events))
	if *s.withheld <= MaxUnsyncedEvents {
		return nil
	}
	tsNs := resp.TsNs
	for _, e := range resp.Events {
		tsNs = max(tsNs, e.TsNs)
	}
	if err := send(&filer_pb.SubscribeMetadataResponse{EventNotification: &filer_pb.EventNotification{}, TsNs: tsNs}); err != nil {
		return err
	}
	*s.withheld = 0
	return nil
}

// sendMsg filters SendMsg too, so the embedded stream never sends unfiltered.
func (s scopedMetadataStream) sendMsg(m any, send func(*filer_pb.SubscribeMetadataResponse) error) error {
	resp, ok := m.(*filer_pb.SubscribeMetadataResponse)
	if !ok {
		return fmt.Errorf("scoped metadata stream: unexpected message %T", m)
	}
	return s.send(resp, send)
}

// endErr reports an expired token in place of whatever ended the stream.
func (s scopedMetadataStream) endErr(err error) error {
	if !time.Now().Before(s.scope.ExpiresAt) {
		return errMetadataSubscriptionExpired
	}
	return err
}

type scopedSubscribeStream struct {
	filer_pb.SeaweedFiler_SubscribeMetadataServer
	scopedMetadataStream
}

func (s *scopedSubscribeStream) Context() context.Context { return s.ctx }

func (s *scopedSubscribeStream) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	return s.send(resp, s.SeaweedFiler_SubscribeMetadataServer.Send)
}

func (s *scopedSubscribeStream) SendMsg(m any) error {
	return s.sendMsg(m, s.SeaweedFiler_SubscribeMetadataServer.Send)
}

type scopedSubscribeLocalStream struct {
	filer_pb.SeaweedFiler_SubscribeLocalMetadataServer
	scopedMetadataStream
}

func (s *scopedSubscribeLocalStream) Context() context.Context { return s.ctx }

func (s *scopedSubscribeLocalStream) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	return s.send(resp, s.SeaweedFiler_SubscribeLocalMetadataServer.Send)
}

func (s *scopedSubscribeLocalStream) SendMsg(m any) error {
	return s.sendMsg(m, s.SeaweedFiler_SubscribeLocalMetadataServer.Send)
}
