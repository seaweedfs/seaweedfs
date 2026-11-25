package s3api

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// inFlightRequests holds the authorized requests being served, so a change
// that revokes their access aborts them (-s3.abortRevokedRequests). Otherwise
// access is only checked when a request starts, and a download or upload that
// started before an identity, policy or bucket policy change runs to its end.
type inFlightRequests struct {
	mu       sync.Mutex
	requests map[*inFlightRequest]struct{}
	// Re-checks waiting for revalidateLoop: of every request, or of those on
	// these buckets.
	all     bool
	buckets map[string]struct{}
	kick    chan struct{}
}

// identitySource is how a re-check finds a request's identity again.
type identitySource int

const (
	// A session's identity (JWT, STS), which no configuration lists.
	fromSession identitySource = iota
	// Signed with a configured access key.
	fromAccessKey
	// No signature or token: anonymous, or a client certificate.
	fromUnsigned
	// A configured identity whose signing key the request did not record.
	fromName
)

type inFlightRequest struct {
	// req is a copy taken before the handler ran, which may change the
	// original. Only revalidateLoop uses it.
	req       *http.Request
	identity  *Identity
	action    Action
	bucket    string
	source    identitySource
	accessKey string
	cancel    context.CancelFunc
	// conn is closed to abort an HTTP/1.x request. An HTTP/2 connection
	// carries other streams, so those requests are only canceled.
	conn net.Conn
}

func newInFlightRequests() *inFlightRequests {
	return &inFlightRequests{
		requests: make(map[*inFlightRequest]struct{}),
		kick:     make(chan struct{}, 1),
	}
}

// abortWhenRevoked wraps the handler of an authorized request, so the request
// is checked again whenever its access may have changed, and aborted once it
// is denied.
func (iam *IdentityAccessManagement) abortWhenRevoked(f http.HandlerFunc, identity *Identity, action Action) http.HandlerFunc {
	tracked := iam.inFlight
	if tracked == nil || identity == nil {
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		r = r.WithContext(ctx)
		bucket, _ := s3_constants.GetBucketAndObject(r)
		req := &inFlightRequest{req: r.Clone(ctx), identity: identity, action: action, bucket: bucket, cancel: cancel}
		req.source, req.accessKey = iam.identitySource(r, identity)
		if r.ProtoMajor == 1 {
			req.conn = util.ConnFromContext(ctx)
		}
		tracked.add(req)
		defer tracked.remove(req)
		f(w, r)
	}
}

func (iam *IdentityAccessManagement) identitySource(r *http.Request, identity *Identity) (identitySource, string) {
	switch r.Header.Get(s3_constants.AmzAuthType) {
	case "Anonymous", "Certificate":
		return fromUnsigned, ""
	}
	iam.m.RLock()
	defer iam.m.RUnlock()
	if key := s3_constants.GetAccessKeyIdFromContext(r); key != "" && iam.accessKeyIdent[key] == identity {
		return fromAccessKey, key
	}
	if iam.nameToIdentity[identity.Name] == identity {
		return fromName, ""
	}
	return fromSession, ""
}

// currentIdentity returns the request's identity as the configuration has it
// now, or nil when it no longer authenticates the request. A session identity
// stays as it is: its policies are evaluated afresh.
func (iam *IdentityAccessManagement) currentIdentity(req *inFlightRequest) *Identity {
	var identity *Identity
	switch req.source {
	case fromAccessKey:
		var cred *Credential
		var found bool
		if identity, cred, found = iam.lookupByAccessKey(req.accessKey); !found || cred.isCredentialExpired() {
			return nil
		}
	case fromUnsigned:
		identity, _, _ = iam.authenticateUnsigned(req.req)
	case fromName:
		if identity = iam.lookupByIdentityName(req.identity.Name); identity != nil && identity.Disabled {
			return nil
		}
	default:
		return req.identity
	}
	if identity == nil || identity.Name != req.identity.Name {
		return nil
	}
	return identity
}

// revalidateInFlight schedules a re-check of every request in flight.
func (iam *IdentityAccessManagement) revalidateInFlight() {
	iam.scheduleRevalidation("")
}

// revalidateBucketInFlight schedules a re-check of the requests on bucket.
func (iam *IdentityAccessManagement) revalidateBucketInFlight(bucket string) {
	if bucket != "" {
		iam.scheduleRevalidation(bucket)
	}
}

func (iam *IdentityAccessManagement) scheduleRevalidation(bucket string) {
	if iam == nil || iam.inFlight == nil {
		return
	}
	t := iam.inFlight
	t.mu.Lock()
	if bucket == "" {
		t.all = true
	} else {
		if t.buckets == nil {
			t.buckets = make(map[string]struct{})
		}
		t.buckets[bucket] = struct{}{}
	}
	t.mu.Unlock()
	select {
	case t.kick <- struct{}{}:
	default:
	}
}

// revalidateLoop runs the re-checks outside their callers, which can hold
// locks that authorization takes itself.
func (iam *IdentityAccessManagement) revalidateLoop() {
	t := iam.inFlight
	for {
		select {
		case <-iam.stopChan:
			return
		case <-t.kick:
		}
		for _, req := range t.pending() {
			if identity := iam.currentIdentity(req); identity != nil && iam.authorizeRequest(req.req, identity, req.action) == s3err.ErrNone {
				continue
			}
			if t.abort(req) {
				glog.V(0).Infof("aborted %s %s of %s: its access was revoked", req.req.Method, req.req.URL.Path, req.identity.Name)
			}
		}
	}
}

func (t *inFlightRequests) add(req *inFlightRequest) {
	t.mu.Lock()
	t.requests[req] = struct{}{}
	t.mu.Unlock()
}

func (t *inFlightRequests) remove(req *inFlightRequest) {
	t.mu.Lock()
	delete(t.requests, req)
	t.mu.Unlock()
}

// pending takes the requests waiting for a re-check.
func (t *inFlightRequests) pending() []*inFlightRequest {
	t.mu.Lock()
	defer t.mu.Unlock()
	var requests []*inFlightRequest
	for req := range t.requests {
		if _, onBucket := t.buckets[req.bucket]; t.all || onBucket {
			requests = append(requests, req)
		}
	}
	t.all, t.buckets = false, nil
	return requests
}

// abort ends a request that is still in flight. It holds mu, which remove
// takes before the handler returns, so the connection it closes is still
// serving this request and not a later one on the same keep-alive connection.
func (t *inFlightRequests) abort(req *inFlightRequest) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.requests[req]; !ok {
		return false
	}
	delete(t.requests, req)
	req.cancel()
	if req.conn != nil {
		_ = req.conn.Close()
	}
	return true
}
