package s3api

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// inFlightRequests holds the requests the S3 auth middleware admitted for a
// configured identity, so a change to identities, policies or a bucket policy
// that revokes their access aborts them, rather than letting a download or
// upload that started before the change run to its end.
//
// Only the middleware's decision is checked again, and as of when the request
// was admitted: time conditions such as aws:CurrentTime, and a key's
// expiration, are evaluated at that time, so expiry alone never aborts a
// request. STS and JWT sessions are not tracked, so their requests run to
// their end: re-validating a session token also checks its expiry. Nor is what a handler authorizes itself: anonymous reads a
// public ACL or bucket policy allows, browser form uploads, the source of a
// copy, and conditions on object tags.
type inFlightRequests struct {
	mu       sync.Mutex
	requests map[*inFlightRequest]struct{}
	// Re-checks waiting for revalidateLoop: of every request, or of those on
	// these buckets.
	all     bool
	buckets map[string]struct{}
	kick    chan struct{}
	// generation counts scheduled re-checks, so a request whose access changed
	// while it was being admitted is checked again once it is tracked.
	generation atomic.Uint64
}

// identitySource is how a re-check finds a request's identity again.
type identitySource int

const (
	// A session (JWT, STS), which is not tracked.
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
	// original, and judged as of admitted. Only revalidateLoop uses it.
	req      *http.Request
	admitted time.Time
	identity *Identity
	action   Action
	bucket   string
	source   identitySource
	// The key the request was signed with. Removing or deactivating it revokes
	// the request; giving it a new secret is a rotation, which does not.
	accessKey string
	cancel    context.CancelFunc
	// An HTTP/1.x request is aborted by closing its connection. An HTTP/2
	// connection carries other streams, so such a request has its context
	// canceled, its body closed and its stream reset instead.
	conn   net.Conn
	body   io.Closer
	stream *http.ResponseController
}

func newInFlightRequests() *inFlightRequests {
	return &inFlightRequests{
		requests: make(map[*inFlightRequest]struct{}),
		kick:     make(chan struct{}, 1),
	}
}

// inFlightGeneration is taken before a request authenticates and handed to
// abortWhenRevoked.
func (iam *IdentityAccessManagement) inFlightGeneration() uint64 {
	if iam.inFlight == nil {
		return 0
	}
	return iam.inFlight.generation.Load()
}

// abortWhenRevoked wraps the handler of an authorized request, so the request
// is checked again whenever its access may have changed, and aborted once it
// is denied. since is the generation from before the request authenticated.
func (iam *IdentityAccessManagement) abortWhenRevoked(f http.HandlerFunc, identity *Identity, action Action, since uint64) http.HandlerFunc {
	tracked := iam.inFlight
	if tracked == nil || identity == nil {
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		source, cred := iam.identitySource(r, identity)
		if source == fromSession {
			f(w, r)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ctx = context.WithValue(ctx, servedObjectEntriesKey{}, &sync.Map{})
		r = r.WithContext(ctx)
		bucket, _ := s3_constants.GetBucketAndObject(r)
		admitted := time.Now()
		req := &inFlightRequest{req: r.Clone(policy_engine.WithRequestTime(ctx, admitted)), admitted: admitted, identity: identity, action: action, bucket: bucket, source: source, cancel: cancel}
		if cred != nil {
			req.accessKey = cred.AccessKey
		}
		if r.ProtoMajor == 1 {
			req.conn = util.ConnFromContext(ctx)
		} else {
			req.body = r.Body
			req.stream = http.NewResponseController(w)
		}
		r = r.WithContext(context.WithValue(ctx, inFlightRequestKey{}, req))
		tracked.add(req)
		defer tracked.remove(req)
		if tracked.generation.Load() != since {
			iam.scheduleRevalidation(bucket, false)
		}
		f(w, r)
	}
}

type inFlightRequestKey struct{}

type servedObjectEntriesKey struct{}

// recordServedObjectEntry keeps the entry a tracked request was authorized
// against, so a re-check decides tag conditions on the version being served
// rather than on the one current by then.
func recordServedObjectEntry(r *http.Request, bucket, object string, extended map[string][]byte) {
	if served, ok := r.Context().Value(servedObjectEntriesKey{}).(*sync.Map); ok {
		served.Store(bucket+"/"+object, extended)
	}
}

func servedObjectEntry(r *http.Request, bucket, object string) (map[string][]byte, bool) {
	served, ok := r.Context().Value(servedObjectEntriesKey{}).(*sync.Map)
	if !ok {
		return nil, false
	}
	extended, found := served.Load(bucket + "/" + object)
	if !found {
		return nil, false
	}
	return extended.(map[string][]byte), true
}

// untrackInFlight stops re-checking r before it stores a change to access,
// which would otherwise judge r by its own change and abort it after the
// change is stored. It reports false when r was aborted already, so the change
// must not be stored.
func (iam *IdentityAccessManagement) untrackInFlight(r *http.Request) bool {
	req, ok := r.Context().Value(inFlightRequestKey{}).(*inFlightRequest)
	if !ok || iam == nil || iam.inFlight == nil {
		return true
	}
	return iam.inFlight.remove(req)
}

// identitySource tells from how the request authenticated how a re-check finds
// its identity again, and the credential it was signed with.
func (iam *IdentityAccessManagement) identitySource(r *http.Request, identity *Identity) (identitySource, *Credential) {
	switch r.Header.Get(s3_constants.AmzAuthType) {
	case "Anonymous", "Certificate":
		return fromUnsigned, nil
	case "Jwt":
		return fromSession, nil
	case "SigV4":
		// SigV4 marks a token it tolerated for a configured key ignored; SigV2
		// never reads one, so a token there does not make a session.
		if extractSessionToken(r) != "" {
			return fromSession, nil
		}
	}
	key := s3_constants.GetAccessKeyIdFromContext(r)
	iam.m.RLock()
	defer iam.m.RUnlock()
	for _, cred := range identity.Credentials {
		if key != "" && cred.AccessKey == key {
			return fromAccessKey, cred
		}
	}
	return fromName, nil
}

// currentIdentity returns the request's identity as the configuration has it
// now, or nil when it no longer authenticates the request.
func (iam *IdentityAccessManagement) currentIdentity(req *inFlightRequest) *Identity {
	var identity *Identity
	switch req.source {
	case fromAccessKey:
		var cred *Credential
		var found bool
		if identity, cred, found = iam.lookupByAccessKey(req.accessKey); !found || cred.Expiration > 0 && cred.Expiration < req.admitted.Unix() {
			return nil
		}
	case fromUnsigned:
		identity, _, _ = iam.authenticateUnsigned(req.req)
	default:
		if identity = iam.lookupByIdentityName(req.identity.Name); identity != nil && identity.Disabled {
			return nil
		}
	}
	if identity == nil || identity.Name != req.identity.Name {
		return nil
	}
	return identity
}

// revalidateInFlight schedules a re-check of every request in flight.
func (iam *IdentityAccessManagement) revalidateInFlight() {
	iam.scheduleRevalidation("", true)
}

// revalidateBucketInFlight schedules a re-check of the requests on bucket.
func (iam *IdentityAccessManagement) revalidateBucketInFlight(bucket string) {
	if bucket != "" {
		iam.scheduleRevalidation(bucket, true)
	}
}

// scheduleRevalidation re-checks the requests on bucket, or all of them. Only
// an access change advances the generation: a catch-up for a request tracked
// late must not, or overlapping admissions would keep scheduling each other.
func (iam *IdentityAccessManagement) scheduleRevalidation(bucket string, changed bool) {
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
	// Bumped before pending can take this scope, so a request tracked after
	// that pass sees the change.
	if changed {
		t.generation.Add(1)
	}
	t.mu.Unlock()
	select {
	case t.kick <- struct{}{}:
	default:
	}
}

// revalidateLoop runs the re-checks outside their callers, which can hold
// locks that authorization takes itself. It holds reloadMu, so it never sees a
// reload halfway, with identities installed but their policies not yet synced.
func (iam *IdentityAccessManagement) revalidateLoop() {
	t := iam.inFlight
	for {
		select {
		case <-iam.stopChan:
			return
		case <-t.kick:
		}
		iam.reloadMu.Lock()
		for _, req := range t.pending() {
			if identity := iam.currentIdentity(req); identity != nil && iam.authorizeRequest(req.req, identity, req.action) == s3err.ErrNone {
				continue
			}
			if t.abort(req) {
				glog.V(0).Infof("aborted %s %s of %s: its access was revoked", req.req.Method, req.req.URL.Path, req.identity.Name)
			}
		}
		iam.reloadMu.Unlock()
	}
}

func (t *inFlightRequests) add(req *inFlightRequest) {
	t.mu.Lock()
	t.requests[req] = struct{}{}
	t.mu.Unlock()
}

// remove reports whether req was still tracked, that is not aborted.
func (t *inFlightRequests) remove(req *inFlightRequest) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, tracked := t.requests[req]
	delete(t.requests, req)
	return tracked
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
		return true
	}
	if req.body != nil {
		_ = req.body.Close()
	}
	if req.stream != nil {
		// A deadline in the past resets the stream, releasing a write blocked
		// on a client that stopped reading.
		_ = req.stream.SetWriteDeadline(time.Now())
	}
	return true
}
