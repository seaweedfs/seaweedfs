package s3api

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	revokeKeyA    = "AKIAREVOKEALICE00000"
	revokeSecretA = "alice-secret-for-loopback-tests-only"
	revokeKeyB    = "AKIAREVOKEBOB0000000"
	revokeSecretB = "bob-secret-for-loopback-tests-only00"
	revokeKeyC    = "AKIAREVOKECAROL00000"
	revokeSecretC = "carol-secret-for-loopback-tests-only"
)

func revokeIdentity(name, key, secret string, actions ...string) *iam_pb.Identity {
	identity := &iam_pb.Identity{Name: name, Actions: actions}
	if key != "" {
		identity.Credentials = []*iam_pb.Credential{{AccessKey: key, SecretKey: secret}}
	}
	return identity
}

// newRevokeIAM loads the identities into an IAM that aborts revoked requests.
func newRevokeIAM(t *testing.T, identities ...*iam_pb.Identity) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
		stopChan:     make(chan struct{}),
		inFlight:     newInFlightRequests(),
	}
	go iam.revalidateLoop()
	t.Cleanup(iam.Shutdown)
	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: identities}))
	return iam
}

// streamBody answers with a body streamed for three seconds, and reports on
// started once it streams.
func streamBody(started chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		chunk := make([]byte, 1024)
		for i := 0; i < 300; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

// serveRevokeS3 serves GET /{bucket}/{object} through auth on a listener like
// the gateway's, answering with streamBody.
func serveRevokeS3(t *testing.T, started chan<- struct{}, auth func(http.HandlerFunc) http.HandlerFunc) string {
	t.Helper()
	stream := streamBody(started)
	router := mux.NewRouter()
	router.Methods(http.MethodGet).Path("/{bucket}/{object:.+}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth(stream)(w, s3_constants.EnsureIdentityHolder(r))
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: router, ConnContext: util.ConnContext}
	go func() { _ = server.Serve(&util.Listener{Listener: listener}) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String()
}

func authRead(iam *IdentityAccessManagement) func(http.HandlerFunc) http.HandlerFunc {
	return func(f http.HandlerFunc) http.HandlerFunc { return iam.Auth(f, s3_constants.ACTION_READ) }
}

// download reads an object to its end, signed with key unless it is empty,
// and reports how the read ended.
func download(t *testing.T, url, key, secret string) <-chan error {
	t.Helper()
	req, err := newTestRequest(http.MethodGet, url, 0, nil)
	require.NoError(t, err)
	if key != "" {
		require.NoError(t, signRequestV4(req, key, secret))
	}
	return fetch(req)
}

// fetch reads the response to req to its end, and reports how the read ended.
func fetch(req *http.Request) <-chan error {
	return fetchWith(&http.Transport{}, req)
}

func fetchWith(transport http.RoundTripper, req *http.Request) <-chan error {
	done := make(chan error, 1)
	go func() {
		resp, err := transport.RoundTrip(req)
		if err != nil {
			done <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			done <- fmt.Errorf("status %d", resp.StatusCode)
			return
		}
		_, err = io.Copy(io.Discard, resp.Body)
		done <- err
	}()
	return done
}

func waitStarted(t *testing.T, started <-chan struct{}, n int) {
	t.Helper()
	for range n {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("download did not start")
		}
	}
}

func downloadResult(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("download still running after %v", within)
		return nil
	}
}

// Revoking access aborts the downloads it denies: signed with an access key
// that lost its grant or was deactivated, or anonymous. A download whose
// access stays runs to its end, also when its key's secret is rotated.
func TestAbortRevokedRequests(t *testing.T) {
	const revokeKeyD, revokeSecretD = "AKIAREVOKEDAVE000000", "dave-secret-for-loopback-tests-only0"
	iam := newRevokeIAM(t,
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"),
		revokeIdentity("carol", revokeKeyC, revokeSecretC, "Read:bkt"),
		revokeIdentity("dave", revokeKeyD, revokeSecretD, "Read:bkt"),
		revokeIdentity(s3_constants.AccountAnonymousId, "", "", "Read:bkt"),
	)
	started := make(chan struct{}, 5)
	url := serveRevokeS3(t, started, authRead(iam)) + "/bkt/object"
	alice := download(t, url, revokeKeyA, revokeSecretA)
	bob := download(t, url, revokeKeyB, revokeSecretB)
	carol := download(t, url, revokeKeyC, revokeSecretC)
	dave := download(t, url, revokeKeyD, revokeSecretD)
	anonymous := download(t, url, "", "")
	waitStarted(t, started, 5)

	deactivated := revokeIdentity("dave", revokeKeyD, revokeSecretD, "Read:bkt")
	deactivated.Credentials[0].Status = iamAccessKeyStatusInactive
	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt", "Write:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
		revokeIdentity("carol", revokeKeyC, "carol-rotated-secret-for-loopback-only", "Read:bkt"),
		deactivated,
		revokeIdentity(s3_constants.AccountAnonymousId, "", ""),
	}}))

	assert.Error(t, downloadResult(t, bob, time.Second), "bob lost Read")
	assert.Error(t, downloadResult(t, dave, time.Second), "dave's key was deactivated")
	assert.Error(t, downloadResult(t, anonymous, time.Second), "anonymous lost Read")
	assert.NoError(t, downloadResult(t, alice, 5*time.Second), "alice kept Read")
	assert.NoError(t, downloadResult(t, carol, 5*time.Second), "carol's secret was rotated")
}

// A session token sent with a configured key does not keep the request from
// being checked again: SigV4 ignores it for such a key, SigV2 never reads one.
func TestAbortRevokedRequestsCarryingSessionToken(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"))
	started := make(chan struct{}, 2)
	url := serveRevokeS3(t, started, authRead(iam)) + "/bkt/object"

	v4, err := newTestRequest(http.MethodGet, url, 0, nil)
	require.NoError(t, err)
	v4.Header.Set("X-Amz-Security-Token", "not-a-session-token")
	require.NoError(t, signRequestV4(v4, revokeKeyB, revokeSecretB))

	v2, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	v2.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	v2.Header.Set("X-Amz-Security-Token", "not-a-session-token")
	resource, code := iam.encodedResourceV2(v2)
	require.Equal(t, s3err.ErrNone, code)
	v2.Header.Set("Authorization", signatureV2(&Credential{AccessKey: revokeKeyB, SecretKey: revokeSecretB}, v2.Method, resource, v2.URL.Query().Encode(), v2.Header))

	sigV4, sigV2 := fetch(v4), fetch(v2)
	waitStarted(t, started, 2)

	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
	}}))

	assert.Error(t, downloadResult(t, sigV4, time.Second), "SigV4: bob lost Read")
	assert.Error(t, downloadResult(t, sigV2, time.Second), "SigV2: bob lost Read")
}

// A request whose access is revoked after it authenticated, but before it is
// tracked, is still aborted.
func TestAbortRequestRevokedBeforeItIsTracked(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"))
	bob := iam.lookupByIdentityName("bob")
	since := iam.inFlightGeneration()
	started := make(chan struct{}, 1)
	// As if Auth had admitted bob just before the change below.
	url := serveRevokeS3(t, started, func(f http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set(s3_constants.AmzAuthType, "SigV4")
			r = r.WithContext(s3_constants.SetAccessKeyIdInContext(r.Context(), revokeKeyB))
			iam.abortWhenRevoked(f, bob, s3_constants.ACTION_READ, since)(w, r)
		}
	}) + "/bkt/object"
	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
	}}))
	// Let the re-check the change scheduled pass before bob is tracked, so only
	// the generation check can catch him.
	require.Eventually(t, func() bool {
		iam.inFlight.mu.Lock()
		defer iam.inFlight.mu.Unlock()
		return !iam.inFlight.all
	}, 5*time.Second, time.Millisecond)

	done := download(t, url, "", "")
	waitStarted(t, started, 1)
	assert.Error(t, downloadResult(t, done, time.Second))
}

// A catch-up re-check of a request that authenticated before a change is not
// a change itself, so it leaves the generation as it is.
func TestCatchUpRecheckKeepsTheGeneration(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"))
	bob := iam.lookupByIdentityName("bob")
	since := iam.inFlightGeneration()
	iam.revalidateInFlight()
	changed := iam.inFlightGeneration()
	require.NotEqual(t, since, changed)

	r := httptest.NewRequest(http.MethodGet, "/bkt/object", nil)
	r.Header.Set(s3_constants.AmzAuthType, "SigV4")
	r = r.WithContext(s3_constants.SetAccessKeyIdInContext(r.Context(), revokeKeyB))
	iam.abortWhenRevoked(func(http.ResponseWriter, *http.Request) {}, bob, s3_constants.ACTION_READ, since)(httptest.NewRecorder(), r)
	assert.Equal(t, changed, iam.inFlightGeneration())
}

// A bucket policy that starts denying aborts the requests on its bucket only.
func TestAbortRequestsDeniedByBucketPolicy(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read"))
	iam.policyEngine = NewBucketPolicyEngine()
	started := make(chan struct{}, 2)
	base := serveRevokeS3(t, started, authRead(iam))
	denied := download(t, base+"/bkt/object", revokeKeyB, revokeSecretB)
	other := download(t, base+"/other/object", revokeKeyB, revokeSecretB)
	waitStarted(t, started, 2)

	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("bkt",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}`))
	iam.revalidateBucketInFlight("bkt")

	assert.Error(t, downloadResult(t, denied, time.Second), "the bucket policy denies bkt")
	assert.NoError(t, downloadResult(t, other, 5*time.Second), "no policy covers other")
}

// A request that untracks itself before storing its change to access keeps
// running when that change denies it, while a read the change denies is
// aborted. A request aborted before it untracks itself must not store it.
func TestUntrackInFlightBeforeOwnChange(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Admin"))
	iam.policyEngine = NewBucketPolicyEngine()
	started := make(chan struct{}, 2)
	read := download(t, serveRevokeS3(t, started, authRead(iam))+"/bkt/object", revokeKeyB, revokeSecretB)
	change := download(t, serveRevokeS3(t, started, func(f http.HandlerFunc) http.HandlerFunc {
		return iam.Auth(func(w http.ResponseWriter, r *http.Request) {
			if iam.untrackInFlight(r) {
				f(w, r)
			}
		}, s3_constants.ACTION_PUT_BUCKET_POLICY)
	})+"/bkt/object", revokeKeyB, revokeSecretB)
	waitStarted(t, started, 2)

	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("bkt",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::bkt","arn:aws:s3:::bkt/*"]}]}`))
	iam.revalidateBucketInFlight("bkt")

	assert.Error(t, downloadResult(t, read, time.Second), "the bucket policy denies the read")
	assert.NoError(t, downloadResult(t, change, 5*time.Second), "the change untracked itself first")

	r := mux.SetURLVars(httptest.NewRequest(http.MethodPut, "/bkt?policy", nil), map[string]string{"bucket": "bkt"})
	r.Header.Set(s3_constants.AmzAuthType, "SigV4")
	r = r.WithContext(s3_constants.SetAccessKeyIdInContext(r.Context(), revokeKeyB))
	stored := true
	iam.abortWhenRevoked(func(w http.ResponseWriter, r *http.Request) {
		iam.revalidateBucketInFlight("bkt")
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
			t.Fatal("the denied request was not aborted")
		}
		stored = iam.untrackInFlight(r)
	}, iam.lookupByIdentityName("bob"), s3_constants.ACTION_PUT_BUCKET_POLICY, iam.inFlightGeneration())(httptest.NewRecorder(), r)
	assert.False(t, stored, "an aborted request must not store its change")
}

// A re-check judges time conditions as of when the request was admitted, so a
// grant that expires while the request runs does not abort it.
func TestAbortRevokedRequestsJudgesTimeAsOfAdmission(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("dave", revokeKeyB, revokeSecretB))
	iam.policyEngine = NewBucketPolicyEngine()
	until := time.Now().Add(time.Second).Truncate(time.Second).Add(time.Second)
	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("bkt", fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*","Condition":{"DateLessThan":{"aws:CurrentTime":%q}}}]}`,
		until.UTC().Format(time.RFC3339))))
	started := make(chan struct{}, 1)
	done := download(t, serveRevokeS3(t, started, authRead(iam))+"/bkt/object", revokeKeyB, revokeSecretB)
	waitStarted(t, started, 1)

	time.Sleep(time.Until(until) + 100*time.Millisecond)
	iam.revalidateInFlight()

	assert.NoError(t, downloadResult(t, done, 5*time.Second), "the grant held when the download was admitted")
}

// A revoked HTTP/2 upload is released from its blocked body read, while a
// download on another stream of the same connection runs to its end.
func TestAbortRevokedHTTP2Upload(t *testing.T) {
	iam := newRevokeIAM(t,
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB, "Write:bkt"),
	)
	started := make(chan struct{}, 2)
	connections := make(chan string, 2)
	uploadRead := make(chan error, 1)
	upload := func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		_, err := io.Copy(io.Discard, r.Body)
		uploadRead <- err
	}
	route := func(f http.HandlerFunc, action Action) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			connections <- r.Proto + " " + r.RemoteAddr
			iam.Auth(f, action)(w, s3_constants.EnsureIdentityHolder(r))
		}
	}
	router := mux.NewRouter()
	router.Methods(http.MethodGet).Path("/{bucket}/{object:.+}").HandlerFunc(route(streamBody(started), s3_constants.ACTION_READ))
	router.Methods(http.MethodPut).Path("/{bucket}/{object:.+}").HandlerFunc(route(upload, s3_constants.ACTION_WRITE))
	server := httptest.NewUnstartedServer(router)
	server.EnableHTTP2 = true
	server.Config.ConnContext = util.ConnContext
	server.StartTLS()
	t.Cleanup(server.Close)
	transport := server.Client().Transport

	body, bodyWriter := io.Pipe()
	t.Cleanup(func() { _ = bodyWriter.Close() })
	put, err := http.NewRequest(http.MethodPut, server.URL+"/bkt/upload", body)
	require.NoError(t, err)
	put.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	require.NoError(t, signRequestV4(put, revokeKeyB, revokeSecretB))
	go func() {
		if resp, err := transport.RoundTrip(put); err == nil {
			_ = resp.Body.Close()
		}
	}()
	go func() { _, _ = bodyWriter.Write(make([]byte, 1024)) }()
	get, err := newTestRequest(http.MethodGet, server.URL+"/bkt/object", 0, nil)
	require.NoError(t, err)
	require.NoError(t, signRequestV4(get, revokeKeyA, revokeSecretA))
	alice := fetchWith(transport, get)
	waitStarted(t, started, 2)
	first, second := <-connections, <-connections
	require.Equal(t, first, second, "both streams share one connection")
	require.Contains(t, first, "HTTP/2")

	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
	}}))

	select {
	case err := <-uploadRead:
		assert.Error(t, err, "bob lost Write")
	case <-time.After(time.Second):
		t.Fatal("bob's upload is still reading its body")
	}
	assert.NoError(t, downloadResult(t, alice, 5*time.Second), "alice's stream on the same connection")
}

// A revoked HTTP/2 download blocked writing to a client that stopped reading
// has its stream reset, also behind the gateway's request tracking.
func TestAbortRevokedHTTP2DownloadBlockedWriting(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"))
	started := make(chan struct{}, 1)
	written := make(chan error, 1)
	writeUntilError := func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		chunk := make([]byte, 64*1024)
		for {
			if _, err := w.Write(chunk); err != nil {
				written <- err
				return
			}
		}
	}
	router := mux.NewRouter()
	router.Methods(http.MethodGet).Path("/{bucket}/{object:.+}").HandlerFunc(track(iam.Auth(writeUntilError, s3_constants.ACTION_READ), "GET"))
	server := httptest.NewUnstartedServer(router)
	server.EnableHTTP2 = true
	server.Config.ConnContext = util.ConnContext
	server.StartTLS()
	t.Cleanup(server.Close)

	get, err := newTestRequest(http.MethodGet, server.URL+"/bkt/object", 0, nil)
	require.NoError(t, err)
	require.NoError(t, signRequestV4(get, revokeKeyB, revokeSecretB))
	resp, err := server.Client().Transport.RoundTrip(get)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, "HTTP/2.0", resp.Proto)
	waitStarted(t, started, 1)
	select {
	case err := <-written:
		t.Fatalf("the download stopped writing before the revoke: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
	}}))

	select {
	case err := <-written:
		assert.Error(t, err, "bob lost Read")
	case <-time.After(time.Second):
		t.Fatal("bob's download is still blocked writing")
	}
}
