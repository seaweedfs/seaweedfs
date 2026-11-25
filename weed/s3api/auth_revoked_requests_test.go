package s3api

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	revokeKeyA    = "AKIAREVOKEALICE00000"
	revokeSecretA = "alice-secret-for-loopback-tests-only"
	revokeKeyB    = "AKIAREVOKEBOB0000000"
	revokeSecretB = "bob-secret-for-loopback-tests-only00"
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

// serveRevokeS3 serves GET /{bucket}/{object} through iam.Auth on a listener
// like the gateway's, streaming each body for two seconds. Every request
// reports on started once it streams.
func serveRevokeS3(t *testing.T, iam *IdentityAccessManagement, started chan<- struct{}) string {
	t.Helper()
	stream := func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		chunk := make([]byte, 1024)
		for i := 0; i < 200; i++ {
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
	router := mux.NewRouter()
	router.Methods(http.MethodGet).Path("/{bucket}/{object:.+}").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		iam.Auth(stream, s3_constants.ACTION_READ)(w, s3_constants.EnsureIdentityHolder(r))
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: router, ConnContext: util.ConnContext}
	go func() { _ = server.Serve(&util.Listener{Listener: listener}) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String()
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
	done := make(chan error, 1)
	go func() {
		resp, err := (&http.Transport{}).RoundTrip(req)
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

// Revoking access aborts the downloads it denies, signed with an access key or
// anonymous, while a download whose access stays runs to its end.
func TestAbortRevokedRequests(t *testing.T) {
	iam := newRevokeIAM(t,
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read:bkt"),
		revokeIdentity(s3_constants.AccountAnonymousId, "", "", "Read:bkt"),
	)
	started := make(chan struct{}, 3)
	url := serveRevokeS3(t, iam, started) + "/bkt/object"
	alice := download(t, url, revokeKeyA, revokeSecretA)
	bob := download(t, url, revokeKeyB, revokeSecretB)
	anonymous := download(t, url, "", "")
	waitStarted(t, started, 3)

	require.NoError(t, iam.ReplaceS3ApiConfiguration(&iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		revokeIdentity("alice", revokeKeyA, revokeSecretA, "Read:bkt", "Write:bkt"),
		revokeIdentity("bob", revokeKeyB, revokeSecretB),
		revokeIdentity(s3_constants.AccountAnonymousId, "", ""),
	}}))

	assert.Error(t, downloadResult(t, bob, time.Second), "bob lost Read")
	assert.Error(t, downloadResult(t, anonymous, time.Second), "anonymous lost Read")
	assert.NoError(t, downloadResult(t, alice, 5*time.Second), "alice kept Read")
}

// A bucket policy that starts denying aborts the requests on its bucket only.
func TestAbortRequestsDeniedByBucketPolicy(t *testing.T) {
	iam := newRevokeIAM(t, revokeIdentity("bob", revokeKeyB, revokeSecretB, "Read"))
	iam.policyEngine = NewBucketPolicyEngine()
	started := make(chan struct{}, 2)
	base := serveRevokeS3(t, iam, started)
	denied := download(t, base+"/bkt/object", revokeKeyB, revokeSecretB)
	other := download(t, base+"/other/object", revokeKeyB, revokeSecretB)
	waitStarted(t, started, 2)

	require.NoError(t, iam.policyEngine.engine.SetBucketPolicy("bkt",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bkt/*"}]}`))
	iam.revalidateBucketInFlight("bkt")

	assert.Error(t, downloadResult(t, denied, time.Second), "the bucket policy denies bkt")
	assert.NoError(t, downloadResult(t, other, 5*time.Second), "no policy covers other")
}
