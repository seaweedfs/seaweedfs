package s3api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

const (
	bpcBucket       = "policy-bucket"
	bpcObject       = "incoming/part.bin"
	bpcAccessKey    = "LOCALPOLICYKEY000001"
	bpcSecretKey    = "local-policy-secret-for-loopback-only"
	bpcIdentityName = "policy-writer"
	bpcAccountID    = "000000000000"
)

// An identity with credentials and nothing else, plus a bucket policy that lets it put objects.
func newBucketPolicyOnlyIAM(t *testing.T) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Accounts: []*iam_pb.Account{{Id: bpcAccountID, DisplayName: bpcIdentityName}},
		Identities: []*iam_pb.Identity{{
			Name:        bpcIdentityName,
			Account:     &iam_pb.Account{Id: bpcAccountID, DisplayName: bpcIdentityName},
			Credentials: []*iam_pb.Credential{{AccessKey: bpcAccessKey, SecretKey: bpcSecretKey}},
		}},
	})
	require.NoError(t, err)

	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[
	  {"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::%s:user/%s"},
	   "Action":"s3:PutObject","Resource":"arn:aws:s3:::%s/*"}]}`, bpcAccountID, bpcIdentityName, bpcBucket)
	engine := NewBucketPolicyEngine()
	require.NoError(t, engine.engine.SetBucketPolicy(bpcBucket, policy))
	iam.policyEngine = engine
	return iam
}

// A SigV4 PutObject whose body is aws-chunked with an unsigned payload and a CRC32 trailer.
func bpcStreamingPut(t *testing.T, secretKey string) *http.Request {
	t.Helper()
	payload := generateStreamingUnsignedPayloadTrailerPayload(true)
	urlStr := fmt.Sprintf("http://127.0.0.1:9000/%s/%s", bpcBucket, bpcObject)
	req := mustNewRequest(http.MethodPut, urlStr, int64(len(payload)), bytes.NewReader([]byte(payload)), t)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", "17408")
	req.Header.Set("x-amz-content-sha256", streamingUnsignedPayload)
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	require.NoError(t, signRequestV4(req, bpcAccessKey, secretKey))
	return mux.SetURLVars(req, map[string]string{"bucket": bpcBucket, "object": bpcObject})
}

func TestChunkedReaderDoesNotReauthorizeBucketPolicyPrincipal(t *testing.T) {
	iam := newBucketPolicyOnlyIAM(t)

	// The middleware authorizes the write through the bucket policy.
	req := bpcStreamingPut(t, bpcSecretKey)
	identity, errCode := iam.authRequest(req, s3_constants.ACTION_WRITE)
	require.Equal(t, s3err.ErrNone, errCode, "the bucket policy must allow the PutObject")
	require.NotNil(t, identity)
	require.Empty(t, identity.Actions)
	require.Empty(t, identity.PolicyNames)

	// The handler then builds the body reader for the same request.
	reader, errCode := iam.newChunkedReader(req)
	require.Equal(t, s3err.ErrNone, errCode, "the chunked reader must not deny what the middleware allowed")
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("a", 17408), string(data))

	// The seed signature is still verified.
	_, errCode = iam.newChunkedReader(bpcStreamingPut(t, "not-the-secret"))
	require.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
}
