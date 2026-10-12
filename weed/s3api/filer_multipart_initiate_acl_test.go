package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

func TestParseS3MetadataIgnoresInternalAclHeaders(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://s3/b/o?uploads", nil)
	req.Header.Set(s3_constants.ExtAmzAclKey, `[{"Grantee":{"Type":"Group","URI":"http://acs.amazonaws.com/groups/global/AllUsers"},"Permission":"READ"}]`)
	req.Header.Set(s3_constants.ExtAmzOwnerKey, "forged-owner")
	req.Header.Set("X-Amz-Meta-Color", "red")

	metadata, code := ParseS3Metadata(req, nil, false)
	require.Equal(t, s3err.ErrNone, code)
	require.NotContains(t, metadata, s3_constants.ExtAmzAclKey)
	require.NotContains(t, metadata, s3_constants.ExtAmzOwnerKey)
	require.Equal(t, "red", string(metadata["X-Amz-Meta-Color"]))
}

// Multipart initiation previously trusted the internal Seaweed-X-Amz-*
// headers, so a caller could forge ownership or a public ACL onto the
// completed object without s3:PutObjectAcl. Initiate must run the same ACL
// validation as PUT and store only the validated grants.
func TestNewMultipartUploadInitiateACL(t *testing.T) {
	const bucket, object, writer, bucketOwner = "mpu-bucket", "dir/blob.bin", "mpu-writer", "bucket-owner"
	forgedACL := `[{"Grantee":{"Type":"Group","URI":"http://acs.amazonaws.com/groups/global/AllUsers"},"Permission":"READ"}]`

	setup := func(t *testing.T, writeAcp bool) (*S3ApiServer, *ambiguousPutFiler) {
		volume := startFakeVolumeServer(t)
		f := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
		s3a := newPutTestServer(t, startFakeFiler(t, f))
		s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
		t.Cleanup(s3a.iam.Shutdown)
		s3a.iam.isAuthEnabled = true
		account := &Account{Id: writer, DisplayName: writer}
		identity := &Identity{Name: "mpu-acl-test", Account: account, IsStatic: true,
			Actions:     []Action{"Write:" + bucket + "/*"},
			Credentials: []*Credential{{AccessKey: routingTestAccessKey, SecretKey: routingTestSecretKey}}}
		if writeAcp {
			identity.Actions = append(identity.Actions, Action("WriteAcp:"+bucket+"/*"))
		}
		s3a.iam.accessKeyIdent[routingTestAccessKey] = identity
		s3a.iam.nameToIdentity[identity.Name] = identity
		s3a.iam.accounts[writer] = account
		s3a.iam.accounts[bucketOwner] = &Account{Id: bucketOwner, DisplayName: bucketOwner}
		bucketEntry := &filer_pb.Entry{Name: bucket, IsDirectory: true, Attributes: &filer_pb.FuseAttributes{},
			Extended: map[string][]byte{s3_constants.ExtAmzOwnerKey: []byte(bucketOwner)}}
		bucketEntry.Extended[s3_constants.ExtOwnershipKey] = []byte(s3_constants.OwnershipObjectWriter)
		f.entries["/buckets/"+bucket] = bucketEntry
		s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
		s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket, Owner: bucketOwner, Ownership: s3_constants.OwnershipObjectWriter})
		return s3a, f
	}

	initiate := func(t *testing.T, s3a *S3ApiServer, headers map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "http://s3/"+bucket+"/"+object+"?uploads", nil)
		req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		signRoutingTestRequest(t, req, "", "s3")
		rr := httptest.NewRecorder()
		s3a.iam.Auth(s3a.NewMultipartUploadHandler, s3_constants.ACTION_WRITE)(rr, req)
		return rr
	}

	uploadEntry := func(t *testing.T, f *ambiguousPutFiler) *filer_pb.Entry {
		t.Helper()
		f.mu.Lock()
		defer f.mu.Unlock()
		prefix := "/buckets/" + bucket + "/" + s3_constants.MultipartUploadsFolder + "/"
		for key, entry := range f.entries {
			if strings.HasPrefix(key, prefix) {
				return entry
			}
		}
		t.Fatalf("no upload entry recorded under %s", prefix)
		return nil
	}

	t.Run("forged internal headers are ignored", func(t *testing.T) {
		s3a, f := setup(t, false)
		rr := initiate(t, s3a, map[string]string{
			s3_constants.ExtAmzAclKey:   forgedACL,
			s3_constants.ExtAmzOwnerKey: "forged-owner",
		})
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		entry := uploadEntry(t, f)
		require.Equal(t, writer, string(entry.Extended[s3_constants.ExtAmzOwnerKey]))
		require.NotEqual(t, forgedACL, string(entry.Extended[s3_constants.ExtAmzAclKey]))
		for _, grant := range GetAcpGrants(entry.Extended) {
			require.Nil(t, grant.Grantee.URI)
		}
	})

	t.Run("explicit acl requires write acp", func(t *testing.T) {
		s3a, f := setup(t, false)
		rr := initiate(t, s3a, map[string]string{s3_constants.AmzCannedAcl: "public-read"})
		require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
		require.Empty(t, f.entries["/buckets/"+bucket+"/"+s3_constants.MultipartUploadsFolder])
	})

	t.Run("explicit acl lands on upload entry", func(t *testing.T) {
		s3a, f := setup(t, true)
		rr := initiate(t, s3a, map[string]string{s3_constants.AmzCannedAcl: "public-read"})
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		grants := GetAcpGrants(uploadEntry(t, f).Extended)
		var publicRead bool
		for _, grant := range grants {
			if grant.Grantee.URI != nil && *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" && grant.Permission != nil && *grant.Permission == "READ" {
				publicRead = true
			}
		}
		require.True(t, publicRead, "public-read grants should persist on the upload entry: %s", string(uploadEntry(t, f).Extended[s3_constants.ExtAmzAclKey]))
	})
}
