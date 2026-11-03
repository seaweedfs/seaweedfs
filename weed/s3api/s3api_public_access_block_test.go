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

const testPublicAccessBlockXML = `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
	`<BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls>` +
	`<BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets>` +
	`</PublicAccessBlockConfiguration>`

func newPublicAccessBlockTestServer(t *testing.T, entry *filer_pb.Entry) (*S3ApiServer, *fakeLookupFiler) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	filer := &fakeLookupFiler{entry: entry}
	s3a := newHeadBucketTestServer(t, filer)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.iam = NewIdentityAccessManagementWithStore(s3a.option, nil, "memory")
	t.Cleanup(s3a.iam.Shutdown)
	return s3a, filer
}

func publicAccessBlockRequest(method, bucket, body string) *http.Request {
	req := httptest.NewRequest(method, "http://s3/"+bucket+"?publicAccessBlock", strings.NewReader(body))
	return mux.SetURLVars(req, map[string]string{"bucket": bucket})
}

func TestPutPublicAccessBlockStoresConfiguration(t *testing.T) {
	const bucket = "pab-put"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	rr := httptest.NewRecorder()
	s3a.PutPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodPut, bucket, testPublicAccessBlockXML))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	require.Len(t, filer.txnSeen, 1)
	stored := filer.txnSeen[0].Mutations[0].SetExtended[s3_constants.ExtPublicAccessBlockKey]
	config, err := parsePublicAccessBlock(stored)
	require.NoError(t, err)
	require.Equal(t, PublicAccessBlockConfiguration{
		BlockPublicAcls:       true,
		IgnorePublicAcls:      true,
		RestrictPublicBuckets: true,
	}, *config)

	// The PUT loaded the bucket into the cache; a GET must not serve that copy.
	filer.entry.Extended = filer.txnSeen[0].Mutations[0].SetExtended
	rr = httptest.NewRecorder()
	s3a.GetPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodGet, bucket, ""))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "<IgnorePublicAcls>true</IgnorePublicAcls>")
}

func TestUnreadablePublicAccessBlockBlocksPublicAccess(t *testing.T) {
	s3a, _ := newPublicAccessBlockTestServer(t, nil)
	config := s3a.newBucketConfigFromEntry("b", &filer_pb.Entry{Name: "b", IsDirectory: true, Extended: map[string][]byte{
		s3_constants.ExtPublicAccessBlockKey: []byte("<PublicAccessBlockConfiguration><BlockPublicAcls>"),
		s3_constants.ExtAmzAclKey:            []byte(`[{"Grantee":{"Type":"Group","URI":"` + s3_constants.GranteeGroupAllUsers + `"},"Permission":"READ"}]`),
	}})
	require.True(t, config.PublicAccessBlock.blocksPublicAcls())
	require.True(t, config.PublicAccessBlock.ignoresPublicAcls())
	require.False(t, config.IsPublicRead)
}

func TestBucketUpdateKeepsStoredPublicAccessBlock(t *testing.T) {
	const bucket = "pab-keep"
	unreadable := []byte("<PublicAccessBlockConfiguration><BlockPublicAcls>")
	for _, stored := range [][]byte{unreadable, []byte(testPublicAccessBlockXML)} {
		s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{
			Name:        bucket,
			IsDirectory: true,
			Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: stored},
		})
		require.Equal(t, s3err.ErrNone, s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
			config.Versioning = s3_constants.VersioningEnabled
			return nil
		}))
		require.Len(t, filer.txnSeen, 1)
		require.NotContains(t, filer.txnSeen[0].Mutations[0].SetExtended, s3_constants.ExtPublicAccessBlockKey)
		require.NotContains(t, filer.txnSeen[0].Mutations[0].DeleteExtended, s3_constants.ExtPublicAccessBlockKey)
	}

	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: unreadable},
	})
	all := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls>` +
		`<BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	rr := httptest.NewRecorder()
	s3a.PutPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodPut, bucket, all))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Len(t, filer.txnSeen, 1)
	written, err := parsePublicAccessBlock(filer.txnSeen[0].Mutations[0].SetExtended[s3_constants.ExtPublicAccessBlockKey])
	require.NoError(t, err)
	require.True(t, written.BlockPublicAcls && written.IgnorePublicAcls && written.BlockPublicPolicy && written.RestrictPublicBuckets)
}

func TestPutPublicAccessBlockRejectsMalformedXML(t *testing.T) {
	const bucket = "pab-malformed"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	for _, body := range []string{
		"",
		"<PublicAccessBlockConfiguration><BlockPublicAcls>yes</BlockPublicAcls></PublicAccessBlockConfiguration>",
		"<PublicAccessBlockConfiguration><BlockPublicAcl>true</BlockPublicAcl></PublicAccessBlockConfiguration>",
		"<PublicAccessBlock><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlock>",
	} {
		rr := httptest.NewRecorder()
		s3a.PutPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodPut, bucket, body))
		require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
		require.Contains(t, rr.Body.String(), "MalformedXML")
	}
	require.Empty(t, filer.txnSeen)
}

func TestGetPublicAccessBlock(t *testing.T) {
	const bucket = "pab-get"
	s3a, _ := newPublicAccessBlockTestServer(t, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	rr := httptest.NewRecorder()
	s3a.GetPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodGet, bucket, ""))
	require.Equal(t, http.StatusNotFound, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "NoSuchPublicAccessBlockConfiguration")

	s3a, _ = newPublicAccessBlockTestServer(t, &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: []byte(testPublicAccessBlockXML)},
	})
	rr = httptest.NewRecorder()
	s3a.GetPublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodGet, bucket, ""))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	body := rr.Body.String()
	require.Equal(t, 1, strings.Count(body, "xmlns="), body)
	require.Contains(t, body, `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	require.Contains(t, body, "<BlockPublicAcls>true</BlockPublicAcls>")
	require.Contains(t, body, "<IgnorePublicAcls>true</IgnorePublicAcls>")
	require.Contains(t, body, "<BlockPublicPolicy>false</BlockPublicPolicy>")
	require.Contains(t, body, "<RestrictPublicBuckets>true</RestrictPublicBuckets>")
}

func TestDeletePublicAccessBlockRemovesConfiguration(t *testing.T) {
	const bucket = "pab-delete"
	s3a, filer := newPublicAccessBlockTestServer(t, &filer_pb.Entry{
		Name:        bucket,
		IsDirectory: true,
		Extended:    map[string][]byte{s3_constants.ExtPublicAccessBlockKey: []byte(testPublicAccessBlockXML)},
	})

	rr := httptest.NewRecorder()
	s3a.DeletePublicAccessBlockHandler(rr, publicAccessBlockRequest(http.MethodDelete, bucket, ""))
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())
	require.Len(t, filer.txnSeen, 1)
	require.Contains(t, filer.txnSeen[0].Mutations[0].DeleteExtended, s3_constants.ExtPublicAccessBlockKey)
}

func TestPublicAccessBlockMissingBucket(t *testing.T) {
	s3a, _ := newPublicAccessBlockTestServer(t, nil)
	for _, tc := range []struct {
		method  string
		body    string
		handler http.HandlerFunc
	}{
		{http.MethodGet, "", s3a.GetPublicAccessBlockHandler},
		{http.MethodPut, testPublicAccessBlockXML, s3a.PutPublicAccessBlockHandler},
		{http.MethodDelete, "", s3a.DeletePublicAccessBlockHandler},
	} {
		rr := httptest.NewRecorder()
		tc.handler(rr, publicAccessBlockRequest(tc.method, "pab-missing", tc.body))
		require.Equal(t, http.StatusNotFound, rr.Code, tc.method)
		require.Contains(t, rr.Body.String(), "NoSuchBucket", tc.method)
	}
}

func TestBlockPublicAclsRejectsPublicBucketAndObjectAcls(t *testing.T) {
	const bucket, object = "pab-acl", "doc.txt"
	newServer := func(t *testing.T) (*S3ApiServer, *fakeLookupFiler) {
		// The fake filer answers every lookup with this entry, so it serves
		// as both the bucket and the object.
		return newPublicAccessBlockTestServer(t, &filer_pb.Entry{
			Name:       object,
			Attributes: &filer_pb.FuseAttributes{},
			Extended: map[string][]byte{
				s3_constants.ExtPublicAccessBlockKey: []byte(`<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`),
				s3_constants.ExtAmzOwnerKey:          []byte(s3_constants.AccountAdminId),
			},
		})
	}
	for _, tc := range []struct {
		acl    string
		status int
	}{
		{s3_constants.CannedAclPrivate, http.StatusOK},
		{s3_constants.CannedAclPublicRead, http.StatusForbidden},
		{s3_constants.CannedAclPublicReadWrite, http.StatusForbidden},
		{s3_constants.CannedAclAuthenticatedRead, http.StatusForbidden},
	} {
		t.Run("bucket "+tc.acl, func(t *testing.T) {
			s3a, filer := newServer(t)
			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"?acl", http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
			req.Header.Set(s3_constants.AmzCannedAcl, tc.acl)
			rr := httptest.NewRecorder()
			s3a.PutBucketAclHandler(rr, req)
			require.Equal(t, tc.status, rr.Code, rr.Body.String())
			if tc.status == http.StatusForbidden {
				require.Contains(t, rr.Body.String(), "AccessDenied")
				require.Empty(t, filer.txnSeen)
			}
		})
		t.Run("object "+tc.acl, func(t *testing.T) {
			s3a, filer := newServer(t)
			req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"/"+object+"?acl", http.NoBody)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket, "object": object})
			req.Header.Set(s3_constants.AmzCannedAcl, tc.acl)
			rr := httptest.NewRecorder()
			s3a.PutObjectAclHandler(rr, req)
			require.Equal(t, tc.status, rr.Code, rr.Body.String())
			if tc.status == http.StatusForbidden {
				require.Contains(t, rr.Body.String(), "AccessDenied")
				require.Equal(t, s3_constants.AccountAdminId, string(filer.entry.Extended[s3_constants.ExtAmzOwnerKey]))
				require.NotContains(t, filer.entry.Extended, s3_constants.ExtAmzAclKey)
			}
		})
	}

	t.Run("bucket xml all users grant", func(t *testing.T) {
		s3a, filer := newServer(t)
		body := `<AccessControlPolicy><Owner><ID>admin</ID></Owner><AccessControlList>` +
			`<Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>` + s3_constants.GranteeGroupAllUsers + `</URI></Grantee><Permission>READ</Permission></Grant>` +
			`</AccessControlList></AccessControlPolicy>`
		req := httptest.NewRequest(http.MethodPut, "http://s3/"+bucket+"?acl", strings.NewReader(body))
		req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
		rr := httptest.NewRecorder()
		s3a.PutBucketAclHandler(rr, req)
		require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
		require.Empty(t, filer.txnSeen)
	})
}
