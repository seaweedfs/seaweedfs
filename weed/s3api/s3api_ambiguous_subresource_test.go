package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAmbiguousSubresource pins the rule that a request may name only one
// operation. The router picks a handler by registration order and the IAM action
// resolver picks an action by its own order, so a request carrying two operation
// subresources gets authorized as one and served as the other.
func TestAmbiguousSubresource(t *testing.T) {
	for _, query := range []string{
		"",
		"policy=",
		"tagging=",
		"acl=&versionId=abc",
		"tagging=&versionId=abc",
		"retention=&versionId=abc",
		"uploadId=xyz&partNumber=3",
		"attributes=&partNumber=3&versionId=abc",
		"versions=&prefix=a&delimiter=/",
		"uploads=&prefix=a&x-id=CreateMultipartUpload",
		"list-type=2&prefix=a&continuation-token=x",
		"acl=&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef",
		"seaweedfs-quota=",
	} {
		req, _ := http.NewRequest("GET", "http://localhost/bucket/key?"+query, nil)
		assert.False(t, hasAmbiguousSubresource(req.URL.Query()), "%q names one operation", query)
	}

	for _, query := range []string{
		"policy=&tagging=",
		"tagging=&policy=",
		"cors=&tagging=",
		"lifecycle=&tagging=",
		"versioning=&tagging=",
		"object-lock=&tagging=",
		"requestPayment=&tagging=",
		"acl=&policy=",
		"policy=&cors=",
		"delete=&policy=",
		"uploads=&uploadId=xyz",
		"policy=&tagging=&cors=",
		"list-type=2&ownershipControls=",
		"list-type=2&tagging=",
		"ownershipControls=&list-type=2",
		"list-type=2&versions=",
		"policy=&seaweedfs-quota=",
		"seaweedfs-quota=&policy=",
		"seaweedfs-quota=&tagging=",
	} {
		req, _ := http.NewRequest("PUT", "http://localhost/bucket?"+query, nil)
		assert.True(t, hasAmbiguousSubresource(req.URL.Query()), "%q names two operations", query)
	}
}

// The bucket tagger's escalation: PUT /bucket?policy&tagging routes to the
// bucket-policy handler while resolving as s3:PutBucketTagging. The guard has to
// reject it before either the handler or the IAM check runs.
func TestAmbiguousSubresourceRejectedBeforeHandler(t *testing.T) {
	served := false
	handler := validateRequestPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = true
	}))

	req, _ := http.NewRequest("PUT", "http://localhost/bucket?policy=&tagging=", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.False(t, served, "an ambiguous request must not reach a handler")
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	served = false
	req, _ = http.NewRequest("PUT", "http://localhost/bucket?policy=", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.True(t, served, "an unambiguous request must still be served")
}

// The listing disclosure: GET /bucket?list-type=2&ownershipControls= routes to
// ListObjectsV2 while resolving as s3:GetBucketOwnershipControls, so a principal
// denied s3:ListBucket but allowed the ownership-controls read would list the
// bucket. The guard has to reject the combined request before the listing handler.
func TestListTypeOwnershipControlsRejectedBeforeHandler(t *testing.T) {
	served := false
	handler := validateRequestPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = true
	}))

	req, _ := http.NewRequest("GET", "http://localhost/bucket?list-type=2&ownershipControls=", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	require.False(t, served, "a list-type+ownershipControls request must not reach a handler")
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	served = false
	req, _ = http.NewRequest("GET", "http://localhost/bucket?list-type=2&prefix=a", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.True(t, served, "a plain list-type request must still be served")
}

// Every query key a route can be selected by must be classified: an operation
// subresource counted by the ambiguity guard, or a parameter that only
// modifies the operation it accompanies. A selector left out of both
// authorizes as one operation while the router serves another.
func TestRouteQueryKeysAreClassified(t *testing.T) {
	// Action selects STS routes on the root router, id narrows a config
	// subresource, partNumber accompanies uploadId; none select an S3
	// operation on their own.
	modifiers := map[string]bool{
		"Action": true, "id": true, "partNumber": true,
	}

	router := mux.NewRouter()
	setupRoutingTestServer(t).registerRouter(router)
	err := router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		templates, err := route.GetQueriesTemplates()
		if err != nil {
			return nil
		}
		for _, q := range templates {
			key, _, _ := strings.Cut(q, "=")
			if modifiers[key] {
				continue
			}
			_, resolved := bucketQueryActions[key]
			assert.True(t, operationSubresources[key] || resolved,
				"route query key %q selects an operation but is not counted by hasAmbiguousSubresource", key)
		}
		return nil
	})
	require.NoError(t, err)
}

// mux matches Queries() on the raw query, splitting pairs on ';', before any
// router middleware runs, while handlers and the IAM resolver read the escaped
// query. The server escapes ahead of the router so both see one operation.
func TestSemicolonQueryRoutesAsAuthorized(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	setupRoutingTestServer(t).registerRouter(router)
	match := func(r *http.Request) *mux.Route {
		var m mux.RouteMatch
		require.True(t, router.Match(r, &m))
		return m.Route
	}
	raw := func(query string) *mux.Route {
		return match(httptest.NewRequest(http.MethodGet, "/bucket?"+query, nil))
	}
	escaped := func(query string) (route *mux.Route) {
		util_http.EscapeSemicolonsInQuery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.False(t, r.URL.Query().Has("uploads"))
			route = match(r)
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket?"+query, nil))
		return route
	}

	uploads, listing := raw("uploads"), raw("x=1")
	require.NotSame(t, uploads, listing)
	assert.Same(t, uploads, raw("uploads;x=1"), "unescaped, mux reads the ';' pair as uploads")
	assert.Same(t, listing, escaped("uploads;x=1"))
	assert.Same(t, listing, escaped("x=1;uploads"))
}
