package s3vectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

type tagClient struct {
	t  *testing.T
	ts *httptest.Server
	fs *s3tablestest.MemFiler
}

func newTagClient(t *testing.T) (*Server, *tagClient) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), headerAuth{}, newFakeVectors(0))
	router := mux.NewRouter().SkipClean(true)
	s.RegisterRoutes(router)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)
	return s, &tagClient{t: t, ts: ts, fs: fs}
}

func (c *tagClient) do(identity, method, path string, header http.Header, payload interface{}) (int, string) {
	c.t.Helper()
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			c.t.Fatalf("marshal: %v", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.ts.URL+path, body)
	if err != nil {
		c.t.Fatalf("build %s %s: %v", method, path, err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if identity != "" {
		req.Header.Set("X-Test-Identity", identity)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

func (c *tagClient) op(identity, op string, payload interface{}) (int, string) {
	return c.do(identity, http.MethodPost, "/"+op, nil, payload)
}

func (c *tagClient) tag(identity, arn string, tags map[string]string) (int, string) {
	return c.do(identity, http.MethodPost, tagsPathPrefix+url.PathEscape(arn), nil, map[string]interface{}{"tags": tags})
}

func (c *tagClient) untag(identity, arn string, keys ...string) (int, string) {
	q := url.Values{"tagKeys": keys}
	return c.do(identity, http.MethodDelete, tagsPathPrefix+url.PathEscape(arn)+"?"+q.Encode(), nil, nil)
}

func (c *tagClient) list(identity, arn string) (int, map[string]string) {
	c.t.Helper()
	code, body := c.do(identity, http.MethodGet, tagsPathPrefix+url.PathEscape(arn), nil, nil)
	if code != http.StatusOK {
		return code, nil
	}
	var resp struct {
		Tags map[string]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil || resp.Tags == nil {
		c.t.Fatalf("decode ListTagsForResource %q: %v", body, err)
	}
	return code, resp.Tags
}

func mustStatus(t *testing.T, what string, code int, body string, want int) {
	t.Helper()
	if code != want {
		t.Fatalf("%s = %d %s, want %d", what, code, body, want)
	}
}

func TestTagVectorBucketsAndIndexes(t *testing.T) {
	s, c := newTagClient(t)
	code, body := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "create bucket", code, body, http.StatusOK)
	code, body = c.op("", "CreateIndex", &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3,
	})
	mustStatus(t, "create index", code, body, http.StatusOK)
	bucketArn, indexArn := s.vectorBucketArn("vec-bucket"), s.indexArn("vec-bucket", "idx")

	if _, tags := c.list("", bucketArn); len(tags) != 0 {
		t.Fatalf("new bucket tags = %v, want none", tags)
	}
	code, body = c.tag("", bucketArn, map[string]string{"team": "search", "env": "dev"})
	mustStatus(t, "tag bucket", code, body, http.StatusOK)
	code, body = c.tag("", indexArn, map[string]string{"team": "ranking"})
	mustStatus(t, "tag index", code, body, http.StatusOK)
	code, body = c.tag("", bucketArn, map[string]string{"env": "prod"})
	mustStatus(t, "retag bucket", code, body, http.StatusOK)

	if _, tags := c.list("", bucketArn); !reflect.DeepEqual(tags, map[string]string{"team": "search", "env": "prod"}) {
		t.Fatalf("bucket tags = %v", tags)
	}
	if _, tags := c.list("", indexArn); !reflect.DeepEqual(tags, map[string]string{"team": "ranking"}) {
		t.Fatalf("index tags = %v", tags)
	}

	code, body = c.untag("", bucketArn, "env", "absent")
	mustStatus(t, "untag bucket", code, body, http.StatusOK)
	if _, tags := c.list("", bucketArn); !reflect.DeepEqual(tags, map[string]string{"team": "search"}) {
		t.Fatalf("bucket tags after untag = %v", tags)
	}

	// Clients that post the operation by name carry resourceArn in the body.
	code, body = c.do("", http.MethodPost, "/", http.Header{"X-Amz-Target": {"AmazonS3Vectors.ListTagsForResource"}},
		&ListTagsForResourceRequest{ResourceArn: indexArn})
	mustStatus(t, "list by target header", code, body, http.StatusOK)
	var resp ListTagsForResourceResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil || resp.Tags["team"] != "ranking" {
		t.Fatalf("list by target header = %s (%v)", body, err)
	}
}

func TestTagResourceValidation(t *testing.T) {
	s, c := newTagClient(t)
	code, body := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "create bucket", code, body, http.StatusOK)
	bucketArn := s.vectorBucketArn("vec-bucket")

	code, body = c.tag("", s.indexArn("vec-bucket", "missing"), map[string]string{"a": "b"})
	mustStatus(t, "tag missing index", code, body, http.StatusNotFound)
	code, _ = c.list("", s.vectorBucketArn("no-such-bucket"))
	mustStatus(t, "list missing bucket", code, "", http.StatusNotFound)
	code, body = c.tag("", "vec-bucket", map[string]string{"a": "b"})
	mustStatus(t, "tag by name", code, body, http.StatusBadRequest)
	code, body = c.tag("", "arn:aws:s3vectors:elsewhere:1:bucket/vec-bucket", map[string]string{"a": "b"})
	mustStatus(t, "tag foreign ARN", code, body, http.StatusBadRequest)
	code, body = c.tag("", bucketArn, map[string]string{})
	mustStatus(t, "tag with no tags", code, body, http.StatusBadRequest)
	code, body = c.tag("", bucketArn, map[string]string{"bad key!": "v"})
	mustStatus(t, "tag with bad key", code, body, http.StatusBadRequest)
	code, body = c.untag("", bucketArn)
	mustStatus(t, "untag with no keys", code, body, http.StatusBadRequest)
	code, body = c.untag("", bucketArn, "")
	mustStatus(t, "untag an empty key", code, body, http.StatusBadRequest)
	code, body = c.untag("", bucketArn, strings.Repeat("k", 129))
	mustStatus(t, "untag a key past 128 characters", code, body, http.StatusBadRequest)

	tags := map[string]string{}
	for i := 0; i < maxResourceTags; i++ {
		tags[fmt.Sprintf("k%d", i)] = "v"
	}
	code, body = c.tag("", bucketArn, tags)
	mustStatus(t, "tag up to the limit", code, body, http.StatusOK)
	code, body = c.tag("", bucketArn, map[string]string{"k0": "changed"})
	mustStatus(t, "overwrite at the limit", code, body, http.StatusOK)
	code, body = c.tag("", bucketArn, map[string]string{"one-more": "v"})
	mustStatus(t, "tag past the limit", code, body, http.StatusBadRequest)
	tags["one-more"] = "v"
	code, body = c.tag("", s.vectorBucketArn("vec-bucket"), tags)
	mustStatus(t, "request past the limit", code, body, http.StatusBadRequest)
	if _, got := c.list("", bucketArn); len(got) != maxResourceTags || got["k0"] != "changed" {
		t.Fatalf("tags after refused requests: %d tags, k0=%q", len(got), got["k0"])
	}
}

func TestTaggingNeedsItsOwnGrant(t *testing.T) {
	s, c := newTagClient(t)
	code, body := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "create bucket", code, body, http.StatusOK)
	bucketArn := s.vectorBucketArn("vec-bucket")
	code, body = c.tag("", bucketArn, map[string]string{"team": "search"})
	mustStatus(t, "owner tags", code, body, http.StatusOK)

	code, _ = c.list("reader", bucketArn)
	mustStatus(t, "list without a grant", code, "", http.StatusForbidden)
	code, body = c.op("", "PutVectorBucketPolicy", &PutVectorBucketPolicyRequest{
		VectorBucketName: "vec-bucket",
		Policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
			`"Action":["s3vectors:ListTagsForResource","s3vectors:GetVectorBucket"],"Resource":"` + bucketArn + `"}]}`,
	})
	mustStatus(t, "put policy", code, body, http.StatusOK)
	if code, tags := c.list("reader", bucketArn); code != http.StatusOK || tags["team"] != "search" {
		t.Fatalf("list under grant = %d %v", code, tags)
	}
	code, body = c.tag("reader", bucketArn, map[string]string{"team": "other"})
	mustStatus(t, "tag under list grant", code, body, http.StatusForbidden)
	code, body = c.untag("reader", bucketArn, "team")
	mustStatus(t, "untag under list grant", code, body, http.StatusForbidden)
	code, body = c.untag("reader", bucketArn, "")
	mustStatus(t, "untag an empty key under list grant", code, body, http.StatusForbidden)
	if _, tags := c.list("", bucketArn); tags["team"] != "search" {
		t.Fatalf("refused writes changed tags: %v", tags)
	}
}

func TestCreateWithTagsNeedsTagResource(t *testing.T) {
	e := newAuthTestEnv(t)
	tags := map[string]string{"team": "search"}
	listed := func(arn string) map[string]string {
		t.Helper()
		w := e.vectorResp(adminIdentity(), e.s.handleListTagsForResource, &ListTagsForResourceRequest{ResourceArn: arn})
		var resp ListTagsForResourceResponse
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil {
			t.Fatalf("list tags of %s: %d %s", arn, w.Code, w.Body.String())
		}
		return resp.Tags
	}
	creator := func(actions ...s3api.Action) *s3api.Identity {
		return &s3api.Identity{Name: "creator", Account: &s3api.Account{Id: "creator"}, Actions: actions}
	}
	createOnly := creator("s3vectors:CreateVectorBucket", "s3vectors:CreateIndex")
	withTag := creator("s3vectors:CreateVectorBucket", "s3vectors:CreateIndex", "s3vectors:TagResource")

	if code := e.vector(createOnly, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "denied", Tags: tags}); code != http.StatusForbidden {
		t.Fatalf("tagged bucket create without TagResource: %d, want 403", code)
	}
	if code := e.vector(adminIdentity(), e.s.handleGetVectorBucket, &GetVectorBucketRequest{VectorBucketName: "denied"}); code != http.StatusNotFound {
		t.Fatalf("refused create left a bucket: %d", code)
	}
	if code := e.vector(withTag, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket", Tags: tags}); code != http.StatusOK {
		t.Fatalf("tagged bucket create: %d", code)
	}
	if got := listed(e.s.vectorBucketArn("vec-bucket")); !reflect.DeepEqual(got, tags) {
		t.Fatalf("bucket tags = %v, want %v", got, tags)
	}

	e.createBuckets(adminIdentity(), "shared")
	index := func(ident *s3api.Identity, name string, tags map[string]string) int {
		return e.vector(ident, e.s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "v-shared", IndexName: name, DataType: "float32", Dimension: 3, Tags: tags,
		})
	}
	if code := index(createOnly, "denied", tags); code != http.StatusForbidden {
		t.Fatalf("tagged index create without TagResource: %d, want 403", code)
	}
	if code := index(createOnly, "plain", nil); code != http.StatusOK {
		t.Fatalf("untagged index create: %d", code)
	}
	if code := index(withTag, "tagged", tags); code != http.StatusOK {
		t.Fatalf("tagged index create: %d", code)
	}
	if got := listed(e.s.indexArn("v-shared", "tagged")); !reflect.DeepEqual(got, tags) {
		t.Fatalf("index tags = %v, want %v", got, tags)
	}
	if got := listed(e.s.indexArn("v-shared", "plain")); len(got) != 0 {
		t.Fatalf("untagged index tags = %v", got)
	}

	scoped := creator("s3vectors:CreateVectorBucket", "s3vectors:TagResource:team-bucket")
	if code := e.vector(scoped, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "other-bucket", Tags: tags}); code != http.StatusForbidden {
		t.Fatalf("tagged create outside the scoped TagResource grant: %d, want 403", code)
	}
	if code := e.vector(scoped, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "team-bucket", Tags: tags}); code != http.StatusOK {
		t.Fatalf("tagged create under a TagResource grant scoped to the bucket: %d", code)
	}

	tooMany := map[string]string{}
	for i := 0; i <= maxResourceTags; i++ {
		tooMany[fmt.Sprintf("k%d", i)] = "v"
	}
	if code := e.vector(withTag, e.s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "too-many", Tags: tooMany}); code != http.StatusBadRequest {
		t.Fatalf("bucket create past the tag limit: %d, want 400", code)
	}
	if code := index(withTag, "too-many", tooMany); code != http.StatusBadRequest {
		t.Fatalf("index create past the tag limit: %d, want 400", code)
	}
}

func TestTaggingRefusesTablesThatAreNotIndexes(t *testing.T) {
	s, c := newTagClient(t)
	code, body := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "create bucket", code, body, http.StatusOK)
	code, body = c.op("", "CreateIndex", &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3,
	})
	mustStatus(t, "create index", code, body, http.StatusOK)
	indexArn := s.indexArn("vec-bucket", "idx")
	code, body = c.tag("", indexArn, map[string]string{"team": "search"})
	mustStatus(t, "tag index", code, body, http.StatusOK)

	dir, name := splitEntryPath(s3tables.GetTablePath("vec-bucket", indexNamespace, "idx"))
	entry := c.fs.Get(dir, name)
	var meta map[string]interface{}
	if err := json.Unmarshal(entry.Extended[s3tables.ExtendedKeyMetadata], &meta); err != nil {
		t.Fatalf("decode index metadata: %v", err)
	}
	meta["format"] = s3tables.FormatIceberg
	data, _ := json.Marshal(meta)
	entry.Extended[s3tables.ExtendedKeyMetadata] = data
	c.fs.Put(dir, name, entry.Extended)

	code, _ = c.list("", indexArn)
	mustStatus(t, "list tags of an Iceberg table", code, "", http.StatusBadRequest)
	code, body = c.tag("", indexArn, map[string]string{"team": "other"})
	mustStatus(t, "tag an Iceberg table", code, body, http.StatusBadRequest)
	code, body = c.untag("", indexArn, "team")
	mustStatus(t, "untag an Iceberg table", code, body, http.StatusBadRequest)
	if got := string(c.fs.Get(dir, name).Extended[s3tables.ExtendedKeyTags]); got != `{"team":"search"}` {
		t.Fatalf("refused calls changed the table's tags: %s", got)
	}
}

func TestUntagManyKeysAllocatesLikeDecoding(t *testing.T) {
	s, c := newTagClient(t)
	code, out := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "create bucket", code, out, http.StatusOK)
	body, err := json.Marshal(&UntagResourceRequest{ResourceArn: s.vectorBucketArn("vec-bucket"), TagKeys: make([]string, 1<<18)})
	if err != nil {
		t.Fatal(err)
	}
	allocated := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	decode := allocated(func() {
		var req UntagResourceRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatal(err)
		}
	})
	w := httptest.NewRecorder()
	handle := allocated(func() {
		s.handleUntagResource(w, httptest.NewRequest(http.MethodPost, "/UntagResource", bytes.NewReader(body)))
	})
	mustStatus(t, "untag empty keys", w.Code, w.Body.String(), http.StatusBadRequest)
	if handle > decode*3/2 {
		t.Fatalf("untag allocated %d bytes, decoding the request takes %d", handle, decode)
	}
}
