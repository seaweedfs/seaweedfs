package s3vectors

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// storePolicy writes a vector bucket policy straight to the bucket entry.
func (c *tagClient) storePolicy(bucket, policy string) {
	c.t.Helper()
	dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
	entry := c.fs.Get(dir, name)
	if entry == nil {
		c.t.Fatalf("bucket %s has no entry", bucket)
	}
	ext := maps.Clone(entry.Extended)
	ext[vectorPolicyKey] = []byte(policy)
	c.fs.Put(dir, name, ext)
}

func (c *tagClient) setup(bucket string, indexes ...string) {
	c.t.Helper()
	code, body := c.op("", "CreateVectorBucket", &CreateVectorBucketRequest{VectorBucketName: bucket})
	mustStatus(c.t, "create bucket "+bucket, code, body, http.StatusOK)
	for _, index := range indexes {
		code, body = c.op("", "CreateIndex", &CreateIndexRequest{
			VectorBucketName: bucket, IndexName: index, DataType: "float32", Dimension: 3,
		})
		mustStatus(c.t, "create index "+index, code, body, http.StatusOK)
	}
}

func (c *tagClient) getIndex(identity, bucket, index string) int {
	code, _ := c.op(identity, "GetIndex", &GetIndexRequest{VectorBucketName: bucket, IndexName: index})
	return code
}

func statement(effect, action, resource, conditions string) string {
	return `{"Effect":"` + effect + `","Principal":"*","Action":` + action + `,"Resource":"` + resource + `","Condition":` + conditions + `}`
}

func policyOf(statements ...string) string {
	out := `{"Version":"2012-10-17","Statement":[`
	for i, stmt := range statements {
		if i > 0 {
			out += ","
		}
		out += stmt
	}
	return out + `]}`
}

func TestResourceTagReadsTheTargetsOwnTags(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket", "live-idx", "test-idx")
	bucketArn := s.vectorBucketArn("vec-bucket")
	mustStatus(t, "tag bucket", tagCode(c, bucketArn, map[string]string{"env": "prod"}), "", http.StatusOK)
	mustStatus(t, "tag live", tagCode(c, s.indexArn("vec-bucket", "live-idx"), map[string]string{"stage": "live"}), "", http.StatusOK)
	mustStatus(t, "tag test", tagCode(c, s.indexArn("vec-bucket", "test-idx"), map[string]string{"stage": "test"}), "", http.StatusOK)
	c.storePolicy("vec-bucket", policyOf(
		statement("Allow", `"s3vectors:GetIndex"`, bucketArn+"/index/*", `{"StringEquals":{"aws:ResourceTag/stage":"live"}}`),
		statement("Allow", `"s3vectors:GetVectorBucket"`, bucketArn, `{"StringEquals":{"aws:ResourceTag/env":"prod"}}`),
	))

	if code := c.getIndex("reader", "vec-bucket", "live-idx"); code != http.StatusOK {
		t.Fatalf("GetIndex on the index tagged stage=live: %d", code)
	}
	if code := c.getIndex("reader", "vec-bucket", "test-idx"); code != http.StatusForbidden {
		t.Fatalf("GetIndex on the index tagged stage=test: %d, want 403", code)
	}
	getBucket := func() int {
		code, _ := c.op("reader", "GetVectorBucket", &GetVectorBucketRequest{VectorBucketName: "vec-bucket"})
		return code
	}
	if code := getBucket(); code != http.StatusOK {
		t.Fatalf("GetVectorBucket on the bucket tagged env=prod: %d", code)
	}
	mustStatus(t, "retag bucket", tagCode(c, bucketArn, map[string]string{"env": "dev"}), "", http.StatusOK)
	if code := getBucket(); code != http.StatusForbidden {
		t.Fatalf("GetVectorBucket after retagging env=dev: %d, want 403", code)
	}
}

func TestVectorBucketTagReachesIndexes(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket", "idx")
	bucketArn := s.vectorBucketArn("vec-bucket")
	mustStatus(t, "tag bucket", tagCode(c, bucketArn, map[string]string{"env": "prod"}), "", http.StatusOK)
	mustStatus(t, "tag index", tagCode(c, s.indexArn("vec-bucket", "idx"), map[string]string{"env": "dev"}), "", http.StatusOK)
	c.storePolicy("vec-bucket", policyOf(
		statement("Allow", `"s3vectors:GetIndex"`, bucketArn+"/index/*", `{"StringEquals":{"s3vectors:VectorBucketTag/env":"prod"}}`),
	))
	if code := c.getIndex("reader", "vec-bucket", "idx"); code != http.StatusOK {
		t.Fatalf("GetIndex in a bucket tagged env=prod: %d", code)
	}
	mustStatus(t, "retag bucket", tagCode(c, bucketArn, map[string]string{"env": "dev"}), "", http.StatusOK)
	if code := c.getIndex("reader", "vec-bucket", "idx"); code != http.StatusForbidden {
		t.Fatalf("GetIndex after retagging the bucket env=dev: %d, want 403", code)
	}
}

func TestResourceTagDenyProtectsTaggedIndexes(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket", "kept", "open")
	bucketArn := s.vectorBucketArn("vec-bucket")
	mustStatus(t, "tag index", tagCode(c, s.indexArn("vec-bucket", "kept"), map[string]string{"protected": "true"}), "", http.StatusOK)
	c.storePolicy("vec-bucket", policyOf(
		`{"Effect":"Allow","Principal":"*","Action":"s3vectors:*","Resource":["`+bucketArn+`","`+bucketArn+`/index/*"]}`,
		statement("Deny", `["s3vectors:DeleteIndex","s3vectors:GetIndex"]`, bucketArn+"/index/*", `{"StringEquals":{"aws:ResourceTag/protected":"true"}}`),
	))
	if code := c.getIndex("reader", "vec-bucket", "kept"); code != http.StatusForbidden {
		t.Fatalf("GetIndex on a protected index: %d, want 403", code)
	}
	if code := c.getIndex("reader", "vec-bucket", "open"); code != http.StatusOK {
		t.Fatalf("GetIndex on an unprotected index: %d", code)
	}
	code, body := c.op("reader", "DeleteIndex", &DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "kept"})
	mustStatus(t, "DeleteIndex on a protected index", code, body, http.StatusForbidden)
	if c.fs.Get(s3tables.GetNamespacePath("vec-bucket", indexNamespace), "kept") == nil {
		t.Fatal("a refused delete removed the protected index")
	}
}

func TestRequestTagsAndTagKeysGateTagging(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket")
	bucketArn := s.vectorBucketArn("vec-bucket")
	mustStatus(t, "tag bucket", tagCode(c, bucketArn, map[string]string{"team": "search", "env": "dev"}), "", http.StatusOK)
	c.storePolicy("vec-bucket", policyOf(
		statement("Allow", `"s3vectors:TagResource"`, bucketArn,
			`{"StringEquals":{"aws:RequestTag/team":"search"},"ForAllValues:StringEquals":{"aws:TagKeys":["team","env"]}}`),
		statement("Allow", `"s3vectors:UntagResource"`, bucketArn, `{"ForAllValues:StringEquals":{"aws:TagKeys":["env"]}}`),
	))
	for _, tc := range []struct {
		tags map[string]string
		want int
	}{
		{map[string]string{"team": "search"}, http.StatusOK},
		{map[string]string{"team": "search", "env": "prod"}, http.StatusOK},
		{map[string]string{"team": "other"}, http.StatusForbidden},
		{map[string]string{"team": "search", "owner": "x"}, http.StatusForbidden},
		{map[string]string{"env": "prod"}, http.StatusForbidden},
	} {
		code, body := c.tag("reader", bucketArn, tc.tags)
		mustStatus(t, "TagResource "+jsonOf(tc.tags), code, body, tc.want)
	}
	code, body := c.untag("reader", bucketArn, "team")
	mustStatus(t, "UntagResource team", code, body, http.StatusForbidden)
	code, body = c.untag("reader", bucketArn, "env")
	mustStatus(t, "UntagResource env", code, body, http.StatusOK)
	if _, tags := c.list("", bucketArn); len(tags) != 1 || tags["team"] != "search" {
		t.Fatalf("tags = %v, want only team=search", tags)
	}
}

func TestRequestTagGatesCreateIndex(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket")
	c.storePolicy("vec-bucket", policyOf(
		statement("Allow", `["s3vectors:CreateIndex","s3vectors:TagResource"]`, s.vectorBucketArn("vec-bucket")+"/index/*",
			`{"StringEquals":{"aws:RequestTag/team":"search"}}`),
	))
	create := func(name string, tags map[string]string) int {
		code, _ := c.op("reader", "CreateIndex", &CreateIndexRequest{
			VectorBucketName: "vec-bucket", IndexName: name, DataType: "float32", Dimension: 3, Tags: tags,
		})
		return code
	}
	if code := create("other", map[string]string{"team": "other"}); code != http.StatusForbidden {
		t.Fatalf("CreateIndex tagged team=other: %d, want 403", code)
	}
	if code := create("untagged", nil); code != http.StatusForbidden {
		t.Fatalf("CreateIndex without tags: %d, want 403", code)
	}
	if code := create("search", map[string]string{"team": "search"}); code != http.StatusOK {
		t.Fatalf("CreateIndex tagged team=search: %d", code)
	}
	if code := c.getIndex("", "vec-bucket", "other"); code != http.StatusNotFound {
		t.Fatalf("refused create left an index: %d", code)
	}
}

func TestResourceTagFiltersVectorBucketListing(t *testing.T) {
	s, c := newTagClient(t)
	for bucket, env := range map[string]string{"prod-bucket": "prod", "dev-bucket": "dev"} {
		c.setup(bucket)
		arn := s.vectorBucketArn(bucket)
		mustStatus(t, "tag "+bucket, tagCode(c, arn, map[string]string{"env": env}), "", http.StatusOK)
		c.storePolicy(bucket, policyOf(
			statement("Allow", `"s3vectors:ListVectorBuckets"`, arn, `{"StringEquals":{"aws:ResourceTag/env":"prod"}}`),
		))
	}
	code, body := c.op("reader", "ListVectorBuckets", &ListVectorBucketsRequest{})
	mustStatus(t, "ListVectorBuckets", code, body, http.StatusOK)
	var resp ListVectorBucketsResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if len(resp.VectorBuckets) != 1 || resp.VectorBuckets[0].VectorBucketName != "prod-bucket" {
		t.Fatalf("listed %s, want only prod-bucket", body)
	}
}

func TestResourceTagGatesPolicyDelete(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket")
	bucketArn := s.vectorBucketArn("vec-bucket")
	mustStatus(t, "tag bucket", tagCode(c, bucketArn, map[string]string{"env": "dev"}), "", http.StatusOK)
	c.storePolicy("vec-bucket", policyOf(
		statement("Allow", `"s3vectors:DeleteVectorBucketPolicy"`, bucketArn, `{"StringEquals":{"aws:ResourceTag/env":"prod"}}`),
	))
	del := func() int {
		code, _ := c.op("reader", "DeleteVectorBucketPolicy", &DeleteVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"})
		return code
	}
	if code := del(); code != http.StatusForbidden {
		t.Fatalf("DeleteVectorBucketPolicy on env=dev: %d, want 403", code)
	}
	mustStatus(t, "retag bucket", tagCode(c, bucketArn, map[string]string{"env": "prod"}), "", http.StatusOK)
	if code := del(); code != http.StatusOK {
		t.Fatalf("DeleteVectorBucketPolicy on env=prod: %d", code)
	}
}

func TestReadsResourceTags(t *testing.T) {
	for policy, want := range map[string]bool{
		"": false,
		`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:*","Resource":"*"}]}`:                                                                  false,
		`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:*","Resource":"*","Condition":{"StringEquals":{"s3vectors:VectorBucketTag/a":"b"}}}]}`: false,
		`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/a":"b"}}}]}`:           true,
		`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag\/a":"b"}}}]}`:           true,
		`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/a":"b"}}}]}`:            true,
		`not-json`: true,
	} {
		if got := readsResourceTags(policy); got != want {
			t.Errorf("readsResourceTags(%s) = %v, want %v", policy, got, want)
		}
	}
}

func tagCode(c *tagClient, arn string, tags map[string]string) int {
	code, _ := c.tag("", arn, tags)
	return code
}

func jsonOf(v interface{}) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func TestValidateVectorPolicyConditions(t *testing.T) {
	withCondition := func(condition string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":` + condition + `}]}`
	}
	for _, ok := range []string{
		`{"StringEquals":{"aws:ResourceTag/env":"prod"}}`,
		`{"StringLike":{"aws:RequestTag/team":"search-*"}}`,
		`{"ForAllValues:StringEquals":{"aws:TagKeys":["team","env"]}}`,
		`{"StringNotEquals":{"s3vectors:VectorBucketTag/env":"dev"}}`,
		`{"Null":{"aws:ResourceTag/owner":true}}`,
		`{"StringEquals":{"aws:ResourceTag/env":["prod","staging"]}}`,
		`{"NumericLessThan":{"aws:RequestTag/priority":10}}`,
	} {
		if err := validateVectorPolicy(withCondition(ok)); err != nil {
			t.Errorf("%s rejected: %v", ok, err)
		}
	}
	for _, bad := range []string{
		`{"StringEquals":{"aws:SourceIp":"10.0.0.1"}}`,
		`{"StringEquals":{"s3tables:TableBucketTag/env":"prod"}}`,
		`{"StringEquals":{"aws:ResourceTag/":"prod"}}`,
		`{"StringEquals":{"aws:resourcetag/env":"prod"}}`,
		`{"StringEqualsIfExists":{"aws:ResourceTag/env":"prod"}}`,
		`{"NoSuchOperator":{"aws:ResourceTag/env":"prod"}}`,
		`"aws:ResourceTag/env"`,
		`{"StringEquals":{"aws:ResourceTag/env":null}}`,
		`{"StringEquals":{"aws:ResourceTag/env":[]}}`,
		`{"StringEquals":{"aws:ResourceTag/env":[null]}}`,
		`{"StringEquals":{"aws:ResourceTag/env":{"a":"b"}}}`,
		`{"StringEquals":{"aws:ResourceTag/env":["prod",["dev"]]}}`,
		`{"NumericLessThan":{"aws:RequestTag/priority":"low"}}`,
		`{"StringEquals":{}}`,
		`null`,
	} {
		if err := validateVectorPolicy(withCondition(bad)); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	lower := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`
	if err := validateVectorPolicy(lower); err == nil {
		t.Error("a lower-case condition field was accepted")
	}
	for _, misspelled := range []string{
		`{"Version":"2012-10-17","Statement":[{"effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null}]}`,
		`{"Version":"2012-10-17","statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null}]}`,
	} {
		if err := validateVectorPolicy(misspelled); err == nil {
			t.Errorf("%s accepted", misspelled)
		}
	}
	for _, repeated := range []string{
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null,"Condition":{}}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"Condition":{"StringEquals":{"aws:ResourceTag/team":"x"}}}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"Condition":{"StringEquals":{"aws:ResourceTag/team":"x"}}}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null,"Condition":{}}]}`,
	} {
		if err := validateVectorPolicy(repeated); err == nil {
			t.Errorf("%s accepted", repeated)
		}
	}
	denyNull := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:GetIndex","Resource":"*","Condition":null}]}`
	if err := validateVectorPolicy(denyNull); err != nil {
		t.Errorf("a Deny with a null Condition was rejected: %v", err)
	}
}

func TestPutVectorBucketPolicyWithTagConditions(t *testing.T) {
	s, c := newTagClient(t)
	c.setup("vec-bucket", "live-idx", "test-idx")
	mustStatus(t, "tag live", tagCode(c, s.indexArn("vec-bucket", "live-idx"), map[string]string{"stage": "live"}), "", http.StatusOK)
	policy := policyOf(statement("Allow", `"s3vectors:GetIndex"`, s.vectorBucketArn("vec-bucket")+"/index/*",
		`{"StringEquals":{"aws:ResourceTag/stage":"live"}}`))
	code, body := c.op("", "PutVectorBucketPolicy", &PutVectorBucketPolicyRequest{VectorBucketName: "vec-bucket", Policy: policy})
	mustStatus(t, "PutVectorBucketPolicy", code, body, http.StatusOK)
	code, body = c.op("", "GetVectorBucketPolicy", &GetVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"})
	mustStatus(t, "GetVectorBucketPolicy", code, body, http.StatusOK)
	var got GetVectorBucketPolicyResponse
	if err := json.Unmarshal([]byte(body), &got); err != nil || got.Policy != policy {
		t.Fatalf("stored policy = %s (%v), want %s", body, err, policy)
	}
	if code := c.getIndex("reader", "vec-bucket", "live-idx"); code != http.StatusOK {
		t.Fatalf("GetIndex on stage=live: %d", code)
	}
	if code := c.getIndex("reader", "vec-bucket", "test-idx"); code != http.StatusForbidden {
		t.Fatalf("GetIndex on an untagged index: %d, want 403", code)
	}
}
