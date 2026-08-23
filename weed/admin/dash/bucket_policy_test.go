package dash

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func validBucketPolicyJSON(bucket string) []byte {
	return []byte(fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/*"}]}`, bucket))
}

func validBucketPolicyDoc(bucket string) *policy_engine.PolicyDocument {
	var doc policy_engine.PolicyDocument
	if err := json.Unmarshal(validBucketPolicyJSON(bucket), &doc); err != nil {
		panic(err)
	}
	return &doc
}

func TestBucketPolicyMutation_SetsPolicy(t *testing.T) {
	policyJSON := validBucketPolicyJSON("mybucket")
	m := bucketPolicyMutation("/buckets", "mybucket", policyJSON)

	if m.Type != filer_pb.ObjectMutation_PATCH_EXTENDED {
		t.Fatalf("expected a PATCH_EXTENDED mutation, got %v", m.Type)
	}
	if m.Directory != "/buckets" || m.Name != "mybucket" {
		t.Fatalf("expected the mutation to target /buckets/mybucket, got %s/%s", m.Directory, m.Name)
	}
	if got := m.SetExtended[s3api.BUCKET_POLICY_METADATA_KEY]; string(got) != string(policyJSON) {
		t.Fatalf("expected the policy key to carry the marshaled document, got %q", got)
	}
	if len(m.DeleteExtended) != 0 {
		t.Fatalf("expected no key deletions when saving a policy, got %v", m.DeleteExtended)
	}
}

func TestBucketPolicyMutation_ClearsKey(t *testing.T) {
	m := bucketPolicyMutation("/buckets", "mybucket", nil)

	if len(m.SetExtended) != 0 {
		t.Fatalf("expected no key writes when clearing, got %v", m.SetExtended)
	}
	if len(m.DeleteExtended) != 1 || m.DeleteExtended[0] != s3api.BUCKET_POLICY_METADATA_KEY {
		t.Fatalf("expected only the policy key to be cleared, got %v", m.DeleteExtended)
	}
}

// A whole-entry write would have carried the rest of the bucket entry with
// it; the patch must name only the key it owns, so a concurrent owner,
// quota, or lifecycle change survives.
func TestBucketPolicyMutation_TouchesOnlyPolicyKey(t *testing.T) {
	for _, m := range []*filer_pb.ObjectMutation{
		bucketPolicyMutation("/buckets", "mybucket", validBucketPolicyJSON("mybucket")),
		bucketPolicyMutation("/buckets", "mybucket", nil),
	} {
		if m.Entry != nil {
			t.Fatal("expected the mutation to carry no entry snapshot")
		}
		if m.SetContent {
			t.Fatal("expected the mutation to leave entry content alone")
		}
		for k := range m.SetExtended {
			if k != s3api.BUCKET_POLICY_METADATA_KEY {
				t.Fatalf("unexpected key written: %s", k)
			}
		}
		for _, k := range m.DeleteExtended {
			if k != s3api.BUCKET_POLICY_METADATA_KEY {
				t.Fatalf("unexpected key deleted: %s", k)
			}
		}
	}
}

func TestExtractPolicyStatementCountFromEntry(t *testing.T) {
	tests := []struct {
		name  string
		entry *filer_pb.Entry
		want  int
	}{
		{"no extended attrs", &filer_pb.Entry{}, 0},
		{"absent key", &filer_pb.Entry{Extended: map[string][]byte{"other": []byte("x")}}, 0},
		{"one statement", &filer_pb.Entry{Extended: map[string][]byte{
			s3api.BUCKET_POLICY_METADATA_KEY: validBucketPolicyJSON("b"),
		}}, 1},
		{"three statements", &filer_pb.Entry{Extended: map[string][]byte{
			s3api.BUCKET_POLICY_METADATA_KEY: []byte(`{"Version":"2012-10-17","Statement":[
				{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"},
				{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::b/*"},
				{"Effect":"Deny","Principal":"*","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::b/*"}
			]}`),
		}}, 3},
		{"garbage bytes", &filer_pb.Entry{Extended: map[string][]byte{
			s3api.BUCKET_POLICY_METADATA_KEY: []byte("not json"),
		}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractPolicyStatementCountFromEntry(tt.entry); got != tt.want {
				t.Errorf("extractPolicyStatementCountFromEntry() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBucketPolicyErrorStatus(t *testing.T) {
	if got := bucketPolicyErrorStatus(fmt.Errorf("%w: mybucket", ErrBucketNotFound)); got != http.StatusNotFound {
		t.Fatalf("expected a missing bucket to map to 404, got %d", got)
	}
	if got := bucketPolicyErrorStatus(fmt.Errorf("%w: bad statement", ErrInvalidBucketPolicy)); got != http.StatusBadRequest {
		t.Fatalf("expected an invalid policy to map to 400, got %d", got)
	}
	if got := bucketPolicyErrorStatus(errors.New("filer unreachable")); got != http.StatusInternalServerError {
		t.Fatalf("expected an unrelated failure to stay 500, got %d", got)
	}
}

func TestSetBucketPolicy_RejectsOversized(t *testing.T) {
	// A resource list long enough to blow the cap: this must fail before
	// any filer call, which is what makes it testable without one.
	doc := validBucketPolicyDoc("mybucket")
	doc.Statement[0].Sid = strings.Repeat("x", policy_engine.MaxBucketPolicySize+1)

	err := (&AdminServer{}).SetBucketPolicy("mybucket", doc)
	if err == nil {
		t.Fatal("expected an oversized bucket policy to be rejected")
	}
	if !errors.Is(err, ErrInvalidBucketPolicy) {
		t.Fatalf("expected an ErrInvalidBucketPolicy, got: %v", err)
	}
}

func TestSetBucketPolicy_RejectsForeignResource(t *testing.T) {
	// Proves the shared policy_engine.ValidateBucketPolicy validator is
	// actually wired in: this is exactly the check the S3 gateway applies.
	doc := validBucketPolicyDoc("mybucket")
	doc.Statement[0].Resource = policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::other-bucket/*")

	err := (&AdminServer{}).SetBucketPolicy("mybucket", doc)
	if err == nil {
		t.Fatal("expected a policy referencing a different bucket to be rejected")
	}
	if !errors.Is(err, ErrInvalidBucketPolicy) {
		t.Fatalf("expected an ErrInvalidBucketPolicy, got: %v", err)
	}
}

func TestSetBucketPolicy_RejectsMissingPrincipal(t *testing.T) {
	doc := validBucketPolicyDoc("mybucket")
	doc.Statement[0].Principal = nil

	err := (&AdminServer{}).SetBucketPolicy("mybucket", doc)
	if err == nil {
		t.Fatal("expected a policy with no Principal to be rejected")
	}
	if !errors.Is(err, ErrInvalidBucketPolicy) {
		t.Fatalf("expected an ErrInvalidBucketPolicy, got: %v", err)
	}
}
