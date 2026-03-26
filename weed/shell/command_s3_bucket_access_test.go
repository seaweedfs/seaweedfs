package shell

import (
	"encoding/json"
	"slices"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestBuildBucketActions(t *testing.T) {
	tests := []struct {
		name        string
		permissions []string
		wantActions []string
	}{
		{
			name:        "read",
			permissions: []string{"read"},
			wantActions: []string{s3_constants.S3_ACTION_GET_BUCKET_LOCATION},
		},
		{
			name:        "list",
			permissions: []string{"list"},
			wantActions: []string{s3_constants.S3_ACTION_GET_BUCKET_LOCATION, s3_constants.S3_ACTION_LIST_BUCKET},
		},
		{
			name:        "write",
			permissions: []string{"write"},
			wantActions: []string{s3_constants.S3_ACTION_GET_BUCKET_LOCATION, s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS},
		},
		{
			name:        "read,write,list",
			permissions: []string{"read", "write", "list"},
			wantActions: []string{s3_constants.S3_ACTION_GET_BUCKET_LOCATION, s3_constants.S3_ACTION_LIST_BUCKET, s3_constants.S3_ACTION_LIST_MULTIPART_UPLOADS},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildBucketActions(tt.permissions)
			sort.Strings(got)
			sort.Strings(tt.wantActions)
			if !slices.Equal(got, tt.wantActions) {
				t.Errorf("buildBucketActions(%v) = %v, want %v", tt.permissions, got, tt.wantActions)
			}
		})
	}
}

func TestBuildObjectActions(t *testing.T) {
	tests := []struct {
		name        string
		permissions []string
		wantActions []string
	}{
		{
			name:        "read",
			permissions: []string{"read"},
			wantActions: []string{s3_constants.S3_ACTION_GET_OBJECT},
		},
		{
			name:        "write",
			permissions: []string{"write"},
			wantActions: []string{s3_constants.S3_ACTION_ABORT_MULTIPART, s3_constants.S3_ACTION_DELETE_OBJECT, s3_constants.S3_ACTION_LIST_PARTS, s3_constants.S3_ACTION_PUT_OBJECT},
		},
		{
			name:        "list only",
			permissions: []string{"list"},
			wantActions: nil,
		},
		{
			name:        "read,write",
			permissions: []string{"read", "write"},
			wantActions: []string{s3_constants.S3_ACTION_ABORT_MULTIPART, s3_constants.S3_ACTION_DELETE_OBJECT, s3_constants.S3_ACTION_GET_OBJECT, s3_constants.S3_ACTION_LIST_PARTS, s3_constants.S3_ACTION_PUT_OBJECT},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildObjectActions(tt.permissions)
			sort.Strings(got)
			sort.Strings(tt.wantActions)
			if !slices.Equal(got, tt.wantActions) {
				t.Errorf("buildObjectActions(%v) = %v, want %v", tt.permissions, got, tt.wantActions)
			}
		})
	}
}

func TestDetectPermissions(t *testing.T) {
	tests := []struct {
		name      string
		policy    policy_engine.PolicyDocument
		wantPerms []string
	}{
		{
			name:      "empty policy",
			policy:    policy_engine.PolicyDocument{},
			wantPerms: nil,
		},
		{
			name: "read,list",
			policy: policy_engine.PolicyDocument{
				Version: policy_engine.PolicyVersion2012_10_17,
				Statement: []policy_engine.PolicyStatement{
					{
						Sid:       publicAccessBucketSid,
						Effect:    policy_engine.PolicyEffectAllow,
						Principal: policy_engine.NewStringOrStringSlicePtr("*"),
						Action:    policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_GET_BUCKET_LOCATION, s3_constants.S3_ACTION_LIST_BUCKET),
					},
					{
						Sid:       publicAccessObjectSid,
						Effect:    policy_engine.PolicyEffectAllow,
						Principal: policy_engine.NewStringOrStringSlicePtr("*"),
						Action:    policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_GET_OBJECT),
					},
				},
			},
			wantPerms: []string{"list", "read"},
		},
		{
			name: "ignores non-public statements",
			policy: policy_engine.PolicyDocument{
				Version: policy_engine.PolicyVersion2012_10_17,
				Statement: []policy_engine.PolicyStatement{
					{
						Sid:    "CustomStatement",
						Effect: policy_engine.PolicyEffectAllow,
						Action: policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_GET_OBJECT),
					},
				},
			},
			wantPerms: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectPermissions(&tt.policy)
			sort.Strings(got)
			sort.Strings(tt.wantPerms)
			if !slices.Equal(got, tt.wantPerms) {
				t.Errorf("detectPermissions() = %v, want %v", got, tt.wantPerms)
			}
		})
	}
}

func TestPolicyRoundTrip(t *testing.T) {
	permissions := []string{"read", "write", "list"}
	bucketName := "test-bucket"

	bucketActions := buildBucketActions(permissions)
	objectActions := buildObjectActions(permissions)
	sort.Strings(bucketActions)
	sort.Strings(objectActions)

	policyDoc := policy_engine.PolicyDocument{
		Version: policy_engine.PolicyVersion2012_10_17,
		Statement: []policy_engine.PolicyStatement{
			{
				Sid:       publicAccessBucketSid,
				Effect:    policy_engine.PolicyEffectAllow,
				Principal: policy_engine.NewStringOrStringSlicePtr("*"),
				Action:    policy_engine.NewStringOrStringSlice(bucketActions...),
				Resource:  policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::" + bucketName),
			},
			{
				Sid:       publicAccessObjectSid,
				Effect:    policy_engine.PolicyEffectAllow,
				Principal: policy_engine.NewStringOrStringSlicePtr("*"),
				Action:    policy_engine.NewStringOrStringSlice(objectActions...),
				Resource:  policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::" + bucketName + "/*"),
			},
		},
	}

	data, err := json.Marshal(&policyDoc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var parsed policy_engine.PolicyDocument
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	perms := detectPermissions(&parsed)
	sort.Strings(perms)
	expected := []string{"list", "read", "write"}
	if !slices.Equal(perms, expected) {
		t.Errorf("round-trip detectPermissions() = %v, want %v", perms, expected)
	}
}

func TestSetNonePreservesOtherStatements(t *testing.T) {
	policyDoc := policy_engine.PolicyDocument{
		Version: policy_engine.PolicyVersion2012_10_17,
		Statement: []policy_engine.PolicyStatement{
			{
				Sid:       publicAccessBucketSid,
				Effect:    policy_engine.PolicyEffectAllow,
				Principal: policy_engine.NewStringOrStringSlicePtr("*"),
				Action:    policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_LIST_BUCKET),
			},
			{
				Sid:    "CustomCrossAccountAccess",
				Effect: policy_engine.PolicyEffectAllow,
				Action: policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_GET_OBJECT),
			},
			{
				Sid:       publicAccessObjectSid,
				Effect:    policy_engine.PolicyEffectAllow,
				Principal: policy_engine.NewStringOrStringSlicePtr("*"),
				Action:    policy_engine.NewStringOrStringSlice(s3_constants.S3_ACTION_GET_OBJECT),
			},
		},
	}

	// Simulate removing public-access statements (same logic as setAccess with "none")
	var preserved []policy_engine.PolicyStatement
	for _, stmt := range policyDoc.Statement {
		if stmt.Sid != publicAccessBucketSid && stmt.Sid != publicAccessObjectSid {
			preserved = append(preserved, stmt)
		}
	}

	if len(preserved) != 1 {
		t.Fatalf("expected 1 preserved statement, got %d", len(preserved))
	}
	if preserved[0].Sid != "CustomCrossAccountAccess" {
		t.Errorf("preserved wrong statement: %s", preserved[0].Sid)
	}
}
