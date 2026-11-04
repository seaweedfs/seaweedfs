package s3tables

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func TestMatchesActionPattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		action   string
		expected bool
	}{
		// Exact matches
		{"exact match", "GetTable", "GetTable", true},
		{"no match", "GetTable", "DeleteTable", false},

		// Universal wildcard
		{"universal wildcard", "*", "anything", true},

		// Suffix wildcards
		{"suffix wildcard match", "s3tables:*", "s3tables:GetTable", true},
		{"suffix wildcard no match", "s3tables:*", "iam:GetUser", false},

		// Middle wildcards (new capability from policy_engine)
		{"middle wildcard Get*Table", "s3tables:Get*Table", "s3tables:GetTable", true},
		{"middle wildcard Get*Table no match GetTableBucket", "s3tables:Get*Table", "s3tables:GetTableBucket", false},
		{"middle wildcard Get*Table no match DeleteTable", "s3tables:Get*Table", "s3tables:DeleteTable", false},
		{"middle wildcard *Table*", "s3tables:*Table*", "s3tables:GetTableBucket", true},
		{"middle wildcard *Table* match CreateTable", "s3tables:*Table*", "s3tables:CreateTable", true},

		// Question mark wildcards
		{"question mark single char", "GetTable?", "GetTableX", true},
		{"question mark no match", "GetTable?", "GetTableXY", false},

		// Combined wildcards
		{"combined * and ? singular", "s3tables:Get?able*", "s3tables:GetTable", true},
		{"combined * and ? plural", "s3tables:Get?able*", "s3tables:GetTables", true},
		{"combined no match - ? needs 1 char", "s3tables:Get?able*", "s3tables:Getable", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesActionPattern(tt.pattern, tt.action)
			if result != tt.expected {
				t.Errorf("matchesActionPattern(%q, %q) = %v, want %v", tt.pattern, tt.action, result, tt.expected)
			}
		})
	}
}

func TestMatchesPrincipal(t *testing.T) {
	tests := []struct {
		name          string
		principalSpec interface{}
		principal     string
		expected      bool
	}{
		// String principals
		{"exact match", "user123", "user123", true},
		{"no match", "user123", "user456", false},
		{"universal wildcard", "*", "anyone", true},

		// Wildcard principals
		{"prefix wildcard", "arn:aws:iam::123456789012:user/*", "arn:aws:iam::123456789012:user/admin", true},
		{"prefix wildcard no match", "arn:aws:iam::123456789012:user/*", "arn:aws:iam::987654321098:user/admin", false},
		{"middle wildcard", "arn:aws:iam::*:user/admin", "arn:aws:iam::123456789012:user/admin", true},

		// Array of principals
		{"array match first", []interface{}{"user1", "user2"}, "user1", true},
		{"array match second", []interface{}{"user1", "user2"}, "user2", true},
		{"array no match", []interface{}{"user1", "user2"}, "user3", false},
		{"array wildcard", []interface{}{"user1", "arn:aws:iam::*:user/admin"}, "arn:aws:iam::123:user/admin", true},

		// Map-style AWS principals
		{"AWS map exact", map[string]interface{}{"AWS": "user123"}, "user123", true},
		{"AWS map wildcard", map[string]interface{}{"AWS": "arn:aws:iam::*:user/admin"}, "arn:aws:iam::123:user/admin", true},
		{"AWS map array", map[string]interface{}{"AWS": []interface{}{"user1", "user2"}}, "user1", true},

		// ARN statements do not reach normalized-name callers on the allow
		// path: normalization drops the ARN's account and path, so the grant
		// could land on a same-named caller the statement never verified.
		{"arn spec exact caller", "arn:aws:iam::123456789012:user/bob", "bob", false},
		{"arn spec wildcard caller", "arn:aws:iam::*:user/alice", "alice", false},
		{"arn spec no match", "arn:aws:iam::123456789012:user/carol", "dan", false},

		// Nil/empty cases
		{"nil principal", nil, "user123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesPrincipal(tt.principalSpec, tt.principal)
			if result != tt.expected {
				t.Errorf("matchesPrincipal(%v, %q) = %v, want %v", tt.principalSpec, tt.principal, result, tt.expected)
			}
		})
	}
}

func TestEvaluatePolicyWithConditions(t *testing.T) {
	policy := &PolicyDocument{
		Statement: []Statement{
			{
				Effect:    "Allow",
				Principal: "*",
				Action:    "s3tables:GetTable",
				Condition: map[string]map[string]interface{}{
					"StringEquals": {
						"s3tables:namespace": "default",
					},
					"StringLike": {
						"s3tables:tableName": "test_*",
					},
					"NumericGreaterThan": {
						"aws:RequestTag/priority": "10",
					},
					"Bool": {
						"aws:ResourceTag/is_public": "true",
					},
				},
			},
		},
	}
	policyBytes, _ := json.Marshal(policy)
	policyStr := string(policyBytes)
	if err := validatePolicyDocument(policyStr); err != nil {
		t.Fatalf("conditional policy rejected by the validator: %v", err)
	}

	tests := []struct {
		name     string
		ctx      *PolicyContext
		expected bool
	}{
		{
			"all conditions match",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			true,
		},
		{
			"namespace mismatch",
			&PolicyContext{
				Namespace: "other",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"table name mismatch",
			&PolicyContext{
				Namespace: "default",
				TableName: "other_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"numeric condition failure",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "5",
				},
				ResourceTags: map[string]string{
					"is_public": "true",
				},
			},
			false,
		},
		{
			"bool condition failure",
			&PolicyContext{
				Namespace: "default",
				TableName: "test_table",
				RequestTags: map[string]string{
					"priority": "15",
				},
				ResourceTags: map[string]string{
					"is_public": "false",
				},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// principal="user123", owner="owner123"
			result := CheckPermissionWithContext("s3tables:GetTable", "user123", "owner123", policyStr, "", tt.ctx)
			if result != tt.expected {
				t.Errorf("CheckPermissionWithContext() = %v, want %v", result, tt.expected)
			}
		})
	}
}
func TestCheckPermissionWithDefaultAllow(t *testing.T) {
	tests := []struct {
		name         string
		defaultAllow bool
		policy       string
		expected     bool
	}{
		{
			"default deny (no policy, DefaultAllow=false)",
			false,
			"",
			false,
		},
		{
			"default allow (no policy, DefaultAllow=true)",
			true,
			"",
			true,
		},
		{
			"explicit deny overrides DefaultAllow=true",
			true,
			`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable"}]}`,
			false,
		},
		{
			"explicit allow works with DefaultAllow=false",
			false,
			`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable"}]}`,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &PolicyContext{
				DefaultAllow: tt.defaultAllow,
			}
			result := CheckPermissionWithContext("s3tables:GetTable", "user123", "owner123", tt.policy, "", ctx)
			if result != tt.expected {
				t.Errorf("CheckPermissionWithContext() = %v, want %v (DefaultAllow=%v, Policy=%s)", result, tt.expected, tt.defaultAllow, tt.policy)
			}
		})
	}
}

func TestCheckTableOrBucketPermissionDenyVeto(t *testing.T) {
	tablePolicy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3tables:DeleteTable",
			"Resource": "*"
		}]
	}`
	bucketAllow := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3tables:*",
			"Resource": "*"
		}]
	}`
	tableARN := "arn:aws:s3tables:us-east-1:acct:bucket/b/table/ns/t"
	bucketARN := "arn:aws:s3tables:us-east-1:acct:bucket/b"
	ctx := &PolicyContext{TableBucketName: "b", Namespace: "ns", TableName: "t"}

	// A Deny on the table policy vetoes the bucket Allow.
	if CheckTableOrBucketPermission("DeleteTable", "user123", "owner", tablePolicy, tableARN, "owner", bucketAllow, bucketARN, ctx) {
		t.Error("table Deny should veto bucket Allow")
	}

	// The same Deny in the bucket policy vetoes a table Allow.
	if CheckTableOrBucketPermission("DeleteTable", "user123", "owner", bucketAllow, tableARN, "owner", tablePolicy, bucketARN, ctx) {
		t.Error("bucket Deny should veto table Allow")
	}

	// A table Deny also vetoes ownership of the table.
	if CheckTableOrBucketPermission("DeleteTable", "owner", "owner", tablePolicy, tableARN, "owner", "", bucketARN, ctx) {
		t.Error("table Deny should veto the owner grant")
	}

	// No Deny anywhere: either policy may allow.
	if !CheckTableOrBucketPermission("DeleteTable", "user123", "other-owner", "", tableARN, "owner", bucketAllow, bucketARN, ctx) {
		t.Error("bucket Allow should grant when no policy denies")
	}
}

// A principal that merely normalizes to the admin id must not bypass policy
// denies: when the caller's actions are known, the admin bypass requires an
// admin action; transports carrying no action list keep it.
func TestCheckPermissionAdminRequiresAdminAction(t *testing.T) {
	denyAll := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:DeleteTable","Resource":"*"}]}`
	arn := "arn:aws:s3tables:us-east-1:acct:bucket/b/table/ns/t"

	reader := &PolicyContext{IdentityActions: []string{"Read"}}
	if CheckPermissionWithContext("s3tables:DeleteTable", "admin", "admin", denyAll, arn, reader) {
		t.Error("a name-only admin must not bypass an explicit deny")
	}
	admin := &PolicyContext{IdentityActions: []string{"Admin"}}
	if !CheckPermissionWithContext("s3tables:DeleteTable", "admin", "admin", denyAll, arn, admin) {
		t.Error("an identity holding the admin action keeps the bypass")
	}
	if !CheckPermissionWithContext("s3tables:DeleteTable", "admin", "admin", denyAll, arn, nil) {
		t.Error("a caller with no action list keeps the trusted bypass")
	}
	empty := &PolicyContext{IdentityActions: []string{}}
	if CheckPermissionWithContext("s3tables:DeleteTable", "admin", "admin", denyAll, arn, empty) {
		t.Error("an identified admin with an empty action list must not bypass a deny")
	}
}

func TestCheckTableOrBucketPermissionOwnerlessFailsClosed(t *testing.T) {
	ctx := &PolicyContext{
		IdentityActions: []string{"s3tables:*"},
		DefaultAllow:    true,
	}
	allowPolicy := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*"}]}`

	if CheckTableOrBucketPermission("GetTable", "user1",
		"", "", "arn:aws:s3tables:t",
		"", "", "arn:aws:s3tables:b", ctx) {
		t.Error("identity actions and default allow must not reach an ownerless resource")
	}

	// With no identity or default grants in play, an ownerless resource
	// side contributes nothing even when its own policy allows.
	plainCtx := &PolicyContext{}
	if CheckTableOrBucketPermission("GetTable", "user1",
		"", allowPolicy, "arn:aws:s3tables:t",
		"owner", "", "arn:aws:s3tables:b", plainCtx) {
		t.Error("an ownerless resource side must not grant even via an explicit allow")
	}

	if !CheckTableOrBucketPermission("GetTable", "user1",
		"", "", "arn:aws:s3tables:t",
		"owner", allowPolicy, "arn:aws:s3tables:b", plainCtx) {
		t.Error("the bucket side can still grant when its owner is set")
	}
}

// Owners written before issuer scoping hold a bare subject. Re-anchoring it
// is only unambiguous when exactly one OIDC issuer is configured; under two
// or more issuers a bare subject could belong to either, so it must not
// match anyone.
func TestCheckPermissionLegacyOwnerScopedToIssuer(t *testing.T) {
	owner := "alice-sub"
	principal := sts.ComputeParentUser(owner, "corp-idp")
	noLocal := &PolicyContext{
		TrustedIssuers:   []string{"corp-idp"},
		IsLocalAccountID: func(string) bool { return false },
	}

	if !CheckPermissionWithContext("GetTable", principal, owner, "", "arn", noLocal) {
		t.Error("single-issuer deployments must keep recognizing the legacy owner")
	}

	if CheckPermissionWithContext("GetTable", principal, owner, "", "arn", &PolicyContext{
		TrustedIssuers:   []string{"corp-idp", "evil-idp"},
		IsLocalAccountID: func(string) bool { return false },
	}) {
		t.Error("a bare subject is ambiguous across multiple issuers and must not match")
	}

	if CheckPermissionWithContext("GetTable", sts.ComputeParentUser(owner, "evil-idp"), owner, "", "arn", &PolicyContext{
		TrustedIssuers:   []string{"corp-idp", "evil-idp"},
		IsLocalAccountID: func(string) bool { return false },
	}) {
		t.Error("a same-sub caller from another issuer must not claim the legacy owner")
	}

	if CheckPermissionWithContext("GetTable", principal, owner, "", "arn", &PolicyContext{
		IsLocalAccountID: func(string) bool { return false },
	}) {
		t.Error("legacy owner matching must not engage without a trusted issuer")
	}

	// With no local-account predicate only the unconditional exclusions apply;
	// a plain legacy subject still re-anchors.
	if !CheckPermissionWithContext("GetTable", principal, owner, "", "arn", &PolicyContext{
		TrustedIssuers: []string{"corp-idp"},
	}) {
		t.Error("a missing local-account check keeps plain legacy subjects working")
	}
}

// A stored owner that names a reserved or configured local account was never
// an OIDC subject, and an already issuer-scoped id is not a legacy subject
// either; re-anchoring them would let a caller pick sub=<stored owner> and
// claim the resource.
func TestCheckPermissionLegacyOwnerExcludesLocalAndScoped(t *testing.T) {
	ctx := &PolicyContext{
		TrustedIssuers:   []string{"corp-idp"},
		IsLocalAccountID: func(id string) bool { return id == "admin" || id == "owner2" },
	}

	for _, owner := range []string{"admin", "owner2"} {
		principal := sts.ComputeParentUser(owner, "corp-idp")
		if CheckPermissionWithContext("GetTable", principal, owner, "", "arn", ctx) {
			t.Errorf("sub=%q must not re-anchor onto the local account owner", owner)
		}
	}

	scoped := sts.ComputeParentUser("victim-sub", "corp-idp")
	if CheckPermissionWithContext("GetTable", sts.ComputeParentUser(scoped, "corp-idp"), scoped, "", "arn", ctx) {
		t.Error("sub equal to a stored scoped id must not re-anchor")
	}

	// An ARN-shaped subject claims nothing; ARNs name IAM principals, not OIDC
	// subjects, and this shape is what let a forged sub reach the admin account.
	arn := "arn:aws:iam::123456789012:user/admin"
	if CheckPermissionWithContext("GetTable", sts.ComputeParentUser(arn, "corp-idp"), arn, "", "arn", ctx) {
		t.Error("an ARN-shaped stored owner must not re-anchor")
	}
}

// Legacy policies can carry the bare subject in the AWS map or array forms,
// not only as a top-level string; re-anchoring must reach all of them or a
// pre-scoping Deny silently stops matching the caller.
func TestCheckPermissionLegacyPolicyPrincipalShapes(t *testing.T) {
	ctx := &PolicyContext{
		TrustedIssuers:   []string{"corp-idp"},
		IsLocalAccountID: func(string) bool { return false },
	}
	principal := sts.ComputeParentUser("alice-sub", "corp-idp")
	other := "other-owner"

	// The caller owns the resource, so only a matched Deny can veto it.
	for _, policy := range []string{
		`{"Statement":[{"Effect":"Deny","Principal":{"AWS":"alice-sub"},"Action":"s3tables:GetTable"}]}`,
		`{"Statement":[{"Effect":"Deny","Principal":["alice-sub"],"Action":"s3tables:GetTable"}]}`,
	} {
		if CheckPermissionWithContext("GetTable", principal, principal, policy, "arn", ctx) {
			t.Errorf("legacy Deny must veto the scoped caller's owner grant: %s", policy)
		}
	}

	allow := `{"Statement":[{"Effect":"Allow","Principal":["alice-sub"],"Action":"s3tables:GetTable"}]}`
	if !CheckPermissionWithContext("GetTable", principal, other, allow, "arn", ctx) {
		t.Error("legacy Allow with array principal must still match the scoped caller")
	}
}

func TestValidatePolicyDocument(t *testing.T) {
	valid := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}]}`
	if err := validatePolicyDocument(valid); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if err := validatePolicyDocument("not-json"); err == nil {
		t.Fatal("malformed policy should be rejected")
	}
	dead := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:PutVectors","Resource":"*"}]}`
	if err := validatePolicyDocument(dead); err == nil {
		t.Fatal("s3vectors action should be rejected")
	}
	deadList := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["s3tables:*","s3vectors:*"],"Resource":"*"}]}`
	if err := validatePolicyDocument(deadList); err == nil {
		t.Fatal("action list containing s3vectors should be rejected")
	}
	deadResource := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"arn:aws:s3vectors:r:a:bucket/b"}]}`
	if err := validatePolicyDocument(deadResource); err == nil {
		t.Fatal("s3vectors resource should be rejected")
	}
	deadPartition := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"arn:aws-cn:s3vectors:cn-1:a:bucket/b"}]}`
	if err := validatePolicyDocument(deadPartition); err == nil {
		t.Fatal("s3vectors resource in another partition should be rejected")
	}
	deadWildcard := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"arn:*:s3vectors:r:a:bucket/b"}]}`
	if err := validatePolicyDocument(deadWildcard); err == nil {
		t.Fatal("s3vectors resource in a wildcard partition should be rejected")
	}
	deadMixed := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":["arn:aws:s3tables:r:a:bucket/b","arn:aws:s3vectors:r:a:bucket/b"]}]}`
	if err := validatePolicyDocument(deadMixed); err == nil {
		t.Fatal("resource list containing s3vectors should be rejected")
	}

	withCondition := func(condition string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":` + condition + `}]}`
	}
	for _, tc := range []struct {
		condition string
		mentions  []string // the error must name these; empty means accepted
	}{
		{`{"StringEquals":{"aws:ResourceTag/env":"prod"}}`, nil},
		{`{"StringEquals":{"aws:ResourceTag/env":["a","b"]}}`, nil},
		{`{"NumericLessThan":{"aws:RequestTag/priority":10}}`, nil},
		{`{"Bool":{"aws:ResourceTag/is_public":true}}`, nil},
		{`{"ForAnyValue:StringEquals":{"aws:TagKeys":["team","env"]}}`, nil},
		{`{"Null":{"aws:ResourceTag/owner":"true"}}`, nil},
		{`{"StringEquals":{"s3tables:namespace":"default"}}`, nil},
		{`{"StringEquals":{"aws:ResourceTag/env":null}}`, []string{"StringEquals", `"aws:ResourceTag/env"`}},
		{`{"StringEquals":{"aws:ResourceTag/env":[null]}}`, []string{"StringEquals", `"aws:ResourceTag/env"`}},
		{`{"StringNotEquals":{"aws:ResourceTag/env":[]}}`, []string{"StringNotEquals", `"aws:ResourceTag/env"`}},
		{`{"StringEquals":{"aws:ResourceTag/env":{}}}`, []string{"StringEquals", `"aws:ResourceTag/env"`}},
		{`{"StringEquals":{"aws:ResourceTag/env":{"a":1}}}`, []string{"StringEquals", `"aws:ResourceTag/env"`}},
		{`{"StringEquals":{"aws:ResourceTag/env":[["a"]]}}`, []string{"StringEquals", `"aws:ResourceTag/env"`}},
		{`{"StringEquals":null}`, []string{"StringEquals", "at least one key"}},
		{`{"StringEquals":{}}`, []string{"StringEquals", "at least one key"}},
		{`{"StringEqualsBogus":{"aws:ResourceTag/env":"prod"}}`, []string{`"StringEqualsBogus"`, "not supported"}},
		{`{"NumericLessThan":{"aws:RequestTag/priority":"10"}}`, nil},
		{`{"DateLessThan":{"aws:ResourceTag/created":"2026-01-01T00:00:00Z"}}`, nil},
		{`{"Bool":{"aws:ResourceTag/is_public":"TRUE"}}`, nil},
		{`{"IpAddress":{"aws:ResourceTag/net":"10.0.0.0/8"}}`, nil},
		{`{"StringEquals":{"aws:ResourceTag/env":10}}`, nil},
		{`{"NumericLessThan":{"aws:RequestTag/priority":"low"}}`, []string{"NumericLessThan", `"aws:RequestTag/priority"`}},
		{`{"NumericLessThan":{"aws:RequestTag/priority":true}}`, []string{"NumericLessThan", `"aws:RequestTag/priority"`}},
		{`{"NumericLessThan":{"aws:RequestTag/priority":["low","10"]}}`, []string{"NumericLessThan", `"aws:RequestTag/priority"`}},
		{`{"ForAnyValue:NumericLessThan":{"aws:RequestTag/priority":"low"}}`, []string{"ForAnyValue:NumericLessThan", `"aws:RequestTag/priority"`}},
		{`{"DateLessThan":{"aws:ResourceTag/created":"2026-01-01"}}`, []string{"DateLessThan", `"aws:ResourceTag/created"`}},
		{`{"Bool":{"aws:ResourceTag/is_public":"yes"}}`, []string{"Bool", `"aws:ResourceTag/is_public"`}},
		{`{"Null":{"aws:ResourceTag/owner":"maybe"}}`, []string{"Null", `"aws:ResourceTag/owner"`}},
		{`{"IpAddress":{"aws:ResourceTag/net":"not-an-ip"}}`, []string{"IpAddress", `"aws:ResourceTag/net"`}},
		{`{"StringEquals":{"s3tables:tableName":"t","s3tables:tableBucketName":"b","s3tables:StorageClass":"STANDARD"}}`, nil},
		{`{"StringEquals":{"s3tables:SSEAlgorithm":"AES256","s3tables:KMSKeyArn":"arn:k","s3tables:TableBucketTag/env":"prod"}}`, nil},
		{`{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}`, []string{`"aws:SourceIp"`, "not supported"}},
		{`{"StringEquals":{"aws:resourcetag/env":"prod"}}`, []string{`"aws:resourcetag/env"`, "not supported"}},
		{`{"StringEquals":{"aws:ResourceTag/":"prod"}}`, []string{`"aws:ResourceTag/"`, "not supported"}},
		{`{"Null":{"aws:PrincipalArn":"true"}}`, []string{`"aws:PrincipalArn"`, "not supported"}},
	} {
		err := validatePolicyDocument(withCondition(tc.condition))
		if len(tc.mentions) == 0 {
			if err != nil {
				t.Errorf("%s rejected: %v", tc.condition, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s accepted", tc.condition)
			continue
		}
		for _, want := range tc.mentions {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s: error %q does not mention %s", tc.condition, err, want)
			}
		}
	}

	// The typed decode reads a null Condition as none, so an Allow carrying
	// one would grant unconditionally; a Deny carrying one only vetoes more.
	const (
		allowNull      = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null}`
		allowNullLower = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","condition":null}`
		allowValid     = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}`
		allowNone      = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}`
		denyNull       = `{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null}`
		denyNone       = `{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}`
		// Duplicate keys reach the evaluator in sorted order, not document
		// order: the lower-case null wins here, the object wins in the mirror,
		// and both are refused whichever spelling is null.
		allowNullWins   = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","condition":null,"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}`
		allowObjectWins = `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null,"condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}`
	)
	for _, tc := range []struct {
		statement string
		rejected  bool
	}{
		{`[` + allowNull + `]`, true},
		{`[` + allowNullLower + `]`, true},
		{allowNull, true},
		{`[` + denyNone + `,` + allowNull + `]`, true},
		{`[` + allowNullWins + `]`, true},
		{`[` + denyNull + `]`, false},
		{`[` + denyNull + `,` + allowValid + `]`, false},
		{`[` + allowNone + `]`, false},
		{`[` + allowObjectWins + `]`, true},
	} {
		raw := `{"Version":"2012-10-17","Statement":` + tc.statement + `}`
		err := validatePolicyDocument(raw)
		switch {
		case tc.rejected && err == nil:
			t.Errorf("%s accepted", tc.statement)
		case tc.rejected && !strings.Contains(err.Error(), "Condition must not be null"):
			t.Errorf("%s: unexpected error %v", tc.statement, err)
		case !tc.rejected && err != nil:
			t.Errorf("%s rejected: %v", tc.statement, err)
		}
	}
}

func TestConditionValue(t *testing.T) {
	for _, tc := range []struct {
		operator string
		raw      string
		want     bool
	}{
		{"StringEquals", `"prod"`, true},
		{"StringEquals", `["a","b"]`, true},
		{"StringEquals", `10`, true},
		{"StringEquals", `true`, true},
		{"StringEquals", `null`, false},
		{"StringEquals", `[null]`, false},
		{"StringEquals", `["a",null]`, false},
		{"StringEquals", `[]`, false},
		{"StringEquals", `{}`, false},
		{"StringEquals", `{"a":1}`, false},
		{"StringEquals", `[["a"]]`, false},

		{"NumericLessThan", `10`, true},
		{"NumericLessThan", `"10"`, true},
		{"NumericLessThan", `"low"`, false},
		{"NumericLessThan", `true`, false},
		{"NumericLessThan", `["low","10"]`, false},
		{"NumericLessThan", `"NaN"`, false},
		{"NumericNotEquals", `"nan"`, false},
		{"NumericNotEquals", `"Inf"`, false},
		{"NumericGreaterThan", `"-Infinity"`, false},
		{"ForAnyValue:NumericLessThan", `"low"`, false},
		{"ForAllValues:NumericLessThan", `"10"`, true},

		{"DateLessThan", `"2026-01-01T00:00:00Z"`, true},
		{"DateLessThan", `"2026-01-01"`, false},

		{"Bool", `true`, true},
		{"Bool", `"TRUE"`, true},
		{"Bool", `"yes"`, false},
		{"Null", `"false"`, true},
		{"Null", `"maybe"`, false},

		{"IpAddress", `"10.0.0.0/8"`, true},
		{"IpAddress", `"10.0.0.1"`, true},
		{"IpAddress", `"not-an-ip"`, false},

		{"StringEqualsBogus", `"prod"`, false},
	} {
		var v interface{}
		if err := json.Unmarshal([]byte(tc.raw), &v); err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if got := ConditionValue(tc.operator, v); got != tc.want {
			t.Errorf("ConditionValue(%s, %s) = %v, want %v", tc.operator, tc.raw, got, tc.want)
		}
	}
}

// Every operator the policy engine evaluates must have its own case in
// ConditionValue: one that falls through to the default refuses every value,
// so a newly added operator turns this test red instead of being accepted
// unchecked or refused unnoticed.
func TestConditionValueCoversEveryOperator(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", "policy_engine", "conditions.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var operators []string
	ast.Inspect(file, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "baseConditionEvaluator" {
			return true
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if clause, ok := n.(*ast.CaseClause); ok {
				for _, expr := range clause.List {
					if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
						operator, _ := strconv.Unquote(lit.Value)
						operators = append(operators, operator)
					}
				}
			}
			return true
		})
		return false
	})
	if len(operators) < 20 {
		t.Fatalf("found only %d operators in baseConditionEvaluator", len(operators)) // positive control
	}
	samples := []interface{}{"x", "10", "2026-01-01T00:00:00Z", "true", "10.0.0.0/8"}
	for _, operator := range operators {
		if _, err := policy_engine.GetConditionEvaluator(operator); err != nil {
			t.Fatalf("%s: %v", operator, err)
		}
		readable := false
		for _, sample := range samples {
			readable = readable || ConditionValue(operator, sample)
		}
		if !readable {
			t.Errorf("ConditionValue has no case for %s", operator)
		}
	}
}

// A value the operator cannot read is skipped by its evaluator, so before
// write-time checks such a Deny never matched and the owner kept access. A
// stored one is now unusable: the Deny applies, and the admin can still
// remove the policy.
func TestUnreadableConditionDenyApplies(t *testing.T) {
	arn := "arn:aws:s3tables:us-east-1:acct:bucket/b"
	ctx := &PolicyContext{
		RequestTags:  map[string]string{"priority": "5"},
		ResourceTags: map[string]string{"is_public": "true", "created": "2025-06-01T00:00:00Z"},
	}
	admin := &PolicyContext{IdentityActions: []string{"Admin"}, RequestTags: ctx.RequestTags, ResourceTags: ctx.ResourceTags}
	for _, condition := range []string{
		`{"NumericLessThan":{"aws:RequestTag/priority":"low"}}`,
		`{"DateLessThan":{"aws:ResourceTag/created":"2026-01-01"}}`,
		`{"Bool":{"aws:ResourceTag/is_public":"yes"}}`,
		`{"NumericLessThan":{"aws:RequestTag/priority":"NaN"}}`,
	} {
		var conditions map[string]map[string]interface{}
		if err := json.Unmarshal([]byte(condition), &conditions); err != nil {
			t.Fatalf("%s: %v", condition, err)
		}
		if _, usable := matchesConditions(conditions, ctx); usable {
			t.Errorf("%s reported usable", condition)
		}
		deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":` + condition + `}]}`
		if checkPermission("s3tables:GetTableBucket", "owner", "owner", deny, arn, ctx) {
			t.Errorf("%s: the Deny must apply to the owner", condition)
		}
		if !CheckPermissionWithContext("s3tables:DeleteTableBucketPolicy", "admin", "owner", deny, arn, admin) {
			t.Errorf("%s: the admin must keep the bypass", condition)
		}
	}
}

// A policy stored before conditions were validated may still carry a null
// value. Evaluating it must not panic: the Deny it sits on applies to the
// owner too, while the admin keeps the bypass needed to remove the policy.
func TestNullConditionDoesNotPanic(t *testing.T) {
	arn := "arn:aws:s3tables:us-east-1:acct:bucket/b"
	denyNull := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":null}}}]}`
	ctx := &PolicyContext{ResourceTags: map[string]string{"env": "prod"}}

	if checkPermission("s3tables:GetTableBucket", "owner", "owner", denyNull, arn, ctx) {
		t.Error("a Deny whose condition cannot be evaluated must still deny the owner")
	}
	if CheckTableOrBucketPermission("s3tables:GetTable", "owner", "owner", "", arn+"/table/ns/t", "owner", denyNull, arn, ctx) {
		t.Error("a broken bucket Deny must veto the table request too")
	}
	admin := &PolicyContext{IdentityActions: []string{"Admin"}, ResourceTags: ctx.ResourceTags}
	if !CheckPermissionWithContext("s3tables:DeleteTableBucketPolicy", "admin", "owner", denyNull, arn, admin) {
		t.Error("the admin must keep the bypass to remove a broken policy")
	}
}

// An Allow whose condition cannot be evaluated grants nothing. A [null] value
// used to normalize to "<nil>", so StringNotEquals matched every request.
func TestUnusableConditionAllowNeverGrants(t *testing.T) {
	ctx := &PolicyContext{ResourceTags: map[string]string{"env": "prod"}}
	for _, condition := range []string{
		`{"StringNotEquals":{"aws:ResourceTag/env":[null]}}`,
		`{"StringNotEquals":{"aws:ResourceTag/env":[]}}`,
		`{"StringNotEquals":{"aws:ResourceTag/env":{"a":1}}}`,
		`{"StringEquals":{"aws:ResourceTag/env":null}}`,
		`{"StringEqualsBogus":{"aws:ResourceTag/env":"prod"}}`,
		`{"NumericNotEquals":{"aws:ResourceTag/env":"low"}}`,
		`{"NotIpAddress":{"aws:ResourceTag/env":"not-an-ip"}}`,
		`{"NumericNotEquals":{"aws:ResourceTag/env":"NaN"}}`,
		`{"StringEquals":{}}`,
		`{"StringEquals":null}`,
	} {
		allow := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":` + condition + `}]}`
		if CheckPermissionWithContext("s3tables:GetTable", "reader", "owner", allow, "", ctx) {
			t.Errorf("%s granted a non-owner", condition)
		}
	}
	// A keyless operator is unusable whether or not there is a context.
	if _, usable := matchesConditions(map[string]map[string]interface{}{"StringEquals": {}}, nil); usable {
		t.Error("a keyless operator without a context reported usable")
	}
}

// A stored Allow whose Condition is null never grants; a Deny with one still
// applies.
func TestStoredNullConditionAllowNeverGrants(t *testing.T) {
	ctx := &PolicyContext{}
	for _, tc := range []struct{ action, policy string }{
		{"s3tables:GetTable", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null}]}`},
		{"s3tables:GetTable", `{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null}}`},
		{"s3tables:GetTable", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","condition":null}]}`},
		{"s3tables:GetTable", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"condition":null}]}`},
		{"s3vectors:GetVectorBucket", `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":"*","Condition":null}]}`},
	} {
		if CheckPermissionWithContext(tc.action, "reader", "owner", tc.policy, "", ctx) {
			t.Errorf("%s granted a non-owner", tc.policy)
		}
	}
	allow := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}]}`
	if !CheckPermissionWithContext("s3tables:GetTable", "reader", "owner", allow, "", ctx) {
		t.Error("an Allow without a Condition must still grant")
	}
	deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":null}]}`
	if CheckPermissionWithContext("s3tables:GetTable", "owner", "owner", deny, "", ctx) {
		t.Error("a Deny with a null Condition must apply")
	}
}

// Condition objects under two keys, repeated or respelled, decode into one map
// in which an operator they share keeps only the last one's keys. Such a
// statement is refused, and one stored before that never grants and always
// vetoes.
func TestConditionSplitAcrossSpellings(t *testing.T) {
	for _, split := range []string{
		`"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"condition":{"StringEquals":{"aws:ResourceTag/team":"x"}}`,
		`"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"condition":{}`,
		`"Condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}},"Condition":{"StringEquals":{"aws:ResourceTag/team":"x"}}`,
		`"Condition":null,"Condition":{}`,
	} {
		for _, wrap := range []string{`{"Statement":[%s]}`, `{"Statement":%s}`} {
			allow := fmt.Sprintf(wrap, `{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*",`+split+`}`)
			if err := validatePolicyDocument(allow); err == nil {
				t.Errorf("%s: validatePolicyDocument accepted it", allow)
			}
			if CheckPermissionWithContext("s3tables:GetTable", "reader", "owner", allow, "", &PolicyContext{ResourceTags: map[string]string{"env": "dev", "team": "x"}}) {
				t.Errorf("%s: the Allow granted", allow)
			}
			deny := fmt.Sprintf(wrap, `{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*",`+split+`}`)
			if err := validatePolicyDocument(deny); err == nil {
				t.Errorf("%s: validatePolicyDocument accepted it", deny)
			}
			if CheckPermissionWithContext("s3tables:GetTable", "owner", "owner", deny, "", &PolicyContext{ResourceTags: map[string]string{"env": "dev", "team": "x"}}) {
				t.Errorf("%s: the Deny was skipped", deny)
			}
		}
	}
	single := `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*","condition":{"StringEquals":{"aws:ResourceTag/env":"prod"}}}]}`
	if err := validatePolicyDocument(single); err != nil {
		t.Errorf("one lowercase spelling: %v", err)
	}
	if !CheckPermissionWithContext("s3tables:GetTable", "reader", "owner", single, "", &PolicyContext{ResourceTags: map[string]string{"env": "prod"}}) {
		t.Error("one lowercase spelling no longer grants")
	}
}

// Only the Condition keys are read from the original bytes. Every other field
// keeps the spelling the re-marshaled decode always picked, so a stored Deny
// written twice stays a Deny.
func TestStatementFieldSpellingsDecodeAsBefore(t *testing.T) {
	stmt := `{"effect":"Deny","Effect":"Allow","Principal":"*","Action":"s3tables:*","Resource":"*"}`
	for _, policy := range []string{`{"Statement":[` + stmt + `]}`, `{"Statement":` + stmt + `}`} {
		if CheckPermissionWithContext("s3tables:GetTable", "owner", "owner", policy, "", nil) {
			t.Errorf("%s: the Deny no longer applies", policy)
		}
	}
}

// A Deny whose operator the evaluator does not know applies instead of being
// skipped: the writer meant it to protect the resource.
func TestUnsupportedOperatorDenyApplies(t *testing.T) {
	deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":{"StringEqualsBogus":{"aws:ResourceTag/env":"prod"}}}]}`
	ctx := &PolicyContext{ResourceTags: map[string]string{"env": "dev"}}
	if CheckPermissionWithContext("s3tables:GetTable", "owner", "owner", deny, "", ctx) {
		t.Error("a Deny with an unsupported operator must apply")
	}
}

// One unusable entry makes the whole block unusable whichever entry the map
// yields first, so a non-matching sibling cannot hide it.
func TestUnusableConditionIgnoresMapOrder(t *testing.T) {
	ctx := &PolicyContext{ResourceTags: map[string]string{"env": "prod"}}
	for _, condition := range []string{
		`{"StringEquals":{"aws:ResourceTag/env":"dev","aws:ResourceTag/team":null}}`,
		`{"StringEquals":{"aws:ResourceTag/env":"dev"},"StringNotEquals":{"aws:ResourceTag/team":[null]}}`,
	} {
		deny := `{"Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:GetTable","Resource":"*","Condition":` + condition + `}]}`
		for i := 0; i < 32; i++ {
			if CheckPermissionWithContext("s3tables:GetTable", "owner", "owner", deny, "", ctx) {
				t.Fatalf("%s: the Deny was skipped on attempt %d", condition, i)
			}
		}
	}
}

// An ARN-scoped statement reaches a caller recorded under the ARN's trailing
// name only for denies: normalization drops the account and path, so an allow
// matched that way could grant a same-named caller the statement never
// verified, while a deny that missed would let the caller duck the rule.
func TestArnScopedPrincipalMatchesOnlyForDeny(t *testing.T) {
	arn := "arn:aws:s3vectors:us-east-1:acct:bucket/b"
	denyPolicy := `{"Statement":[{"Effect":"Deny","Principal":"arn:aws:iam::111:user/bob","Action":"s3vectors:DeleteVectors","Resource":"*"}]}`
	allowPolicy := `{"Statement":[{"Effect":"Allow","Principal":"arn:aws:iam::111:user/bob","Action":"s3vectors:DeleteVectors","Resource":"*"}]}`

	if CheckPermissionWithContext("s3vectors:DeleteVectors", "bob", "owner", denyPolicy, arn, &PolicyContext{}) {
		t.Error("an ARN-scoped deny must still reach the normalized caller")
	}
	if CheckPermissionWithContext("s3vectors:DeleteVectors", "bob", "owner", allowPolicy, arn, &PolicyContext{}) {
		t.Error("an ARN-scoped allow must not grant on the normalized name alone")
	}
}

// A service wildcard administers only its own service: s3tables:* satisfies
// the admin and identity grants for table operations but must not reach an
// s3vectors operation.
func TestServiceWildcardScopesToOwnService(t *testing.T) {
	vectorOp := "s3vectors:DeleteVectors"
	vectorArn := "arn:aws:s3vectors:us-east-1:acct:bucket/b"
	tableOnly := &PolicyContext{IdentityActions: []string{"s3tables:*"}}

	if CheckPermissionWithContext(vectorOp, "bob", "owner", "", vectorArn, tableOnly) {
		t.Error("s3tables:* must not grant a vector operation")
	}
	if CheckPermissionWithContext(vectorOp, "admin", "owner", "", vectorArn, tableOnly) {
		t.Error("s3tables:* must not fire the admin bypass for a vector operation")
	}
	if !CheckPermissionWithContext("GetTable", "bob", "owner", "", "arn:aws:s3tables:b", tableOnly) {
		t.Error("s3tables:* keeps administering table operations")
	}

	vectorAdmin := &PolicyContext{IdentityActions: []string{"s3vectors:*"}}
	if !CheckPermissionWithContext(vectorOp, "bob", "owner", "", vectorArn, vectorAdmin) {
		t.Error("s3vectors:* administers vector operations")
	}
	global := &PolicyContext{IdentityActions: []string{"Admin"}}
	if !CheckPermissionWithContext(vectorOp, "admin", "owner", "", vectorArn, global) {
		t.Error("the global admin action keeps the bypass on vector operations")
	}
}

func TestBucketTagConditionKeys(t *testing.T) {
	ctx := &PolicyContext{
		TableBucketTags: map[string]string{"env": "prod"},
		ResourceTags:    map[string]string{"env": "dev"},
	}
	for key, want := range map[string]string{
		"s3tables:TableBucketTag/env":   "prod",
		"s3vectors:VectorBucketTag/env": "prod",
		"aws:ResourceTag/env":           "dev",
	} {
		if got := getConditionContextValues(key, ctx); len(got) != 1 || got[0] != want {
			t.Errorf("%s = %v, want %s", key, got, want)
		}
	}
	if got := getConditionContextValues("s3vectors:VectorBucketTag/team", ctx); got != nil {
		t.Errorf("absent bucket tag = %v, want none", got)
	}
}
