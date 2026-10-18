package s3tables

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
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
