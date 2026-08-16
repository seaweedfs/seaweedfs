package s3api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestTablePrefixSessionPolicyScopesToTheTable(t *testing.T) {
	raw, err := tablePrefixSessionPolicy("warehouse", "analytics/events")
	if err != nil {
		t.Fatalf("tablePrefixSessionPolicy() error = %v", err)
	}

	var policy struct {
		Statement []struct {
			Effect   string   `json:"Effect"`
			Action   []string `json:"Action"`
			Resource []string `json:"Resource"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatalf("policy is not valid JSON: %v", err)
	}
	if len(policy.Statement) != 2 {
		t.Fatalf("policy has %d statements, want 2", len(policy.Statement))
	}

	objects := policy.Statement[0]
	if objects.Resource[0] != "arn:aws:s3:::warehouse/analytics/events/*" {
		t.Errorf("object resource = %q, want the table prefix only", objects.Resource[0])
	}
	if policy.Statement[1].Resource[0] != "arn:aws:s3:::warehouse" {
		t.Errorf("bucket resource = %q", policy.Statement[1].Resource[0])
	}
	for _, statement := range policy.Statement {
		if statement.Effect != "Allow" {
			t.Errorf("statement effect = %q, want Allow", statement.Effect)
		}
	}
}

func TestTablePrefixSessionPolicyWithoutPrefix(t *testing.T) {
	raw, err := tablePrefixSessionPolicy("warehouse", "")
	if err != nil {
		t.Fatalf("tablePrefixSessionPolicy() error = %v", err)
	}
	if !strings.Contains(raw, `"arn:aws:s3:::warehouse/*"`) {
		t.Errorf("policy = %s, want the whole bucket when there is no prefix", raw)
	}
}

func TestIcebergSessionNameIsBounded(t *testing.T) {
	name := icebergSessionName(strings.Repeat("principal", 10), "warehouse", "ns/table")
	if len(name) > 64 {
		t.Errorf("session name is %d chars, want at most 64", len(name))
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			t.Errorf("session name %q contains %q, which AWS rejects", name, r)
		}
	}
}

// Vending stays off until an operator names a role, so the catalog keeps
// telling clients to use their own credentials.
func TestVendTableCredentialsDisabledWithoutRole(t *testing.T) {
	s3a := &S3ApiServer{}
	credentials, err := s3a.VendTableCredentials(context.Background(), "admin", "warehouse", "ns/t")
	if err != nil {
		t.Fatalf("VendTableCredentials() error = %v, want nil", err)
	}
	if credentials != nil {
		t.Errorf("VendTableCredentials() = %v, want nil while no role is configured", credentials)
	}
}

func TestVendTableCredentialsNeedsSTS(t *testing.T) {
	s3a := &S3ApiServer{}
	s3a.SetIcebergCredentialRole("arn:aws:iam::role/IcebergTableAccess", 3600)

	if _, err := s3a.VendTableCredentials(context.Background(), "admin", "warehouse", "ns/t"); err == nil {
		t.Error("VendTableCredentials() error = nil, want an error when STS is not configured")
	}
}

// Listing is granted on the bucket, so the prefix condition is what keeps a
// vended credential from enumerating other tables.
func TestTablePrefixSessionPolicyRestrictsListing(t *testing.T) {
	raw, err := tablePrefixSessionPolicy("warehouse", "analytics/events")
	if err != nil {
		t.Fatalf("tablePrefixSessionPolicy() error = %v", err)
	}

	var policy struct {
		Statement []struct {
			Action    []string                       `json:"Action"`
			Condition map[string]map[string][]string `json:"Condition"`
		} `json:"Statement"`
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatalf("policy is not valid JSON: %v", err)
	}

	var bucketStatement *struct {
		Action    []string                       `json:"Action"`
		Condition map[string]map[string][]string `json:"Condition"`
	}
	for i := range policy.Statement {
		for _, action := range policy.Statement[i].Action {
			if action == "s3:ListBucket" {
				bucketStatement = &policy.Statement[i]
			}
		}
	}
	if bucketStatement == nil {
		t.Fatal("no statement grants s3:ListBucket")
	}

	prefixes := bucketStatement.Condition["StringLike"]["s3:prefix"]
	want := map[string]bool{"analytics/events": true, "analytics/events/*": true}
	if len(prefixes) != len(want) {
		t.Fatalf("s3:prefix = %v, want %v", prefixes, want)
	}
	for _, prefix := range prefixes {
		if !want[prefix] {
			t.Errorf("s3:prefix contains %q, which is outside the table", prefix)
		}
	}
}

// A location carrying a wildcard would widen the resource pattern to sibling
// tables, so it is refused instead of vended.
func TestTablePrefixSessionPolicyRefusesWildcards(t *testing.T) {
	for _, prefix := range []string{"analytics/*", "analytics/ev?nts", "*"} {
		if _, err := tablePrefixSessionPolicy("warehouse", prefix); err == nil {
			t.Errorf("tablePrefixSessionPolicy(%q) error = nil, want a refusal", prefix)
		}
	}
	if _, err := tablePrefixSessionPolicy("ware*house", "analytics/events"); err == nil {
		t.Error("a wildcard in the bucket name was accepted")
	}
}
