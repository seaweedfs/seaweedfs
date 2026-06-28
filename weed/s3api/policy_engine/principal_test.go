package policy_engine

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
)

// TestPolicyPrincipalUnmarshal covers every Principal shape AWS documents, plus
// the bare string/array forms SeaweedFS accepts for backward compatibility.
func TestPolicyPrincipalUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"bare wildcard", `"*"`, []string{"*"}},
		{"bare arn", `"arn:aws:iam::123456789012:user/alice"`, []string{"arn:aws:iam::123456789012:user/alice"}},
		{"bare array", `["arn:aws:iam::123:root","arn:aws:iam::456:root"]`, []string{"arn:aws:iam::123:root", "arn:aws:iam::456:root"}},
		{"AWS single", `{"AWS":"arn:aws:iam::123456789012:root"}`, []string{"arn:aws:iam::123456789012:root"}},
		{"AWS array", `{"AWS":["arn:aws:iam::123:root","999999999999"]}`, []string{"arn:aws:iam::123:root", "999999999999"}},
		{"AWS wildcard", `{"AWS":"*"}`, []string{"*"}},
		{"AWS array wildcard (public read)", `{"AWS":["*"]}`, []string{"*"}},
		{"service", `{"Service":"s3.amazonaws.com"}`, []string{"s3.amazonaws.com"}},
		{"canonical user", `{"CanonicalUser":"79a59df900b949e55d96a1e698fbace"}`, []string{"79a59df900b949e55d96a1e698fbace"}},
		// Object keys are flattened in sorted-key order: AWS < CanonicalUser.
		{"mixed keys", `{"CanonicalUser":"79a59","AWS":"arn:aws:iam::123:root"}`, []string{"arn:aws:iam::123:root", "79a59"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var p PolicyPrincipal
			if err := json.Unmarshal([]byte(c.in), &p); err != nil {
				t.Fatalf("unmarshal %s: %v", c.in, err)
			}
			if got := p.Strings(); !slices.Equal(got, c.want) {
				t.Errorf("Strings() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPolicyPrincipalUnmarshalInvalid(t *testing.T) {
	for _, in := range []string{`123`, `{}`, `{"AWS":123}`} {
		var p PolicyPrincipal
		if err := json.Unmarshal([]byte(in), &p); err == nil {
			t.Errorf("expected error unmarshaling %s, got values %v", in, p.Strings())
		}
	}
}

// TestPolicyPrincipalRoundTrip guards idempotency for IaC tools (Terraform,
// Ansible): GetBucketPolicy must return the same shape PutBucketPolicy received,
// otherwise those tools see perpetual drift (cf. NotResource normalization bug).
func TestPolicyPrincipalRoundTrip(t *testing.T) {
	inputs := []string{
		`"*"`,
		`{"AWS":"arn:aws:iam::123456789012:root"}`,
		`{"AWS":["arn:aws:iam::123456789012:root","arn:aws:iam::555555555555:root"]}`,
		`{"AWS":["*"]}`,
		`["arn:aws:iam::123:root","arn:aws:iam::456:root"]`,
	}
	for _, in := range inputs {
		var p PolicyPrincipal
		if err := json.Unmarshal([]byte(in), &p); err != nil {
			t.Fatalf("unmarshal %s: %v", in, err)
		}
		out, err := json.Marshal(&p)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !jsonSemanticEqual(t, string(out), in) {
			t.Errorf("round-trip mismatch: got %s, want %s", out, in)
		}
	}
}

// TestBucketPolicyAWSObjectPrincipalEndToEnd parses, compiles and evaluates a
// public-read policy written with the AWS object form {"AWS":["*"]} — the
// AWS-documented shape that previously failed to parse.
func TestBucketPolicyAWSObjectPrincipalEndToEnd(t *testing.T) {
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowPublicRead",
			"Effect": "Allow",
			"Principal": {"AWS": ["*"]},
			"Action": ["s3:GetObject"],
			"Resource": ["arn:aws:s3:::my-bucket/*"]
		}]
	}`
	doc, err := ParsePolicy(policyJSON)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	compiled, err := CompilePolicy(doc)
	if err != nil {
		t.Fatalf("CompilePolicy: %v", err)
	}

	for _, principal := range []string{"arn:aws:iam::123456789012:user/bob", "*"} {
		allow, effect := compiled.EvaluatePolicy(&PolicyEvaluationArgs{
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::my-bucket/object.txt",
			Principal: principal,
		})
		if !allow || effect != PolicyEffectAllow {
			t.Errorf("principal %q: allow=%v effect=%v, want allow", principal, allow, effect)
		}
	}
}

// TestBucketPolicySpecificAWSPrincipal verifies a named-principal object form
// only matches the named principal.
func TestBucketPolicySpecificAWSPrincipal(t *testing.T) {
	policyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::123456789012:user/alice"},` +
		`"Action":"s3:*","Resource":"arn:aws:s3:::b/*"}]}`
	doc, err := ParsePolicy(policyJSON)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	compiled, err := CompilePolicy(doc)
	if err != nil {
		t.Fatalf("CompilePolicy: %v", err)
	}
	args := func(p string) *PolicyEvaluationArgs {
		return &PolicyEvaluationArgs{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x", Principal: p}
	}
	if allow, _ := compiled.EvaluatePolicy(args("arn:aws:iam::123456789012:user/alice")); !allow {
		t.Errorf("alice should be allowed")
	}
	if allow, _ := compiled.EvaluatePolicy(args("arn:aws:iam::123456789012:user/bob")); allow {
		t.Errorf("bob should NOT be allowed")
	}
}

// TestBucketPolicyNotPrincipalDeny exercises the common "deny everyone except X"
// pattern through the real bucket-policy evaluator (PolicyEngine). A Deny with
// NotPrincipal applies to every principal NOT named, so the named principal is
// spared and everyone else is denied.
func TestBucketPolicyNotPrincipalDeny(t *testing.T) {
	engine := NewPolicyEngine()
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{"Sid":"AllowAll","Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*"},
			{"Sid":"DenyExceptAlice","Effect":"Deny","NotPrincipal":{"AWS":"arn:aws:iam::123456789012:user/alice"},"Action":"s3:*","Resource":"arn:aws:s3:::b/*"}
		]
	}`
	if err := engine.SetBucketPolicy("b", policyJSON); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	eval := func(p string) PolicyEvaluationResult {
		return engine.EvaluatePolicy("b", &PolicyEvaluationArgs{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x", Principal: p})
	}
	// alice is excluded from the NotPrincipal deny, so the Allow wins.
	if got := eval("arn:aws:iam::123456789012:user/alice"); got != PolicyResultAllow {
		t.Errorf("alice: got %v, want Allow", got)
	}
	// bob is not named in NotPrincipal, so the Deny applies.
	if got := eval("arn:aws:iam::123456789012:user/bob"); got != PolicyResultDeny {
		t.Errorf("bob: got %v, want Deny", got)
	}
}

// TestBucketPolicyNotPrincipalCompiledPath covers the matcher-based evaluator
// (CompiledPolicy). An Allow-all paired with a Deny/NotPrincipal makes the
// "excluded vs denied" difference observable (a lone Deny would yield implicit
// deny for both).
func TestBucketPolicyNotPrincipalCompiledPath(t *testing.T) {
	policyJSON := `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*"},
		{"Effect":"Deny","NotPrincipal":{"AWS":["arn:aws:iam::123456789012:user/alice"]},"Action":"s3:*","Resource":"arn:aws:s3:::b/*"}
	]}`
	doc, err := ParsePolicy(policyJSON)
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	compiled, err := CompilePolicy(doc)
	if err != nil {
		t.Fatalf("CompilePolicy: %v", err)
	}
	args := func(p string) *PolicyEvaluationArgs {
		return &PolicyEvaluationArgs{Action: "s3:GetObject", Resource: "arn:aws:s3:::b/x", Principal: p}
	}
	// alice is named in NotPrincipal -> deny excluded -> allowed.
	if allow, _ := compiled.EvaluatePolicy(args("arn:aws:iam::123456789012:user/alice")); !allow {
		t.Errorf("alice should be allowed (excluded from NotPrincipal deny)")
	}
	// bob is not named -> deny applies.
	if allow, effect := compiled.EvaluatePolicy(args("arn:aws:iam::123456789012:user/bob")); allow || effect != PolicyEffectDeny {
		t.Errorf("bob should be denied, got allow=%v effect=%v", allow, effect)
	}
}

// TestPolicyBothPrincipalAndNotPrincipalRejected verifies AWS's rule that a
// single statement may not contain both Principal and NotPrincipal.
func TestPolicyBothPrincipalAndNotPrincipalRejected(t *testing.T) {
	policyJSON := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",` +
		`"Principal":"*","NotPrincipal":{"AWS":"arn:aws:iam::1:user/a"},` +
		`"Action":"s3:*","Resource":"arn:aws:s3:::b/*"}]}`
	if _, err := ParsePolicy(policyJSON); err == nil {
		t.Errorf("expected error when both Principal and NotPrincipal are set")
	}
}

func jsonSemanticEqual(t *testing.T, a, b string) bool {
	t.Helper()
	var av, bv interface{}
	if err := json.Unmarshal([]byte(a), &av); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal([]byte(b), &bv); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(av, bv)
}
