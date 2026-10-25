package policy

import (
	"context"
	"encoding/json"
	"testing"
)

const subjectBoundTrustPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
	"Principal":{"Federated":"https://oidc.example"},"Action":["sts:AssumeRoleWithWebIdentity"],
	"Condition":{"StringEquals":{"oidc:sub":"spiffe://example.org/ns/app/sa/app"}}}]}`

func webIdentityContext(sub string) *EvaluationContext {
	return &EvaluationContext{
		Principal: "web-identity-user",
		Action:    "sts:AssumeRoleWithWebIdentity",
		Resource:  "arn:aws:iam::role/app",
		RequestContext: map[string]interface{}{
			"aws:FederatedProvider": "https://oidc.example",
			"oidc:iss":              "https://oidc.example",
			"oidc:sub":              sub,
		},
	}
}

// A trust policy is deny-by-default. The engine's DefaultEffect decides
// requests no identity policy speaks to; it must not let a principal the trust
// policy does not allow assume the role.
func TestEvaluateTrustPolicyIsImplicitDenyWhateverTheDefaultEffect(t *testing.T) {
	var trust PolicyDocument
	if err := json.Unmarshal([]byte(subjectBoundTrustPolicy), &trust); err != nil {
		t.Fatalf("parse test trust policy: %v", err)
	}
	for _, defaultEffect := range []string{"Allow", "Deny"} {
		t.Run("defaultEffect="+defaultEffect, func(t *testing.T) {
			engine := NewPolicyEngine()
			if err := engine.Initialize(&PolicyEngineConfig{DefaultEffect: defaultEffect, StoreType: "memory"}); err != nil {
				t.Fatalf("initialize policy engine: %v", err)
			}

			res, err := engine.EvaluateTrustPolicy(context.Background(), &trust, webIdentityContext("spiffe://example.org/ns/other/sa/x"))
			if err != nil {
				t.Fatalf("evaluate trust policy: %v", err)
			}
			if res.Effect != EffectDeny {
				t.Errorf("a subject the trust policy does not allow got %s", res.Effect)
			}

			res, err = engine.EvaluateTrustPolicy(context.Background(), &trust, webIdentityContext("spiffe://example.org/ns/app/sa/app"))
			if err != nil {
				t.Fatalf("evaluate trust policy: %v", err)
			}
			if res.Effect != EffectAllow {
				t.Errorf("the subject the trust policy allows got %s", res.Effect)
			}
		})
	}
}

func TestEvaluateTrustPolicyExplicitDenyWins(t *testing.T) {
	var trust PolicyDocument
	doc := `{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Principal":{"Federated":"https://oidc.example"},"Action":["sts:AssumeRoleWithWebIdentity"]},
		{"Effect":"Deny","Principal":{"Federated":"https://oidc.example"},"Action":["sts:AssumeRoleWithWebIdentity"],
		 "Condition":{"StringEquals":{"oidc:sub":"spiffe://example.org/ns/app/sa/app"}}}]}`
	if err := json.Unmarshal([]byte(doc), &trust); err != nil {
		t.Fatalf("parse test trust policy: %v", err)
	}
	engine := NewPolicyEngine()
	if err := engine.Initialize(&PolicyEngineConfig{DefaultEffect: "Allow", StoreType: "memory"}); err != nil {
		t.Fatalf("initialize policy engine: %v", err)
	}
	res, err := engine.EvaluateTrustPolicy(context.Background(), &trust, webIdentityContext("spiffe://example.org/ns/app/sa/app"))
	if err != nil {
		t.Fatalf("evaluate trust policy: %v", err)
	}
	if res.Effect != EffectDeny {
		t.Errorf("explicit Deny did not win: %s", res.Effect)
	}
}
