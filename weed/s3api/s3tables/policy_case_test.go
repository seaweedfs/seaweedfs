package s3tables

import "testing"

// Service prefixes are case-insensitive in IAM, so a vector rule in any
// letter case is kept out of the shared policy slot.
func TestValidatePolicyDocumentRejectsVectorServiceInAnyCase(t *testing.T) {
	for _, doc := range []string{
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"S3Vectors:GetIndex","Resource":"*"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["s3tables:*","S3VECTORS:*"],"Resource":"*"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3tables:*","Resource":"arn:aws:S3Vectors:r:a:bucket/b"}]}`,
	} {
		if err := validatePolicyDocument(doc); err == nil {
			t.Errorf("vector rule accepted into the shared slot: %s", doc)
		}
	}
}
