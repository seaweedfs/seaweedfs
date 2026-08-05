package s3api

import (
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/require"
)

// The advanced IAM file doubles as the S3 identity config when only
// -s3.iam.config is given, so its "document" policies must survive the parse.
func TestLoadAdvancedIAMConfigPolicyDocument(t *testing.T) {
	config := []byte(`{
  "sts": {"issuer": "seaweedfs-sts"},
  "policies": [
    {
      "name": "ClientPolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["s3:*"], "Resource": ["*"]}]
      }
    }
  ]
}`)

	parsed := &iam_pb.S3ApiConfiguration{}
	require.NoError(t, filer.ParseS3ConfigurationFromBytes(normalizeAdvancedIAMPolicies(config), parsed))
	require.Len(t, parsed.Policies, 1)
	require.Equal(t, "ClientPolicy", parsed.Policies[0].Name)

	var document map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(parsed.Policies[0].Content), &document))
	require.Equal(t, "2012-10-17", document["Version"])
}

func TestNormalizeAdvancedIAMPolicies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  string
		content string
	}{
		{
			name:    "document object",
			config:  `{"policies":[{"name":"p","document":{"Version":"2012-10-17"}}]}`,
			content: `{"Version":"2012-10-17"}`,
		},
		{
			name:    "document already encoded as a string",
			config:  `{"policies":[{"name":"p","document":"{\"Version\":\"2012-10-17\"}"}]}`,
			content: `{"Version":"2012-10-17"}`,
		},
		{
			name:    "content wins over document",
			config:  `{"policies":[{"name":"p","content":"{\"Version\":\"keep\"}","document":{"Version":"drop"}}]}`,
			content: `{"Version":"keep"}`,
		},
		{
			name:    "content only",
			config:  `{"policies":[{"name":"p","content":"{\"Version\":\"keep\"}"}]}`,
			content: `{"Version":"keep"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed := &iam_pb.S3ApiConfiguration{}
			require.NoError(t, filer.ParseS3ConfigurationFromBytes(normalizeAdvancedIAMPolicies([]byte(tc.config)), parsed))
			require.Len(t, parsed.Policies, 1)
			require.JSONEq(t, tc.content, parsed.Policies[0].Content)
		})
	}
}

// Anything that is not a policy list is handed to the proto parser untouched.
func TestNormalizeAdvancedIAMPoliciesLeavesOtherConfigsAlone(t *testing.T) {
	for _, config := range []string{
		`not json`,
		`{"identities":[{"name":"admin"}]}`,
		`{"policies":{"p":{"document":{}}}}`,
	} {
		require.Equal(t, config, string(normalizeAdvancedIAMPolicies([]byte(config))))
	}
}
