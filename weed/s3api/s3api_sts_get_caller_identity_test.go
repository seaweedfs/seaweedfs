package s3api

import (
	"encoding/xml"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCallerIdentityResponse_XMLMarshal(t *testing.T) {
	response := &GetCallerIdentityResponse{
		Result: GetCallerIdentityResult{
			Arn:     fmt.Sprintf("arn:aws:iam::%s:user/alice", defaultAccountID),
			UserId:  "AKIAIOSFODNN7EXAMPLE",
			Account: defaultAccountID,
		},
	}
	response.ResponseMetadata.RequestId = "test-request-id"

	data, err := xml.MarshalIndent(response, "", "  ")
	require.NoError(t, err)

	xmlStr := string(data)
	assert.Contains(t, xmlStr, "GetCallerIdentityResponse")
	assert.Contains(t, xmlStr, "GetCallerIdentityResult")
	assert.Contains(t, xmlStr, "<Arn>arn:aws:iam::000000000000:user/alice</Arn>")
	assert.Contains(t, xmlStr, "<UserId>AKIAIOSFODNN7EXAMPLE</UserId>")
	assert.Contains(t, xmlStr, "<Account>000000000000</Account>")
	assert.Contains(t, xmlStr, "<RequestId>test-request-id</RequestId>")
	assert.Contains(t, xmlStr, "https://sts.amazonaws.com/doc/2011-06-15/")
}
