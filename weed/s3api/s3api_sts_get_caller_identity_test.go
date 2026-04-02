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

func TestGetCallerIdentityResponse_XMLUnmarshal(t *testing.T) {
	xmlData := `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <GetCallerIdentityResult>
    <Arn>arn:aws:iam::000000000000:user/bob</Arn>
    <UserId>AKIAI44QH8DHBEXAMPLE</UserId>
    <Account>000000000000</Account>
  </GetCallerIdentityResult>
  <ResponseMetadata>
    <RequestId>req-123</RequestId>
  </ResponseMetadata>
</GetCallerIdentityResponse>`

	var response GetCallerIdentityResponse
	err := xml.Unmarshal([]byte(xmlData), &response)
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::000000000000:user/bob", response.Result.Arn)
	assert.Equal(t, "AKIAI44QH8DHBEXAMPLE", response.Result.UserId)
	assert.Equal(t, "000000000000", response.Result.Account)
	assert.Equal(t, "req-123", response.ResponseMetadata.RequestId)
}

func TestGetCallerIdentity_HandleSTSRequest_Routing(t *testing.T) {
	// Verify the action constant is correct
	assert.Equal(t, "GetCallerIdentity", actionGetCallerIdentity)
}
