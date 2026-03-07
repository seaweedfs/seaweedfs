package iam

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListUsersResponseXMLOrdering(t *testing.T) {
	resp := ListUsersResponse{}
	resp.SetRequestId("test-req-id")

	output, err := xml.Marshal(resp)
	require.NoError(t, err)

	xmlString := string(output)
	listUsersResultIndex := strings.Index(xmlString, "<ListUsersResult>")
	responseMetadataIndex := strings.Index(xmlString, "<ResponseMetadata>")

	assert.NotEqual(t, -1, listUsersResultIndex, "ListUsersResult should be present")
	assert.NotEqual(t, -1, responseMetadataIndex, "ResponseMetadata should be present")
	assert.Less(t, listUsersResultIndex, responseMetadataIndex,
		"ListUsersResult should appear before ResponseMetadata")
}

func TestErrorResponseXMLUsesTopLevelRequestId(t *testing.T) {
	errCode := "NoSuchEntity"
	errMsg := "the requested IAM entity does not exist"

	resp := ErrorResponse{}
	resp.Error.Type = "Sender"
	resp.Error.Code = &errCode
	resp.Error.Message = &errMsg
	resp.SetRequestId("request-123")

	output, err := xml.Marshal(resp)
	require.NoError(t, err)

	xmlString := string(output)
	errorIndex := strings.Index(xmlString, "<Error>")
	requestIDIndex := strings.Index(xmlString, "<RequestId>request-123</RequestId>")

	assert.NotEqual(t, -1, errorIndex, "Error should be present")
	assert.NotEqual(t, -1, requestIDIndex, "RequestId should be present")
	assert.NotContains(t, xmlString, "<ResponseMetadata>",
		"ErrorResponse should use bare RequestId, not ResponseMetadata wrapper")
	assert.Less(t, errorIndex, requestIDIndex,
		"RequestId should appear after Error at the root level")
}
