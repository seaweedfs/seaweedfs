package iam

import (
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponseXMLUsesTopLevelRequestId(t *testing.T) {
	errCode := "NoSuchEntity"
	errMsg := "the requested IAM entity does not exist"

	resp := ErrorResponse{
		RequestId: "request-123",
	}
	resp.Error.Type = "Sender"
	resp.Error.Code = &errCode
	resp.Error.Message = &errMsg

	output, err := xml.Marshal(resp)
	require.NoError(t, err)

	xmlString := string(output)
	errorIndex := strings.Index(xmlString, "<Error>")
	requestIDIndex := strings.Index(xmlString, "<RequestId>request-123</RequestId>")

	assert.NotEqual(t, -1, errorIndex, "Error should be present")
	assert.NotEqual(t, -1, requestIDIndex, "RequestId should be present")
	assert.NotContains(t, xmlString, "<ResponseMetadata>")
	assert.Less(t, errorIndex, requestIDIndex, "RequestId should appear after Error at the root level")
}
