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
	resp.SetRequestId()

	output, err := xml.Marshal(resp)
	require.NoError(t, err)

	xmlString := string(output)
	listUsersResultIndex := strings.Index(xmlString, "<ListUsersResult>")
	responseMetadataIndex := strings.Index(xmlString, "<ResponseMetadata>")

	assert.NotEqual(t, -1, listUsersResultIndex, "ListUsersResult should be present")
	assert.NotEqual(t, -1, responseMetadataIndex, "ResponseMetadata should be present")
	assert.Less(t, listUsersResultIndex, responseMetadataIndex, "ListUsersResult should appear before ResponseMetadata")
}
