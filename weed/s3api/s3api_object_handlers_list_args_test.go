package s3api

import (
	"encoding/xml"
	"net/url"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetListObjectsV2ArgsContinuationToken(t *testing.T) {
	cases := []struct {
		query   string
		want    string
		wantErr s3err.ErrorCode
	}{
		{query: "continuation-token=&start-after=a", want: ""},
		{query: "continuation-token=photos%2F2006%2FJanuary%2Fsample.jpg", want: "photos/2006/January/sample.jpg"},
		{query: "continuation-token=" + encodeListContinuationToken("ctrl/\x01key"), want: "ctrl/\x01key"},
		{query: "continuation-token=base64%3Aplain", want: "base64:plain"},
		{query: "start-after=a"},
	}
	for _, tc := range cases {
		values, err := url.ParseQuery(tc.query)
		require.NoError(t, err)
		_, _, _, token, _, _, _, _, errCode := getListObjectsV2Args(values)
		assert.Equal(t, tc.wantErr, errCode, tc.query)
		if tc.wantErr == s3err.ErrNone {
			assert.Equal(t, values.Has("continuation-token"), token.set, tc.query)
			assert.Equal(t, tc.want, decodeListContinuationToken(token.string), tc.query)
		}
	}
}

func TestListArgsEncodingType(t *testing.T) {
	cases := []struct {
		query   string
		wantURL bool
		wantErr s3err.ErrorCode
	}{
		{query: ""},
		{query: "encoding-type=url", wantURL: true},
		{query: "encoding-type=URL", wantErr: s3err.ErrInvalidEncodingMethod},
		{query: "encoding-type=bogus", wantErr: s3err.ErrInvalidEncodingMethod},
	}
	for _, tc := range cases {
		values, err := url.ParseQuery(tc.query)
		require.NoError(t, err)

		_, _, _, _, urlV2, _, _, _, errV2 := getListObjectsV2Args(values)
		assert.Equal(t, tc.wantErr, errV2, "v2 %q", tc.query)
		assert.Equal(t, tc.wantURL, urlV2, "v2 %q", tc.query)

		_, _, _, urlV1, _, _, errV1 := getListObjectsV1Args(values)
		assert.Equal(t, tc.wantErr, errV1, "v1 %q", tc.query)
		assert.Equal(t, tc.wantURL, urlV1, "v1 %q", tc.query)

		_, _, _, _, _, _, errUploads := getBucketMultipartResources(values)
		assert.Equal(t, tc.wantErr, errUploads, "uploads %q", tc.query)

		_, _, _, _, errParts := getObjectResources(values)
		assert.Equal(t, tc.wantErr, errParts, "parts %q", tc.query)
	}
}

func TestListObjectVersionsEncodeURL(t *testing.T) {
	result := &S3ListObjectVersionsResult{
		Name:          "b",
		Prefix:        "dir a/",
		Delimiter:     "/",
		KeyMarker:     encodeListContinuationToken("dir a/résumé+1"),
		NextKeyMarker: encodeListContinuationToken("dir a/100%"),
		Entries: []VersionListEntry{
			{Version: &VersionEntry{Key: "dir a/résumé+1"}},
			{DeleteMarker: &DeleteMarkerEntry{Key: "dir a/100%"}},
			{Prefix: &PrefixEntry{Prefix: "dir a/sub dir/"}},
		},
	}
	result.encodeURL()

	out, err := xml.Marshal(result)
	require.NoError(t, err)
	body := string(out)
	for _, want := range []string{
		"<Prefix>dir%20a/</Prefix>",
		"<Delimiter>/</Delimiter>",
		"<KeyMarker>dir%20a/r%C3%A9sum%C3%A9%2B1</KeyMarker>",
		"<NextKeyMarker>dir%20a/100%25</NextKeyMarker>",
		"<Version><Key>dir%20a/r%C3%A9sum%C3%A9%2B1</Key>",
		"<DeleteMarker><Key>dir%20a/100%25</Key>",
		"<CommonPrefixes><Prefix>dir%20a/sub%20dir/</Prefix></CommonPrefixes>",
		"<EncodingType>url</EncodingType>",
	} {
		assert.Contains(t, body, want)
	}
	decoded, err := url.PathUnescape("dir%20a/r%C3%A9sum%C3%A9%2B1")
	require.NoError(t, err)
	assert.Equal(t, "dir a/résumé+1", decoded)
}

func TestListObjectsV1EncodesDelimiterNotPrefix(t *testing.T) {
	response := ListBucketResult{Name: "b", Prefix: "dir a/\n+", Delimiter: " ", MaxKeys: 1000, EncodingType: "url"}
	body := string(s3err.EncodeXMLResponse(toListBucketResultV1(response)))
	assert.Contains(t, body, "<Prefix>dir a/&#xA;+</Prefix>")
	assert.Contains(t, body, "<Delimiter>%20</Delimiter>")

	response.Prefix = "dir a/\x01"
	body = string(s3err.EncodeXMLResponse(toListBucketResultV1(response)))
	assert.Contains(t, body, "<Prefix>dir%20a/%01</Prefix>")

	response.EncodingType = ""
	body = string(s3err.EncodeXMLResponse(toListBucketResultV1(response)))
	assert.Contains(t, body, "<Delimiter> </Delimiter>")
}
