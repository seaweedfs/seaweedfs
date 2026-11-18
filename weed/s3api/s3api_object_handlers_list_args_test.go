package s3api

import (
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
		{query: "continuation-token=", wantErr: s3err.ErrInvalidContinuationToken},
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
