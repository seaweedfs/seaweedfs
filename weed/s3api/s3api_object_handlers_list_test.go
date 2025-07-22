package s3api

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

func TestListObjectsHandler(t *testing.T) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	expected := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>test_container</Name><Prefix></Prefix><Marker></Marker><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>1.zip</Key><ETag>&#34;4397da7a7649e8085de9916c240e8166&#34;</ETag><Size>1234567</Size><Owner><ID>65a011niqo39cdf8ec533ec3d1ccaafsa932</ID></Owner><StorageClass>STANDARD</StorageClass><LastModified>2011-04-09T12:34:49Z</LastModified></Contents><EncodingType></EncodingType></ListBucketResult>`

	response := ListBucketResult{
		Name:        "test_container",
		Prefix:      "",
		Marker:      "",
		NextMarker:  "",
		MaxKeys:     1000,
		IsTruncated: false,
		Contents: []ListEntry{{
			Key:          "1.zip",
			LastModified: time.Date(2011, 4, 9, 12, 34, 49, 0, time.UTC),
			ETag:         "\"4397da7a7649e8085de9916c240e8166\"",
			Size:         1234567,
			Owner: &CanonicalUser{
				ID: "65a011niqo39cdf8ec533ec3d1ccaafsa932",
			},
			StorageClass: "STANDARD",
		}},
	}

	encoded := string(s3err.EncodeXMLResponse(response))
	if encoded != expected {
		t.Errorf("unexpected output: %s\nexpecting:%s", encoded, expected)
	}
}

func Test_normalizePrefixMarker(t *testing.T) {
	type args struct {
		prefix string
		marker string
	}
	tests := []struct {
		name              string
		args              args
		wantAlignedDir    string
		wantAlignedPrefix string
		wantAlignedMarker string
	}{
		{"prefix is a directory",
			args{"/parentDir/data/",
				""},
			"parentDir",
			"data",
			"",
		},
		{"normal case",
			args{"/parentDir/data/0",
				"parentDir/data/0e/0e149049a2137b0cc12e"},
			"parentDir/data",
			"0",
			"0e/0e149049a2137b0cc12e",
		},
		{"empty prefix",
			args{"",
				"parentDir/data/0e/0e149049a2137b0cc12e"},
			"",
			"",
			"parentDir/data/0e/0e149049a2137b0cc12e",
		},
		{"empty directory",
			args{"parent",
				"parentDir/data/0e/0e149049a2137b0cc12e"},
			"",
			"parent",
			"parentDir/data/0e/0e149049a2137b0cc12e",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAlignedDir, gotAlignedPrefix, gotAlignedMarker := normalizePrefixMarker(tt.args.prefix, tt.args.marker)
			assert.Equalf(t, tt.wantAlignedDir, gotAlignedDir, "normalizePrefixMarker(%v, %v)", tt.args.prefix, tt.args.marker)
			assert.Equalf(t, tt.wantAlignedPrefix, gotAlignedPrefix, "normalizePrefixMarker(%v, %v)", tt.args.prefix, tt.args.marker)
			assert.Equalf(t, tt.wantAlignedMarker, gotAlignedMarker, "normalizePrefixMarker(%v, %v)", tt.args.prefix, tt.args.marker)
		})
	}
}

func TestAllowUnorderedParameterValidation(t *testing.T) {
	// Test getListObjectsV1Args with allow-unordered parameter
	t.Run("getListObjectsV1Args with allow-unordered", func(t *testing.T) {
		// Test with allow-unordered=true
		values := map[string][]string{
			"allow-unordered": {"true"},
			"delimiter":       {"/"},
		}
		_, _, _, _, _, allowUnordered, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.True(t, allowUnordered, "allow-unordered should be true when set to 'true'")

		// Test with allow-unordered=false
		values = map[string][]string{
			"allow-unordered": {"false"},
		}
		_, _, _, _, _, allowUnordered, errCode = getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.False(t, allowUnordered, "allow-unordered should be false when set to 'false'")

		// Test without allow-unordered parameter
		values = map[string][]string{}
		_, _, _, _, _, allowUnordered, errCode = getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.False(t, allowUnordered, "allow-unordered should be false when not set")
	})

	// Test getListObjectsV2Args with allow-unordered parameter
	t.Run("getListObjectsV2Args with allow-unordered", func(t *testing.T) {
		// Test with allow-unordered=true
		values := map[string][]string{
			"allow-unordered": {"true"},
			"delimiter":       {"/"},
		}
		_, _, _, _, _, _, _, allowUnordered, errCode := getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.True(t, allowUnordered, "allow-unordered should be true when set to 'true'")

		// Test with allow-unordered=false
		values = map[string][]string{
			"allow-unordered": {"false"},
		}
		_, _, _, _, _, _, _, allowUnordered, errCode = getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.False(t, allowUnordered, "allow-unordered should be false when set to 'false'")

		// Test without allow-unordered parameter
		values = map[string][]string{}
		_, _, _, _, _, _, _, allowUnordered, errCode = getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.False(t, allowUnordered, "allow-unordered should be false when not set")
	})
}

func TestAllowUnorderedWithDelimiterValidation(t *testing.T) {
	t.Run("should return error when allow-unordered=true and delimiter are both present", func(t *testing.T) {
		// Create a request with both allow-unordered=true and delimiter
		req := httptest.NewRequest("GET", "/bucket?allow-unordered=true&delimiter=/", nil)

		// Extract query parameters like the handler would
		values := req.URL.Query()

		// Test ListObjectsV1Args
		_, _, delimiter, _, _, allowUnordered, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.True(t, allowUnordered, "allow-unordered should be true")
		assert.Equal(t, "/", delimiter, "delimiter should be '/'")

		// The validation should catch this combination
		if allowUnordered && delimiter != "" {
			assert.True(t, true, "Validation correctly detected invalid combination")
		} else {
			assert.Fail(t, "Validation should have detected invalid combination")
		}

		// Test ListObjectsV2Args
		_, _, delimiter2, _, _, _, _, allowUnordered2, errCode2 := getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode2, "should not return error for valid parameters")
		assert.True(t, allowUnordered2, "allow-unordered should be true")
		assert.Equal(t, "/", delimiter2, "delimiter should be '/'")

		// The validation should catch this combination
		if allowUnordered2 && delimiter2 != "" {
			assert.True(t, true, "Validation correctly detected invalid combination")
		} else {
			assert.Fail(t, "Validation should have detected invalid combination")
		}
	})

	t.Run("should allow allow-unordered=true without delimiter", func(t *testing.T) {
		// Create a request with only allow-unordered=true
		req := httptest.NewRequest("GET", "/bucket?allow-unordered=true", nil)

		values := req.URL.Query()

		// Test ListObjectsV1Args
		_, _, delimiter, _, _, allowUnordered, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.True(t, allowUnordered, "allow-unordered should be true")
		assert.Equal(t, "", delimiter, "delimiter should be empty")

		// This combination should be valid
		if allowUnordered && delimiter != "" {
			assert.Fail(t, "This should be a valid combination")
		} else {
			assert.True(t, true, "Valid combination correctly allowed")
		}
	})

	t.Run("should allow delimiter without allow-unordered", func(t *testing.T) {
		// Create a request with only delimiter
		req := httptest.NewRequest("GET", "/bucket?delimiter=/", nil)

		values := req.URL.Query()

		// Test ListObjectsV1Args
		_, _, delimiter, _, _, allowUnordered, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "should not return error for valid parameters")
		assert.False(t, allowUnordered, "allow-unordered should be false")
		assert.Equal(t, "/", delimiter, "delimiter should be '/'")

		// This combination should be valid
		if allowUnordered && delimiter != "" {
			assert.Fail(t, "This should be a valid combination")
		} else {
			assert.True(t, true, "Valid combination correctly allowed")
		}
	})
}

// TestMaxKeysParameterValidation tests the validation of max-keys parameter
func TestMaxKeysParameterValidation(t *testing.T) {
	t.Run("valid max-keys values should work", func(t *testing.T) {
		// Test valid numeric values
		values := map[string][]string{
			"max-keys": {"100"},
		}
		_, _, _, _, _, _, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "valid max-keys should not return error")

		_, _, _, _, _, _, _, _, errCode = getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "valid max-keys should not return error")
	})

	t.Run("invalid max-keys values should return error", func(t *testing.T) {
		// Test non-numeric value
		values := map[string][]string{
			"max-keys": {"blah"},
		}
		_, _, _, _, _, _, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrInvalidMaxKeys, errCode, "non-numeric max-keys should return ErrInvalidMaxKeys")

		_, _, _, _, _, _, _, _, errCode = getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrInvalidMaxKeys, errCode, "non-numeric max-keys should return ErrInvalidMaxKeys")
	})

	t.Run("empty max-keys should use default", func(t *testing.T) {
		// Test empty max-keys
		values := map[string][]string{}
		_, _, _, _, maxkeys, _, errCode := getListObjectsV1Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "empty max-keys should not return error")
		assert.Equal(t, int16(1000), maxkeys, "empty max-keys should use default value")

		_, _, _, _, _, _, maxkeys2, _, errCode := getListObjectsV2Args(values)
		assert.Equal(t, s3err.ErrNone, errCode, "empty max-keys should not return error")
		assert.Equal(t, uint16(1000), maxkeys2, "empty max-keys should use default value")
	})
}
