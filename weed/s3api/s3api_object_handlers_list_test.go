package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
			Owner: CanonicalUser{
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
