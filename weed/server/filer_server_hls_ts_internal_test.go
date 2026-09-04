package weed_server

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestHlsTsMetadataKeyIsInternal(t *testing.T) {
	if !s3_constants.IsSeaweedFSInternalHeader(hlsTsMetadataKey) {
		t.Fatalf("HLS metadata key %q would be exposed as a normal filer response header", hlsTsMetadataKey)
	}
}
