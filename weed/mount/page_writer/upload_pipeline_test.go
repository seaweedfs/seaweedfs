package page_writer

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"testing"
)

func TestUploadPipeline(t *testing.T) {

	uploadPipeline := NewUploadPipeline(nil, 2*1024*1024, nil, 16, "")

	writeRange(uploadPipeline, 0, 131072)
	writeRange(uploadPipeline, 131072, 262144)
	writeRange(uploadPipeline, 262144, 1025536)

	confirmRange(t, uploadPipeline, 0, 1025536)

	writeRange(uploadPipeline, 1025536, 1296896)

	confirmRange(t, uploadPipeline, 1025536, 1296896)

	writeRange(uploadPipeline, 1296896, 2162688)

	confirmRange(t, uploadPipeline, 1296896, 2162688)

	confirmRange(t, uploadPipeline, 1296896, 2162688)
}

// startOff and stopOff must be divided by 4
func writeRange(uploadPipeline *UploadPipeline, startOff, stopOff int64) {
	p := make([]byte, 4)
	for i := startOff / 4; i < stopOff/4; i += 4 {
		util.Uint32toBytes(p, uint32(i))
		uploadPipeline.SaveDataAt(p, i, false, 0)
	}
}

func confirmRange(t *testing.T, uploadPipeline *UploadPipeline, startOff, stopOff int64) {
	p := make([]byte, 4)
	for i := startOff; i < stopOff/4; i += 4 {
		uploadPipeline.MaybeReadDataAt(p, i, 0)
		x := util.BytesToUint32(p)
		if x != uint32(i) {
			t.Errorf("expecting %d found %d at offset [%d,%d)", i, x, i, i+4)
		}
	}
}
