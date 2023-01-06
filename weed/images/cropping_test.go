package images

import (
	"bytes"
	"os"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCropping(t *testing.T) {
	fname := "sample1.jpg"

	dat, _ := os.ReadFile(fname)

	cropped, _ := Cropped(".jpg", bytes.NewReader(dat), 1072, 932, 1751, 1062)
	buf := new(bytes.Buffer)
	buf.ReadFrom(cropped)

	util.WriteFile("cropped1.jpg", buf.Bytes(), 0644)

}
