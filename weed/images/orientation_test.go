package images

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestXYZ(t *testing.T) {
	fname := "sample1.jpg"

	dat, _ := ioutil.ReadFile(fname)

	fixed_data := FixJpgOrientation(dat)

	ioutil.WriteFile("fixed1.jpg", fixed_data, 0644)

	os.Remove("fixed1.jpg")

}
