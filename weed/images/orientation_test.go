package images

import (
	"io/ioutil"
	"testing"
)

func TestXYZ(t *testing.T) {
	fname := "sample1.jpg"

	dat, _ := ioutil.ReadFile(fname)

	fixed_data := FixJpgOrientation(dat)

	ioutil.WriteFile("fixed1.jpg", fixed_data, 0644)

}
