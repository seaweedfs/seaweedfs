package images

import (
	"os"
	"testing"
)

func TestXYZ(t *testing.T) {
	fname := "sample1.jpg"

	dat, _ := os.ReadFile(fname)

	fixed_data := FixJpgOrientation(dat)

	os.WriteFile("fixed1.jpg", fixed_data, 0644)

	os.Remove("fixed1.jpg")

}
