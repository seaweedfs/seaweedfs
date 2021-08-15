package images

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestResizing(t *testing.T) {
	fname := "sample2.webp"

	dat, _ := ioutil.ReadFile(fname)

	resized, _, _ := Resized(".webp", bytes.NewReader(dat), 100, 30, "")
	buf := new(bytes.Buffer)
	buf.ReadFrom(resized)

	ioutil.WriteFile("resized1.png", buf.Bytes(), 0644)

	os.Remove("resized1.png")

}
