package util

import (
	"testing"

	"golang.org/x/tools/godoc/util"
)

func TestIsGzippable(t *testing.T) {
	buf := make([]byte, 1024)

	isText := util.IsText(buf)

	if isText {
		t.Error("buf with zeros are not text")
	}

	compressed, _ := GzipData(buf)

	t.Logf("compressed size %d\n", len(compressed))
}
