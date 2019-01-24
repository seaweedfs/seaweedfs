package util

import (
	"github.com/dustin/go-humanize"
	"testing"
)

func TestParseVolumeSizeLimit(t *testing.T) {
	volumeSizeLimit := ParseVolumeSizeLimit(10, "")
	if volumeSizeLimit != 10*humanize.MiByte {
		t.Fail()
	}

	volumeSizeLimit = ParseVolumeSizeLimit(10, "11GiB")
	if volumeSizeLimit != 11*humanize.GiByte {
		t.Fail()
	}

	volumeSizeLimit = ParseVolumeSizeLimit(10, "11811160064")
	if volumeSizeLimit != 11*humanize.GiByte {
		t.Fail()
	}
}
