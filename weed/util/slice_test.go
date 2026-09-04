package util

import (
	"reflect"
	"testing"
)

func TestReorderToFront_StringSlice(t *testing.T) {
	localUrls := map[string]bool{
		"http://local1": true,
		"http://local2": true,
	}

	sameDcTargetUrls := []string{
		"http://remote1",
		"http://local1",
		"http://remote2",
		"http://local2",
	}

	expected := []string{
		"http://local1",
		"http://local2",
		"http://remote1",
		"http://remote2",
	}

	result := ReorderToFront(localUrls, sameDcTargetUrls)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("ReorderToFront failed for strings. Got: %v, Expected: %v", result, expected)
	}
}
