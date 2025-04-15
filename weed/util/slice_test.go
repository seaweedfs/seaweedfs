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
		"http://local2",
		"http://remote2",
		"http://local1",
	}

	expected1 := []string{
		"http://local1",
		"http://local2",
		"http://remote1",
		"http://remote2",
	}

	expected2 := []string{
		"http://local2",
		"http://local1",
		"http://remote1",
		"http://remote2",
	}

	result := ReorderToFront(localUrls, sameDcTargetUrls)

	if !reflect.DeepEqual(result, expected1) && !reflect.DeepEqual(result, expected2) {
		t.Errorf("ReorderToFront failed for strings. Got: %v, Expected1: %v, Expected2: %v", result, expected1, expected2)
	}
}
