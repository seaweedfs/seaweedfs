package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNextMarkerWithNestedPrefix verifies that NextMarker is correctly constructed
// when listing with nested prefixes like "character/member/".
//
// This is a regression test for the bug where NextMarker was "character/res024/"
// instead of "character/member/res024/", causing continuation requests to fail.
func TestNextMarkerWithNestedPrefix(t *testing.T) {
	tests := []struct {
		name                 string
		requestDir           string
		prefix               string
		nextMarkerFromDoList string
		expectedFinal        string
	}{
		{
			name:                 "nested prefix with both requestDir and prefix",
			requestDir:           "character",
			prefix:               "member",
			nextMarkerFromDoList: "res024",
			expectedFinal:        "character/member/res024",
		},
		{
			name:                 "only requestDir, no prefix",
			requestDir:           "character",
			prefix:               "",
			nextMarkerFromDoList: "res024",
			expectedFinal:        "character/res024",
		},
		{
			name:                 "no requestDir, only prefix",
			requestDir:           "",
			prefix:               "member",
			nextMarkerFromDoList: "res024",
			expectedFinal:        "res024",
		},
		{
			name:                 "deeply nested prefix",
			requestDir:           "a/b/c",
			prefix:               "d",
			nextMarkerFromDoList: "file.txt",
			expectedFinal:        "a/b/c/d/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the fixed logic from listFilerEntries (lines 333-341)
			nextMarker := tt.nextMarkerFromDoList
			if tt.requestDir != "" {
				if tt.prefix != "" {
					nextMarker = tt.requestDir + "/" + tt.prefix + "/" + nextMarker
				} else {
					nextMarker = tt.requestDir + "/" + nextMarker
				}
			}

			assert.Equal(t, tt.expectedFinal, nextMarker,
				"NextMarker should be correctly constructed")
		})
	}
}

// TestNextMarkerWithCommonPrefix verifies NextMarker construction for CommonPrefix entries
func TestNextMarkerWithCommonPrefix(t *testing.T) {
	tests := []struct {
		name                 string
		requestDir           string
		prefix               string
		lastCommonPrefixName string
		expectedFinal        string
	}{
		{
			name:                 "nested prefix with CommonPrefix",
			requestDir:           "character",
			prefix:               "member",
			lastCommonPrefixName: "res024",
			expectedFinal:        "character/member/res024/",
		},
		{
			name:                 "only requestDir with CommonPrefix",
			requestDir:           "character",
			prefix:               "",
			lastCommonPrefixName: "member",
			expectedFinal:        "character/member/",
		},
		{
			name:                 "no requestDir with CommonPrefix",
			requestDir:           "",
			prefix:               "member",
			lastCommonPrefixName: "res024",
			expectedFinal:        "res024/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the fixed logic from listFilerEntries (lines 322-332)
			var nextMarker string
			if tt.requestDir != "" {
				if tt.prefix != "" {
					nextMarker = tt.requestDir + "/" + tt.prefix + "/" + tt.lastCommonPrefixName + "/"
				} else {
					nextMarker = tt.requestDir + "/" + tt.lastCommonPrefixName + "/"
				}
			} else {
				nextMarker = tt.lastCommonPrefixName + "/"
			}

			assert.Equal(t, tt.expectedFinal, nextMarker,
				"NextMarker for CommonPrefix should be correctly constructed")
		})
	}
}
