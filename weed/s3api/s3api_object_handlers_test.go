package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveDuplicateSlashes(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedResult string
	}{
		{
			name:           "empty",
			path:           "",
			expectedResult: "",
		},
		{
			name:           "slash",
			path:           "/",
			expectedResult: "/",
		},
		{
			name:           "object",
			path:           "object",
			expectedResult: "object",
		},
		{
			name:           "correct path",
			path:           "/path/to/object",
			expectedResult: "/path/to/object",
		},
		{
			name:           "path with duplicates",
			path:           "///path//to/object//",
			expectedResult: "/path/to/object/",
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			obj := removeDuplicateSlashes(tst.path)
			assert.Equal(t, tst.expectedResult, obj)
		})
	}
}

func TestS3ApiServer_toFilerUrl(t *testing.T) {
	tests := []struct {
		name string
		args string
		want string
	}{
		{
			"simple",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto 2022-09-19 um 21.38.37.png",
			"/uploads/eaf10b3b-3b3a-4dcd-92a7-edf2a512276e/67b8b9bf-7cca-4cb6-9b34-22fcb4d6e27d/Bildschirmfoto%202022-09-19%20um%2021.38.37.png",
		},
		{
			"double prefix",
			"//uploads/t.png",
			"/uploads/t.png",
		},
		{
			"triple prefix",
			"///uploads/t.png",
			"/uploads/t.png",
		},
		{
			"empty prefix",
			"uploads/t.png",
			"/uploads/t.png",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, urlEscapeObject(tt.args), "clean %v", tt.args)
		})
	}
}
