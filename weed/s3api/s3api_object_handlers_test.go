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
