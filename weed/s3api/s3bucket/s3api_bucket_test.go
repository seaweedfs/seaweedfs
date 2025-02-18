package s3bucket

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_verifyBucketName(t *testing.T) {
	invalidS3BucketNames := []string{
		"A9325325b",
		"123.12.153.10",
		"abc214..2",
		"d",
		"aa",
		".ewfs3253543",
		"grehtrry-",
		"----------",
		"x@fdsgr032",
	}
	for _, invalidName := range invalidS3BucketNames {
		err := VerifyS3BucketName(invalidName)
		assert.NotNil(t, err)
	}
	validS3BucketName := []string{
		"a9325325b",
		"999.12.153.10",
		"abc214.2",
		"3d3d3d",
		"ewfs3253543",
		"grehtrry-a",
		"0----------0",
		"xafdsgr032",
	}
	for _, invalidName := range validS3BucketName {
		err := VerifyS3BucketName(invalidName)
		assert.Nil(t, err)
	}
}
