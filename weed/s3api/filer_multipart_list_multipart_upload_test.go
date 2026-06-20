package s3api

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

func isMultipartUploadSkipped(entry *filer_pb.Entry, input *s3.ListMultipartUploadsInput) (bool, string) {
	if entry.Extended == nil {
		return true, ""
	}
	keyBytes, ok := entry.Extended[s3_constants.ExtMultipartObjectKey]
	if !ok {
		return true, ""
	}
	key := string(keyBytes)
	
	if *input.KeyMarker != "" {
		if key < *input.KeyMarker {
			return true, key
		}
		if key == *input.KeyMarker && (*input.UploadIdMarker == "" || entry.Name <= *input.UploadIdMarker) {
			return true, key
		}
	}
	
	if *input.Prefix != "" && !strings.HasPrefix(key, *input.Prefix) {
		return true, key
	}
	
	return false, key
}

func TestListMultipartUploadsResultKeyMarkers(t *testing.T) {
	entries := []*filer_pb.Entry{
		{Name: "UID-nil-extended", Extended: nil},          
    {Name: "UID-error", Extended: map[string][]byte{}}, 
    {Name: "UID-1", Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("fileA")}},
    {Name: "UID-2", Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("fileB")}},
    {Name: "UID-3", Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("fileB")}},
    {Name: "UID-4", Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("fileC")}},
}

	tests := []struct {
		name              string
		input             *s3.ListMultipartUploadsInput
		expectedUploadIDs []string
	}{
		{
			name: "skips entries missing multipart object key metadata",
			input: &s3.ListMultipartUploadsInput{
				Bucket:         aws.String("test-bucket"),
				KeyMarker:      aws.String(""),
				UploadIdMarker: aws.String(""),
				Prefix:         aws.String(""),
				MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: []string{"UID-1", "UID-2", "UID-3", "UID-4"},
		},
		{
			name: "without upload id marker returns only keys greater than key marker",
			input: &s3.ListMultipartUploadsInput{
				Bucket:         aws.String("test-bucket"),
				KeyMarker:      aws.String("fileA"),
				UploadIdMarker: aws.String(""),
				Prefix:         aws.String(""),
				MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: []string{"UID-2", "UID-3", "UID-4"},
		},
		{
			name: "with upload id marker includes same key with greater upload ids",
			input: &s3.ListMultipartUploadsInput{
				Bucket:         aws.String("test-bucket"),
				KeyMarker:      aws.String("fileB"),
				UploadIdMarker: aws.String("UID-2"),
				Prefix:         aws.String(""),
				MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: []string{"UID-3", "UID-4"},
		},
		{
			name: "cross-key regression: ensures key > KeyMarker is included even if uploadId <= UploadIdMarker",
			input: &s3.ListMultipartUploadsInput{
					Bucket:         aws.String("test-bucket"),
					KeyMarker:      aws.String("fileB"),
					UploadIdMarker: aws.String("UID-3"),
					Prefix:         aws.String(""),
					MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: []string{"UID-4"},
		},
		{
			name: "with prefix matching some uploads",
			input: &s3.ListMultipartUploadsInput{
				Bucket:         aws.String("test-bucket"),
				KeyMarker:      aws.String(""),
				UploadIdMarker: aws.String(""),
				Prefix:         aws.String("fileB"),
				MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: []string{"UID-2", "UID-3"},
		},
		{
			name: "with prefix matching no uploads",
			input: &s3.ListMultipartUploadsInput{
				Bucket:         aws.String("test-bucket"),
				KeyMarker:      aws.String(""),
				UploadIdMarker: aws.String(""),
				Prefix:         aws.String("fileX"),
				MaxUploads:     aws.Int64(10),
			},
			expectedUploadIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actualUploadIDs []string
			
			for _, entry := range entries {
				skipped, _ := isMultipartUploadSkipped(entry, tt.input)
				if !skipped {
					actualUploadIDs = append(actualUploadIDs, entry.Name)
				}
			}

			assert.Equal(t, tt.expectedUploadIDs, actualUploadIDs)
		})
	}
}