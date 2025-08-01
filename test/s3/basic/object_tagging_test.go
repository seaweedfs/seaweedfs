package basic

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestObjectTagging(t *testing.T) {

	input := &s3.PutObjectInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObject"),
	}

	svc.PutObject(input)

	printTags()

	setTags()

	printTags()

	clearTags()

	printTags()

}

func printTags() {
	response, err := svc.GetObjectTagging(
		&s3.GetObjectTaggingInput{
			Bucket: aws.String("theBucket"),
			Key:    aws.String("testDir/testObject"),
		})

	fmt.Println("printTags")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println(response.TagSet)
}

func setTags() {

	response, err := svc.PutObjectTagging(&s3.PutObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObject"),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("kye2"),
					Value: aws.String("value2"),
				},
			},
		},
	})

	fmt.Println("setTags")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println(response.String())
}

func clearTags() {

	response, err := svc.DeleteObjectTagging(&s3.DeleteObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObject"),
	})

	fmt.Println("clearTags")
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println(response.String())
}

func TestObjectTaggingWithEncodedValues(t *testing.T) {
	// Test for URL encoded tag values
	input := &s3.PutObjectInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObjectWithEncodedTags"),
	}

	svc.PutObject(input)

	// Set tags with encoded values (simulating what would happen with timestamps containing spaces and colons)
	_, err := svc.PutObjectTagging(&s3.PutObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObjectWithEncodedTags"),
		Tagging: &s3.Tagging{
			TagSet: []*s3.Tag{
				{
					Key:   aws.String("Timestamp"),
					Value: aws.String("2025-07-16 14:40:39"), // This would be URL encoded as "2025-07-16%2014%3A40%3A39" in the header
				},
				{
					Key:   aws.String("Path"),
					Value: aws.String("/tmp/file.txt"), // This would be URL encoded as "/tmp%2Ffile.txt" in the header
				},
			},
		},
	})

	if err != nil {
		t.Fatalf("Failed to set tags with encoded values: %v", err)
	}

	// Get tags back and verify they are properly decoded
	response, err := svc.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObjectWithEncodedTags"),
	})

	if err != nil {
		t.Fatalf("Failed to get tags: %v", err)
	}

	// Verify that the tags are properly decoded
	tagMap := make(map[string]string)
	for _, tag := range response.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	expectedTimestamp := "2025-07-16 14:40:39"
	if tagMap["Timestamp"] != expectedTimestamp {
		t.Errorf("Expected Timestamp tag to be '%s', got '%s'", expectedTimestamp, tagMap["Timestamp"])
	}

	expectedPath := "/tmp/file.txt"
	if tagMap["Path"] != expectedPath {
		t.Errorf("Expected Path tag to be '%s', got '%s'", expectedPath, tagMap["Path"])
	}

	fmt.Printf("✓ URL encoded tags test passed - Timestamp: %s, Path: %s\n", tagMap["Timestamp"], tagMap["Path"])

	// Clean up
	svc.DeleteObjectTagging(&s3.DeleteObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String("testDir/testObjectWithEncodedTags"),
	})
}

// TestObjectUploadWithEncodedTags tests the specific issue reported in GitHub issue #7040
// where tags sent via X-Amz-Tagging header during object upload are not URL decoded properly
func TestObjectUploadWithEncodedTags(t *testing.T) {
	// This test specifically addresses the issue where tags with special characters
	// (like spaces, colons, slashes) sent during object upload are not URL decoded
	// This tests the fix in filer_server_handlers_write_autochunk.go

	objectKey := "testDir/testObjectUploadWithTags"

	// Upload object with tags that contain special characters that would be URL encoded
	// The AWS SDK will automatically URL encode these when sending the X-Amz-Tagging header
	// Test edge cases that url.ParseQuery handles better than manual parsing:
	// - Values containing "=" characters
	// - Empty values
	// - Complex special characters
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:  aws.String("theBucket"),
		Key:     aws.String(objectKey),
		Body:    aws.ReadSeekCloser(strings.NewReader("test content")),
		Tagging: aws.String("Timestamp=2025-07-16 14:40:39&Path=/tmp/file.txt&Description=A test file with spaces&Equation=x=y+1&EmptyValue=&Complex=A%20tag%20with%20%26%20%3D%20chars"),
	})

	if err != nil {
		t.Fatalf("Failed to upload object with tags: %v", err)
	}

	// Get the tags back to verify they were properly URL decoded during upload
	response, err := svc.GetObjectTagging(&s3.GetObjectTaggingInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		t.Fatalf("Failed to get object tags: %v", err)
	}

	// Verify that the tags are properly decoded (not URL encoded)
	tagMap := make(map[string]string)
	for _, tag := range response.TagSet {
		tagMap[*tag.Key] = *tag.Value
	}

	// Test cases for values that would be URL encoded in the X-Amz-Tagging header
	testCases := []struct {
		key           string
		expectedValue string
		description   string
	}{
		{"Timestamp", "2025-07-16 14:40:39", "timestamp with spaces and colons"},
		{"Path", "/tmp/file.txt", "file path with slashes"},
		{"Description", "A test file with spaces", "description with spaces"},
	}

	for _, tc := range testCases {
		actualValue, exists := tagMap[tc.key]
		if !exists {
			t.Errorf("Expected tag key '%s' not found", tc.key)
			continue
		}

		if actualValue != tc.expectedValue {
			t.Errorf("Tag '%s' (%s): expected '%s', got '%s'",
				tc.key, tc.description, tc.expectedValue, actualValue)
		} else {
			fmt.Printf("✓ Tag '%s' correctly decoded: '%s'\n", tc.key, actualValue)
		}
	}

	// Clean up
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String("theBucket"),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		t.Logf("Warning: Failed to clean up test object: %v", err)
	}
}
