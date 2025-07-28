package basic

import (
	"fmt"
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
