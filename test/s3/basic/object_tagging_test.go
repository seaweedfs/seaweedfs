package basic

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
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
