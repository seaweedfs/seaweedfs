package example

import (
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/stretchr/testify/require"
)

// TestIAMOperations tests authenticated IAM operations with AWS Signature V4
// All IAM operations require proper authentication.
func TestIAMOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Set credentials before starting cluster
	accessKey := "testkey123"
	secretKey := "testsecret456"
	os.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")

	// Create and start test cluster
	cluster, err := startMiniCluster(t)
	require.NoError(t, err)
	defer cluster.Stop()

	// Wait for services to be fully ready
	time.Sleep(500 * time.Millisecond)

	// Create IAM client with credentials
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-west-2"),
		Endpoint:         aws.String(cluster.s3Endpoint),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
		S3ForcePathStyle: aws.Bool(true),
	})
	require.NoError(t, err)

	iamClient := iam.New(sess)

	// Run all IAM tests with authentication
	t.Run("CreateUser", func(t *testing.T) {
		testCreateUserAuthenticated(t, iamClient)
	})

	t.Run("ListUsers", func(t *testing.T) {
		testListUsersAuthenticated(t, iamClient)
	})

	t.Run("GetUser", func(t *testing.T) {
		testGetUserAuthenticated(t, iamClient)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		testDeleteUserAuthenticated(t, iamClient)
	})
}

// testCreateUserAuthenticated tests CreateUser with AWS Signature V4 authentication
func testCreateUserAuthenticated(t *testing.T, iamClient *iam.IAM) {
	userName := "alice-" + randomString(8)

	input := &iam.CreateUserInput{
		UserName: aws.String(userName),
	}

	result, err := iamClient.CreateUser(input)
	require.NoError(t, err, "Authenticated CreateUser should succeed")
	require.NotNil(t, result.User)
	require.Equal(t, userName, *result.User.UserName)

	t.Logf("✓ Created user with authentication: %s", userName)
}

// testListUsersAuthenticated tests ListUsers with authentication
func testListUsersAuthenticated(t *testing.T, iamClient *iam.IAM) {
	// First create a user
	userName := "listauth-" + randomString(8)
	_, err := iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Wait for user to be persisted
	time.Sleep(100 * time.Millisecond)

	// List users
	result, err := iamClient.ListUsers(&iam.ListUsersInput{})
	require.NoError(t, err, "Authenticated ListUsers should succeed")
	require.NotNil(t, result.Users)

	// Verify our user is in the list
	found := false
	for _, user := range result.Users {
		if *user.UserName == userName {
			found = true
			break
		}
	}
	require.True(t, found, "Created user should be in the list")

	t.Logf("✓ Listed %d users with authentication", len(result.Users))
}

// testGetUserAuthenticated tests GetUser with authentication
func testGetUserAuthenticated(t *testing.T, iamClient *iam.IAM) {
	userName := "getauth-" + randomString(8)

	// Create user
	_, err := iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Wait for user to be persisted
	time.Sleep(100 * time.Millisecond)

	// Get user
	result, err := iamClient.GetUser(&iam.GetUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err, "Authenticated GetUser should succeed")
	require.NotNil(t, result.User)
	require.Equal(t, userName, *result.User.UserName)

	t.Logf("✓ Got user with authentication: %s", userName)
}

// testDeleteUserAuthenticated tests DeleteUser with authentication
func testDeleteUserAuthenticated(t *testing.T, iamClient *iam.IAM) {
	userName := "delauth-" + randomString(8)

	// Create user
	_, err := iamClient.CreateUser(&iam.CreateUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err)

	// Wait for user to be persisted
	time.Sleep(100 * time.Millisecond)

	// Delete user
	_, err = iamClient.DeleteUser(&iam.DeleteUserInput{
		UserName: aws.String(userName),
	})
	require.NoError(t, err, "Authenticated DeleteUser should succeed")

	t.Logf("✓ Deleted user with authentication: %s", userName)
}
