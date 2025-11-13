package s3api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3PolicyTemplates(t *testing.T) {
	templates := NewS3PolicyTemplates()

	t.Run("S3ReadOnlyPolicy", func(t *testing.T) {
		policy := templates.GetS3ReadOnlyPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, "2012-10-17", policy.Version)
		assert.Len(t, policy.Statement, 1)

		stmt := policy.Statement[0]
		assert.Equal(t, "Allow", stmt.Effect)
		assert.Equal(t, "S3ReadOnlyAccess", stmt.Sid)
		assert.Contains(t, stmt.Action, "s3:GetObject")
		assert.Contains(t, stmt.Action, "s3:ListBucket")
		assert.NotContains(t, stmt.Action, "s3:PutObject")
		assert.NotContains(t, stmt.Action, "s3:DeleteObject")

		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*")
		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*/*")
	})

	t.Run("S3WriteOnlyPolicy", func(t *testing.T) {
		policy := templates.GetS3WriteOnlyPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, "2012-10-17", policy.Version)
		assert.Len(t, policy.Statement, 1)

		stmt := policy.Statement[0]
		assert.Equal(t, "Allow", stmt.Effect)
		assert.Equal(t, "S3WriteOnlyAccess", stmt.Sid)
		assert.Contains(t, stmt.Action, "s3:PutObject")
		assert.Contains(t, stmt.Action, "s3:CreateMultipartUpload")
		assert.NotContains(t, stmt.Action, "s3:GetObject")
		assert.NotContains(t, stmt.Action, "s3:DeleteObject")

		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*")
		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*/*")
	})

	t.Run("S3AdminPolicy", func(t *testing.T) {
		policy := templates.GetS3AdminPolicy()

		require.NotNil(t, policy)
		assert.Equal(t, "2012-10-17", policy.Version)
		assert.Len(t, policy.Statement, 1)

		stmt := policy.Statement[0]
		assert.Equal(t, "Allow", stmt.Effect)
		assert.Equal(t, "S3FullAccess", stmt.Sid)
		assert.Contains(t, stmt.Action, "s3:*")

		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*")
		assert.Contains(t, stmt.Resource, "arn:aws:s3:::*/*")
	})
}

func TestBucketSpecificPolicies(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "test-bucket"

	t.Run("BucketSpecificReadPolicy", func(t *testing.T) {
		policy := templates.GetBucketSpecificReadPolicy(bucketName)

		require.NotNil(t, policy)
		assert.Equal(t, "2012-10-17", policy.Version)
		assert.Len(t, policy.Statement, 1)

		stmt := policy.Statement[0]
		assert.Equal(t, "Allow", stmt.Effect)
		assert.Equal(t, "BucketSpecificReadAccess", stmt.Sid)
		assert.Contains(t, stmt.Action, "s3:GetObject")
		assert.Contains(t, stmt.Action, "s3:ListBucket")
		assert.NotContains(t, stmt.Action, "s3:PutObject")

		expectedBucketArn := "arn:aws:s3:::" + bucketName
		expectedObjectArn := "arn:aws:s3:::" + bucketName + "/*"
		assert.Contains(t, stmt.Resource, expectedBucketArn)
		assert.Contains(t, stmt.Resource, expectedObjectArn)
	})

	t.Run("BucketSpecificWritePolicy", func(t *testing.T) {
		policy := templates.GetBucketSpecificWritePolicy(bucketName)

		require.NotNil(t, policy)
		assert.Equal(t, "2012-10-17", policy.Version)
		assert.Len(t, policy.Statement, 1)

		stmt := policy.Statement[0]
		assert.Equal(t, "Allow", stmt.Effect)
		assert.Equal(t, "BucketSpecificWriteAccess", stmt.Sid)
		assert.Contains(t, stmt.Action, "s3:PutObject")
		assert.Contains(t, stmt.Action, "s3:CreateMultipartUpload")
		assert.NotContains(t, stmt.Action, "s3:GetObject")

		expectedBucketArn := "arn:aws:s3:::" + bucketName
		expectedObjectArn := "arn:aws:s3:::" + bucketName + "/*"
		assert.Contains(t, stmt.Resource, expectedBucketArn)
		assert.Contains(t, stmt.Resource, expectedObjectArn)
	})
}

func TestPathBasedAccessPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "shared-bucket"
	pathPrefix := "user123/documents"

	policy := templates.GetPathBasedAccessPolicy(bucketName, pathPrefix)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 2)

	// First statement: List bucket with prefix condition
	listStmt := policy.Statement[0]
	assert.Equal(t, "Allow", listStmt.Effect)
	assert.Equal(t, "ListBucketPermission", listStmt.Sid)
	assert.Contains(t, listStmt.Action, "s3:ListBucket")
	assert.Contains(t, listStmt.Resource, "arn:aws:s3:::"+bucketName)
	assert.NotNil(t, listStmt.Condition)

	// Second statement: Object operations on path
	objectStmt := policy.Statement[1]
	assert.Equal(t, "Allow", objectStmt.Effect)
	assert.Equal(t, "PathBasedObjectAccess", objectStmt.Sid)
	assert.Contains(t, objectStmt.Action, "s3:GetObject")
	assert.Contains(t, objectStmt.Action, "s3:PutObject")
	assert.Contains(t, objectStmt.Action, "s3:DeleteObject")

	expectedObjectArn := "arn:aws:s3:::" + bucketName + "/" + pathPrefix + "/*"
	assert.Contains(t, objectStmt.Resource, expectedObjectArn)
}

func TestIPRestrictedPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	allowedCIDRs := []string{"192.168.1.0/24", "10.0.0.0/8"}

	policy := templates.GetIPRestrictedPolicy(allowedCIDRs)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 1)

	stmt := policy.Statement[0]
	assert.Equal(t, "Allow", stmt.Effect)
	assert.Equal(t, "IPRestrictedS3Access", stmt.Sid)
	assert.Contains(t, stmt.Action, "s3:*")
	assert.NotNil(t, stmt.Condition)

	// Check IP condition structure
	condition := stmt.Condition
	ipAddress, exists := condition["IpAddress"]
	assert.True(t, exists)

	sourceIp, exists := ipAddress["aws:SourceIp"]
	assert.True(t, exists)
	assert.Equal(t, allowedCIDRs, sourceIp)
}

func TestTimeBasedAccessPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	startHour := 9 // 9 AM
	endHour := 17  // 5 PM

	policy := templates.GetTimeBasedAccessPolicy(startHour, endHour)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 1)

	stmt := policy.Statement[0]
	assert.Equal(t, "Allow", stmt.Effect)
	assert.Equal(t, "TimeBasedS3Access", stmt.Sid)
	assert.Contains(t, stmt.Action, "s3:GetObject")
	assert.Contains(t, stmt.Action, "s3:PutObject")
	assert.Contains(t, stmt.Action, "s3:ListBucket")
	assert.NotNil(t, stmt.Condition)

	// Check time condition structure
	condition := stmt.Condition
	_, hasGreater := condition["DateGreaterThan"]
	_, hasLess := condition["DateLessThan"]
	assert.True(t, hasGreater)
	assert.True(t, hasLess)
}

func TestMultipartUploadPolicyTemplate(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "large-files"

	policy := templates.GetMultipartUploadPolicy(bucketName)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 2)

	// First statement: Multipart operations
	multipartStmt := policy.Statement[0]
	assert.Equal(t, "Allow", multipartStmt.Effect)
	assert.Equal(t, "MultipartUploadOperations", multipartStmt.Sid)
	assert.Contains(t, multipartStmt.Action, "s3:CreateMultipartUpload")
	assert.Contains(t, multipartStmt.Action, "s3:UploadPart")
	assert.Contains(t, multipartStmt.Action, "s3:CompleteMultipartUpload")
	assert.Contains(t, multipartStmt.Action, "s3:AbortMultipartUpload")
	assert.Contains(t, multipartStmt.Action, "s3:ListMultipartUploads")
	assert.Contains(t, multipartStmt.Action, "s3:ListParts")

	expectedObjectArn := "arn:aws:s3:::" + bucketName + "/*"
	assert.Contains(t, multipartStmt.Resource, expectedObjectArn)

	// Second statement: List bucket
	listStmt := policy.Statement[1]
	assert.Equal(t, "Allow", listStmt.Effect)
	assert.Equal(t, "ListBucketForMultipart", listStmt.Sid)
	assert.Contains(t, listStmt.Action, "s3:ListBucket")

	expectedBucketArn := "arn:aws:s3:::" + bucketName
	assert.Contains(t, listStmt.Resource, expectedBucketArn)
}

func TestPresignedURLPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "shared-files"

	policy := templates.GetPresignedURLPolicy(bucketName)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 1)

	stmt := policy.Statement[0]
	assert.Equal(t, "Allow", stmt.Effect)
	assert.Equal(t, "PresignedURLAccess", stmt.Sid)
	assert.Contains(t, stmt.Action, "s3:GetObject")
	assert.Contains(t, stmt.Action, "s3:PutObject")
	assert.NotNil(t, stmt.Condition)

	expectedObjectArn := "arn:aws:s3:::" + bucketName + "/*"
	assert.Contains(t, stmt.Resource, expectedObjectArn)

	// Check signature version condition
	condition := stmt.Condition
	stringEquals, exists := condition["StringEquals"]
	assert.True(t, exists)

	signatureVersion, exists := stringEquals["s3:x-amz-signature-version"]
	assert.True(t, exists)
	assert.Equal(t, "AWS4-HMAC-SHA256", signatureVersion)
}

func TestTemporaryAccessPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "temp-bucket"
	expirationHours := 24

	policy := templates.GetTemporaryAccessPolicy(bucketName, expirationHours)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 1)

	stmt := policy.Statement[0]
	assert.Equal(t, "Allow", stmt.Effect)
	assert.Equal(t, "TemporaryS3Access", stmt.Sid)
	assert.Contains(t, stmt.Action, "s3:GetObject")
	assert.Contains(t, stmt.Action, "s3:PutObject")
	assert.Contains(t, stmt.Action, "s3:ListBucket")
	assert.NotNil(t, stmt.Condition)

	// Check expiration condition
	condition := stmt.Condition
	dateLessThan, exists := condition["DateLessThan"]
	assert.True(t, exists)

	currentTime, exists := dateLessThan["aws:CurrentTime"]
	assert.True(t, exists)
	assert.IsType(t, "", currentTime) // Should be a string timestamp
}

func TestContentTypeRestrictedPolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()
	bucketName := "media-bucket"
	allowedTypes := []string{"image/jpeg", "image/png", "video/mp4"}

	policy := templates.GetContentTypeRestrictedPolicy(bucketName, allowedTypes)

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 2)

	// First statement: Upload with content type restriction
	uploadStmt := policy.Statement[0]
	assert.Equal(t, "Allow", uploadStmt.Effect)
	assert.Equal(t, "ContentTypeRestrictedUpload", uploadStmt.Sid)
	assert.Contains(t, uploadStmt.Action, "s3:PutObject")
	assert.Contains(t, uploadStmt.Action, "s3:CreateMultipartUpload")
	assert.NotNil(t, uploadStmt.Condition)

	// Check content type condition
	condition := uploadStmt.Condition
	stringEquals, exists := condition["StringEquals"]
	assert.True(t, exists)

	contentType, exists := stringEquals["s3:content-type"]
	assert.True(t, exists)
	assert.Equal(t, allowedTypes, contentType)

	// Second statement: Read access without restrictions
	readStmt := policy.Statement[1]
	assert.Equal(t, "Allow", readStmt.Effect)
	assert.Equal(t, "ReadAccess", readStmt.Sid)
	assert.Contains(t, readStmt.Action, "s3:GetObject")
	assert.Contains(t, readStmt.Action, "s3:ListBucket")
	assert.Nil(t, readStmt.Condition) // No conditions for read access
}

func TestDenyDeletePolicy(t *testing.T) {
	templates := NewS3PolicyTemplates()

	policy := templates.GetDenyDeletePolicy()

	require.NotNil(t, policy)
	assert.Equal(t, "2012-10-17", policy.Version)
	assert.Len(t, policy.Statement, 2)

	// First statement: Allow everything except delete
	allowStmt := policy.Statement[0]
	assert.Equal(t, "Allow", allowStmt.Effect)
	assert.Equal(t, "AllowAllExceptDelete", allowStmt.Sid)
	assert.Contains(t, allowStmt.Action, "s3:GetObject")
	assert.Contains(t, allowStmt.Action, "s3:PutObject")
	assert.Contains(t, allowStmt.Action, "s3:ListBucket")
	assert.NotContains(t, allowStmt.Action, "s3:DeleteObject")
	assert.NotContains(t, allowStmt.Action, "s3:DeleteBucket")

	// Second statement: Explicitly deny delete operations
	denyStmt := policy.Statement[1]
	assert.Equal(t, "Deny", denyStmt.Effect)
	assert.Equal(t, "DenyDeleteOperations", denyStmt.Sid)
	assert.Contains(t, denyStmt.Action, "s3:DeleteObject")
	assert.Contains(t, denyStmt.Action, "s3:DeleteObjectVersion")
	assert.Contains(t, denyStmt.Action, "s3:DeleteBucket")
}

func TestPolicyTemplateMetadata(t *testing.T) {
	templates := NewS3PolicyTemplates()

	t.Run("GetAllPolicyTemplates", func(t *testing.T) {
		allTemplates := templates.GetAllPolicyTemplates()

		assert.Greater(t, len(allTemplates), 10) // Should have many templates

		// Check that each template has required fields
		for _, template := range allTemplates {
			assert.NotEmpty(t, template.Name)
			assert.NotEmpty(t, template.Description)
			assert.NotEmpty(t, template.Category)
			assert.NotEmpty(t, template.UseCase)
			assert.NotNil(t, template.Policy)
			assert.Equal(t, "2012-10-17", template.Policy.Version)
		}
	})

	t.Run("GetPolicyTemplateByName", func(t *testing.T) {
		// Test existing template
		template := templates.GetPolicyTemplateByName("S3ReadOnlyAccess")
		require.NotNil(t, template)
		assert.Equal(t, "S3ReadOnlyAccess", template.Name)
		assert.Equal(t, "Basic Access", template.Category)

		// Test non-existing template
		nonExistent := templates.GetPolicyTemplateByName("NonExistentTemplate")
		assert.Nil(t, nonExistent)
	})

	t.Run("GetPolicyTemplatesByCategory", func(t *testing.T) {
		basicAccessTemplates := templates.GetPolicyTemplatesByCategory("Basic Access")
		assert.GreaterOrEqual(t, len(basicAccessTemplates), 2)

		for _, template := range basicAccessTemplates {
			assert.Equal(t, "Basic Access", template.Category)
		}

		// Test non-existing category
		emptyCategory := templates.GetPolicyTemplatesByCategory("NonExistentCategory")
		assert.Empty(t, emptyCategory)
	})

	t.Run("PolicyTemplateParameters", func(t *testing.T) {
		allTemplates := templates.GetAllPolicyTemplates()

		// Find a template with parameters (like BucketSpecificRead)
		var templateWithParams *PolicyTemplateDefinition
		for _, template := range allTemplates {
			if template.Name == "BucketSpecificRead" {
				templateWithParams = &template
				break
			}
		}

		require.NotNil(t, templateWithParams)
		assert.Greater(t, len(templateWithParams.Parameters), 0)

		param := templateWithParams.Parameters[0]
		assert.Equal(t, "bucketName", param.Name)
		assert.Equal(t, "string", param.Type)
		assert.True(t, param.Required)
		assert.NotEmpty(t, param.Description)
		assert.NotEmpty(t, param.Example)
	})
}

func TestFormatHourHelper(t *testing.T) {
	tests := []struct {
		hour     int
		expected string
	}{
		{0, "00"},
		{5, "05"},
		{9, "09"},
		{10, "10"},
		{15, "15"},
		{23, "23"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Hour_%d", tt.hour), func(t *testing.T) {
			result := formatHour(tt.hour)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPolicyTemplateCategories(t *testing.T) {
	templates := NewS3PolicyTemplates()
	allTemplates := templates.GetAllPolicyTemplates()

	// Extract all categories
	categoryMap := make(map[string]int)
	for _, template := range allTemplates {
		categoryMap[template.Category]++
	}

	// Expected categories
	expectedCategories := []string{
		"Basic Access",
		"Administrative",
		"Bucket-Specific",
		"Path-Restricted",
		"Security",
		"Upload-Specific",
		"Presigned URLs",
		"Content Control",
		"Data Protection",
	}

	for _, expectedCategory := range expectedCategories {
		count, exists := categoryMap[expectedCategory]
		assert.True(t, exists, "Category %s should exist", expectedCategory)
		assert.Greater(t, count, 0, "Category %s should have at least one template", expectedCategory)
	}
}

func TestPolicyValidation(t *testing.T) {
	templates := NewS3PolicyTemplates()
	allTemplates := templates.GetAllPolicyTemplates()

	// Test that all policies have valid structure
	for _, template := range allTemplates {
		t.Run("Policy_"+template.Name, func(t *testing.T) {
			policy := template.Policy

			// Basic validation
			assert.Equal(t, "2012-10-17", policy.Version)
			assert.Greater(t, len(policy.Statement), 0)

			// Validate each statement
			for i, stmt := range policy.Statement {
				assert.NotEmpty(t, stmt.Effect, "Statement %d should have effect", i)
				assert.Contains(t, []string{"Allow", "Deny"}, stmt.Effect, "Statement %d effect should be Allow or Deny", i)
				assert.Greater(t, len(stmt.Action), 0, "Statement %d should have actions", i)
				assert.Greater(t, len(stmt.Resource), 0, "Statement %d should have resources", i)

				// Check resource format
				for _, resource := range stmt.Resource {
					if resource != "*" {
						assert.Contains(t, resource, "arn:aws:s3:::", "Resource should be valid AWS S3 ARN: %s", resource)
					}
				}
			}
		})
	}
}
