package s3api

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

// S3PolicyTemplates provides pre-built IAM policy templates for common S3 use cases
type S3PolicyTemplates struct{}

// NewS3PolicyTemplates creates a new policy templates provider
func NewS3PolicyTemplates() *S3PolicyTemplates {
	return &S3PolicyTemplates{}
}

// GetS3ReadOnlyPolicy returns a policy that allows read-only access to all S3 resources
func (t *S3PolicyTemplates) GetS3ReadOnlyPolicy() *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "S3ReadOnlyAccess",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:GetObjectVersion",
					"s3:ListBucket",
					"s3:ListBucketVersions",
					"s3:GetBucketLocation",
					"s3:GetBucketVersioning",
					"s3:ListAllMyBuckets",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}
}

// GetS3WriteOnlyPolicy returns a policy that allows write-only access to all S3 resources
func (t *S3PolicyTemplates) GetS3WriteOnlyPolicy() *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "S3WriteOnlyAccess",
				Effect: "Allow",
				Action: []string{
					"s3:PutObject",
					"s3:PutObjectAcl",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
					"s3:ListMultipartUploads",
					"s3:ListParts",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}
}

// GetS3AdminPolicy returns a policy that allows full admin access to all S3 resources
func (t *S3PolicyTemplates) GetS3AdminPolicy() *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "S3FullAccess",
				Effect: "Allow",
				Action: []string{
					"s3:*",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}
}

// GetBucketSpecificReadPolicy returns a policy for read-only access to a specific bucket
func (t *S3PolicyTemplates) GetBucketSpecificReadPolicy(bucketName string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "BucketSpecificReadAccess",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:GetObjectVersion",
					"s3:ListBucket",
					"s3:ListBucketVersions",
					"s3:GetBucketLocation",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
					"arn:aws:s3:::" + bucketName + "/*",
				},
			},
		},
	}
}

// GetBucketSpecificWritePolicy returns a policy for write-only access to a specific bucket
func (t *S3PolicyTemplates) GetBucketSpecificWritePolicy(bucketName string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "BucketSpecificWriteAccess",
				Effect: "Allow",
				Action: []string{
					"s3:PutObject",
					"s3:PutObjectAcl",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
					"s3:ListMultipartUploads",
					"s3:ListParts",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
					"arn:aws:s3:::" + bucketName + "/*",
				},
			},
		},
	}
}

// GetPathBasedAccessPolicy returns a policy that restricts access to a specific path within a bucket
func (t *S3PolicyTemplates) GetPathBasedAccessPolicy(bucketName, pathPrefix string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "ListBucketPermission",
				Effect: "Allow",
				Action: []string{
					"s3:ListBucket",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
				},
				Condition: map[string]map[string]interface{}{
					"StringLike": map[string]interface{}{
						"s3:prefix": []string{pathPrefix + "/*"},
					},
				},
			},
			{
				Sid:    "PathBasedObjectAccess",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName + "/" + pathPrefix + "/*",
				},
			},
		},
	}
}

// GetIPRestrictedPolicy returns a policy that restricts access based on source IP
func (t *S3PolicyTemplates) GetIPRestrictedPolicy(allowedCIDRs []string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "IPRestrictedS3Access",
				Effect: "Allow",
				Action: []string{
					"s3:*",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
				Condition: map[string]map[string]interface{}{
					"IpAddress": map[string]interface{}{
						"aws:SourceIp": allowedCIDRs,
					},
				},
			},
		},
	}
}

// GetTimeBasedAccessPolicy returns a policy that allows access only during specific hours
func (t *S3PolicyTemplates) GetTimeBasedAccessPolicy(startHour, endHour int) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "TimeBasedS3Access",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:ListBucket",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
				Condition: map[string]map[string]interface{}{
					"DateGreaterThan": map[string]interface{}{
						"aws:CurrentTime": time.Now().Format("2006-01-02") + "T" +
							formatHour(startHour) + ":00:00Z",
					},
					"DateLessThan": map[string]interface{}{
						"aws:CurrentTime": time.Now().Format("2006-01-02") + "T" +
							formatHour(endHour) + ":00:00Z",
					},
				},
			},
		},
	}
}

// GetMultipartUploadPolicy returns a policy specifically for multipart upload operations
func (t *S3PolicyTemplates) GetMultipartUploadPolicy(bucketName string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "MultipartUploadOperations",
				Effect: "Allow",
				Action: []string{
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
					"s3:ListMultipartUploads",
					"s3:ListParts",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName + "/*",
				},
			},
			{
				Sid:    "ListBucketForMultipart",
				Effect: "Allow",
				Action: []string{
					"s3:ListBucket",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
				},
			},
		},
	}
}

// GetPresignedURLPolicy returns a policy for generating and using presigned URLs
func (t *S3PolicyTemplates) GetPresignedURLPolicy(bucketName string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "PresignedURLAccess",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:PutObject",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName + "/*",
				},
				Condition: map[string]map[string]interface{}{
					"StringEquals": map[string]interface{}{
						"s3:x-amz-signature-version": "AWS4-HMAC-SHA256",
					},
				},
			},
		},
	}
}

// GetTemporaryAccessPolicy returns a policy for temporary access with expiration
func (t *S3PolicyTemplates) GetTemporaryAccessPolicy(bucketName string, expirationHours int) *policy.PolicyDocument {
	expirationTime := time.Now().Add(time.Duration(expirationHours) * time.Hour)

	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "TemporaryS3Access",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:ListBucket",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
					"arn:aws:s3:::" + bucketName + "/*",
				},
				Condition: map[string]map[string]interface{}{
					"DateLessThan": map[string]interface{}{
						"aws:CurrentTime": expirationTime.UTC().Format("2006-01-02T15:04:05Z"),
					},
				},
			},
		},
	}
}

// GetContentTypeRestrictedPolicy returns a policy that restricts uploads to specific content types
func (t *S3PolicyTemplates) GetContentTypeRestrictedPolicy(bucketName string, allowedContentTypes []string) *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "ContentTypeRestrictedUpload",
				Effect: "Allow",
				Action: []string{
					"s3:PutObject",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName + "/*",
				},
				Condition: map[string]map[string]interface{}{
					"StringEquals": map[string]interface{}{
						"s3:content-type": allowedContentTypes,
					},
				},
			},
			{
				Sid:    "ReadAccess",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:ListBucket",
				},
				Resource: []string{
					"arn:aws:s3:::" + bucketName,
					"arn:aws:s3:::" + bucketName + "/*",
				},
			},
		},
	}
}

// GetDenyDeletePolicy returns a policy that allows all operations except delete
func (t *S3PolicyTemplates) GetDenyDeletePolicy() *policy.PolicyDocument {
	return &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "AllowAllExceptDelete",
				Effect: "Allow",
				Action: []string{
					"s3:GetObject",
					"s3:GetObjectVersion",
					"s3:PutObject",
					"s3:PutObjectAcl",
					"s3:ListBucket",
					"s3:ListBucketVersions",
					"s3:CreateMultipartUpload",
					"s3:UploadPart",
					"s3:CompleteMultipartUpload",
					"s3:AbortMultipartUpload",
					"s3:ListMultipartUploads",
					"s3:ListParts",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
			{
				Sid:    "DenyDeleteOperations",
				Effect: "Deny",
				Action: []string{
					"s3:DeleteObject",
					"s3:DeleteObjectVersion",
					"s3:DeleteBucket",
				},
				Resource: []string{
					"arn:aws:s3:::*",
					"arn:aws:s3:::*/*",
				},
			},
		},
	}
}

// Helper function to format hour with leading zero
func formatHour(hour int) string {
	if hour < 10 {
		return "0" + string(rune('0'+hour))
	}
	return string(rune('0'+hour/10)) + string(rune('0'+hour%10))
}

// PolicyTemplateDefinition represents metadata about a policy template
type PolicyTemplateDefinition struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Category    string                 `json:"category"`
	UseCase     string                 `json:"use_case"`
	Parameters  []PolicyTemplateParam  `json:"parameters,omitempty"`
	Policy      *policy.PolicyDocument `json:"policy"`
}

// PolicyTemplateParam represents a parameter for customizing policy templates
type PolicyTemplateParam struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Description  string `json:"description"`
	Required     bool   `json:"required"`
	DefaultValue string `json:"default_value,omitempty"`
	Example      string `json:"example,omitempty"`
}

// GetAllPolicyTemplates returns all available policy templates with metadata
func (t *S3PolicyTemplates) GetAllPolicyTemplates() []PolicyTemplateDefinition {
	return []PolicyTemplateDefinition{
		{
			Name:        "S3ReadOnlyAccess",
			Description: "Provides read-only access to all S3 buckets and objects",
			Category:    "Basic Access",
			UseCase:     "Data consumers, backup services, monitoring applications",
			Policy:      t.GetS3ReadOnlyPolicy(),
		},
		{
			Name:        "S3WriteOnlyAccess",
			Description: "Provides write-only access to all S3 buckets and objects",
			Category:    "Basic Access",
			UseCase:     "Data ingestion services, backup applications",
			Policy:      t.GetS3WriteOnlyPolicy(),
		},
		{
			Name:        "S3AdminAccess",
			Description: "Provides full administrative access to all S3 resources",
			Category:    "Administrative",
			UseCase:     "S3 administrators, service accounts with full control",
			Policy:      t.GetS3AdminPolicy(),
		},
		{
			Name:        "BucketSpecificRead",
			Description: "Provides read-only access to a specific bucket",
			Category:    "Bucket-Specific",
			UseCase:     "Applications that need access to specific data sets",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket to grant access to",
					Required:    true,
					Example:     "my-data-bucket",
				},
			},
			Policy: t.GetBucketSpecificReadPolicy("${bucketName}"),
		},
		{
			Name:        "BucketSpecificWrite",
			Description: "Provides write-only access to a specific bucket",
			Category:    "Bucket-Specific",
			UseCase:     "Upload services, data ingestion for specific datasets",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket to grant access to",
					Required:    true,
					Example:     "my-upload-bucket",
				},
			},
			Policy: t.GetBucketSpecificWritePolicy("${bucketName}"),
		},
		{
			Name:        "PathBasedAccess",
			Description: "Restricts access to a specific path/prefix within a bucket",
			Category:    "Path-Restricted",
			UseCase:     "Multi-tenant applications, user-specific directories",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket",
					Required:    true,
					Example:     "shared-bucket",
				},
				{
					Name:        "pathPrefix",
					Type:        "string",
					Description: "Path prefix to restrict access to",
					Required:    true,
					Example:     "user123/documents",
				},
			},
			Policy: t.GetPathBasedAccessPolicy("${bucketName}", "${pathPrefix}"),
		},
		{
			Name:        "IPRestrictedAccess",
			Description: "Allows access only from specific IP addresses or ranges",
			Category:    "Security",
			UseCase:     "Corporate networks, office-based access, VPN restrictions",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "allowedCIDRs",
					Type:        "array",
					Description: "List of allowed IP addresses or CIDR ranges",
					Required:    true,
					Example:     "[\"192.168.1.0/24\", \"10.0.0.0/8\"]",
				},
			},
			Policy: t.GetIPRestrictedPolicy([]string{"${allowedCIDRs}"}),
		},
		{
			Name:        "MultipartUploadOnly",
			Description: "Allows only multipart upload operations on a specific bucket",
			Category:    "Upload-Specific",
			UseCase:     "Large file upload services, streaming applications",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket for multipart uploads",
					Required:    true,
					Example:     "large-files-bucket",
				},
			},
			Policy: t.GetMultipartUploadPolicy("${bucketName}"),
		},
		{
			Name:        "PresignedURLAccess",
			Description: "Policy for generating and using presigned URLs",
			Category:    "Presigned URLs",
			UseCase:     "Frontend applications, temporary file sharing",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket for presigned URL access",
					Required:    true,
					Example:     "shared-files-bucket",
				},
			},
			Policy: t.GetPresignedURLPolicy("${bucketName}"),
		},
		{
			Name:        "ContentTypeRestricted",
			Description: "Restricts uploads to specific content types",
			Category:    "Content Control",
			UseCase:     "Image galleries, document repositories, media libraries",
			Parameters: []PolicyTemplateParam{
				{
					Name:        "bucketName",
					Type:        "string",
					Description: "Name of the S3 bucket",
					Required:    true,
					Example:     "media-bucket",
				},
				{
					Name:        "allowedContentTypes",
					Type:        "array",
					Description: "List of allowed MIME content types",
					Required:    true,
					Example:     "[\"image/jpeg\", \"image/png\", \"video/mp4\"]",
				},
			},
			Policy: t.GetContentTypeRestrictedPolicy("${bucketName}", []string{"${allowedContentTypes}"}),
		},
		{
			Name:        "DenyDeleteAccess",
			Description: "Allows all operations except delete (immutable storage)",
			Category:    "Data Protection",
			UseCase:     "Compliance storage, audit logs, backup retention",
			Policy:      t.GetDenyDeletePolicy(),
		},
	}
}

// GetPolicyTemplateByName returns a specific policy template by name
func (t *S3PolicyTemplates) GetPolicyTemplateByName(name string) *PolicyTemplateDefinition {
	templates := t.GetAllPolicyTemplates()
	for _, template := range templates {
		if template.Name == name {
			return &template
		}
	}
	return nil
}

// GetPolicyTemplatesByCategory returns all policy templates in a specific category
func (t *S3PolicyTemplates) GetPolicyTemplatesByCategory(category string) []PolicyTemplateDefinition {
	var result []PolicyTemplateDefinition
	templates := t.GetAllPolicyTemplates()
	for _, template := range templates {
		if template.Category == category {
			result = append(result, template)
		}
	}
	return result
}
