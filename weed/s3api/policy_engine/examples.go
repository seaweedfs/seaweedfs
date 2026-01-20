//go:build ignore
// +build ignore

package policy_engine

import (
	"encoding/json"
	"fmt"
)

// This file contains examples and documentation for the policy engine

// ExampleIdentityJSON shows the existing identities.json format (unchanged)
var ExampleIdentityJSON = `{
	"identities": [
		{
			"name": "user1",
			"credentials": [
				{
					"accessKey": "AKIAIOSFODNN7EXAMPLE",
					"secretKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
				}
			],
			"actions": [
				"Read:bucket1/*",
				"Write:bucket1/*",
				"Admin:bucket2"
			]
		},
		{
			"name": "readonly-user",
			"credentials": [
				{
					"accessKey": "AKIAI44QH8DHBEXAMPLE",
					"secretKey": "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY"
				}
			],
			"actions": [
				"Read:bucket1/*",
				"List:bucket1"
			]
		}
	]
}`

// ExampleBucketPolicy shows an AWS S3 bucket policy with conditions
var ExampleBucketPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowGetObjectFromSpecificIP",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::my-bucket/*",
			"Condition": {
				"IpAddress": {
					"aws:SourceIp": "192.168.1.0/24"
				}
			}
		},
		{
			"Sid": "AllowPutObjectWithSSL",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::my-bucket/*",
			"Condition": {
				"Bool": {
					"aws:SecureTransport": "true"
				}
			}
		},
		{
			"Sid": "DenyDeleteFromProduction",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:DeleteObject",
			"Resource": "arn:aws:s3:::my-bucket/production/*"
		}
	]
}`

// ExampleTimeBasedPolicy shows a policy with time-based conditions
var ExampleTimeBasedPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowAccessDuringBusinessHours",
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": "arn:aws:s3:::my-bucket/*",
			"Condition": {
				"DateGreaterThan": {
					"aws:RequestTime": "2023-01-01T08:00:00Z"
				},
				"DateLessThan": {
					"aws:RequestTime": "2023-12-31T18:00:00Z"
				}
			}
		}
	]
}`

// ExampleIPRestrictedPolicy shows a policy with IP restrictions
var ExampleIPRestrictedPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowFromOfficeNetwork",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::my-bucket",
				"arn:aws:s3:::my-bucket/*"
			],
			"Condition": {
				"IpAddress": {
					"aws:SourceIp": [
						"203.0.113.0/24",
						"198.51.100.0/24"
					]
				}
			}
		},
		{
			"Sid": "DenyFromRestrictedIPs",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "*",
			"Resource": "*",
			"Condition": {
				"IpAddress": {
					"aws:SourceIp": [
						"192.0.2.0/24"
					]
				}
			}
		}
	]
}`

// ExamplePublicReadPolicy shows a policy for public read access
var ExamplePublicReadPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "PublicReadGetObject",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::my-public-bucket/*"
		}
	]
}`

// ExampleCORSPolicy shows a policy with CORS-related conditions
var ExampleCORSPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowCrossOriginRequests",
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": "arn:aws:s3:::my-bucket/*",
			"Condition": {
				"StringLike": {
					"aws:Referer": [
						"https://example.com/*",
						"https://*.example.com/*"
					]
				}
			}
		}
	]
}`

// ExampleUserAgentPolicy shows a policy with user agent restrictions
var ExampleUserAgentPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowSpecificUserAgents",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::my-bucket/*",
			"Condition": {
				"StringLike": {
					"aws:UserAgent": [
						"MyApp/*",
						"curl/*"
					]
				}
			}
		}
	]
}`

// ExamplePrefixBasedPolicy shows a policy with prefix-based access
var ExamplePrefixBasedPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowUserFolderAccess",
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
			"Resource": "arn:aws:s3:::my-bucket/${aws:username}/*",
			"Condition": {
				"StringEquals": {
					"s3:prefix": "${aws:username}/"
				}
			}
		}
	]
}`

// ExampleMultiStatementPolicy shows a complex policy with multiple statements
var ExampleMultiStatementPolicy = `{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowListBucket",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::my-bucket",
			"Condition": {
				"StringEquals": {
					"s3:prefix": "public/"
				}
			}
		},
		{
			"Sid": "AllowGetPublicObjects",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::my-bucket/public/*"
		},
		{
			"Sid": "AllowAuthenticatedUpload",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::my-bucket/uploads/*",
			"Condition": {
				"StringEquals": {
					"s3:x-amz-acl": "private"
				}
			}
		},
		{
			"Sid": "DenyInsecureConnections",
			"Effect": "Deny",
			"Principal": "*",
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::my-bucket",
				"arn:aws:s3:::my-bucket/*"
			],
			"Condition": {
				"Bool": {
					"aws:SecureTransport": "false"
				}
			}
		}
	]
}`

// GetAllExamples returns all example policies
func GetAllExamples() map[string]string {
	return map[string]string{
		"basic-bucket-policy":    ExampleBucketPolicy,
		"time-based-policy":      ExampleTimeBasedPolicy,
		"ip-restricted-policy":   ExampleIPRestrictedPolicy,
		"public-read-policy":     ExamplePublicReadPolicy,
		"cors-policy":            ExampleCORSPolicy,
		"user-agent-policy":      ExampleUserAgentPolicy,
		"prefix-based-policy":    ExamplePrefixBasedPolicy,
		"multi-statement-policy": ExampleMultiStatementPolicy,
	}
}

// ValidateExamplePolicies validates all example policies
func ValidateExamplePolicies() error {
	examples := GetAllExamples()

	for name, policyJSON := range examples {
		_, err := ParsePolicy(policyJSON)
		if err != nil {
			return fmt.Errorf("invalid example policy %s: %v", name, err)
		}
	}

	return nil
}

// GetExamplePolicy returns a specific example policy
func GetExamplePolicy(name string) (string, error) {
	examples := GetAllExamples()

	policy, exists := examples[name]
	if !exists {
		return "", fmt.Errorf("example policy %s not found", name)
	}

	return policy, nil
}

// CreateExamplePolicyDocument creates a PolicyDocument from an example
func CreateExamplePolicyDocument(name string) (*PolicyDocument, error) {
	policyJSON, err := GetExamplePolicy(name)
	if err != nil {
		return nil, err
	}

	return ParsePolicy(policyJSON)
}

// PrintExamplePolicyPretty prints an example policy in pretty format
func PrintExamplePolicyPretty(name string) error {
	policyJSON, err := GetExamplePolicy(name)
	if err != nil {
		return err
	}

	var policy interface{}
	if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
		return err
	}

	prettyJSON, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return err
	}

	fmt.Printf("Example Policy: %s\n", name)
	fmt.Printf("================\n")
	fmt.Println(string(prettyJSON))

	return nil
}

// ExampleUsage demonstrates how to use the policy engine
func ExampleUsage() {
	// Create a new policy engine
	engine := NewPolicyEngine()

	// Set a bucket policy
	policyJSON := ExampleBucketPolicy
	err := engine.SetBucketPolicy("my-bucket", policyJSON)
	if err != nil {
		fmt.Printf("Error setting bucket policy: %v\n", err)
		return
	}

	// Evaluate a policy
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::my-bucket/test-object",
		Principal: "*",
		Conditions: map[string][]string{
			"aws:SourceIp": {"192.168.1.100"},
		},
	}

	result := engine.EvaluatePolicy("my-bucket", args)

	switch result {
	case PolicyResultAllow:
		fmt.Println("Access allowed")
	case PolicyResultDeny:
		fmt.Println("Access denied")
	case PolicyResultIndeterminate:
		fmt.Println("Access indeterminate")
	}
}

// ExampleLegacyIntegration demonstrates backward compatibility
func ExampleLegacyIntegration() {
	// Legacy identity actions
	legacyActions := []string{
		"Read:bucket1/*",
		"Write:bucket1/uploads/*",
		"Admin:bucket2",
	}

	// Convert to policy
	policy, err := ConvertIdentityToPolicy(legacyActions, "bucket1")
	if err != nil {
		fmt.Printf("Error converting identity to policy: %v\n", err)
		return
	}

	// Create policy-backed IAM
	policyIAM := NewPolicyBackedIAM()

	// Set the converted policy
	policyJSON, _ := json.MarshalIndent(policy, "", "  ")
	err = policyIAM.SetBucketPolicy("bucket1", string(policyJSON))
	if err != nil {
		fmt.Printf("Error setting bucket policy: %v\n", err)
		return
	}

	fmt.Println("Legacy identity successfully converted to AWS S3 policy")
}

// ExampleConditions demonstrates various condition types
func ExampleConditions() {
	examples := map[string]string{
		"StringEquals":    `"StringEquals": {"s3:prefix": "documents/"}`,
		"StringLike":      `"StringLike": {"aws:UserAgent": "MyApp/*"}`,
		"NumericEquals":   `"NumericEquals": {"s3:max-keys": "10"}`,
		"NumericLessThan": `"NumericLessThan": {"s3:max-keys": "1000"}`,
		"DateGreaterThan": `"DateGreaterThan": {"aws:RequestTime": "2023-01-01T00:00:00Z"}`,
		"DateLessThan":    `"DateLessThan": {"aws:RequestTime": "2023-12-31T23:59:59Z"}`,
		"IpAddress":       `"IpAddress": {"aws:SourceIp": "192.168.1.0/24"}`,
		"NotIpAddress":    `"NotIpAddress": {"aws:SourceIp": "10.0.0.0/8"}`,
		"Bool":            `"Bool": {"aws:SecureTransport": "true"}`,
		"Null":            `"Null": {"s3:x-amz-server-side-encryption": "false"}`,
	}

	fmt.Println("Supported Condition Operators:")
	fmt.Println("==============================")

	for operator, example := range examples {
		fmt.Printf("%s: %s\n", operator, example)
	}
}

// ExampleMigrationStrategy demonstrates migration from legacy to policy-based system
func ExampleMigrationStrategy() {
	fmt.Println("Migration Strategy:")
	fmt.Println("==================")
	fmt.Println("1. Keep existing identities.json unchanged")
	fmt.Println("2. Legacy actions are automatically converted to AWS policies internally")
	fmt.Println("3. Add bucket policies for advanced features:")
	fmt.Println("   - IP restrictions")
	fmt.Println("   - Time-based access")
	fmt.Println("   - SSL-only access")
	fmt.Println("   - User agent restrictions")
	fmt.Println("4. Policy evaluation precedence:")
	fmt.Println("   - Explicit Deny (highest priority)")
	fmt.Println("   - Explicit Allow")
	fmt.Println("   - Default Deny (lowest priority)")
}

// PrintAllExamples prints all example policies
func PrintAllExamples() {
	examples := GetAllExamples()

	for name := range examples {
		fmt.Printf("\n")
		PrintExamplePolicyPretty(name)
		fmt.Printf("\n")
	}
}
