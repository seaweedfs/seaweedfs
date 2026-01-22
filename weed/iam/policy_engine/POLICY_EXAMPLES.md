# Policy Engine Examples

This document contains examples of how to use the SeaweedFS Policy Engine.

## Overview

The examples in `examples.go` demonstrate various policy configurations and usage patterns. The examples file is excluded from production builds using build tags to reduce binary size.

## To Use Examples

If you need to use the examples during development or testing, you can:

1. **Remove the build tag**: Remove the `//go:build ignore` and `// +build ignore` lines from `examples.go`
2. **Use during development**: The examples are available during development but not in production builds
3. **Copy specific examples**: Copy the JSON examples you need into your own code

## Example Categories

The examples file includes:

- **Legacy Identity Format**: Examples of existing identities.json format
- **Policy Documents**: Various AWS S3-compatible policy examples
- **Condition Examples**: Complex condition-based policies
- **Migration Examples**: How to migrate from legacy to policy-based IAM
- **Integration Examples**: How to integrate with existing systems

## Usage Functions

The examples file provides helper functions:

- `GetAllExamples()`: Returns all example policies
- `ValidateExamplePolicies()`: Validates all examples
- `GetExamplePolicy(name)`: Gets a specific example
- `CreateExamplePolicyDocument(name)`: Creates a policy document
- `PrintExamplePolicyPretty(name)`: Pretty-prints an example
- `ExampleUsage()`: Shows basic usage patterns
- `ExampleLegacyIntegration()`: Shows legacy integration
- `ExampleConditions()`: Shows condition usage
- `ExampleMigrationStrategy()`: Shows migration approach

## To Enable Examples in Development

```go
// Remove build tags from examples.go, then:
import "github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"

// Use examples
examples := policy_engine.GetAllExamples()
policy, err := policy_engine.GetExamplePolicy("read-only-user")
```

## Note

The examples are excluded from production builds to keep binary size minimal. They are available for development and testing purposes only. 