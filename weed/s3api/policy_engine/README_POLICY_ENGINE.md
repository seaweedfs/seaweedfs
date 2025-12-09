# SeaweedFS Policy Evaluation Engine

This document describes the comprehensive policy evaluation engine that has been added to SeaweedFS, providing AWS S3-compatible policy support while maintaining full backward compatibility with existing `identities.json` configuration.

## Overview

The policy engine provides:
- **Full AWS S3 policy compatibility** - JSON policies with conditions, wildcards, and complex logic
- **Backward compatibility** - Existing `identities.json` continues to work unchanged
- **Bucket policies** - Per-bucket access control policies
- **IAM policies** - User and group-level policies
- **Condition evaluation** - IP restrictions, time-based access, SSL-only, etc.
- **AWS-compliant evaluation order** - Explicit Deny > Explicit Allow > Default Deny

## Architecture

### Files Created

1. **`policy_engine/types.go`** - Core policy data structures and validation
2. **`policy_engine/conditions.go`** - Condition evaluators (StringEquals, IpAddress, etc.)
3. **`policy_engine/engine.go`** - Main policy evaluation engine
4. **`policy_engine/integration.go`** - Integration with existing IAM system
5. **`policy_engine/engine_test.go`** - Comprehensive tests
6. **`policy_engine/examples.go`** - Usage examples and documentation (excluded from builds)
7. **`policy_engine/wildcard_matcher.go`** - Optimized wildcard pattern matching
8. **`policy_engine/wildcard_matcher_test.go`** - Wildcard matching tests

### Key Components

```
PolicyEngine
├── Bucket Policies (per-bucket JSON policies)
├── User Policies (converted from identities.json + new IAM policies)
├── Condition Evaluators (IP, time, string, numeric, etc.)
└── Evaluation Logic (AWS-compliant precedence)
```

## Backward Compatibility

### Existing identities.json (No Changes Required)

Your existing configuration continues to work exactly as before:

```json
{
  "identities": [
    {
      "name": "readonly_user",
      "credentials": [{"accessKey": "key123", "secretKey": "secret123"}],
      "actions": ["Read:public-bucket/*", "List:public-bucket"]
    }
  ]
}
```

Legacy actions are automatically converted to AWS-style policies:
- `Read:bucket/*` → `s3:GetObject` on `arn:aws:s3:::bucket/*`
- `Write:bucket` → `s3:PutObject`, `s3:DeleteObject` on `arn:aws:s3:::bucket/*`
- `Admin` → `s3:*` on `arn:aws:s3:::*`

## New Capabilities

### 1. Bucket Policies

Set bucket-level policies using standard S3 API:

```bash
# Set bucket policy
curl -X PUT "http://localhost:8333/bucket?policy" \
     -H "Authorization: AWS access_key:signature" \
     -d '{
       "Version": "2012-10-17",
       "Statement": [
         {
           "Effect": "Allow",
           "Principal": "*",
           "Action": "s3:GetObject",
           "Resource": "arn:aws:s3:::bucket/*"
         }
       ]
     }'

# Get bucket policy
curl "http://localhost:8333/bucket?policy"

# Delete bucket policy
curl -X DELETE "http://localhost:8333/bucket?policy"
```

### 2. Advanced Conditions

Support for all AWS condition operators:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::secure-bucket/*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": ["192.168.1.0/24", "10.0.0.0/8"]
        },
        "Bool": {
          "aws:SecureTransport": "true"
        },
        "DateGreaterThan": {
          "aws:CurrentTime": "2023-01-01T00:00:00Z"
        }
      }
    }
  ]
}
```

### 3. Supported Condition Operators

- **String**: `StringEquals`, `StringNotEquals`, `StringLike`, `StringNotLike`
- **Numeric**: `NumericEquals`, `NumericLessThan`, `NumericGreaterThan`, etc.
- **Date**: `DateEquals`, `DateLessThan`, `DateGreaterThan`, etc.
- **IP**: `IpAddress`, `NotIpAddress` (supports CIDR notation)
- **Boolean**: `Bool`
- **ARN**: `ArnEquals`, `ArnLike`
- **Null**: `Null`

### 4. Condition Keys

Standard AWS condition keys are supported:
- `aws:CurrentTime` - Current request time
- `aws:SourceIp` - Client IP address
- `aws:SecureTransport` - Whether HTTPS is used
- `aws:UserAgent` - Client user agent
- `s3:x-amz-acl` - Requested ACL
- `s3:VersionId` - Object version ID
- `s3:ExistingObjectTag/<tag-key>` - Value of an existing object tag (see example below)
- And many more...

### 5. Object Tag-Based Access Control

You can control access based on object tags using `s3:ExistingObjectTag/<tag-key>`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*",
      "Condition": {
        "StringEquals": {
          "s3:ExistingObjectTag/status": ["public"]
        }
      }
    }
  ]
}
```

This allows anonymous access only to objects that have a tag `status=public`.

## Policy Evaluation

### Evaluation Order (AWS-Compatible)

1. **Explicit Deny** - If any policy explicitly denies access → **DENY**
2. **Explicit Allow** - If any policy explicitly allows access → **ALLOW**  
3. **Default Deny** - If no policy matches → **DENY**

### Policy Sources (Evaluated Together)

1. **Bucket Policies** - Stored per-bucket, highest priority
2. **User Policies** - Converted from `identities.json` + new IAM policies
3. **Legacy IAM** - For backward compatibility (lowest priority)

## Examples

### Public Read Bucket

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "PublicRead",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::public-bucket/*"
    }
  ]
}
```

### IP-Restricted Bucket

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::secure-bucket/*",
      "Condition": {
        "IpAddress": {
          "aws:SourceIp": "192.168.1.0/24"
        }
      }
    }
  ]
}
```

### SSL-Only Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": ["arn:aws:s3:::ssl-bucket/*", "arn:aws:s3:::ssl-bucket"],
      "Condition": {
        "Bool": {
          "aws:SecureTransport": "false"
        }
      }
    }
  ]
}
```

### Tag-Based Access Control

Allow public read only for objects tagged as public:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*",
      "Condition": {
        "StringEquals": {
          "s3:ExistingObjectTag/visibility": ["public"]
        }
      }
    }
  ]
}
```

Deny access to confidential objects:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*"
    },
    {
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*",
      "Condition": {
        "StringEquals": {
          "s3:ExistingObjectTag/classification": ["confidential", "secret"]
        }
      }
    }
  ]
}
```

## Integration

### For Existing SeaweedFS Users

1. **No changes required** - Your existing setup continues to work
2. **Optional enhancement** - Add bucket policies for fine-grained control
3. **Gradual migration** - Move to full AWS policies over time

### For New Users

1. Start with either `identities.json` or AWS-style policies
2. Use bucket policies for complex access patterns
3. Full feature parity with AWS S3 policies

## Testing

Run the policy engine tests:

```bash
# Core policy tests
go test -v -run TestPolicyEngine

# Condition evaluator tests  
go test -v -run TestConditionEvaluators

# Legacy compatibility tests
go test -v -run TestConvertIdentityToPolicy

# Validation tests
go test -v -run TestPolicyValidation
```

## Performance

- **Compiled patterns** - Regex patterns are pre-compiled for fast matching
- **Cached policies** - Policies are cached in memory with TTL
- **Early termination** - Evaluation stops on first explicit deny
- **Minimal overhead** - Backward compatibility with minimal performance impact

## Migration Path

### Phase 1: Backward Compatible (Current)
- Keep existing `identities.json` unchanged
- Add bucket policies as needed
- Legacy actions automatically converted to AWS policies

### Phase 2: Enhanced (Optional)
- Add advanced conditions to policies
- Use full AWS S3 policy features
- Maintain backward compatibility

### Phase 3: Full Migration (Future)
- Migrate to pure IAM policies
- Use AWS CLI/SDK for policy management
- Complete AWS S3 feature parity

## Compatibility

- Full backward compatibility with existing `identities.json`
- AWS S3 API compatibility for bucket policies
- Standard condition operators and keys
- Proper evaluation precedence (Deny > Allow > Default Deny)
- Performance optimized with caching and compiled patterns

The policy engine provides a seamless upgrade path from SeaweedFS's existing simple IAM system to full AWS S3-compatible policies, giving you the best of both worlds: simplicity for basic use cases and power for complex enterprise scenarios.

## Feature Status

### Implemented

| Feature | Description |
|---------|-------------|
| Bucket Policies | Full AWS S3-compatible bucket policies |
| Condition Operators | StringEquals, IpAddress, Bool, DateGreaterThan, etc. |
| `aws:SourceIp` | IP-based access control with CIDR support |
| `aws:SecureTransport` | Require HTTPS |
| `aws:CurrentTime` | Time-based access control |
| `s3:ExistingObjectTag/<key>` | Tag-based access control for existing objects |
| Wildcard Patterns | Support for `*` and `?` in actions and resources |
| Principal Matching | `*`, account IDs, and user ARNs |

### Planned

| Feature | GitHub Issue |
|---------|--------------|
| `s3:RequestObjectTag/<key>` | For tag conditions on PUT requests |
| `s3:RequestObjectTagKeys` | Check which tag keys are in request |
| `s3:x-amz-content-sha256` | Content hash condition |
| `s3:x-amz-server-side-encryption` | SSE condition |
| `s3:x-amz-storage-class` | Storage class condition |
| Cross-account access | Access across different accounts |
| VPC Endpoint policies | Network-level policies |

For feature requests or to track progress, see the [GitHub Issues](https://github.com/seaweedfs/seaweedfs/issues). 