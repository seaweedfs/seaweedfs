# Governance Permission Implementation

This document explains the implementation of `s3:BypassGovernanceRetention` permission in SeaweedFS, providing AWS S3-compatible governance retention bypass functionality.

## Overview

The governance permission system enables proper AWS S3-compatible object retention with governance mode bypass capabilities. This implementation ensures that only users with the appropriate permissions can bypass governance retention, while maintaining security and compliance requirements.

## Features

### 1. Permission-Based Bypass Control

- **s3:BypassGovernanceRetention**: New permission that allows users to bypass governance retention
- **Admin Override**: Admin users can always bypass governance retention
- **Header Detection**: Automatic detection of `x-amz-bypass-governance-retention` header
- **Permission Validation**: Validates user permissions before allowing bypass

### 2. Retention Mode Support

- **GOVERNANCE Mode**: Can be bypassed with proper permission and header
- **COMPLIANCE Mode**: Cannot be bypassed (highest security level)
- **Legal Hold**: Always blocks operations regardless of permissions

### 3. Integration Points

- **DELETE Operations**: Checks governance permissions before object deletion
- **PUT Operations**: Validates permissions before object overwrite
- **Retention Modification**: Ensures proper permissions for retention changes

## Implementation Details

### Core Components

1. **Permission Checker**
   ```go
   func (s3a *S3ApiServer) checkGovernanceBypassPermission(r *http.Request, bucket, object string) bool
   ```
   - Checks if user has `s3:BypassGovernanceRetention` permission
   - Validates admin status
   - Integrates with existing IAM system

2. **Object Lock Permission Validation**
   ```go
   func (s3a *S3ApiServer) checkObjectLockPermissions(r *http.Request, bucket, object, versionId string, bypassGovernance bool) error
   ```
   - Validates governance bypass permissions
   - Checks retention mode (GOVERNANCE vs COMPLIANCE)
   - Enforces legal hold restrictions

3. **IAM Integration**
   - Added `ACTION_BYPASS_GOVERNANCE_RETENTION` constant
   - Updated policy engine with `s3:BypassGovernanceRetention` action
   - Integrated with existing identity-based access control

### Permission Flow

```
Request with x-amz-bypass-governance-retention: true
        ↓
Check if object is under retention
        ↓
If GOVERNANCE mode:
    ↓
Check if user has s3:BypassGovernanceRetention permission
    ↓
If permission granted: Allow operation
If permission denied: Deny operation
        ↓
If COMPLIANCE mode: Always deny
```

## Configuration

### 1. Identity-Based Configuration

Add governance bypass permission to user actions in `identities.json`:

```json
{
  "identities": [
    {
      "name": "governance-admin",
      "credentials": [{"accessKey": "admin123", "secretKey": "secret123"}],
      "actions": [
        "Read:my-bucket/*",
        "Write:my-bucket/*",
        "BypassGovernanceRetention:my-bucket/*"
      ]
    }
  ]
}
```

### 2. Bucket Policy Configuration

Grant governance bypass permission via bucket policies:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:BypassGovernanceRetention",
      "Resource": "arn:aws:s3:::bucket/*"
    }
  ]
}
```

**Note**: The policy version should use the standard AWS policy version `PolicyVersion2012_10_17` constant (which equals `"2012-10-17"`).

## Usage Examples

### 1. Delete Object with Governance Bypass

```bash
# User with bypass permission
aws s3api delete-object \
    --bucket my-bucket \
    --key my-object \
    --bypass-governance-retention

# Admin user (always allowed)
aws s3api delete-object \
    --bucket my-bucket \
    --key my-object \
    --bypass-governance-retention
```

### 2. Update Object Retention

```bash
# Extend retention period (requires bypass permission for governance mode)
aws s3api put-object-retention \
    --bucket my-bucket \
    --key my-object \
    --retention Mode=GOVERNANCE,RetainUntilDate=2025-01-01T00:00:00Z \
    --bypass-governance-retention
```

### 3. Bulk Object Deletion

```bash
# Delete multiple objects with governance bypass
aws s3api delete-objects \
    --bucket my-bucket \
    --delete file://delete-objects.json \
    --bypass-governance-retention
```

## Error Handling

### Permission Errors

- **ErrAccessDenied**: User lacks `s3:BypassGovernanceRetention` permission
- **ErrGovernanceModeActive**: Governance mode protection without bypass
- **ErrComplianceModeActive**: Compliance mode cannot be bypassed

### Example Error Response

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>AccessDenied</Code>
    <Message>User does not have permission to bypass governance retention</Message>
    <RequestId>abc123</RequestId>
    <Resource>/my-bucket/my-object</Resource>
</Error>
```

## Security Considerations

### 1. Least Privilege Principle

- Grant bypass permission only to users who absolutely need it
- Use bucket-specific permissions rather than global permissions
- Regularly audit users with bypass permissions

### 2. Compliance Mode Protection

- COMPLIANCE mode objects cannot be bypassed by any user
- Use COMPLIANCE mode for regulatory requirements
- GOVERNANCE mode provides flexibility while maintaining audit trails

### 3. Admin Privileges

- Admin users can always bypass governance retention
- Ensure admin access is properly secured
- Use admin privileges responsibly

## Testing

### Unit Tests

```bash
# Run governance permission tests
go test -v ./weed/s3api/ -run TestGovernance

# Run all object retention tests
go test -v ./weed/s3api/ -run TestObjectRetention
```

### Integration Tests

```bash
# Test with real S3 clients
cd test/s3/retention
go test -v ./... -run TestGovernanceBypass
```

## AWS Compatibility

This implementation provides full AWS S3 compatibility for:

- ✅ `x-amz-bypass-governance-retention` header support
- ✅ `s3:BypassGovernanceRetention` permission
- ✅ GOVERNANCE vs COMPLIANCE mode behavior
- ✅ Legal hold enforcement
- ✅ Error responses and codes
- ✅ Bucket policy integration
- ✅ IAM policy integration

## Troubleshooting

### Common Issues

1. **User cannot bypass governance retention**
   - Check if user has `s3:BypassGovernanceRetention` permission
   - Verify the header `x-amz-bypass-governance-retention: true` is set
   - Ensure object is in GOVERNANCE mode (not COMPLIANCE)

2. **Admin bypass not working**
   - Verify user has admin privileges in the IAM system
   - Check that object is not under legal hold
   - Ensure versioning is enabled on the bucket

3. **Policy not taking effect**
   - Verify bucket policy JSON syntax
   - Check resource ARN format
   - Ensure principal has proper format

## Future Enhancements

- [ ] AWS STS integration for temporary credentials
- [ ] CloudTrail-compatible audit logging
- [ ] Advanced condition evaluation (IP, time, etc.)
- [ ] Integration with external identity providers
- [ ] Fine-grained permissions for different retention operations 