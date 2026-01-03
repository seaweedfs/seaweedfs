# Fix for GitHub Issue #7941: AWS SDK Signature V4 with STS Credentials

## Problem

AWS SDK clients were failing with `InvalidAccessKeyId` when using temporary credentials obtained from `AssumeRoleWithWebIdentity`. The issue occurred because:

1. SeaweedFS STS service correctly returns temporary credentials (AccessKeyId, SecretAccessKey, SessionToken)
2. AWS SDKs send requests with these credentials using AWS Signature V4:
   - Authorization header contains the temporary AccessKeyId
   - X-Amz-Security-Token header contains the JWT session token
3. SeaweedFS was only checking the `accessKeyIdent` map for the AccessKeyId, which doesn't contain temporary STS credentials (they're stateless, stored in JWT tokens)
4. This caused the lookup to fail with `InvalidAccessKeyId` error

## Root Cause

The authentication flow in `auth_signature_v4.go` was:
```
getRequestAuthType() → authTypeSigned
reqSignatureV4Verify() → verifyV4Signature()
verifyV4Signature() → lookupByAccessKey(accessKey) → FAILS
```

The code never checked for the `X-Amz-Security-Token` header, which is required for STS temporary credentials.

## Solution

Modified `verifyV4Signature()` in `weed/s3api/auth_signature_v4.go` to:

1. Check for `X-Amz-Security-Token` header (or query parameter for presigned URLs)
2. If present, validate the session token using the STS service
3. Extract the temporary credentials from the JWT session token
4. Use those credentials for signature verification
5. If no session token, fall back to normal access key lookup

Added new function `validateSTSSessionToken()` that:
- Validates the JWT session token using the STS service
- Extracts AccessKeyId and SecretAccessKey from the session
- Verifies the access key in the request matches the one in the token
- Checks session expiration
- Returns an Identity and Credential for use in signature verification

## Files Modified

1. **weed/s3api/auth_signature_v4.go**
   - Modified `verifyV4Signature()` to check for X-Amz-Security-Token
   - Added `validateSTSSessionToken()` function

2. **weed/s3api/auth_sts_session_token_test.go** (new file)
   - Added tests to verify X-Amz-Security-Token header detection
   - Tests for both standard requests and presigned URLs
   - Tests for requests with and without session tokens

## Testing

All existing tests pass, including:
- Signature V4 tests
- Authentication tests
- Presigned URL tests

New tests added specifically for STS session token handling:
- `TestSTSSessionTokenHeaderDetection`: Verifies session token extraction
- `TestXAmzSecurityTokenInCanonicalRequest`: Verifies token handling in signature verification

## AWS SDK Compatibility

This fix enables full AWS SDK compatibility with STS temporary credentials:

```python
import boto3

# Get temporary credentials from AssumeRoleWithWebIdentity
# (this already worked)

# Use credentials with AWS SDK (this now works!)
client = boto3.client('s3',
    aws_access_key_id='AKIA593f0bfac081db46',
    aws_secret_access_key='...',
    aws_session_token='eyJhbGciOiJIUzI1NiIs...',  # Now properly handled!
    endpoint_url='http://seaweedfs:8333/'
)

client.list_buckets()  # ✅ Works!
```

## Implementation Notes

- The fix maintains backward compatibility - requests without session tokens continue to work as before
- Session token validation leverages the existing STS service infrastructure
- The solution is stateless - no session storage required, all info is in the JWT
- Supports both header-based and query-parameter-based session tokens (for presigned URLs)

## References

- GitHub Issue: https://github.com/seaweedfs/seaweedfs/issues/7941
- AWS STS AssumeRoleWithWebIdentity: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html
- AWS Signature V4 with session tokens: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
