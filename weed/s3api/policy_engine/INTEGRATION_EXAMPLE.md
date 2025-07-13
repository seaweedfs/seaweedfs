# Integration Example

This shows how to integrate the new policy engine with the existing S3ApiServer.

## Minimal Integration

```go
// In s3api_server.go - modify NewS3ApiServerWithStore function

func NewS3ApiServerWithStore(router *mux.Router, option *S3ApiServerOption, explicitStore string) (s3ApiServer *S3ApiServer, err error) {
    // ... existing code ...

    // Create traditional IAM
    iam := NewIdentityAccessManagementWithStore(option, explicitStore)

    s3ApiServer = &S3ApiServer{
        option:            option,
        iam:               iam,  // Keep existing for compatibility
        randomClientId:    util.RandomInt32(),
        filerGuard:        security.NewGuard([]string{}, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec),
        cb:                NewCircuitBreaker(option),
        credentialManager: iam.credentialManager,
        bucketConfigCache: NewBucketConfigCache(5 * time.Minute),
    }

    // Optional: Wrap with policy-backed IAM for enhanced features
    if option.EnablePolicyEngine {  // Add this config option
        // Option 1: Create and set legacy IAM separately
        policyBackedIAM := NewPolicyBackedIAM()
        policyBackedIAM.SetLegacyIAM(iam)
        
        // Option 2: Create with legacy IAM in one call (convenience method)
        // policyBackedIAM := NewPolicyBackedIAMWithLegacy(iam)
        
        // Load existing identities as policies
        if err := policyBackedIAM.LoadIdentityPolicies(); err != nil {
            glog.Warningf("Failed to load identity policies: %v", err)
        }
        
        // Replace IAM with policy-backed version
        s3ApiServer.iam = policyBackedIAM
    }

    // ... rest of existing code ...
}
```

## Router Integration

```go
// In registerRouter function, replace bucket policy handlers:

// Old handlers (if they exist):
// bucket.Methods(http.MethodGet).HandlerFunc(s3a.GetBucketPolicyHandler).Queries("policy", "")
// bucket.Methods(http.MethodPut).HandlerFunc(s3a.PutBucketPolicyHandler).Queries("policy", "")
// bucket.Methods(http.MethodDelete).HandlerFunc(s3a.DeleteBucketPolicyHandler).Queries("policy", "")

// New handlers with policy engine:
if policyBackedIAM, ok := s3a.iam.(*PolicyBackedIAM); ok {
    // Use policy-backed handlers
    bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(policyBackedIAM.GetBucketPolicyHandler, ACTION_READ)), "GET")).Queries("policy", "")
    bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(policyBackedIAM.PutBucketPolicyHandler, ACTION_WRITE)), "PUT")).Queries("policy", "")
    bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(policyBackedIAM.DeleteBucketPolicyHandler, ACTION_WRITE)), "DELETE")).Queries("policy", "")
} else {
    // Use existing/fallback handlers
    bucket.Methods(http.MethodGet).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.GetBucketPolicyHandler, ACTION_READ)), "GET")).Queries("policy", "")
    bucket.Methods(http.MethodPut).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.PutBucketPolicyHandler, ACTION_WRITE)), "PUT")).Queries("policy", "")
    bucket.Methods(http.MethodDelete).HandlerFunc(track(s3a.iam.Auth(s3a.cb.Limit(s3a.DeleteBucketPolicyHandler, ACTION_WRITE)), "DELETE")).Queries("policy", "")
}
```

## Configuration Option

Add to `S3ApiServerOption`:

```go
type S3ApiServerOption struct {
    // ... existing fields ...
    EnablePolicyEngine bool  // Add this field
}
```

## Example Usage

### 1. Existing Users (No Changes)

Your existing `identities.json` continues to work:

```json
{
  "identities": [
    {
      "name": "user1",
      "credentials": [{"accessKey": "key1", "secretKey": "secret1"}],
      "actions": ["Read:bucket1/*", "Write:bucket1/uploads/*"]
    }
  ]
}
```

### 2. New Users (Enhanced Policies)

Set bucket policies via S3 API:

```bash
# Allow public read
aws s3api put-bucket-policy --bucket my-bucket --policy file://policy.json

# Where policy.json contains:
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::my-bucket/*"
    }
  ]
}
```

### 3. Advanced Conditions

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
          "aws:SourceIp": "192.168.1.0/24"
        },
        "Bool": {
          "aws:SecureTransport": "true"
        }
      }
    }
  ]
}
```

## Migration Strategy

### Phase 1: Enable Policy Engine (Opt-in)
- Set `EnablePolicyEngine: true` in server options
- Existing `identities.json` automatically converted to policies
- Add bucket policies as needed

### Phase 2: Full Policy Management
- Use AWS CLI/SDK for policy management
- Gradually migrate from `identities.json` to pure IAM policies
- Take advantage of advanced conditions and features

## Testing

```bash
# Test existing functionality
go test -v -run TestCanDo

# Test new policy engine
go test -v -run TestPolicyEngine

# Test integration
go test -v -run TestPolicyBackedIAM
```

The integration is designed to be:
- **Backward compatible** - Existing setups work unchanged
- **Opt-in** - Enable policy engine only when needed
- **Gradual** - Migrate at your own pace
- **AWS compatible** - Use standard AWS tools and patterns 