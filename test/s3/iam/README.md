# SeaweedFS S3 IAM Integration Tests

This directory contains comprehensive integration tests for the SeaweedFS S3 API with Advanced IAM (Identity and Access Management) system integration.

## Overview

**Important**: The STS service uses a **stateless JWT design** where all session information is embedded directly in the JWT token. No external session storage is required.

The S3 IAM integration tests validate the complete end-to-end functionality of:

- **JWT Authentication**: OIDC token-based authentication with S3 API
- **Policy Enforcement**: Fine-grained access control for S3 operations
- **Stateless Session Management**: JWT-based session token validation and expiration (no external storage)
- **Role-Based Access Control (RBAC)**: IAM roles with different permission levels
- **Bucket Policies**: Resource-based access control integration
- **Multipart Upload IAM**: Policy enforcement for multipart operations
- **Contextual Policies**: IP-based, time-based, and conditional access control
- **Presigned URLs**: IAM-integrated temporary access URL generation

## Test Architecture

### Components Tested

1. **S3 API Gateway** - SeaweedFS S3-compatible API server with IAM integration
2. **IAM Manager** - Core IAM orchestration and policy evaluation
3. **STS Service** - Security Token Service for temporary credentials
4. **Policy Engine** - AWS IAM-compatible policy evaluation
5. **Identity Providers** - OIDC and LDAP authentication providers
6. **Policy Store** - Persistent policy storage using SeaweedFS filer

### Test Framework

- **S3IAMTestFramework**: Comprehensive test utilities and setup
- **Mock OIDC Provider**: In-memory OIDC server with JWT signing
- **Service Management**: Automatic SeaweedFS service lifecycle management
- **Resource Cleanup**: Automatic cleanup of buckets and test data

## Test Scenarios

### 1. Authentication Tests (`TestS3IAMAuthentication`)

- ✅ **Valid JWT Token**: Successful authentication with proper OIDC tokens
- ✅ **Invalid JWT Token**: Rejection of malformed or invalid tokens  
- ✅ **Expired JWT Token**: Proper handling of expired authentication tokens

### 2. Policy Enforcement Tests (`TestS3IAMPolicyEnforcement`)

- ✅ **Read-Only Policy**: Users can only read objects and list buckets
- ✅ **Write-Only Policy**: Users can only create/delete objects but not read
- ✅ **Admin Policy**: Full access to all S3 operations including bucket management

### 3. Session Expiration Tests (`TestS3IAMSessionExpiration`)

- ✅ **Short-Lived Sessions**: Creation and validation of time-limited sessions
- ✅ **Manual Expiration**: Testing session expiration enforcement
- ✅ **Expired Session Rejection**: Proper access denial for expired sessions

### 4. Multipart Upload Tests (`TestS3IAMMultipartUploadPolicyEnforcement`)

- ✅ **Admin Multipart Access**: Full multipart upload capabilities
- ✅ **Read-Only Denial**: Rejection of multipart operations for read-only users
- ✅ **Complete Upload Flow**: Initiate → Upload Parts → Complete workflow

### 5. Bucket Policy Tests (`TestS3IAMBucketPolicyIntegration`)

- ✅ **Public Read Policy**: Bucket-level policies allowing public access
- ✅ **Explicit Deny Policy**: Bucket policies that override IAM permissions
- ✅ **Policy CRUD Operations**: Get/Put/Delete bucket policy operations

### 6. Contextual Policy Tests (`TestS3IAMContextualPolicyEnforcement`)

- 🔧 **IP-Based Restrictions**: Source IP validation in policy conditions
- 🔧 **Time-Based Restrictions**: Temporal access control policies
- 🔧 **User-Agent Restrictions**: Request context-based policy evaluation

### 7. Presigned URL Tests (`TestS3IAMPresignedURLIntegration`)

- ✅ **URL Generation**: IAM-validated presigned URL creation
- ✅ **Permission Validation**: Ensuring users have required permissions
- 🔧 **HTTP Request Testing**: Direct HTTP calls to presigned URLs

## Quick Start

### Prerequisites

1. **Go 1.19+** with modules enabled
2. **SeaweedFS Binary** (`weed`) built with IAM support
3. **Test Dependencies**:
   ```bash
   go get github.com/stretchr/testify
   go get github.com/aws/aws-sdk-go
   go get github.com/golang-jwt/jwt/v5
   ```

### Running Tests

#### Complete Test Suite
```bash
# Run all tests with service management
make test

# Quick test run (assumes services running)
make test-quick
```

#### Specific Test Categories
```bash
# Test only authentication
make test-auth

# Test only policy enforcement  
make test-policy

# Test only session expiration
make test-expiration

# Test only multipart uploads
make test-multipart

# Test only bucket policies
make test-bucket-policy
```

#### Development & Debugging
```bash
# Start services and keep running
make debug

# Show service logs
make logs

# Check service status
make status

# Watch for changes and re-run tests
make watch
```

### Manual Service Management

If you prefer to manage services manually:

```bash
# Start services
make start-services

# Wait for services to be ready
make wait-for-services

# Run tests
make run-tests

# Stop services
make stop-services
```

## Configuration

### Test Configuration (`test_config.json`)

The test configuration defines:

- **Identity Providers**: OIDC and LDAP configurations
- **IAM Roles**: Role definitions with trust policies
- **IAM Policies**: Permission policies for different access levels
- **Policy Stores**: Persistent storage configurations for IAM policies and roles

### Service Ports

| Service | Port | Purpose |
|---------|------|---------|
| Master | 9333 | Cluster coordination |
| Volume | 8080 | Object storage |
| Filer | 8888 | Metadata & IAM storage |
| S3 API | 8333 | S3-compatible API with IAM |

### Environment Variables

```bash
# SeaweedFS binary location
export WEED_BINARY=../../../weed

# Service ports (optional)
export S3_PORT=8333
export FILER_PORT=8888  
export MASTER_PORT=9333
export VOLUME_PORT=8080

# Test timeout
export TEST_TIMEOUT=30m

# Log level (0-4)
export LOG_LEVEL=2
```

## Test Data & Cleanup

### Automatic Cleanup

The test framework automatically:
- 🗑️ **Deletes test buckets** created during tests
- 🗑️ **Removes test objects** and multipart uploads
- 🗑️ **Cleans up IAM sessions** and temporary tokens
- 🗑️ **Stops services** after test completion

### Manual Cleanup

```bash
# Clean everything
make clean

# Clean while keeping services running
rm -rf test-volume-data/
```

## Extending Tests

### Adding New Test Scenarios

1. **Create Test Function**:
   ```go
   func TestS3IAMNewFeature(t *testing.T) {
       framework := NewS3IAMTestFramework(t)
       defer framework.Cleanup()
       
       // Test implementation
   }
   ```

2. **Use Test Framework**:
   ```go
   // Create authenticated S3 client
   s3Client, err := framework.CreateS3ClientWithJWT("user", "TestRole")
   require.NoError(t, err)
   
   // Test S3 operations
   err = framework.CreateBucket(s3Client, "test-bucket")
   require.NoError(t, err)
   ```

3. **Add to Makefile**:
   ```makefile
   test-new-feature: ## Test new feature
   	go test -v -run TestS3IAMNewFeature ./...
   ```

### Creating Custom Policies

Add policies to `test_config.json`:

```json
{
  "policies": {
    "CustomPolicy": {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject"],
          "Resource": ["arn:aws:s3:::specific-bucket/*"],
          "Condition": {
            "StringEquals": {
              "s3:prefix": ["allowed-prefix/"]
            }
          }
        }
      ]
    }
  }
}
```

### Adding Identity Providers

1. **Mock Provider Setup**:
   ```go
   // In test framework
   func (f *S3IAMTestFramework) setupCustomProvider() {
       provider := custom.NewCustomProvider("test-custom")
       // Configure and register
   }
   ```

2. **Configuration**:
   ```json
   {
     "providers": {
       "custom": {
         "test-custom": {
           "endpoint": "http://localhost:8080",
           "clientId": "custom-client"
         }
       }
     }
   }
   ```

## Troubleshooting

### Common Issues

#### 1. Services Not Starting
```bash
# Check if ports are available
netstat -an | grep -E "(8333|8888|9333|8080)"

# Check service logs
make logs

# Try different ports
export S3_PORT=18333
make start-services
```

#### 2. JWT Token Issues
```bash
# Verify OIDC mock server
curl http://localhost:8080/.well-known/openid_configuration

# Check JWT token format in logs
make logs | grep -i jwt
```

#### 3. Permission Denied Errors
```bash
# Verify IAM configuration
cat test_config.json | jq '.policies'

# Check policy evaluation in logs  
export LOG_LEVEL=4
make start-services
```

#### 4. Test Timeouts
```bash
# Increase timeout
export TEST_TIMEOUT=60m
make test

# Run individual tests
make test-auth
```

### Debug Mode

Start services in debug mode to inspect manually:

```bash
# Start and keep running
make debug

# In another terminal, run specific operations
aws s3 ls --endpoint-url http://localhost:8333

# Stop when done (Ctrl+C in debug terminal)
```

### Log Analysis

```bash
# Service-specific logs
tail -f weed-s3.log       # S3 API server
tail -f weed-filer.log    # Filer (IAM storage)  
tail -f weed-master.log   # Master server
tail -f weed-volume.log   # Volume server

# Filter for IAM-related logs
make logs | grep -i iam
make logs | grep -i jwt
make logs | grep -i policy
```

## Performance Testing

### Benchmarks

```bash
# Run performance benchmarks
make benchmark

# Profile memory usage  
go test -bench=. -memprofile=mem.prof
go tool pprof mem.prof
```

### Load Testing

For load testing with IAM:

1. **Create Multiple Clients**:
   ```go
   // Generate multiple JWT tokens
   tokens := framework.GenerateMultipleJWTTokens(100)
   
   // Create concurrent clients
   var wg sync.WaitGroup
   for _, token := range tokens {
       wg.Add(1)
       go func(token string) {
           defer wg.Done()
           // Perform S3 operations
       }(token)
   }
   wg.Wait()
   ```

2. **Measure Performance**:
   ```bash
   # Run with verbose output
   go test -v -bench=BenchmarkS3IAMOperations
   ```

## CI/CD Integration

### GitHub Actions

```yaml
name: S3 IAM Integration Tests
on: [push, pull_request]

jobs:
  s3-iam-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: '1.19'
      
      - name: Build SeaweedFS
        run: go build -o weed ./main.go
      
      - name: Run S3 IAM Tests  
        run: |
          cd test/s3/iam
          make ci
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh 'go build -o weed ./main.go'
            }
        }
        stage('S3 IAM Tests') {
            steps {
                dir('test/s3/iam') {
                    sh 'make ci'
                }
            }
            post {
                always {
                    dir('test/s3/iam') {
                        sh 'make clean'
                    }
                }
            }
        }
    }
}
```

## Contributing

### Adding New Tests

1. **Follow Test Patterns**:
   - Use `S3IAMTestFramework` for setup
   - Include cleanup with `defer framework.Cleanup()`
   - Use descriptive test names and subtests
   - Assert both success and failure cases

2. **Update Documentation**:
   - Add test descriptions to this README
   - Include Makefile targets for new test categories
   - Document any new configuration options

3. **Ensure Test Reliability**:
   - Tests should be deterministic and repeatable
   - Include proper error handling and assertions
   - Use appropriate timeouts for async operations

### Code Style

- Follow standard Go testing conventions
- Use `require.NoError()` for critical assertions
- Use `assert.Equal()` for value comparisons  
- Include descriptive error messages in assertions

## Support

For issues with S3 IAM integration tests:

1. **Check Logs**: Use `make logs` to inspect service logs
2. **Verify Configuration**: Ensure `test_config.json` is correct
3. **Test Services**: Run `make status` to check service health
4. **Clean Environment**: Try `make clean && make test`

## License

This test suite is part of the SeaweedFS project and follows the same licensing terms.
