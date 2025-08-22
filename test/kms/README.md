# 🔐 SeaweedFS KMS Integration Tests

This directory contains comprehensive integration tests for SeaweedFS Server-Side Encryption (SSE) with Key Management Service (KMS) providers. The tests validate the complete encryption/decryption workflow using **OpenBao** (open source fork of HashiCorp Vault) as the KMS provider.

## 🎯 Overview

The KMS integration tests simulate **AWS KMS** functionality using **OpenBao**, providing:

- ✅ **Production-grade KMS testing** with real encryption/decryption operations
- ✅ **S3 API compatibility testing** with SSE-KMS headers and bucket encryption
- ✅ **Per-bucket KMS configuration** validation
- ✅ **Performance benchmarks** for KMS operations
- ✅ **Error handling and edge case** coverage
- ✅ **End-to-end workflows** from S3 API to KMS provider

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   S3 Client     │    │   SeaweedFS     │    │    OpenBao      │
│   (aws s3)      │───▶│   S3 API        │───▶│   Transit       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
        │                       │                       │
        │              ┌─────────────────┐              │
        │              │ KMS Manager     │              │
        └──────────────▶│ - AWS Provider  │◀─────────────┘
                       │ - Azure Provider│
                       │ - GCP Provider  │
                       │ - OpenBao       │
                       └─────────────────┘
```

## 📋 Prerequisites

### Required Tools

- **Docker & Docker Compose** - For running OpenBao and SeaweedFS
- **OpenBao CLI** (`bao`) - For direct OpenBao interaction *(optional)*
- **AWS CLI** - For S3 API testing
- **jq** - For JSON processing in scripts
- **curl** - For HTTP API testing
- **Go 1.19+** - For running Go tests

### Installation

```bash
# Install Docker (macOS)
brew install docker docker-compose

# Install OpenBao (optional - used by some tests)
brew install openbao

# Install AWS CLI  
brew install awscli

# Install jq
brew install jq
```

## 🚀 Quick Start

### 1. Run All Tests

```bash
cd test/kms
make test
```

### 2. Run Specific Test Types

```bash
# Unit tests only
make test-unit

# Integration tests with OpenBao
make test-integration  

# End-to-end S3 API tests
make test-e2e

# Performance benchmarks
make test-benchmark
```

### 3. Manual Setup

```bash
# Start OpenBao only
make dev-openbao

# Start full environment (OpenBao + SeaweedFS)
make setup-seaweedfs

# Run manual tests
make dev-test
```

## 🧪 Test Components

### 1. **OpenBao KMS Provider** (`openbao_integration_test.go`)

**What it tests:**
- KMS provider registration and initialization  
- Data key generation using Transit engine
- Encryption/decryption of data keys
- Key metadata and validation
- Error handling (invalid tokens, missing keys, etc.)
- Multiple key scenarios
- Performance benchmarks

**Key test cases:**
```go
TestOpenBaoKMSProvider_Integration
TestOpenBaoKMSProvider_ErrorHandling  
TestKMSManager_WithOpenBao
BenchmarkOpenBaoKMS_GenerateDataKey
BenchmarkOpenBaoKMS_Decrypt
```

### 2. **S3 API Integration** (`test_s3_kms.sh`)

**What it tests:**
- Bucket encryption configuration via S3 API
- Default bucket encryption behavior
- Explicit SSE-KMS headers in PUT operations
- Object upload/download with encryption
- Multipart uploads with KMS encryption
- Encryption metadata in object headers
- Cross-bucket KMS provider isolation

**Key scenarios:**
```bash
# Bucket encryption setup
aws s3api put-bucket-encryption --bucket test-openbao \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms", 
        "KMSMasterKeyID": "test-key-1"
      }
    }]
  }'

# Object upload with encryption
aws s3 cp file.txt s3://test-openbao/encrypted-file.txt \
  --sse aws:kms --sse-kms-key-id "test-key-2"
```

### 3. **Docker Environment** (`docker-compose.yml`)

**Services:**
- **OpenBao** - KMS provider (port 8200)
- **Vault** - Alternative KMS (port 8201)  
- **SeaweedFS Master** - Cluster coordination (port 9333)
- **SeaweedFS Volume** - Data storage (port 8080)
- **SeaweedFS Filer** - S3 API endpoint (port 8333)

### 4. **Configuration** (`filer.toml`)

**KMS Configuration:**
```toml
[kms]
default_provider = "openbao-test"

[kms.providers.openbao-test]
type = "openbao"
address = "http://openbao:8200"
token = "root-token-for-testing"
transit_path = "transit"

[kms.buckets.test-openbao]
provider = "openbao-test"
```

## 📊 Test Data

### Encryption Keys Created

The setup script creates these test keys in OpenBao:

| Key Name | Type | Purpose |
|----------|------|---------|
| `test-key-1` | AES256-GCM96 | Basic operations |  
| `test-key-2` | AES256-GCM96 | Multi-key scenarios |
| `seaweedfs-test-key` | AES256-GCM96 | Integration testing |
| `bucket-default-key` | AES256-GCM96 | Default bucket encryption |
| `high-security-key` | AES256-GCM96 | Security testing |
| `performance-key` | AES256-GCM96 | Performance benchmarks |
| `multipart-key` | AES256-GCM96 | Multipart upload testing |

### Test Buckets

| Bucket Name | KMS Provider | Purpose |
|-------------|--------------|---------|
| `test-openbao` | openbao-test | OpenBao integration |
| `test-vault` | vault-test | Vault compatibility |
| `test-local` | local-test | Local KMS testing |  
| `secure-data` | openbao-test | High security scenarios |

## 🔧 Configuration Options

### Environment Variables

```bash
# OpenBao configuration
export OPENBAO_ADDR="http://127.0.0.1:8200"
export OPENBAO_TOKEN="root-token-for-testing"

# SeaweedFS configuration  
export SEAWEEDFS_S3_ENDPOINT="http://127.0.0.1:8333"
export ACCESS_KEY="any"
export SECRET_KEY="any"

# Test configuration
export TEST_TIMEOUT="5m"
```

### Makefile Targets

| Target | Description |
|--------|-------------|
| `make help` | Show available commands |
| `make setup` | Set up test environment |
| `make test` | Run all tests |
| `make test-unit` | Run unit tests only |
| `make test-integration` | Run integration tests |
| `make test-e2e` | Run end-to-end tests |
| `make clean` | Clean up environment |
| `make logs` | Show service logs |
| `make status` | Check service status |

## 🧩 How It Works

### 1. **KMS Provider Registration**

OpenBao provider is automatically registered via `init()`:

```go
func init() {
    seaweedkms.RegisterProvider("openbao", NewOpenBaoKMSProvider)
    seaweedkms.RegisterProvider("vault", NewOpenBaoKMSProvider) // Alias
}
```

### 2. **Data Key Generation Flow**

```
1. S3 PUT with SSE-KMS headers
2. SeaweedFS extracts KMS key ID  
3. KMSManager routes to OpenBao provider
4. OpenBao generates random data key
5. OpenBao encrypts data key with master key
6. SeaweedFS encrypts object with data key
7. Encrypted data key stored in metadata
```

### 3. **Decryption Flow**

```
1. S3 GET request for encrypted object
2. SeaweedFS extracts encrypted data key from metadata
3. KMSManager routes to OpenBao provider
4. OpenBao decrypts data key with master key
5. SeaweedFS decrypts object with data key
6. Plaintext object returned to client
```

## 🔍 Troubleshooting

### Common Issues

**OpenBao not starting:**
```bash
# Check if port 8200 is in use
lsof -i :8200

# Check Docker logs
docker-compose logs openbao
```

**KMS provider not found:**
```bash
# Verify provider registration
go test -v -run TestProviderRegistration ./test/kms/

# Check imports in filer_kms.go
grep -n "kms/" weed/command/filer_kms.go
```

**S3 API connection refused:**
```bash  
# Check SeaweedFS services
make status

# Wait for services to be ready
./wait_for_services.sh
```

### Debug Commands

```bash
# Test OpenBao directly
curl -H "X-Vault-Token: root-token-for-testing" \
  http://127.0.0.1:8200/v1/sys/health

# Test transit engine
curl -X POST \
  -H "X-Vault-Token: root-token-for-testing" \
  -d '{"plaintext":"SGVsbG8gV29ybGQ="}' \
  http://127.0.0.1:8200/v1/transit/encrypt/test-key-1

# Test S3 API
aws s3 ls --endpoint-url http://127.0.0.1:8333
```

## 🎯 AWS KMS Integration Testing

This test suite **simulates AWS KMS behavior** using OpenBao, enabling:

### ✅ **Compatibility Validation**

- **S3 API compatibility** - Same headers, same behavior as AWS S3
- **KMS API patterns** - GenerateDataKey, Decrypt, DescribeKey operations  
- **Error codes** - AWS-compatible error responses
- **Encryption context** - Proper context handling and validation

### ✅ **Production Readiness Testing**

- **Key rotation scenarios** - Multiple keys per bucket
- **Performance characteristics** - Latency and throughput metrics
- **Error recovery** - Network failures, invalid keys, timeout handling
- **Security validation** - Encryption/decryption correctness

### ✅ **Integration Patterns**

- **Bucket-level configuration** - Different KMS keys per bucket
- **Cross-region simulation** - Multiple KMS providers
- **Caching behavior** - Data key caching validation  
- **Metadata handling** - Encrypted metadata storage

## 📈 Performance Expectations

**Typical performance metrics** (local testing):

- **Data key generation**: ~50-100ms (including network roundtrip)
- **Data key decryption**: ~30-50ms (cached provider instance)
- **Object encryption**: ~1-5ms per MB (AES-256-GCM)
- **S3 PUT with SSE-KMS**: +100-200ms overhead vs. unencrypted

## 🚀 Production Deployment

After successful integration testing, deploy with real KMS providers:

```toml
[kms.providers.aws-prod]
type = "aws"
region = "us-east-1"
# IAM roles preferred over access keys

[kms.providers.azure-prod]  
type = "azure"
vault_url = "https://prod-vault.vault.azure.net/"
use_default_creds = true # Managed identity

[kms.providers.gcp-prod]
type = "gcp" 
project_id = "prod-project"
use_default_credentials = true # Service account
```

## 🎉 Success Criteria

Tests pass when:

- ✅ All KMS providers register successfully
- ✅ Data key generation/decryption works end-to-end  
- ✅ S3 API encryption headers are handled correctly
- ✅ Bucket-level KMS configuration is respected
- ✅ Multipart uploads maintain encryption consistency
- ✅ Performance meets acceptable thresholds
- ✅ Error scenarios are handled gracefully

---

## 📞 Support

For issues with KMS integration tests:

1. **Check logs**: `make logs`
2. **Verify environment**: `make status` 
3. **Run debug**: `make debug`
4. **Clean restart**: `make clean && make setup`

**Happy testing!** 🔐✨
