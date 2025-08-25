#!/bin/bash

# SeaweedFS S3 IAM Complete Test Setup Script
# This script enables all previously skipped tests by setting up the required environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEAWEEDFS_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TEST_DIR="$SCRIPT_DIR"

# Service ports
KEYCLOAK_PORT=8080
S3_PORT=8333
FILER_PORT=8888
MASTER_PORT=9333
VOLUME_PORT=8080

# Test configuration
export KEYCLOAK_URL="http://localhost:$KEYCLOAK_PORT"
export S3_ENDPOINT="http://localhost:$S3_PORT"
export TEST_TIMEOUT="60m"
export CGO_ENABLED=0

echo -e "${BLUE}🚀 SeaweedFS S3 IAM Complete Test Setup${NC}"
echo -e "${BLUE}======================================${NC}"

# Function to check if a service is running
check_service() {
    local service_name="$1"
    local url="$2"
    local max_attempts=30
    local attempt=1
    
    echo -e "${YELLOW}⏳ Waiting for $service_name to be ready...${NC}"
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service_name is ready${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}❌ $service_name failed to start after $((max_attempts * 2)) seconds${NC}"
    return 1
}

# Function to setup Keycloak
setup_keycloak() {
    echo -e "${BLUE}🔐 Setting up Keycloak for IAM integration tests...${NC}"
    
    # Check if Keycloak is already running
    if curl -s "http://localhost:$KEYCLOAK_PORT/health/ready" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Keycloak is already running${NC}"
        return 0
    fi
    
    # Check if Docker is available
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is required for Keycloak setup${NC}"
        echo -e "${YELLOW}💡 Install Docker or run Keycloak manually${NC}"
        return 1
    fi
    
    # Start Keycloak with Docker
    echo -e "${YELLOW}🐳 Starting Keycloak container...${NC}"
    
    # Stop any existing Keycloak container
    docker stop keycloak-iam-test 2>/dev/null || true
    docker rm keycloak-iam-test 2>/dev/null || true
    
    # Start new Keycloak container
    docker run -d \
        --name keycloak-iam-test \
        -p $KEYCLOAK_PORT:8080 \
        -e KEYCLOAK_ADMIN=admin \
        -e KEYCLOAK_ADMIN_PASSWORD=admin123 \
        -e KC_HTTP_PORT=8080 \
        -e KC_HOSTNAME_STRICT=false \
        -e KC_HOSTNAME_STRICT_HTTPS=false \
        -e KC_HTTP_ENABLED=true \
        -e KC_HEALTH_ENABLED=true \
        -v "$TEST_DIR/keycloak-realm.json:/opt/keycloak/data/import/realm.json:ro" \
        quay.io/keycloak/keycloak:26.0.7 start-dev --import-realm
    
    # Wait for Keycloak to be ready
    if check_service "Keycloak" "http://localhost:$KEYCLOAK_PORT/health/ready"; then
        echo -e "${GREEN}✅ Keycloak setup complete${NC}"
        return 0
    else
        echo -e "${RED}❌ Keycloak setup failed${NC}"
        return 1
    fi
}

# Function to setup distributed environment
setup_distributed_environment() {
    echo -e "${BLUE}🌐 Setting up distributed test environment...${NC}"
    
    # Create distributed configuration
    if [ ! -f "$TEST_DIR/iam_config_distributed.json" ]; then
        echo -e "${YELLOW}📝 Creating distributed IAM configuration...${NC}"
        cat > "$TEST_DIR/iam_config_distributed.json" << 'EOF'
{
  "sts": {
    "tokenDuration": 3600000000000,
    "maxSessionLength": 43200000000000,
    "issuer": "seaweedfs-sts",
    "signingKey": "dGVzdC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmc=",
    "sessionStoreType": "filer",
    "sessionStoreConfig": {
      "filerAddress": "localhost:8888",
      "basePath": "/seaweedfs/iam/sessions"
    }
  },
  "identityProviders": [
    {
      "name": "test-oidc",
      "type": "mock",
      "config": {
        "issuer": "test-oidc-issuer"
      }
    }
  ],
  "policy": {
    "defaultEffect": "Deny",
    "storeType": "filer",
    "storeConfig": {
      "filerAddress": "localhost:8888",
      "basePath": "/seaweedfs/iam/policies"
    }
  },
  "roleStore": {
    "storeType": "filer",
    "storeConfig": {
      "filerAddress": "localhost:8888",
      "basePath": "/seaweedfs/iam/roles"
    }
  },
  "roles": [
    {
      "roleName": "TestAdminRole",
      "roleArn": "arn:seaweed:iam::role/TestAdminRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "test-oidc"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3AdminPolicy"],
      "description": "Admin role for distributed testing"
    },
    {
      "roleName": "TestReadOnlyRole",
      "roleArn": "arn:seaweed:iam::role/TestReadOnlyRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "test-oidc"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3ReadOnlyPolicy"],
      "description": "Read-only role for distributed testing"
    }
  ],
  "policies": [
    {
      "name": "S3AdminPolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": ["*"]
          },
          {
            "Effect": "Allow",
            "Action": ["sts:ValidateSession"],
            "Resource": ["*"]
          }
        ]
      }
    },
    {
      "name": "S3ReadOnlyPolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": [
              "s3:GetObject",
              "s3:ListBucket"
            ],
            "Resource": [
              "arn:seaweed:s3:::*",
              "arn:seaweed:s3:::*/*"
            ]
          },
          {
            "Effect": "Allow",
            "Action": ["sts:ValidateSession"],
            "Resource": ["*"]
          }
        ]
      }
    }
  ]
}
EOF
    fi
    
    echo -e "${GREEN}✅ Distributed environment configuration ready${NC}"
}

# Function to create distributed test functions
create_distributed_tests() {
    echo -e "${BLUE}🧪 Creating distributed test functions...${NC}"
    
    # Create distributed test file if it doesn't exist
    if [ ! -f "$TEST_DIR/s3_iam_distributed_test.go" ]; then
        cat > "$TEST_DIR/s3_iam_distributed_test.go" << 'EOF'
package iam

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3IAMDistributedTests tests IAM functionality across multiple S3 gateway instances
func TestS3IAMDistributedTests(t *testing.T) {
	// Skip if not in distributed test mode
	if os.Getenv("ENABLE_DISTRIBUTED_TESTS") != "true" {
		t.Skip("Distributed tests not enabled. Set ENABLE_DISTRIBUTED_TESTS=true")
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("distributed_session_consistency", func(t *testing.T) {
		// Test that sessions created on one instance are visible on others
		// This requires filer-based session storage
		
		// Create session token
		sessionToken, err := framework.generateSTSSessionToken("test-user", "TestAdminRole", time.Hour)
		require.NoError(t, err)
		
		// Create S3 clients that would connect to different gateway instances
		// In a real distributed setup, these would point to different S3 gateway ports
		client1, err := framework.CreateS3ClientWithJWT("test-user", "TestAdminRole")
		require.NoError(t, err)
		
		client2, err := framework.CreateS3ClientWithJWT("test-user", "TestAdminRole")
		require.NoError(t, err)
		
		// Both clients should be able to perform operations
		bucketName := "test-distributed-session"
		
		err = framework.CreateBucket(client1, bucketName)
		require.NoError(t, err)
		
		// Client2 should see the bucket created by client1
		buckets, err := framework.ListBuckets(client2)
		require.NoError(t, err)
		
		found := false
		for _, bucket := range buckets {
			if bucket == bucketName {
				found = true
				break
			}
		}
		assert.True(t, found, "Bucket should be visible across distributed instances")
		
		// Cleanup
		err = framework.DeleteBucket(client1, bucketName)
		require.NoError(t, err)
	})

	t.Run("distributed_role_consistency", func(t *testing.T) {
		// Test that role definitions are consistent across instances
		// This requires filer-based role storage
		
		// Create clients with different roles
		adminClient, err := framework.CreateS3ClientWithJWT("admin-user", "TestAdminRole")
		require.NoError(t, err)
		
		readOnlyClient, err := framework.CreateS3ClientWithJWT("readonly-user", "TestReadOnlyRole")
		require.NoError(t, err)
		
		bucketName := "test-distributed-roles"
		objectKey := "test-object.txt"
		
		// Admin should be able to create bucket
		err = framework.CreateBucket(adminClient, bucketName)
		require.NoError(t, err)
		
		// Admin should be able to put object
		err = framework.PutObject(adminClient, bucketName, objectKey, "test content")
		require.NoError(t, err)
		
		// Read-only user should be able to get object
		content, err := framework.GetObject(readOnlyClient, bucketName, objectKey)
		require.NoError(t, err)
		assert.Equal(t, "test content", content)
		
		// Read-only user should NOT be able to put object
		err = framework.PutObject(readOnlyClient, bucketName, "forbidden-object.txt", "forbidden content")
		require.Error(t, err, "Read-only user should not be able to put objects")
		
		// Cleanup
		err = framework.DeleteObject(adminClient, bucketName, objectKey)
		require.NoError(t, err)
		err = framework.DeleteBucket(adminClient, bucketName)
		require.NoError(t, err)
	})

	t.Run("distributed_concurrent_operations", func(t *testing.T) {
		// Test concurrent operations across distributed instances
		const numGoroutines = 10
		const numOperationsPerGoroutine = 5
		
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*numOperationsPerGoroutine)
		
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				
				client, err := framework.CreateS3ClientWithJWT(fmt.Sprintf("user-%d", goroutineID), "TestAdminRole")
				if err != nil {
					errors <- err
					return
				}
				
				for j := 0; j < numOperationsPerGoroutine; j++ {
					bucketName := fmt.Sprintf("test-concurrent-%d-%d", goroutineID, j)
					
					// Create bucket
					if err := framework.CreateBucket(client, bucketName); err != nil {
						errors <- err
						continue
					}
					
					// Put object
					objectKey := "test-object.txt"
					if err := framework.PutObject(client, bucketName, objectKey, fmt.Sprintf("content-%d-%d", goroutineID, j)); err != nil {
						errors <- err
						continue
					}
					
					// Get object
					if _, err := framework.GetObject(client, bucketName, objectKey); err != nil {
						errors <- err
						continue
					}
					
					// Delete object
					if err := framework.DeleteObject(client, bucketName, objectKey); err != nil {
						errors <- err
						continue
					}
					
					// Delete bucket
					if err := framework.DeleteBucket(client, bucketName); err != nil {
						errors <- err
						continue
					}
				}
			}(i)
		}
		
		wg.Wait()
		close(errors)
		
		// Check for errors
		var errorList []error
		for err := range errors {
			errorList = append(errorList, err)
		}
		
		if len(errorList) > 0 {
			t.Errorf("Concurrent operations failed with %d errors. First error: %v", len(errorList), errorList[0])
		}
	})
}

// TestS3IAMPerformanceTests tests IAM performance characteristics
func TestS3IAMPerformanceTests(t *testing.T) {
	// Skip if not in performance test mode
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		t.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(t)
	defer framework.Cleanup()

	t.Run("authentication_performance", func(t *testing.T) {
		// Test authentication performance
		const numRequests = 100
		
		client, err := framework.CreateS3ClientWithJWT("perf-user", "TestAdminRole")
		require.NoError(t, err)
		
		bucketName := "test-auth-performance"
		err = framework.CreateBucket(client, bucketName)
		require.NoError(t, err)
		defer framework.DeleteBucket(client, bucketName)
		
		start := time.Now()
		
		for i := 0; i < numRequests; i++ {
			_, err := framework.ListBuckets(client)
			require.NoError(t, err)
		}
		
		duration := time.Since(start)
		avgLatency := duration / numRequests
		
		t.Logf("Authentication performance: %d requests in %v (avg: %v per request)", 
			numRequests, duration, avgLatency)
		
		// Performance assertion - should be under 100ms per request on average
		assert.Less(t, avgLatency, 100*time.Millisecond, 
			"Average authentication latency should be under 100ms")
	})

	t.Run("authorization_performance", func(t *testing.T) {
		// Test authorization performance with different policy complexities
		const numRequests = 50
		
		client, err := framework.CreateS3ClientWithJWT("perf-user", "TestAdminRole")
		require.NoError(t, err)
		
		bucketName := "test-authz-performance"
		err = framework.CreateBucket(client, bucketName)
		require.NoError(t, err)
		defer framework.DeleteBucket(client, bucketName)
		
		start := time.Now()
		
		for i := 0; i < numRequests; i++ {
			objectKey := fmt.Sprintf("perf-object-%d.txt", i)
			err := framework.PutObject(client, bucketName, objectKey, "performance test content")
			require.NoError(t, err)
			
			_, err = framework.GetObject(client, bucketName, objectKey)
			require.NoError(t, err)
			
			err = framework.DeleteObject(client, bucketName, objectKey)
			require.NoError(t, err)
		}
		
		duration := time.Since(start)
		avgLatency := duration / (numRequests * 3) // 3 operations per iteration
		
		t.Logf("Authorization performance: %d operations in %v (avg: %v per operation)", 
			numRequests*3, duration, avgLatency)
		
		// Performance assertion - should be under 50ms per operation on average
		assert.Less(t, avgLatency, 50*time.Millisecond, 
			"Average authorization latency should be under 50ms")
	})
}

// BenchmarkS3IAMAuthentication benchmarks JWT authentication
func BenchmarkS3IAMAuthentication(b *testing.B) {
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		b.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(&testing.T{})
	defer framework.Cleanup()

	client, err := framework.CreateS3ClientWithJWT("bench-user", "TestAdminRole")
	require.NoError(b, err)

	bucketName := "test-bench-auth"
	err = framework.CreateBucket(client, bucketName)
	require.NoError(b, err)
	defer framework.DeleteBucket(client, bucketName)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := framework.ListBuckets(client)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkS3IAMAuthorization benchmarks policy evaluation
func BenchmarkS3IAMAuthorization(b *testing.B) {
	if os.Getenv("ENABLE_PERFORMANCE_TESTS") != "true" {
		b.Skip("Performance tests not enabled. Set ENABLE_PERFORMANCE_TESTS=true")
	}

	framework := NewS3IAMTestFramework(&testing.T{})
	defer framework.Cleanup()

	client, err := framework.CreateS3ClientWithJWT("bench-user", "TestAdminRole")
	require.NoError(b, err)

	bucketName := "test-bench-authz"
	err = framework.CreateBucket(client, bucketName)
	require.NoError(b, err)
	defer framework.DeleteBucket(client, bucketName)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			objectKey := fmt.Sprintf("bench-object-%d.txt", i)
			err := framework.PutObject(client, bucketName, objectKey, "benchmark content")
			if err != nil {
				b.Error(err)
			}
			i++
		}
	})
}
EOF
        echo -e "${GREEN}✅ Distributed test functions created${NC}"
    fi
}

# Function to create performance test runner
create_performance_test_runner() {
    echo -e "${BLUE}🏁 Creating performance test runner...${NC}"
    
    cat > "$TEST_DIR/run_performance_tests.sh" << 'EOF'
#!/bin/bash

# Performance Test Runner for SeaweedFS S3 IAM

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}🏁 Running S3 IAM Performance Tests${NC}"

# Enable performance tests
export ENABLE_PERFORMANCE_TESTS=true
export TEST_TIMEOUT=60m

# Run benchmarks
echo -e "${YELLOW}📊 Running benchmarks...${NC}"
go test -bench=. -benchmem -timeout=$TEST_TIMEOUT ./...

# Run performance tests
echo -e "${YELLOW}🧪 Running performance test suite...${NC}"
go test -v -timeout=$TEST_TIMEOUT -run "TestS3IAMPerformanceTests" ./...

echo -e "${GREEN}✅ Performance tests completed${NC}"
EOF
    
    chmod +x "$TEST_DIR/run_performance_tests.sh"
    echo -e "${GREEN}✅ Performance test runner created${NC}"
}

# Function to create stress test runner
create_stress_test_runner() {
    echo -e "${BLUE}💪 Creating stress test runner...${NC}"
    
    cat > "$TEST_DIR/run_stress_tests.sh" << 'EOF'
#!/bin/bash

# Stress Test Runner for SeaweedFS S3 IAM

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}💪 Running S3 IAM Stress Tests${NC}"

# Enable stress tests
export ENABLE_STRESS_TESTS=true
export TEST_TIMEOUT=60m

# Run stress tests multiple times
STRESS_ITERATIONS=5

echo -e "${YELLOW}🔄 Running stress tests with $STRESS_ITERATIONS iterations...${NC}"

for i in $(seq 1 $STRESS_ITERATIONS); do
    echo -e "${YELLOW}📊 Iteration $i/$STRESS_ITERATIONS${NC}"
    
    if ! go test -v -timeout=$TEST_TIMEOUT -run "TestS3IAMDistributedTests.*concurrent" ./... -count=1; then
        echo -e "${RED}❌ Stress test failed on iteration $i${NC}"
        exit 1
    fi
    
    # Brief pause between iterations
    sleep 2
done

echo -e "${GREEN}✅ All stress test iterations completed successfully${NC}"
EOF
    
    chmod +x "$TEST_DIR/run_stress_tests.sh"
    echo -e "${GREEN}✅ Stress test runner created${NC}"
}

# Function to create versioning stress test setup
setup_versioning_stress_tests() {
    echo -e "${BLUE}📚 Setting up S3 versioning stress tests...${NC}"
    
    # Navigate to versioning test directory
    VERSIONING_DIR="$SEAWEEDFS_ROOT/test/s3/versioning"
    
    if [ ! -d "$VERSIONING_DIR" ]; then
        echo -e "${RED}❌ Versioning test directory not found: $VERSIONING_DIR${NC}"
        return 1
    fi
    
    # Create versioning stress test enabler
    cat > "$VERSIONING_DIR/enable_stress_tests.sh" << 'EOF'
#!/bin/bash

# Enable S3 Versioning Stress Tests

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}📚 Enabling S3 Versioning Stress Tests${NC}"

# Disable short mode to enable stress tests
export ENABLE_STRESS_TESTS=true

# Run versioning stress tests
echo -e "${YELLOW}🧪 Running versioning stress tests...${NC}"
make test-versioning-stress

echo -e "${GREEN}✅ Versioning stress tests completed${NC}"
EOF
    
    chmod +x "$VERSIONING_DIR/enable_stress_tests.sh"
    echo -e "${GREEN}✅ Versioning stress test setup complete${NC}"
}

# Function to create master test runner
create_master_test_runner() {
    echo -e "${BLUE}🎯 Creating master test runner...${NC}"
    
    cat > "$TEST_DIR/run_all_tests.sh" << 'EOF'
#!/bin/bash

# Master Test Runner - Enables and runs all previously skipped tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}🎯 SeaweedFS S3 IAM Complete Test Suite${NC}"
echo -e "${BLUE}=====================================${NC}"

# Set environment variables to enable all tests
export ENABLE_DISTRIBUTED_TESTS=true
export ENABLE_PERFORMANCE_TESTS=true
export ENABLE_STRESS_TESTS=true
export KEYCLOAK_URL="http://localhost:8080"
export S3_ENDPOINT="http://localhost:8333"
export TEST_TIMEOUT=60m
export CGO_ENABLED=0

# Function to run test category
run_test_category() {
    local category="$1"
    local test_pattern="$2"
    local description="$3"
    
    echo -e "${YELLOW}🧪 Running $description...${NC}"
    
    if go test -v -timeout=$TEST_TIMEOUT -run "$test_pattern" ./...; then
        echo -e "${GREEN}✅ $description completed successfully${NC}"
        return 0
    else
        echo -e "${RED}❌ $description failed${NC}"
        return 1
    fi
}

# Track results
TOTAL_CATEGORIES=0
PASSED_CATEGORIES=0

# 1. Standard IAM Integration Tests
echo -e "\n${BLUE}1. Standard IAM Integration Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "standard" "TestS3IAM(?!.*Distributed|.*Performance)" "Standard IAM Integration Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 2. Keycloak Integration Tests (if Keycloak is available)
echo -e "\n${BLUE}2. Keycloak Integration Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if curl -s "http://localhost:8080/health/ready" > /dev/null 2>&1; then
    if run_test_category "keycloak" "TestKeycloak" "Keycloak Integration Tests"; then
        PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
    fi
else
    echo -e "${YELLOW}⚠️ Keycloak not available, skipping Keycloak tests${NC}"
    echo -e "${YELLOW}💡 Run './setup_all_tests.sh' to start Keycloak${NC}"
fi

# 3. Distributed Tests
echo -e "\n${BLUE}3. Distributed IAM Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "distributed" "TestS3IAMDistributedTests" "Distributed IAM Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 4. Performance Tests
echo -e "\n${BLUE}4. Performance Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "performance" "TestS3IAMPerformanceTests" "Performance Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 5. Benchmarks
echo -e "\n${BLUE}5. Benchmark Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if go test -bench=. -benchmem -timeout=$TEST_TIMEOUT ./...; then
    echo -e "${GREEN}✅ Benchmark tests completed successfully${NC}"
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
else
    echo -e "${RED}❌ Benchmark tests failed${NC}"
fi

# 6. Versioning Stress Tests
echo -e "\n${BLUE}6. S3 Versioning Stress Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if [ -f "../versioning/enable_stress_tests.sh" ]; then
    if (cd ../versioning && ./enable_stress_tests.sh); then
        echo -e "${GREEN}✅ Versioning stress tests completed successfully${NC}"
        PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
    else
        echo -e "${RED}❌ Versioning stress tests failed${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Versioning stress tests not available${NC}"
fi

# Summary
echo -e "\n${BLUE}📊 Test Summary${NC}"
echo -e "${BLUE}===============${NC}"
echo -e "Total test categories: $TOTAL_CATEGORIES"
echo -e "Passed: ${GREEN}$PASSED_CATEGORIES${NC}"
echo -e "Failed: ${RED}$((TOTAL_CATEGORIES - PASSED_CATEGORIES))${NC}"

if [ $PASSED_CATEGORIES -eq $TOTAL_CATEGORIES ]; then
    echo -e "\n${GREEN}🎉 All test categories passed!${NC}"
    exit 0
else
    echo -e "\n${RED}❌ Some test categories failed${NC}"
    exit 1
fi
EOF
    
    chmod +x "$TEST_DIR/run_all_tests.sh"
    echo -e "${GREEN}✅ Master test runner created${NC}"
}

# Function to update Makefile with new targets
update_makefile() {
    echo -e "${BLUE}📝 Updating Makefile with new test targets...${NC}"
    
    # Add new targets to Makefile
    cat >> "$TEST_DIR/Makefile" << 'EOF'

# New test targets for previously skipped tests

test-distributed: ## Run distributed IAM tests
	@echo "🌐 Running distributed IAM tests..."
	@export ENABLE_DISTRIBUTED_TESTS=true && go test -v -timeout $(TEST_TIMEOUT) -run "TestS3IAMDistributedTests" ./...

test-performance: ## Run performance tests
	@echo "🏁 Running performance tests..."
	@export ENABLE_PERFORMANCE_TESTS=true && go test -v -timeout $(TEST_TIMEOUT) -run "TestS3IAMPerformanceTests" ./...

test-stress: ## Run stress tests
	@echo "💪 Running stress tests..."
	@export ENABLE_STRESS_TESTS=true && ./run_stress_tests.sh

test-versioning-stress: ## Run S3 versioning stress tests
	@echo "📚 Running versioning stress tests..."
	@cd ../versioning && ./enable_stress_tests.sh

test-keycloak-full: docker-up ## Run complete Keycloak integration tests
	@echo "🔐 Running complete Keycloak integration tests..."
	@export KEYCLOAK_URL="http://localhost:8080" && \
	 export S3_ENDPOINT="http://localhost:8333" && \
	 sleep 15 && \
	 go test -v -timeout $(TEST_TIMEOUT) -run "TestKeycloak" ./...
	@make docker-down

test-all-previously-skipped: ## Run all previously skipped tests
	@echo "🎯 Running all previously skipped tests..."
	@./run_all_tests.sh

setup-all-tests: ## Setup environment for all tests (including Keycloak)
	@echo "🚀 Setting up complete test environment..."
	@./setup_all_tests.sh

# Update help target
help: ## Show this help message
	@echo "SeaweedFS S3 IAM Integration Tests"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Standard Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST) | head -20
	@echo ""
	@echo "New Test Targets (Previously Skipped):"
	@echo "  test-distributed         Run distributed IAM tests"
	@echo "  test-performance         Run performance tests"
	@echo "  test-stress              Run stress tests"
	@echo "  test-versioning-stress   Run S3 versioning stress tests"
	@echo "  test-keycloak-full       Run complete Keycloak integration tests"
	@echo "  test-all-previously-skipped  Run all previously skipped tests"
	@echo "  setup-all-tests          Setup environment for all tests"
	@echo ""
	@echo "Docker Compose Targets:"
	@echo "  docker-test              Run tests with Docker Compose including Keycloak"
	@echo "  docker-up                Start all services with Docker Compose"
	@echo "  docker-down              Stop all Docker Compose services"
	@echo "  docker-logs              Show logs from all services"

EOF
    
    echo -e "${GREEN}✅ Makefile updated with new targets${NC}"
}

# Main execution
main() {
    echo -e "${BLUE}🔧 Starting complete test setup...${NC}"
    
    # Setup distributed environment
    setup_distributed_environment
    
    # Create test functions
    create_distributed_tests
    
    # Create test runners
    create_performance_test_runner
    create_stress_test_runner
    create_master_test_runner
    
    # Setup versioning stress tests
    setup_versioning_stress_tests
    
    # Update Makefile
    update_makefile
    
    # Setup Keycloak (optional)
    echo -e "\n${YELLOW}🔐 Setting up Keycloak...${NC}"
    if setup_keycloak; then
        echo -e "${GREEN}✅ Keycloak setup successful${NC}"
    else
        echo -e "${YELLOW}⚠️ Keycloak setup failed or skipped${NC}"
        echo -e "${YELLOW}💡 You can run Keycloak manually or skip Keycloak tests${NC}"
    fi
    
    echo -e "\n${GREEN}🎉 Complete test setup finished!${NC}"
    echo -e "\n${BLUE}📋 Available commands:${NC}"
    echo -e "  ${YELLOW}./run_all_tests.sh${NC}           - Run all previously skipped tests"
    echo -e "  ${YELLOW}make test-distributed${NC}        - Run distributed tests only"
    echo -e "  ${YELLOW}make test-performance${NC}        - Run performance tests only"
    echo -e "  ${YELLOW}make test-stress${NC}             - Run stress tests only"
    echo -e "  ${YELLOW}make test-keycloak-full${NC}      - Run complete Keycloak tests"
    echo -e "  ${YELLOW}make test-versioning-stress${NC}  - Run versioning stress tests"
    echo -e "  ${YELLOW}make help${NC}                    - Show all available targets"
    
    echo -e "\n${BLUE}🔍 Environment Status:${NC}"
    echo -e "  Keycloak: $(curl -s http://localhost:8080/health/ready > /dev/null 2>&1 && echo -e "${GREEN}✅ Running${NC}" || echo -e "${RED}❌ Not running${NC}")"
    echo -e "  S3 API: $(curl -s http://localhost:8333 > /dev/null 2>&1 && echo -e "${GREEN}✅ Running${NC}" || echo -e "${RED}❌ Not running${NC}")"
    
    if ! curl -s http://localhost:8333 > /dev/null 2>&1; then
        echo -e "\n${YELLOW}💡 To start SeaweedFS services:${NC}"
        echo -e "  ${YELLOW}make start-services wait-for-services${NC}"
    fi
}

# Run main function
main "$@"
