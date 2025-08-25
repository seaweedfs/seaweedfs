# Keycloak Integration for SeaweedFS S3 IAM Tests

This document describes the integration of [Keycloak](https://github.com/keycloak/keycloak) as a real OIDC provider for SeaweedFS S3 IAM integration tests.

## Overview

The integration tests support both **mock OIDC** and **real Keycloak authentication**:

- **Mock OIDC** (default): Fast, no dependencies, generates test JWT tokens locally
- **Keycloak OIDC** (optional): Real-world authentication using Keycloak as OIDC provider

The test framework automatically detects if Keycloak is available and switches modes accordingly.

## Architecture

```
┌─────────────┐    JWT Token    ┌──────────────────┐    S3 API    ┌─────────────────┐
│  Keycloak   │ ────────────► │  SeaweedFS S3    │ ────────► │   SeaweedFS     │
│  OIDC       │   (Bearer)     │  Gateway + IAM   │           │   Storage       │
│  Provider   │                │                  │           │                 │
└─────────────┘                └──────────────────┘           └─────────────────┘
```

1. **Test** authenticates user with Keycloak using username/password
2. **Keycloak** returns JWT access token with user roles and claims  
3. **Test** creates S3 client with JWT Bearer token authentication
4. **SeaweedFS S3 Gateway** validates JWT token and enforces IAM policies
5. **S3 operations** are authorized based on user roles and attached policies

## Quick Start

### Option 1: Docker Compose (Recommended)

Start everything with Docker Compose including Keycloak:

```bash
cd test/s3/iam
make docker-test
```

This will:
- Start Keycloak with pre-configured realm and users
- Start SeaweedFS services (master, volume, filer, S3 gateway) 
- Run Keycloak integration tests
- Clean up all services

### Option 2: Manual Setup

1. Start Keycloak manually:
```bash
docker run -p 8080:8080 \
  -e KEYCLOAK_ADMIN=admin \
  -e KEYCLOAK_ADMIN_PASSWORD=admin123 \
  -v $(pwd)/keycloak-realm.json:/opt/keycloak/data/import/realm.json \
  quay.io/keycloak/keycloak:26.0.7 start-dev --import-realm
```

2. Start SeaweedFS services:
```bash
make start-services
```

3. Run tests with Keycloak:
```bash
export KEYCLOAK_URL="http://localhost:8080"
make test-quick
```

## Configuration

### Keycloak Realm Configuration

The test realm (`seaweedfs-test`) includes:

**Client:**
- **Client ID**: `seaweedfs-s3`
- **Client Secret**: `seaweedfs-s3-secret`
- **Direct Access**: Enabled (for username/password authentication)

**Roles:**
- `s3-admin`: Full S3 access
- `s3-read-only`: Read-only S3 access  
- `s3-read-write`: Read-write S3 access

**Test Users:**
- `admin-user` (password: `admin123`) → `s3-admin` role
- `read-user` (password: `read123`) → `s3-read-only` role
- `write-user` (password: `write123`) → `s3-read-write` role

### SeaweedFS IAM Configuration

The IAM system maps Keycloak roles to SeaweedFS IAM roles:

```json
{
  "roles": [
    {
      "roleName": "S3AdminRole",
      "trustPolicy": {
        "Principal": { "Federated": "keycloak-oidc" },
        "Action": ["sts:AssumeRoleWithWebIdentity"],
        "Condition": { "StringEquals": { "roles": "s3-admin" } }
      },
      "attachedPolicies": ["S3AdminPolicy"]
    }
  ]
}
```

## Test Structure

### Framework Detection

The test framework automatically detects Keycloak availability:

```go
// Check if Keycloak is running
framework.useKeycloak = framework.isKeycloakAvailable(keycloakURL)

if framework.useKeycloak {
    // Use real Keycloak authentication
    token, err = framework.getKeycloakToken(username)
} else {
    // Fall back to mock JWT tokens
    token, err = framework.generateSTSSessionToken(username, roleName, time.Hour)
}
```

### Test Categories

**Keycloak-Specific Tests** (`TestKeycloak*`):
- `TestKeycloakAuthentication`: Real authentication flow
- `TestKeycloakRoleMapping`: Role mapping from Keycloak to S3 policies
- `TestKeycloakTokenExpiration`: JWT token lifecycle
- `TestKeycloakS3Operations`: End-to-end S3 operations with real auth

**General Tests** (work with both modes):
- `TestS3IAMAuthentication`: Basic authentication tests
- `TestS3IAMPolicyEnforcement`: Policy enforcement tests
- All other integration tests

## Environment Variables

- `KEYCLOAK_URL`: Keycloak base URL (default: `http://localhost:8080`)
- `S3_ENDPOINT`: SeaweedFS S3 endpoint (default: `http://localhost:8333`)

## Docker Services

The Docker Compose setup includes:

```yaml
services:
  keycloak:          # Keycloak OIDC provider
  seaweedfs-master:  # SeaweedFS master server
  seaweedfs-volume:  # SeaweedFS volume server  
  seaweedfs-filer:   # SeaweedFS filer server
  seaweedfs-s3:      # SeaweedFS S3 gateway with IAM
```

All services include health checks and proper dependencies.

## Authentication Flow

1. **Test requests authentication**:
   ```go
   tokenResp, err := keycloakClient.AuthenticateUser("admin-user", "admin123")
   ```

2. **Keycloak returns JWT token** with claims:
   ```json
   {
     "sub": "user-id",
     "preferred_username": "admin-user", 
     "roles": ["s3-admin"],
     "iss": "http://keycloak:8080/realms/seaweedfs-test"
   }
   ```

3. **S3 client sends Bearer token**:
   ```http
   GET / HTTP/1.1
   Authorization: Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...
   ```

4. **SeaweedFS validates token** and checks policies:
   - Validate JWT signature with Keycloak JWKS
   - Extract roles from token claims
   - Map roles to IAM roles via trust policies
   - Enforce attached IAM policies for S3 operations

## Troubleshooting

### Keycloak Not Available

If Keycloak is not running, tests automatically fall back to mock mode:

```
Using mock OIDC server for testing
```

### Token Validation Errors

Check that:
- Keycloak realm configuration matches `iam_config_docker.json`
- JWT signing algorithms are compatible (RS256/HS256)
- Trust policies correctly reference the Keycloak provider

### Service Dependencies

Docker Compose includes health checks. Monitor with:

```bash
make docker-logs
```

### Authentication Failures

Enable debug logging:
```bash
export KEYCLOAK_URL="http://localhost:8080"
go test -v -run "TestKeycloak" ./...
```

## Extending the Integration

### Adding New Roles

1. Update `keycloak-realm.json` with new roles
2. Add corresponding IAM role in `iam_config_docker.json`
3. Create trust policy mapping the Keycloak role
4. Define appropriate IAM policies for the role

### Adding New Test Users

1. Add user to `keycloak-realm.json` with credentials and roles
2. Add password mapping in `getTestUserPassword()`
3. Create tests for the new user's permissions

### Custom OIDC Providers

The framework can be extended to support other OIDC providers by:
1. Implementing the provider in the IAM integration system
2. Adding provider configuration to IAM config
3. Updating test framework authentication methods

## Benefits

- **Real-world validation**: Tests against actual OIDC provider
- **Production-like environment**: Mirrors real deployment scenarios  
- **Comprehensive coverage**: Role mapping, token validation, policy enforcement
- **Automatic fallback**: Works without Keycloak dependencies
- **Easy CI/CD**: Docker Compose makes automation simple
