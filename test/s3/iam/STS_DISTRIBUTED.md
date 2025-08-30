# Distributed STS Service for SeaweedFS S3 Gateway

This document explains how to configure and deploy the STS (Security Token Service) for distributed SeaweedFS S3 Gateway deployments with consistent identity provider configurations.

## Problem Solved

Previously, identity providers had to be **manually registered** on each S3 gateway instance, leading to:

- ❌ **Inconsistent authentication**: Different instances might have different providers
- ❌ **Manual synchronization**: No guarantee all instances have same provider configs
- ❌ **Authentication failures**: Users getting different responses from different instances
- ❌ **Operational complexity**: Difficult to manage provider configurations at scale

## Solution: Configuration-Driven Providers

The STS service now supports **automatic provider loading** from configuration files, ensuring:

- ✅ **Consistent providers**: All instances load identical providers from config
- ✅ **Automatic synchronization**: Configuration-driven, no manual registration needed
- ✅ **Reliable authentication**: Same behavior from all instances
- ✅ **Easy management**: Update config file, restart services

## Configuration Schema

### Basic STS Configuration

```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h", 
    "issuer": "seaweedfs-sts",
    "signingKey": "base64-encoded-signing-key-32-chars-min"
  }
}
```

**Note**: The STS service uses a **stateless JWT design** where all session information is embedded directly in the JWT token. No external session storage is required.

### Configuration-Driven Providers

```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-sts",
    "signingKey": "base64-encoded-signing-key",
    "providers": [
      {
        "name": "keycloak-oidc",
        "type": "oidc", 
        "enabled": true,
        "config": {
          "issuer": "https://keycloak.company.com/realms/seaweedfs",
          "clientId": "seaweedfs-s3",
          "clientSecret": "super-secret-key",
          "jwksUri": "https://keycloak.company.com/realms/seaweedfs/protocol/openid-connect/certs",
          "scopes": ["openid", "profile", "email", "roles"],
          "claimsMapping": {
            "usernameClaim": "preferred_username",
            "groupsClaim": "roles"
          }
        }
      },
      {
        "name": "backup-oidc",
        "type": "oidc",
        "enabled": false,
        "config": {
          "issuer": "https://backup-oidc.company.com",
          "clientId": "seaweedfs-backup"
        }
      },
      {
        "name": "dev-mock-provider",
        "type": "mock",
        "enabled": true,
        "config": {
          "issuer": "http://localhost:9999",
          "clientId": "mock-client"
        }
      }
    ]
  }
}
```

## Supported Provider Types

### 1. OIDC Provider (`"type": "oidc"`)

For production authentication with OpenID Connect providers like Keycloak, Auth0, Google, etc.

**Required Configuration:**
- `issuer`: OIDC issuer URL
- `clientId`: OAuth2 client ID

**Optional Configuration:**
- `clientSecret`: OAuth2 client secret (for confidential clients)
- `jwksUri`: JSON Web Key Set URI (auto-discovered if not provided)
- `userInfoUri`: UserInfo endpoint URI (auto-discovered if not provided)
- `scopes`: OAuth2 scopes to request (default: `["openid"]`)
- `claimsMapping`: Map OIDC claims to identity attributes

**Example:**
```json
{
  "name": "corporate-keycloak",
  "type": "oidc",
  "enabled": true,
  "config": {
    "issuer": "https://sso.company.com/realms/production",
    "clientId": "seaweedfs-prod",
    "clientSecret": "confidential-secret", 
    "scopes": ["openid", "profile", "email", "groups"],
    "claimsMapping": {
      "usernameClaim": "preferred_username",
      "groupsClaim": "groups",
      "emailClaim": "email"
    }
  }
}
```

### 2. Mock Provider (`"type": "mock"`)

For development, testing, and staging environments.

**Configuration:**
- `issuer`: Mock issuer URL (default: `http://localhost:9999`)
- `clientId`: Mock client ID

**Example:**
```json
{
  "name": "dev-mock",
  "type": "mock", 
  "enabled": true,
  "config": {
    "issuer": "http://dev-mock:9999",
    "clientId": "dev-client"
  }
}
```

**Built-in Test Tokens:**
- `valid_test_token`: Returns test user with developer groups
- `valid-oidc-token`: Compatible with integration tests
- `expired_token`: Returns token expired error
- `invalid_token`: Returns invalid token error

### 3. Future Provider Types

The factory pattern supports easy addition of new provider types:

- `"type": "ldap"`: LDAP/Active Directory authentication
- `"type": "saml"`: SAML 2.0 authentication  
- `"type": "oauth2"`: Generic OAuth2 providers
- `"type": "custom"`: Custom authentication backends

## Deployment Patterns

### Single Instance (Development)

```bash
# Standard deployment with config-driven providers
weed s3 -filer=localhost:8888 -port=8333 -iam.config=/path/to/sts_config.json
```

### Multiple Instances (Production)

```bash
# Instance 1 
weed s3 -filer=prod-filer:8888 -port=8333 -iam.config=/shared/sts_distributed.json

# Instance 2
weed s3 -filer=prod-filer:8888 -port=8334 -iam.config=/shared/sts_distributed.json

# Instance N
weed s3 -filer=prod-filer:8888 -port=833N -iam.config=/shared/sts_distributed.json
```

**Critical Requirements for Distributed Deployment:**

1. **Identical Configuration Files**: All instances must use the exact same configuration file
2. **Same Signing Keys**: All instances must have identical `signingKey` values
3. **Same Issuer**: All instances must use the same `issuer` value

**Note**: STS now uses stateless JWT tokens, eliminating the need for shared session storage.

### High Availability Setup

```yaml
# docker-compose.yml for production deployment
services:
  filer:
    image: seaweedfs/seaweedfs:latest
    command: "filer -master=master:9333"
    volumes:
      - filer-data:/data
    
  s3-gateway-1:
    image: seaweedfs/seaweedfs:latest
    command: "s3 -filer=filer:8888 -port=8333 -iam.config=/config/sts_distributed.json"
    ports:
      - "8333:8333"
    volumes:
      - ./sts_distributed.json:/config/sts_distributed.json:ro
    depends_on: [filer]
    
  s3-gateway-2:
    image: seaweedfs/seaweedfs:latest 
    command: "s3 -filer=filer:8888 -port=8333 -iam.config=/config/sts_distributed.json"
    ports:
      - "8334:8333"
    volumes:
      - ./sts_distributed.json:/config/sts_distributed.json:ro
    depends_on: [filer]
    
  s3-gateway-3:
    image: seaweedfs/seaweedfs:latest
    command: "s3 -filer=filer:8888 -port=8333 -iam.config=/config/sts_distributed.json"
    ports:
      - "8335:8333"
    volumes:
      - ./sts_distributed.json:/config/sts_distributed.json:ro
    depends_on: [filer]
    
  load-balancer:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on: [s3-gateway-1, s3-gateway-2, s3-gateway-3]
```

## Authentication Flow

### 1. OIDC Authentication Flow

```
1. User authenticates with OIDC provider (Keycloak, Auth0, etc.)
   ↓
2. User receives OIDC JWT token from provider
   ↓  
3. User calls SeaweedFS STS AssumeRoleWithWebIdentity
   POST /sts/assume-role-with-web-identity
   {
     "RoleArn": "arn:seaweed:iam::role/S3AdminRole",
     "WebIdentityToken": "eyJ0eXAiOiJKV1QiLCJhbGc...",
     "RoleSessionName": "user-session"
   }
   ↓
4. STS validates OIDC token with configured provider
   - Verifies JWT signature using provider's JWKS
   - Validates issuer, audience, expiration
   - Extracts user identity and groups
   ↓
5. STS checks role trust policy
   - Verifies user/groups can assume the requested role
   - Validates conditions in trust policy
   ↓
6. STS generates temporary credentials
   - Creates temporary access key, secret key, session token
   - Session token is signed JWT with all session information embedded (stateless)
   ↓
7. User receives temporary credentials
   {
     "Credentials": {
       "AccessKeyId": "AKIA...",
       "SecretAccessKey": "base64-secret",
       "SessionToken": "eyJ0eXAiOiJKV1QiLCJhbGc...",
       "Expiration": "2024-01-01T12:00:00Z"
     }
   }
   ↓
8. User makes S3 requests with temporary credentials
   - AWS SDK signs requests with temporary credentials
   - SeaweedFS S3 gateway validates session token
   - Gateway checks permissions via policy engine
```

### 2. Cross-Instance Token Validation

```
User Request → Load Balancer → Any S3 Gateway Instance
                                      ↓
                              Extract JWT Session Token
                                      ↓
                              Validate JWT Token
                              (Self-contained - no external storage needed)
                                      ↓
                              Check Permissions
                              (Shared policy engine)
                                      ↓
                              Allow/Deny Request
```

## Configuration Management

### Development Environment

```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-dev-sts",
    "signingKey": "ZGV2LXNpZ25pbmcta2V5LTMyLWNoYXJhY3RlcnMtbG9uZw==",
    "providers": [
      {
        "name": "dev-mock",
        "type": "mock",
        "enabled": true,
        "config": {
          "issuer": "http://localhost:9999",
          "clientId": "dev-mock-client"
        }
      }
    ]
  }
}
```

### Production Environment

```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-prod-sts",
    "signingKey": "cHJvZC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmctcmFuZG9t",
    "providers": [
      {
        "name": "corporate-sso",
        "type": "oidc",
        "enabled": true,
        "config": {
          "issuer": "https://sso.company.com/realms/production",
          "clientId": "seaweedfs-prod",
          "clientSecret": "${SSO_CLIENT_SECRET}",
          "scopes": ["openid", "profile", "email", "groups"],
          "claimsMapping": {
            "usernameClaim": "preferred_username",
            "groupsClaim": "groups"
          }
        }
      },
      {
        "name": "backup-auth",
        "type": "oidc", 
        "enabled": false,
        "config": {
          "issuer": "https://backup-sso.company.com",
          "clientId": "seaweedfs-backup"
        }
      }
    ]
  }
}
```

## Operational Best Practices

### 1. Configuration Management

- **Version Control**: Store configurations in Git with proper versioning
- **Environment Separation**: Use separate configs for dev/staging/production
- **Secret Management**: Use environment variable substitution for secrets
- **Configuration Validation**: Test configurations before deployment

### 2. Security Considerations

- **Signing Key Security**: Use strong, randomly generated signing keys (32+ bytes)
- **Key Rotation**: Implement signing key rotation procedures
- **Secret Storage**: Store client secrets in secure secret management systems
- **TLS Encryption**: Always use HTTPS for OIDC providers in production

### 3. Monitoring and Troubleshooting

- **Provider Health**: Monitor OIDC provider availability and response times
- **Session Metrics**: Track active sessions, token validation errors
- **Configuration Drift**: Alert on configuration inconsistencies between instances
- **Authentication Logs**: Log authentication attempts for security auditing

### 4. Capacity Planning

- **Provider Performance**: Monitor OIDC provider response times and rate limits
- **Token Validation**: Monitor JWT validation performance and caching
- **Memory Usage**: Monitor JWT token validation caching and provider metadata

## Migration Guide

### From Manual Provider Registration

**Before (Manual Registration):**
```go
// Each instance needs this code
keycloakProvider := oidc.NewOIDCProvider("keycloak-oidc")
keycloakProvider.Initialize(keycloakConfig)
stsService.RegisterProvider(keycloakProvider)
```

**After (Configuration-Driven):**
```json
{
  "sts": {
    "providers": [
      {
        "name": "keycloak-oidc",
        "type": "oidc",
        "enabled": true,
        "config": {
          "issuer": "https://keycloak.company.com/realms/seaweedfs",
          "clientId": "seaweedfs-s3"
        }
      }
    ]
  }
}
```

### Migration Steps

1. **Create Configuration File**: Convert manual provider registrations to JSON config
2. **Test Single Instance**: Deploy config to one instance and verify functionality
3. **Validate Consistency**: Ensure all instances load identical providers
4. **Rolling Deployment**: Update instances one by one with new configuration
5. **Remove Manual Code**: Clean up manual provider registration code

## Troubleshooting

### Common Issues

#### 1. Provider Inconsistency

**Symptoms**: Authentication works on some instances but not others
**Diagnosis**: 
```bash
# Check provider counts on each instance
curl http://instance1:8333/sts/providers | jq '.providers | length'
curl http://instance2:8334/sts/providers | jq '.providers | length'
```
**Solution**: Ensure all instances use identical configuration files

#### 2. Token Validation Failures

**Symptoms**: "Invalid signature" or "Invalid issuer" errors
**Diagnosis**: Check signing key and issuer consistency
**Solution**: Verify `signingKey` and `issuer` are identical across all instances

#### 3. Provider Loading Failures

**Symptoms**: Providers not loaded at startup
**Diagnosis**: Check logs for provider initialization errors
**Solution**: Validate provider configuration against schema

#### 4. OIDC Provider Connectivity

**Symptoms**: "Failed to fetch JWKS" errors
**Diagnosis**: Test OIDC provider connectivity from all instances
**Solution**: Check network connectivity, DNS resolution, certificates

### Debug Commands

```bash
# Test configuration loading
weed s3 -iam.config=/path/to/config.json -test.config

# Validate JWT tokens
curl -X POST http://localhost:8333/sts/validate-token \
  -H "Content-Type: application/json" \
  -d '{"sessionToken": "eyJ0eXAiOiJKV1QiLCJhbGc..."}'

# List loaded providers
curl http://localhost:8333/sts/providers

# Check session store
curl http://localhost:8333/sts/sessions/count
```

## Performance Considerations

### Token Validation Performance

- **JWT Validation**: ~1-5ms per token validation
- **JWKS Caching**: Cache JWKS responses to reduce OIDC provider load
- **Session Lookup**: Filer session lookup adds ~10-20ms latency
- **Concurrent Requests**: Each instance can handle 1000+ concurrent validations

### Scaling Recommendations

- **Horizontal Scaling**: Add more S3 gateway instances behind load balancer
- **Session Store Optimization**: Use SSD storage for filer session store
- **Provider Caching**: Implement JWKS caching to reduce provider load
- **Connection Pooling**: Use connection pooling for filer communication

## Summary

The configuration-driven provider system solves critical distributed deployment issues:

- ✅ **Automatic Provider Loading**: No manual registration code required
- ✅ **Configuration Consistency**: All instances load identical providers from config
- ✅ **Easy Management**: Update config file, restart services
- ✅ **Production Ready**: Supports OIDC, proper session management, distributed storage
- ✅ **Backwards Compatible**: Existing manual registration still works

This enables SeaweedFS S3 Gateway to **scale horizontally** with **consistent authentication** across all instances, making it truly **production-ready for enterprise deployments**.
