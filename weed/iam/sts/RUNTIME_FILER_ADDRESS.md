# Runtime Filer Address Implementation

This document describes the implementation of runtime filer address passing for the STSService, addressing the requirement that filer addresses should be passed at call-time rather than initialization time.

## Problem Statement

The user identified a critical issue with the original STS implementation:

> "the filer address should be passed when called, not during init time, since the filer may change."

This is important because:
1. **Filer Failover**: Filer addresses can change during runtime due to failover scenarios
2. **Load Balancing**: Different requests may need to hit different filer instances
3. **Environment Agnostic**: Configuration files should work across dev/staging/prod without hardcoded addresses
4. **SeaweedFS Patterns**: Follows existing SeaweedFS patterns used throughout the codebase

## Implementation Changes

### 1. SessionStore Interface Refactoring

**Before:**
```go
type SessionStore interface {
    StoreSession(ctx context.Context, sessionId string, session *SessionInfo) error
    GetSession(ctx context.Context, sessionId string) (*SessionInfo, error)
    RevokeSession(ctx context.Context, sessionId string) error
    CleanupExpiredSessions(ctx context.Context) error
}
```

**After:**
```go
type SessionStore interface {
    // filerAddress ignored for memory stores, required for filer stores
    StoreSession(ctx context.Context, filerAddress string, sessionId string, session *SessionInfo) error
    GetSession(ctx context.Context, filerAddress string, sessionId string) (*SessionInfo, error)
    RevokeSession(ctx context.Context, filerAddress string, sessionId string) error
    CleanupExpiredSessions(ctx context.Context, filerAddress string) error
}
```

### 2. FilerSessionStore Changes

**Before:**
```go
type FilerSessionStore struct {
    filerGrpcAddress string  // ❌ Fixed at init time
    grpcDialOption   grpc.DialOption
    basePath         string
}

func NewFilerSessionStore(filerAddress string, config map[string]interface{}) (*FilerSessionStore, error) {
    store := &FilerSessionStore{
        filerGrpcAddress: filerAddress,  // ❌ Locked in during init
        basePath:         DefaultSessionBasePath,
    }
    // ...
}
```

**After:**
```go
type FilerSessionStore struct {
    grpcDialOption grpc.DialOption  // ✅ No fixed filer address
    basePath       string
}

func NewFilerSessionStore(config map[string]interface{}) (*FilerSessionStore, error) {
    store := &FilerSessionStore{
        basePath: DefaultSessionBasePath,  // ✅ Only path configuration
    }
    // ✅ filerAddress passed at call time
}

func (f *FilerSessionStore) StoreSession(ctx context.Context, filerAddress string, sessionId string, session *SessionInfo) error {
    // ✅ filerAddress provided per call
    return f.withFilerClient(filerAddress, func(client filer_pb.SeaweedFilerClient) error {
        // ... store logic
    })
}
```

### 3. STS Service Method Signatures

**Before:**
```go
func (s *STSService) AssumeRoleWithWebIdentity(ctx context.Context, request *AssumeRoleWithWebIdentityRequest) (*AssumeRoleResponse, error)
func (s *STSService) ValidateSessionToken(ctx context.Context, sessionToken string) (*SessionInfo, error)
func (s *STSService) RevokeSession(ctx context.Context, sessionToken string) error
```

**After:**
```go
func (s *STSService) AssumeRoleWithWebIdentity(ctx context.Context, filerAddress string, request *AssumeRoleWithWebIdentityRequest) (*AssumeRoleResponse, error)
func (s *STSService) ValidateSessionToken(ctx context.Context, filerAddress string, sessionToken string) (*SessionInfo, error)
func (s *STSService) RevokeSession(ctx context.Context, filerAddress string, sessionToken string) error
```

### 4. Configuration Cleanup

**Before (iam_config_distributed.json):**
```json
{
  "sts": {
    "sessionStoreConfig": {
      "filerAddress": "localhost:8888",  // ❌ Environment-specific
      "basePath": "/etc/iam/sessions"
    }
  },
  "policy": {
    "storeConfig": {
      "filerAddress": "localhost:8888",  // ❌ Environment-specific
      "basePath": "/etc/iam/policies"
    }
  }
}
```

**After (iam_config_distributed.json):**
```json
{
  "sts": {
    "sessionStoreConfig": {
      "basePath": "/etc/iam/sessions"    // ✅ Environment-agnostic
    }
  },
  "policy": {
    "storeConfig": {
      "basePath": "/etc/iam/policies"    // ✅ Environment-agnostic
    }
  }
}
```

## Usage Examples

### Caller Perspective (S3 API Server)

**Before:**
```go
// STS service locked to specific filer during init
stsService.Initialize(&STSConfig{
    SessionStoreConfig: map[string]interface{}{
        "filerAddress": "filer-1:8888",  // ❌ Fixed choice
        "basePath": "/etc/iam/sessions",
    },
})

// All calls go to filer-1, no failover possible
response, err := stsService.AssumeRoleWithWebIdentity(ctx, request)
```

**After:**
```go
// STS service configured without specific filer
stsService.Initialize(&STSConfig{
    SessionStoreConfig: map[string]interface{}{
        "basePath": "/etc/iam/sessions",  // ✅ Just the path
    },
})

// Caller determines filer address per request
currentFiler := s.getCurrentFilerAddress()  // ✅ Dynamic selection
response, err := stsService.AssumeRoleWithWebIdentity(ctx, currentFiler, request)
```

### Dynamic Filer Selection

```go
type S3ApiServer struct {
    stsService   *sts.STSService
    filerClient  *filer.Client
}

func (s *S3ApiServer) getCurrentFilerAddress() string {
    // ✅ Can implement any strategy:
    // - Load balancing across multiple filers
    // - Health checking and failover
    // - Geographic routing
    // - Round-robin selection
    return s.filerClient.GetAvailableFiler()
}

func (s *S3ApiServer) handleAssumeRole(ctx context.Context, request *AssumeRoleRequest) {
    // ✅ Filer address determined at request time
    filerAddr := s.getCurrentFilerAddress()
    
    response, err := s.stsService.AssumeRoleWithWebIdentity(ctx, filerAddr, request)
    if err != nil && isNetworkError(err) {
        // ✅ Retry with different filer
        filerAddr = s.getBackupFilerAddress()
        response, err = s.stsService.AssumeRoleWithWebIdentity(ctx, filerAddr, request)
    }
}
```

## Memory Store Compatibility

The `MemorySessionStore` accepts the `filerAddress` parameter but ignores it, maintaining interface consistency:

```go
func (m *MemorySessionStore) StoreSession(ctx context.Context, filerAddress string, sessionId string, session *SessionInfo) error {
    // filerAddress ignored for memory store - maintains interface compatibility
    if sessionId == "" {
        return fmt.Errorf(ErrSessionIDCannotBeEmpty)
    }
    // ... in-memory storage logic
}
```

## Benefits Achieved

### 1. **Dynamic Filer Selection**
```go
// Load balancing
filerAddr := loadBalancer.GetNextFiler()

// Failover support
filerAddr := failoverManager.GetHealthyFiler()

// Geographic routing
filerAddr := geoRouter.GetClosestFiler(clientIP)
```

### 2. **Environment Portability**
```bash
# Same config works everywhere
dev:     STSService.method(ctx, "dev-filer:8888", ...)
staging: STSService.method(ctx, "staging-filer:8888", ...)
prod:    STSService.method(ctx, "prod-filer-lb:8888", ...)
```

### 3. **Operational Flexibility**
- **Hot filer replacement**: Switch filers without restarting STS
- **A/B testing**: Route different requests to different filers
- **Disaster recovery**: Automatic failover to backup filers
- **Performance optimization**: Route to least loaded filer

### 4. **SeaweedFS Consistency**
Follows the same pattern used throughout SeaweedFS codebase where filer addresses are passed to methods, not stored in structs.

## Migration Guide

### For Code Calling STS Methods

**Before:**
```go
response, err := stsService.AssumeRoleWithWebIdentity(ctx, request)
session, err := stsService.ValidateSessionToken(ctx, token)
err := stsService.RevokeSession(ctx, token)
```

**After:**
```go
filerAddr := getCurrentFilerAddress()  // Implement your strategy
response, err := stsService.AssumeRoleWithWebIdentity(ctx, filerAddr, request)
session, err := stsService.ValidateSessionToken(ctx, filerAddr, token)
err := stsService.RevokeSession(ctx, filerAddr, token)
```

### For Configuration Files

Remove `filerAddress` from all store configurations:

```bash
# Update all iam_config*.json files
sed -i 's|"filerAddress": ".*",||g' iam_config*.json
```

## Testing

All tests have been updated to pass a test filer address:

```go
func TestAssumeRoleWithWebIdentity(t *testing.T) {
    service := setupTestSTSService(t)
    testFilerAddress := "localhost:8888" // Test filer address
    
    response, err := service.AssumeRoleWithWebIdentity(ctx, testFilerAddress, request)
    // ... test logic
}
```

## Production Deployment

### High Availability Setup

```go
type FilerManager struct {
    primaryFilers   []string
    backupFilers    []string
    healthChecker   *HealthChecker
}

func (fm *FilerManager) GetAvailableFiler() string {
    // Check primary filers first
    for _, filer := range fm.primaryFilers {
        if fm.healthChecker.IsHealthy(filer) {
            return filer
        }
    }
    
    // Fallback to backup filers
    for _, filer := range fm.backupFilers {
        if fm.healthChecker.IsHealthy(filer) {
            return filer
        }
    }
    
    // Return first primary as last resort
    return fm.primaryFilers[0]
}
```

### Load Balanced Configuration

```json
{
  "sts": {
    "sessionStoreType": "filer",
    "sessionStoreConfig": {
      "basePath": "/etc/iam/sessions"
    }
  }
}
```

```go
// Runtime filer selection
filerLoadBalancer := &RoundRobinBalancer{
    Filers: []string{
        "filer-1.prod:8888",
        "filer-2.prod:8888", 
        "filer-3.prod:8888",
    },
}

response, err := stsService.AssumeRoleWithWebIdentity(
    ctx, 
    filerLoadBalancer.Next(),  // ✅ Dynamic selection
    request,
)
```

## Conclusion

This refactoring successfully addresses the requirement for runtime filer address passing, enabling:

- ✅ **Dynamic filer selection** per request
- ✅ **Automatic failover** capabilities  
- ✅ **Environment-agnostic** configurations
- ✅ **Load balancing** support
- ✅ **SeaweedFS pattern** compliance
- ✅ **Operational flexibility** for production deployments

The implementation maintains backward compatibility for memory stores while enabling powerful distributed deployment scenarios for filer-backed stores.
