# Distributed IAM for SeaweedFS S3 Gateway

This document explains how to configure SeaweedFS S3 Gateway for distributed environments with multiple gateway instances.

## Problem Statement

In distributed environments with multiple S3 gateway instances, the default in-memory storage for IAM components causes **serious consistency issues**:

- **Roles**: Each instance maintains separate in-memory role definitions
- **Policies**: Policy updates don't propagate across instances
- **Authentication inconsistency**: Users may get different responses from different instances

## Solution: Stateless JWT + Filer-Based Storage

SeaweedFS now supports **distributed IAM** using:

- ✅ **Stateless STS**: JWT tokens contain all session information (no session storage needed)
- ✅ **Role definitions**: Filer-based distributed storage (FilerRoleStore)
- ✅ **IAM policies**: Filer-based distributed storage (FilerPolicyStore)

All S3 gateway instances share the same IAM state through the filer, and STS tokens are completely stateless.

## Configuration

### Standard Configuration (Single Instance)
```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-sts",
    "signingKey": "base64-encoded-signing-key"
  },
  "policy": {
    "defaultEffect": "Deny",
    "storeType": "memory"
  }
}
```

### Distributed Configuration (Multiple Instances)
```json
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-sts",
    "signingKey": "base64-encoded-signing-key"
  },
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
  }
}
```

**Key Configuration Changes for Distribution:**

- **STS remains stateless**: No session store configuration needed as JWT tokens are self-contained
- **Policy store uses filer**: `"storeType": "filer"` enables distributed policy storage
- **Role store uses filer**: Ensures all instances share the same role definitions

## Storage Backends

### Memory Storage (Default)
- **Performance**: Fastest access (sub-millisecond)
- **Persistence**: Lost on restart
- **Distribution**: ❌ Not shared across instances
- **Use case**: Single instance or development

### Filer Storage (Distributed)  
- **Performance**: Network latency + filer performance
- **Persistence**: ✅ Survives restarts
- **Distribution**: ✅ Shared across all instances
- **Use case**: Production multi-instance deployments

## Deployment Examples

### Single Instance (Simple)
```bash
# Standard deployment - no special configuration needed
weed s3 -filer=localhost:8888 -port=8333
```

### Multiple Instances (Distributed)
```bash
# Instance 1
weed s3 -filer=localhost:8888 -port=8333 -iam.config=/path/to/distributed_config.json

# Instance 2  
weed s3 -filer=localhost:8888 -port=8334 -iam.config=/path/to/distributed_config.json

# Instance N
weed s3 -filer=localhost:8888 -port=833N -iam.config=/path/to/distributed_config.json
```

All instances share the same IAM state through the filer.

### Load Balancer Configuration
```nginx
upstream seaweedfs-s3 {
    # No need for session affinity with distributed storage
    server gateway-1:8333;
    server gateway-2:8334; 
    server gateway-3:8335;
}

server {
    listen 80;
    location / {
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_pass http://seaweedfs-s3;
    }
}
```

## Storage Locations

When using filer storage, IAM data is stored at:

```
/seaweedfs/iam/
├── policies/           # IAM policy documents  
│   ├── policy_S3AdminPolicy.json
│   └── policy_S3ReadOnlyPolicy.json
└── roles/              # IAM role definitions
    ├── S3AdminRole.json
    └── S3ReadOnlyRole.json
```

## Performance Considerations

### Memory vs Filer Storage

| Aspect | Memory | Filer |
|--------|--------|-------|
| **Read Latency** | <1ms | ~5-20ms |
| **Write Latency** | <1ms | ~10-50ms |
| **Consistency** | Per-instance | Global |
| **Persistence** | None | Full |
| **Scalability** | Limited | Unlimited |

### Optimization Tips

1. **Use fast storage** for the filer (SSD recommended)
2. **Co-locate filer** with S3 gateways to reduce network latency
3. **Enable filer caching** for frequently accessed IAM data
4. **Monitor filer performance** - IAM operations depend on it

## Migration Guide

### From Single to Multi-Instance

1. **Stop existing S3 gateway**
2. **Update configuration** to use filer storage:
   ```json
   {
     "policy": { "storeType": "filer" },
     "roleStore": { "storeType": "filer" }
   }
   ```
3. **Start first instance** with new config
4. **Verify IAM data** was migrated to filer
5. **Start additional instances** with same config

### From Memory to Filer Storage

IAM data in memory is **not automatically migrated**. You'll need to:

1. **Export existing roles/policies** from configuration files
2. **Update configuration** to use filer storage  
3. **Restart with new config** - data will be loaded from config into filer
4. **Remove config-based definitions** (now stored in filer)

## Troubleshooting

### Configuration Issues
```bash
# Check IAM configuration is loaded correctly
grep "advanced IAM" /path/to/s3-gateway.log

# Verify filer connectivity
weed filer.ls /seaweedfs/iam/roles/
```

### Inconsistent Behavior
```bash
# Check if all instances use same filer
curl http://gateway-1:8333/status  
curl http://gateway-2:8334/status

# Verify IAM storage locations exist
weed filer.ls /seaweedfs/iam/
```

### Performance Issues
```bash
# Monitor filer latency
curl http://filer:8888/stats

# Check IAM storage path performance  
time weed filer.cat /seaweedfs/iam/roles/TestRole.json
```

## Best Practices

### Security
- **Secure filer access** - use TLS and authentication
- **Limit IAM path access** - only S3 gateways should access `/seaweedfs/iam/`
- **Regular backups** of IAM data in filer
- **Monitor access** to IAM storage paths

### High Availability
- **Replicate filer** across multiple nodes
- **Use fast, reliable storage** for filer backend
- **Monitor filer health** - critical for IAM operations
- **Implement alerts** for IAM storage issues

### Capacity Planning
- **Estimate IAM data size**: Roles + Policies (no sessions - stateless JWT)
- **Plan for growth**: Role and policy count scales with user/application count  
- **Monitor filer disk usage**: `/seaweedfs/iam/` path
- **Set up log rotation**: For IAM audit logs

## Architecture Comparison

### Before (Memory-Only)
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ S3 Gateway 1│    │ S3 Gateway 2│    │ S3 Gateway 3│
│             │    │             │    │             │  
│ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │
│ │ IAM     │ │    │ │ IAM     │ │    │ │ IAM     │ │
│ │ Memory  │ │    │ │ Memory  │ │    │ │ Memory  │ │
│ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │
└─────────────┘    └─────────────┘    └─────────────┘
      ❌               ❌               ❌
 Inconsistent      Inconsistent    Inconsistent
```

### After (Filer-Distributed)
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ S3 Gateway 1│    │ S3 Gateway 2│    │ S3 Gateway 3│
│             │    │             │    │             │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
                ┌─────────▼─────────┐
                │   SeaweedFS       │
                │   Filer           │
                │ ┌───────────────┐ │
                │ │ /seaweedfs/   │ │
                │ │ iam/          │ │
                │ │ ├─roles/      │ │
                │ │ └─policies/   │ │
                │ └───────────────┘ │
                └───────────────────┘
                        ✅
                   Consistent
```

## Summary

The distributed IAM system solves critical consistency issues in multi-instance deployments by:

- **Centralizing IAM state** in SeaweedFS filer
- **Ensuring consistency** across all gateway instances  
- **Maintaining performance** with configurable storage backends
- **Supporting scalability** for any number of gateway instances

For production deployments with multiple S3 gateway instances, **always use filer-based storage** for all IAM components.
