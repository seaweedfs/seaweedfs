# SeaweedFS IAM Management API Status

## Overview

This document provides a comprehensive analysis of the IAM management APIs currently implemented in SeaweedFS vs. standard AWS IAM APIs.

## Implemented APIs

SeaweedFS currently implements **42 IAM management APIs** covering users, policies, roles, and groups:

### User Management (6 APIs)
| API | Status | Notes |
|-----|--------|-------|
| `CreateUser` | ✅ Implemented | Creates IAM user in S3 config |
| `GetUser` | ✅ Implemented | Retrieves user details |
| `UpdateUser` | ✅ Implemented | Modifies user properties |
| `DeleteUser` | ✅ Implemented | Removes user from system |
| `ListUsers` | ✅ Implemented | Lists all IAM users |
| `CreateAccessKey` | ✅ Implemented | Generates access key for user |

### Access Key Management (4 APIs)
| API | Status | Notes |
|-----|--------|-------|
| `ListAccessKeys` | ✅ Implemented | Lists keys for a user |
| `DeleteAccessKey` | ✅ Implemented | Removes access key |
| `UpdateAccessKey` | ✅ Implemented | Enable/disable access keys |
| `GetAccessKeyLastUsed` | ✅ Implemented | Track key usage (CreateDate) |

### Policy Management (9 APIs)
| API | Status | Notes |
|-----|--------|-------|
| `CreatePolicy` | ✅ Implemented | Creates managed policy |
| `GetPolicy` | ✅ Implemented | Retrieve managed policy metadata |
| `DeletePolicy` | ✅ Implemented | Remove managed policy (with attachment check) |
| `ListPolicies` | ✅ Implemented | List all managed policies |
| `PutUserPolicy` | ✅ Implemented | Attaches inline policy to user |
| `GetUserPolicy` | ✅ Implemented | Retrieves user inline policy |
| `DeleteUserPolicy` | ✅ Implemented | Removes user inline policy |
| `CreatePolicyVersion` | ✅ Implemented | Updates managed policy (simulated versioning) |
| `GetPolicyVersion` | ✅ Implemented | Retrieve specific policy version document |

**Policy Management Notes:**
- `CreatePolicyVersion` allows **in-place updates** of managed policies
- Simple versioning supported (v1 default)
- Policies stored in S3 config at `/etc/iam/policies/`
- `DeletePolicy` enforces AWS-compliant attachment checking
- Metadata includes PolicyId, ARN, Description, timestamps

### Role Management (13 APIs)
| API | Status | Notes |
|-----|--------|-------|
| `CreateRole` | ✅ Implemented | Creates IAM role with trust policy |
| `GetRole` | ✅ Implemented | Retrieves role details |
| `ListRoles` | ✅ Implemented | Lists all IAM roles |
| `DeleteRole` | ✅ Implemented | Removes role from system |
| `UpdateRole` | ✅ Implemented | Updates role description/max session duration |
| `UpdateAssumeRolePolicy` | ✅ Implemented | Updates role trust policy |
| `AttachRolePolicy` | ✅ Implemented | Attaches managed policy to role |
| `DetachRolePolicy` | ✅ Implemented | Detaches managed policy from role |
| `ListAttachedRolePolicies` | ✅ Implemented | Lists managed policies attached to role |
| `PutRolePolicy` | ✅ Implemented | Adds/updates inline policy on role |
| `GetRolePolicy` | ✅ Implemented | Retrieves inline policy from role |
| `DeleteRolePolicy` | ✅ Implemented | Deletes inline policy from role |
| `ListRolePolicies` | ✅ Implemented | Lists inline policies on role |


**Cache Invalidation Implementation**:
- ✅ **Auto Cache Invalidation** - Filer pub/sub automatically invalidates S3 role cache
```go
// S3 API subscribes to Filer metadata events
directoriesToWatch := []string{
    filer.IamConfigDirectory,           // /etc/iam
    filer.IamConfigDirectory + "/roles", // /etc/iam/roles
    s3ApiServer.option.BucketsPath
}

// On role file change → onIamRoleUpdate() → InvalidateCache(roleName)
// This ensures all S3 instances stay synchronized with Filer
```


### Group Management (10 APIs)
| API | Status | Notes |
|-----|--------|-------|
| `CreateGroup` | ✅ Implemented | Creates IAM group with unique GroupId |
| `GetGroup` | ✅ Implemented | Retrieves group details and members |
| `UpdateGroup` | ✅ Implemented | Updates group name (preserves immutable GroupId) |
| `DeleteGroup` | ✅ Implemented | Removes group (validates no members/policies) |
| `ListGroups` | ✅ Implemented | Lists all IAM groups |
| `AddUserToGroup` | ✅ Implemented | Adds user to group (validates user exists) |
| `RemoveUserFromGroup` | ✅ Implemented | Removes user from group (AWS-compliant idempotence) |
| `ListGroupsForUser` | ✅ Implemented | Lists groups a user belongs to |
| `AttachGroupPolicy` | ✅ Implemented | Attaches managed policy to group |
| `DetachGroupPolicy` | ✅ Implemented | Detaches managed policy from group |

**Group Management Notes:**
- Groups stored in Filer at `/etc/iam/groups/{GroupName}.json`
- GroupId is **unique and immutable** across renames (generated via hash)
- `UpdateGroup` uses best-effort semantics with rollback on failure
- `RemoveUserFromGroup` is **idempotent** (succeeds silently if user not in group)
- `AddUserToGroup` validates user existence (AWS compliance)
- `DeleteGroup` enforces AWS-compliant constraints:
  - ❌ Blocks deletion if group has members
  - ❌ Blocks deletion if group has attached policies
- **Cached implementation**: 5-minute TTL for group data, 1-minute TTL for list operations
- Cache invalidation via Filer pub/sub (automatic synchronization)


### Low Priority - Account/Security (8 APIs)
| API | Priority | Notes |
|-----|----------|-------|
| `GetAccountSummary` | 🟢 Low | IAM resource limits |
| `GetAccountPasswordPolicy` | 🟢 Low | Password requirements |
| `UpdateAccountPasswordPolicy` | 🟢 Low | Set password policy |
| `GetCredentialReport` | 🟢 Low | Security audit report |
| `GenerateCredentialReport` | 🟢 Low | Create credential report |
| `ChangePassword` | 🟢 Low | User password change |
| `GetLoginProfile` | 🟢 Low | Console login settings |
| `CreateLoginProfile` | 🟢 Low | Enable console access |

### Low Priority - Advanced Features (10+ APIs)
| Category | APIs | Priority | Notes |
|----------|------|----------|-------|
| MFA | `EnableMFADevice`, `DeactivateMFADevice`, `ListMFADevices`, etc. | 🟢 Low | Multi-factor authentication |
| SAML | `CreateSAMLProvider`, `UpdateSAMLProvider`, `ListSAMLProviders` | 🟢 Low | Federated identity |
| OIDC | `CreateOpenIDConnectProvider`, `DeleteOpenIDConnectProvider` | 🟢 Low | Web identity federation |
| Service Roles | `CreateServiceLinkedRole`, `DeleteServiceLinkedRole` | 🟢 Low | AWS service integration |
| Instance Profiles | `CreateInstanceProfile`, `AddRoleToInstanceProfile` | 🟢 Low | EC2 integration |
| Virtual MFA | `CreateVirtualMFADevice`, `DeleteVirtualMFADevice` | 🟢 Low | Software MFA tokens |

## Current Implementation Architecture

### Storage Model
- **Users**: Stored in Filer at `/etc/iam/identity.json` (centralized) OR `/etc/iam/users/{UserName}.json` (split files)
- **Roles**: Stored in Filer at `/etc/iam/roles/{RoleName}.json` (centralized)
- **Policies**: Stored in Filer at `/etc/iam/policies/{PolicyName}.json` (centralized)
- **Groups**: Stored in Filer at `/etc/iam/groups/{GroupName}.json` (centralized)

**Benefits**:
- ✅ All IAM data centralized on Filer
- ✅ HA-compatible (multiple S3 instances share same user/role/policy data)
- ✅ Cache invalidation via Filer notifications
- ✅ Consistent backup/restore strategy

### Key Design Characteristics

1. **User Storage**: Filer-based with dual storage support
   - **Centralized mode**: `/etc/iam/identity.json` - Single JSON file contains all users
   - **Split file mode**: `/etc/iam/users/{UserName}.json` - One file per user
   - System automatically loads from both locations and merges results
   - Deduplication ensures users aren't listed twice when appearing in both locations
   - Supports HA deployments (shared across S3 instances)
   - Auto-chunked if centralized file exceeds ~256 bytes (typically chunked)

2. **Role Storage**: Filer-based (one file per role)
   - Path: `/etc/iam/roles/{RoleName}.json`
   - Supports HA deployments
   - Cache invalidation via Filer notifications
   - Individual files allow granular updates

3. **Policy Storage**: Filer-based (one file per policy)
   - Path: `/etc/iam/policies/{PolicyName}.json`
   - Hybrid read strategy (inline/chunked)
   - MasterClient required for large policies (≥256 bytes)
   - Cache TTL: 5 minutes

4. **Group Storage**: Filer-based (one file per group)
   - Path: `/etc/iam/groups/{GroupName}.json`
   - Individual files allow granular updates
   - Cache invalidation via Filer notifications
   - Cache TTL: 5 minutes (groups), 1 minute (list)
   - Supports HA deployments with automatic sync

**Note**: Admin UI uses IAM HTTP API endpoints for all operations.

### Cache Invalidation Architecture

```
Filer Metadata Change (/etc/iam/roles/Admin.json updated)
  ↓
Filer publishes event via subscribeMetaEvents
  ↓
All S3 instances receive notification
  ↓
onIamRoleUpdate() callback
  ↓
S3IAMIntegration.OnRoleUpdate("Admin")
  ↓
InvalidateCache("Admin")
  ↓
Next request re-loads role from Filer
```

**Performance**:
- **Event Propagation**: < 1 second
- **Cache Invalidation**: Immediate
- **Role Reload**: On-demand (next request)



### Implemented APIs: ✅ Tested via boto3/AWS CLI

**User Management (6 APIs)**:
- CreateUser, DeleteUser, ListUsers, GetUser, UpdateUser
- CreateAccessKey

**Access Key Management (4 APIs)**:
- DeleteAccessKey, ListAccessKeys, UpdateAccessKey, GetAccessKeyLastUsed

**Policy Management (7 APIs)**:
- PutUserPolicy, GetUserPolicy, DeleteUserPolicy
- CreatePolicy, GetPolicy, DeletePolicy, ListPolicies
- CreatePolicyVersion, GetPolicyVersion

**Role Management (13 APIs)**:
- CreateRole, GetRole, ListRoles, DeleteRole, UpdateRole
- UpdateAssumeRolePolicy
- AttachRolePolicy, DetachRolePolicy, ListAttachedRolePolicies
- PutRolePolicy, GetRolePolicy, DeleteRolePolicy, ListRolePolicies

**Group Management (10 APIs)** ✨ NEW:
- CreateGroup, GetGroup, UpdateGroup, DeleteGroup, ListGroups
- AddUserToGroup, RemoveUserFromGroup, ListGroupsForUser
- AttachGroupPolicy, DetachGroupPolicy

### Missing APIs: ❌ Not Implemented
Advanced APIs (MFA, SAML, OIDC, etc.) return `InvalidAction` error.

## References

- **AWS IAM API Reference**: https://docs.aws.amazon.com/IAM/latest/APIReference/
