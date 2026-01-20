# IAM/STS Feature Documentation

## Quick Start

### Enable IAM on S3 Gateway

```bash
# Create IAM configuration
cat > iam_config.json << 'EOF'
{
  "Identities": [
    {
      "Name": "admin",
      "Credentials": [{
        "AccessKey": "admin_key",
        "SecretKey": "admin_secret"  
      }],
      "Actions": ["Admin", "Read", "Write", "List"]
    }
  ]
}
EOF

# Upload to filer
curl -X PUT http://localhost:8888/etc/iam/iam_config.json \
  --data-binary @iam_config.json

# Start S3 with IAM
weed s3 \
  -filer="localhost:8888" \
  -iam.config=etc/iam/iam_config.json \
  -port=8333
```

### Create IAM User via API

```python
import boto3

iam = boto3.client(
    'iam',
    endpoint_url='http://localhost:8333',
    aws_access_key_id='admin_key',
    aws_secret_access_key='admin_secret',
    region_name='us-east-1'
)

# Create user
iam.create_user(UserName='alice')

# Create access key
response = iam.create_access_key(UserName='alice')
access_key = response['AccessKey']['AccessKeyId']
secret_key = response['AccessKey']['SecretAccessKey']

print(f"Access Key: {access_key}")
print(f"Secret Key: {secret_key}")
```

### Assume Role Example

```python
import boto3

# Create STS client with user credentials
sts = boto3.client(
    'sts',
    endpoint_url='http://localhost:8333',
    aws_access_key_id='alice_access_key',
    aws_secret_access_key='alice_secret_key'
)

# Assume role
response = sts.assume_role(
    RoleArn='arn:aws:iam::seaweedfs:role/data-scientist',
    RoleSessionName='alice-session',
    DurationSeconds=3600
)

# Extract temporary credentials
creds = response['Credentials']
temp_access = creds['AccessKeyId']
temp_secret = creds['SecretAccessKey']
session_token = creds['SessionToken']

# Use temporary credentials
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8333',
    aws_access_key_id=temp_access,
    aws_secret_access_key=temp_secret,
    aws_session_token=session_token
)

# Now you can access S3 with the role's permissions
s3.list_buckets()
```

## Architecture Overview

```
User/Application
      │
      ▼
┌─────────────────┐
│  S3 API Gateway │  ← SigV4 Authentication
│   (Port 8333)   │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│   IAM/STS Middleware            │
│                                 │
│  ┌──────────┐   ┌────────────┐ │
│  │   IAM    │   │    STS     │ │
│  │ Validator│   │  Validator │ │
│  └────┬─────┘   └─────┬──────┘ │
│       │               │        │
│       ▼               ▼        │
│  ┌──────────────────────────┐ │
│  │   Policy Engine          │ │
│  │  • User Policies         │ │
│  │  • Role Policies         │ │
│  │  • Bucket Policies       │ │
│  └──────────────────────────┘ │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Filer Storage  │  ← IAM metadata
│   (Port 8888)   │     /etc/iam/*
└─────────────────┘
```

## IAM Entities

### Users
- **Purpose**: Represent individual identities
- **Authentication**: Access key + secret key
- **Storage**: `/etc/iam/users/{username}`
- **Policies**: Inline and attached managed policies

### Groups
- **Purpose**: Organize users with common permissions
- **Members**: Multiple users
- **Policies**: Shared policies applied to all members
- **Storage**: `/etc/iam/groups/{groupname}`

### Roles
- **Purpose**: Delegate permissions via AssumeRole
- **Trust Policy**: Who can assume the role
- **Permissions**: What the role can do
- **Session**: Temporary credentials via STS
- **Storage**: `/etc/iam/roles/{rolename}`

### Policies
- **Type**: Managed policies (reusable)
- **Format**: AWS-compatible JSON
- **Effect**: Allow or Deny
- **Resources**: ARN-based resource matching
- **Conditions**: Context-based evaluation
- **Storage**: `/etc/iam/policies/{policyname}`

## Policy Examples

### Read-Only S3 Access
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:GetObject",
      "s3:ListBucket"
    ],
    "Resource": [
      "arn:aws:s3:::my-bucket",
      "arn:aws:s3:::my-bucket/*"
    ]
  }]
}
```

### Time-Based Access
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "s3:*",
    "Resource": "*",
    "Condition": {
      "DateGreaterThan": {
        "aws:CurrentTime": "2024-01-01T08:00:00Z"
      },
      "DateLessThan": {
        "aws:CurrentTime": "2024-01-01T18:00:00Z"
      }
    }
  }]
}
```

### IP-Restricted Access
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "s3:*",
    "Resource": "*",
    "Condition": {
      "IpAddress": {
        "aws:SourceIp": "10.0.0.0/8"
      }
    }
  }]
}
```

## Admin UI

Access the admin UI at `http://localhost:23646/admin`

Features:
- ✅ User management (create, edit, delete)
- ✅ Group management with multi-select members
- ✅ Role management with trust policies
- ✅ Policy editor with validation
- ✅ Access key rotation
- ✅ IAM metrics and monitoring

## API Compatibility

### Supported IAM APIs
- ✅ CreateUser, GetUser, ListUsers, DeleteUser
- ✅ CreateGroup, GetGroup, ListGroups, DeleteGroup
- ✅ CreateRole, GetRole, ListRoles, DeleteRole
- ✅ CreatePolicy, GetPolicy, ListPolicies, DeletePolicy
- ✅ AttachUserPolicy, DetachUserPolicy
- ✅ AttachGroupPolicy, DetachGroupPolicy
- ✅ AttachRolePolicy, DetachRolePolicy
- ✅ AddUserToGroup, RemoveUserFromGroup
- ✅ CreateAccessKey, DeleteAccessKey, ListAccessKeys

### Supported STS APIs
- ✅ AssumeRole
- ✅ GetCallerIdentity

## Security Features

1. **Signature Validation**: AWS SigV4 authentication
2. **JWT Tokens**: Stateless session management with HMAC-SHA256
3. **Policy Evaluation**: Fine-grained access control
4. **Cache Invalidation**: Real-time permission updates
5. **Audit Trail**: Request logging with identity info

## Troubleshooting

### Access Denied Errors
- Verify IAM user exists and has valid credentials
- Check attached policies grant required permissions
- Review bucket policies if accessing specific buckets
- Ensure STS session token hasn't expired

### Role Assumption Fails
- Verify trust policy allows your principal
- Check role permissions policy exists
- Ensure session duration within role limits
- Confirm credentials are valid for AssumeRole API

### Policy Not Taking Effect
- Wait for cache invalidation (~1 second)
- Verify policy is attached to user/group/role
- Check policy syntax is valid JSON
- Review condition clauses match request context

## Performance Tuning

- **Policy Caching**: Policies cached in memory, ~0.5ms lookup
- **Credential Validation**: ~1-2ms per request
- **Role Store Cache**: Active invalidation on updates
- **JWT Verification**: ~0.3ms token validation

## Migration Guide

### From Basic Auth to IAM

1. **Phase 1**: Create IAM users matching existing credentials
2. **Phase 2**: Add IAM config to S3 startup
3. **Phase 3**: Migrate applications to use IAM credentials
4. **Phase 4**: Enable strict IAM enforcement

### Existing Applications

No changes required if:
- Using AWS SDK (boto3, aws-sdk-go, etc.)
- Already configured with access key + secret key
- Just point `endpoint_url` to SeaweedFS S3

## Best Practices

1. **Least Privilege**: Grant minimum required permissions
2. **Role-Based Access**: Use roles instead of user credentials
3. **Key Rotation**: Rotate access keys regularly
4. **Temporary Credentials**: Use STS for short-lived access
5. **Policy Testing**: Test policies before production deployment
6. **Monitoring**: Track IAM API usage via metrics

## Resources

- **IAM API Reference**: AWS IAM compatible
- **Policy Language**: AWS IAM Policy Reference
- **Admin UI**: Built-in at `/admin`
- **Metrics**: Prometheus endpoints on port 9327
