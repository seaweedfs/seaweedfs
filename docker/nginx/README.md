# Nginx Reverse Proxy Configuration for SeaweedFS S3 API

This guide explains how to properly configure nginx as a reverse proxy for SeaweedFS S3 API while maintaining AWS Signature V4 authentication compatibility.

## The Challenge

AWS Signature V4 authentication calculates a cryptographic signature based on the exact request including headers, URI, and body. When using nginx as a reverse proxy, any modification to the signed request components will cause signature verification to fail.

## Critical Requirements

1. **Preserve the Authorization header**: Must pass through untouched
2. **Preserve all X-Amz-* headers**: These are part of the signature calculation
3. **Preserve the Host header**: Use `$http_host` instead of `$host` to maintain the original port
4. **Do not modify the request URI**: Avoid path rewriting
5. **Disable buffering for chunked uploads**: Required for streaming uploads
6. **Preserve the request body**: Must not be modified

## Recommended Nginx Configuration

### Basic Configuration for S3 API

```nginx
upstream seaweedfs_s3 {
    server s3:8333;
    keepalive 32;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    # SSL Configuration
    ssl_certificate     /etc/nginx/certs/server.crt;
    ssl_certificate_key /etc/nginx/certs/server.key;

    # Logging
    access_log /var/log/nginx/s3-access.log;
    error_log /var/log/nginx/s3-error.log;

    # Client upload limits
    client_max_body_size 0;  # No limit for S3 uploads
    client_body_timeout 300s;
    
    # Disable buffering for AWS chunked uploads
    proxy_buffering off;
    proxy_request_buffering off;

    # HTTP version and connection settings
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    
    # Timeouts
    proxy_connect_timeout 300s;
    proxy_send_timeout 300s;
    proxy_read_timeout 300s;

    location / {
        proxy_pass http://seaweedfs_s3;

        # CRITICAL: Preserve original Host header including port
        # Use $http_host instead of $host to preserve the port
        proxy_set_header Host $http_host;

        # CRITICAL: Pass all headers through unchanged
        # AWS Signature V4 includes these in signature calculation
        proxy_pass_request_headers on;

        # Optional: Forward client IP information
        # (These are NOT part of AWS signature)
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # CRITICAL: Do not modify request body
        proxy_pass_request_body on;

        # Ignore invalid headers (S3 may send non-standard headers)
        ignore_invalid_headers off;
    }

    # Health check endpoint
    location /health {
        add_header Content-Type text/plain;
        return 200 "OK\n";
    }
}
```

### Configuration with mTLS Client Certificate Authentication

If you need to add mTLS authentication **in addition to** S3 authentication:

```nginx
upstream seaweedfs_s3 {
    server s3:8333;
    keepalive 32;
}

server {
    listen 443 ssl http2;
    server_name your-domain.com;

    # SSL Configuration
    ssl_certificate     /etc/nginx/certs/server.crt;
    ssl_certificate_key /etc/nginx/certs/server.key;
    
    # mTLS Configuration
    ssl_client_certificate /etc/nginx/certs/ca.crt;
    ssl_verify_client optional;  # or 'on' to require client certificates
    ssl_verify_depth 2;

    # Logging with mTLS info
    log_format mtls '$remote_addr - $remote_user [$time_local] '
                    '"$request" $status $body_bytes_sent '
                    'verify=$ssl_client_verify dn="$ssl_client_s_dn"';
    
    access_log /var/log/nginx/s3-mtls-access.log mtls;
    error_log /var/log/nginx/s3-error.log;

    # Client upload limits
    client_max_body_size 0;
    client_body_timeout 300s;
    
    # Disable buffering for AWS chunked uploads
    proxy_buffering off;
    proxy_request_buffering off;

    # HTTP version and connection settings
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    
    # Timeouts
    proxy_connect_timeout 300s;
    proxy_send_timeout 300s;
    proxy_read_timeout 300s;

    location / {
        proxy_pass http://seaweedfs_s3;

        # CRITICAL: Preserve original Host header
        proxy_set_header Host $http_host;

        # CRITICAL: Pass all headers through unchanged
        proxy_pass_request_headers on;

        # Forward client IP information
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Optional: Forward mTLS certificate info to backend
        proxy_set_header X-SSL-Client-Verify $ssl_client_verify;
        proxy_set_header X-SSL-Client-DN $ssl_client_s_dn;

        # CRITICAL: Do not modify request body
        proxy_pass_request_body on;

        # Ignore invalid headers
        ignore_invalid_headers off;
    }
}
```

## Common Mistakes to Avoid

### ❌ DO NOT explicitly set AWS headers

```nginx
# WRONG - Do not do this!
proxy_set_header Authorization $http_authorization;
proxy_set_header X-Amz-Date $http_x_amz_date;
proxy_set_header X-Amz-Content-Sha256 $http_x_amz_content_sha256;
```

**Why?** When you explicitly set headers, nginx may normalize or modify them. Use `proxy_pass_request_headers on` instead to pass ALL headers through unchanged.

### ❌ DO NOT use `proxy_set_header Host $host`

```nginx
# WRONG - Loses port information
proxy_set_header Host $host;
```

**Why?** `$host` contains only the hostname without the port. AWS Signature V4 includes the Host header with port in signature calculation. Use `$http_host` instead.

### ❌ DO NOT enable request buffering for large uploads

```nginx
# WRONG for S3 uploads
proxy_request_buffering on;
```

**Why?** This breaks AWS chunked transfer encoding used for large file uploads. Keep it off for S3 API endpoints.

### ❌ DO NOT rewrite paths

```nginx
# WRONG - Path rewriting breaks signature
location /s3/ {
    rewrite ^/s3/(.*) /$1 break;
    proxy_pass http://seaweedfs_s3;
}
```

**Why?** The URI is part of the AWS Signature V4 calculation. Any path modification will cause signature verification to fail.

## Testing Your Configuration

### Test with AWS CLI

```bash
# Configure AWS CLI
aws configure set aws_access_key_id your_access_key
aws configure set aws_secret_access_key your_secret_key
aws configure set region us-east-1

# Test through nginx proxy
aws s3 ls s3://your-bucket/ --endpoint-url https://your-nginx-domain
```

### Test with boto3 (Python)

```python
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='https://your-nginx-domain',
    aws_access_key_id='your_access_key',
    aws_secret_access_key='your_secret_key',
    region_name='us-east-1'
)

# List buckets
response = s3_client.list_buckets()
print(response)

# Upload a file
s3_client.upload_file('local_file.txt', 'bucket-name', 'remote_file.txt')
```

## Debugging

### Enable detailed logging

Add to your nginx configuration:

```nginx
error_log /var/log/nginx/error_debug.log debug;

log_format detailed '$remote_addr - $remote_user [$time_local] '
                    '"$request" $status $body_bytes_sent '
                    '"$http_authorization" "$http_x_amz_date" '
                    'upstream="$upstream_addr" '
                    'upstream_status=$upstream_status';

access_log /var/log/nginx/detailed.log detailed;
```

### Check SeaweedFS logs

Look for authentication errors in SeaweedFS S3 logs:
- `could not find accessKey` - Access key not configured in SeaweedFS
- `signature mismatch` - Request was modified by proxy
- `InvalidAccessKeyId` - Access key doesn't exist
- `SignatureDoesNotMatch` - Signature calculation failed

### Common Issues

1. **"Could not find accessKey" error**
   - Verify the access key exists in SeaweedFS: `weed shell` → `s3.configure.list`
   - Check that Authorization header is being forwarded
   - Verify SeaweedFS can read its configuration

2. **"SignatureDoesNotMatch" error**
   - Check if nginx is modifying headers or URI
   - Verify `proxy_set_header Host $http_host` is used
   - Disable any header manipulation
   - Check for path rewriting rules

3. **"RequestTimeTooSkewed" error**
   - Ensure server clocks are synchronized (use NTP)
   - Check timezone settings on both nginx and SeaweedFS servers

## Performance Tuning

For production deployments with high throughput:

```nginx
upstream seaweedfs_s3 {
    server s3:8333;
    keepalive 64;  # Increase keepalive connections
    keepalive_timeout 90s;
    keepalive_requests 10000;
}

server {
    # ... other config ...
    
    # Increase worker connections if needed
    # (in nginx.conf, not server block)
    # worker_connections 10000;
    
    # Enable caching for GET requests (optional)
    proxy_cache_path /var/cache/nginx/s3 levels=1:2 keys_zone=s3_cache:10m 
                     max_size=10g inactive=60m use_temp_path=off;
    
    location / {
        # Only cache GET requests
        proxy_cache s3_cache;
        proxy_cache_methods GET HEAD;
        proxy_cache_valid 200 60m;
        proxy_cache_key "$scheme$request_method$host$request_uri";
        
        # Don't cache authenticated requests by default
        proxy_cache_bypass $http_authorization;
        proxy_no_cache $http_authorization;
        
        # ... rest of proxy config ...
    }
}
```

## Docker Compose Example

```yaml
version: '3'

services:
  seaweedfs-s3:
    image: chrislusf/seaweedfs:latest
    ports:
      - "8333:8333"
    command: 's3 -ip=0.0.0.0 -port=8333'
    volumes:
      - ./s3_config:/etc/seaweedfs

  nginx:
    image: nginx:latest
    ports:
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
    depends_on:
      - seaweedfs-s3
```

## Additional Resources

- [AWS Signature Version 4 Documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)
- [SeaweedFS S3 Configuration](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API)
- [Nginx Proxy Module Documentation](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)
