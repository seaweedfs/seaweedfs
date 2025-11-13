#!/bin/bash
set -e

# Keycloak configuration for Docker environment
KEYCLOAK_URL="http://keycloak:8080"
KEYCLOAK_ADMIN_USER="admin"
KEYCLOAK_ADMIN_PASSWORD="admin"
REALM_NAME="seaweedfs-test"
CLIENT_ID="seaweedfs-s3"
CLIENT_SECRET="seaweedfs-s3-secret"

echo "🔧 Setting up Keycloak realm and users for SeaweedFS S3 IAM testing..."
echo "Keycloak URL: $KEYCLOAK_URL"

# Wait for Keycloak to be ready
echo "⏳ Waiting for Keycloak to be ready..."
timeout 120 bash -c '
    until curl -f "$0/health/ready" > /dev/null 2>&1; do 
        echo "Waiting for Keycloak..."
        sleep 5
    done
    echo "[OK] Keycloak health check passed"
' "$KEYCLOAK_URL"

# Download kcadm.sh if not available
if ! command -v kcadm.sh &> /dev/null; then
    echo "📥 Downloading Keycloak admin CLI..."
    wget -q https://github.com/keycloak/keycloak/releases/download/26.0.7/keycloak-26.0.7.tar.gz
    tar -xzf keycloak-26.0.7.tar.gz
    export PATH="$PWD/keycloak-26.0.7/bin:$PATH"
fi

# Wait a bit more for admin user initialization
echo "⏳ Waiting for admin user to be fully initialized..."
sleep 10

# Function to execute kcadm commands with retry and multiple password attempts
kcadm() {
    local max_retries=3
    local retry_count=0
    local passwords=("admin" "admin123" "password")
    
    while [ $retry_count -lt $max_retries ]; do
        for password in "${passwords[@]}"; do
            if kcadm.sh "$@" --server "$KEYCLOAK_URL" --realm master --user "$KEYCLOAK_ADMIN_USER" --password "$password" 2>/dev/null; then
                return 0
            fi
        done
        retry_count=$((retry_count + 1))
        echo "🔄 Retry $retry_count of $max_retries..."
        sleep 5
    done
    
    echo "[FAIL] Failed to execute kcadm command after $max_retries retries"
    return 1
}

# Create realm
echo "📝 Creating realm '$REALM_NAME'..."
kcadm create realms -s realm="$REALM_NAME" -s enabled=true || echo "Realm may already exist"
echo "[OK] Realm created"

# Create OIDC client
echo "📝 Creating client '$CLIENT_ID'..."
CLIENT_UUID=$(kcadm create clients -r "$REALM_NAME" \
    -s clientId="$CLIENT_ID" \
    -s secret="$CLIENT_SECRET" \
    -s enabled=true \
    -s serviceAccountsEnabled=true \
    -s standardFlowEnabled=true \
    -s directAccessGrantsEnabled=true \
    -s 'redirectUris=["*"]' \
    -s 'webOrigins=["*"]' \
    -i 2>/dev/null || echo "existing-client")

if [ "$CLIENT_UUID" != "existing-client" ]; then
    echo "[OK] Client created with ID: $CLIENT_UUID"
else
    echo "[OK] Using existing client"
    CLIENT_UUID=$(kcadm get clients -r "$REALM_NAME" -q clientId="$CLIENT_ID" --fields id --format csv --noquotes | tail -n +2)
fi

# Configure protocol mapper for roles
echo "🔧 Configuring role mapper for client '$CLIENT_ID'..."
MAPPER_CONFIG='{
  "protocol": "openid-connect",
  "protocolMapper": "oidc-usermodel-realm-role-mapper",
  "name": "realm-roles",
  "config": {
    "claim.name": "roles",
    "jsonType.label": "String",
    "multivalued": "true",
    "usermodel.realmRoleMapping.rolePrefix": ""
  }
}'

kcadm create clients/"$CLIENT_UUID"/protocol-mappers/models -r "$REALM_NAME" -b "$MAPPER_CONFIG" 2>/dev/null || echo "[OK] Role mapper already exists"
echo "[OK] Realm roles mapper configured"

# Configure audience mapper to ensure JWT tokens have correct audience claim
echo "🔧 Configuring audience mapper for client '$CLIENT_ID'..."
AUDIENCE_MAPPER_CONFIG='{
  "protocol": "openid-connect",
  "protocolMapper": "oidc-audience-mapper",
  "name": "audience-mapper",
  "config": {
    "included.client.audience": "'$CLIENT_ID'",
    "id.token.claim": "false",
    "access.token.claim": "true"
  }
}'

kcadm create clients/"$CLIENT_UUID"/protocol-mappers/models -r "$REALM_NAME" -b "$AUDIENCE_MAPPER_CONFIG" 2>/dev/null || echo "[OK] Audience mapper already exists"
echo "[OK] Audience mapper configured"

# Create realm roles
echo "📝 Creating realm roles..."
for role in "s3-admin" "s3-read-only" "s3-write-only" "s3-read-write"; do
    kcadm create roles -r "$REALM_NAME" -s name="$role" 2>/dev/null || echo "Role $role may already exist"
done

# Create users with roles
declare -A USERS=(
    ["admin-user"]="s3-admin"
    ["read-user"]="s3-read-only"
    ["write-user"]="s3-read-write"
    ["write-only-user"]="s3-write-only"
)

for username in "${!USERS[@]}"; do
    role="${USERS[$username]}"
    password="${username//[^a-zA-Z]/}123"  # e.g., "admin-user" -> "adminuser123"
    
    echo "📝 Creating user '$username'..."
    kcadm create users -r "$REALM_NAME" \
        -s username="$username" \
        -s enabled=true \
        -s firstName="Test" \
        -s lastName="User" \
        -s email="$username@test.com" 2>/dev/null || echo "User $username may already exist"
    
    echo "🔑 Setting password for '$username'..."
    kcadm set-password -r "$REALM_NAME" --username "$username" --new-password "$password"
    
    echo "➕ Assigning role '$role' to '$username'..."
    kcadm add-roles -r "$REALM_NAME" --uusername "$username" --rolename "$role"
done

# Create IAM configuration for Docker environment
echo "🔧 Setting up IAM configuration for Docker environment..."
cat > iam_config.json << 'EOF'
{
  "sts": {
    "tokenDuration": "1h",
    "maxSessionLength": "12h",
    "issuer": "seaweedfs-sts",
    "signingKey": "dGVzdC1zaWduaW5nLWtleS0zMi1jaGFyYWN0ZXJzLWxvbmc="
  },
  "providers": [
    {
      "name": "keycloak",
      "type": "oidc",
      "enabled": true,
      "config": {
        "issuer": "http://keycloak:8080/realms/seaweedfs-test",
        "clientId": "seaweedfs-s3",
        "clientSecret": "seaweedfs-s3-secret",
        "jwksUri": "http://keycloak:8080/realms/seaweedfs-test/protocol/openid-connect/certs",
        "userInfoUri": "http://keycloak:8080/realms/seaweedfs-test/protocol/openid-connect/userinfo",
        "scopes": ["openid", "profile", "email"],
        "claimsMapping": {
          "username": "preferred_username",
          "email": "email",
          "name": "name"
        },
        "roleMapping": {
          "rules": [
            {
              "claim": "roles",
              "value": "s3-admin",
              "role": "arn:aws:iam::role/KeycloakAdminRole"
            },
            {
              "claim": "roles",
              "value": "s3-read-only",
              "role": "arn:aws:iam::role/KeycloakReadOnlyRole"
            },
            {
              "claim": "roles",
              "value": "s3-write-only",
              "role": "arn:aws:iam::role/KeycloakWriteOnlyRole"
            },
            {
              "claim": "roles",
              "value": "s3-read-write",
              "role": "arn:aws:iam::role/KeycloakReadWriteRole"
            }
          ],
          "defaultRole": "arn:aws:iam::role/KeycloakReadOnlyRole"
        }
      }
    }
  ],
  "policy": {
    "defaultEffect": "Deny"
  },
  "roles": [
    {
      "roleName": "KeycloakAdminRole",
      "roleArn": "arn:aws:iam::role/KeycloakAdminRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "keycloak"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3AdminPolicy"],
      "description": "Admin role for Keycloak users"
    },
    {
      "roleName": "KeycloakReadOnlyRole",
      "roleArn": "arn:aws:iam::role/KeycloakReadOnlyRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "keycloak"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3ReadOnlyPolicy"],
      "description": "Read-only role for Keycloak users"
    },
    {
      "roleName": "KeycloakWriteOnlyRole",
      "roleArn": "arn:aws:iam::role/KeycloakWriteOnlyRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "keycloak"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3WriteOnlyPolicy"],
      "description": "Write-only role for Keycloak users"
    },
    {
      "roleName": "KeycloakReadWriteRole",
      "roleArn": "arn:aws:iam::role/KeycloakReadWriteRole",
      "trustPolicy": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": {
              "Federated": "keycloak"
            },
            "Action": ["sts:AssumeRoleWithWebIdentity"]
          }
        ]
      },
      "attachedPolicies": ["S3ReadWritePolicy"],
      "description": "Read-write role for Keycloak users"
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
              "arn:aws:s3:::*",
              "arn:aws:s3:::*/*"
            ]
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
      "name": "S3WriteOnlyPolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
              "arn:aws:s3:::*",
              "arn:aws:s3:::*/*"
            ]
          },
          {
            "Effect": "Deny",
            "Action": [
              "s3:GetObject",
              "s3:ListBucket"
            ],
            "Resource": [
              "arn:aws:s3:::*",
              "arn:aws:s3:::*/*"
            ]
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
      "name": "S3ReadWritePolicy",
      "document": {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
              "arn:aws:s3:::*",
              "arn:aws:s3:::*/*"
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

# Validate setup by testing authentication
echo "🔍 Validating setup by testing admin-user authentication and role mapping..."
KEYCLOAK_TOKEN_URL="http://keycloak:8080/realms/$REALM_NAME/protocol/openid-connect/token"

# Get access token for admin-user
ACCESS_TOKEN=$(curl -s -X POST "$KEYCLOAK_TOKEN_URL" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=password" \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" \
    -d "username=admin-user" \
    -d "password=adminuser123" \
    -d "scope=openid profile email" | jq -r '.access_token')

if [ "$ACCESS_TOKEN" = "null" ] || [ -z "$ACCESS_TOKEN" ]; then
    echo "[FAIL] Failed to obtain access token"
    exit 1
fi

echo "[OK] Authentication validation successful"

# Decode and check JWT claims
PAYLOAD=$(echo "$ACCESS_TOKEN" | cut -d'.' -f2)
# Add padding for base64 decode
while [ $((${#PAYLOAD} % 4)) -ne 0 ]; do
    PAYLOAD="${PAYLOAD}="
done

CLAIMS=$(echo "$PAYLOAD" | base64 -d 2>/dev/null | jq .)
ROLES=$(echo "$CLAIMS" | jq -r '.roles[]?')

if [ -n "$ROLES" ]; then
    echo "[OK] JWT token includes roles: [$(echo "$ROLES" | tr '\n' ',' | sed 's/,$//' | sed 's/,/, /g')]"
else
    echo "⚠️  No roles found in JWT token"
fi

echo "[OK] Keycloak test realm '$REALM_NAME' configured for Docker environment"
echo "🐳 Setup complete! You can now run: docker-compose up -d"
