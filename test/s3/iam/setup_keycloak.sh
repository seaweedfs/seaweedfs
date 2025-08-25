#!/bin/bash

# Keycloak Setup Script for CI/CD
# This script sets up a Keycloak realm with test users and roles for SeaweedFS S3 IAM testing

set -e

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
ADMIN_USER="${KEYCLOAK_ADMIN:-admin}"
ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-admin}"
REALM_NAME="seaweedfs-test"
CLIENT_ID="seaweedfs-s3"
CLIENT_SECRET="seaweedfs-s3-secret"

echo "🔧 Setting up Keycloak realm and users for SeaweedFS S3 IAM testing..."
echo "Keycloak URL: $KEYCLOAK_URL"

# Function to get admin access token
get_admin_token() {
    curl -s -X POST "$KEYCLOAK_URL/realms/master/protocol/openid-connect/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=$ADMIN_USER" \
        -d "password=$ADMIN_PASSWORD" \
        -d "grant_type=password" \
        -d "client_id=admin-cli" | jq -r '.access_token'
}

# Function to check if realm exists
realm_exists() {
    local token=$1
    curl -s -H "Authorization: Bearer $token" \
        "$KEYCLOAK_URL/admin/realms/$REALM_NAME" \
        -o /dev/null -w "%{http_code}" | grep -q "200"
}

# Function to create realm
create_realm() {
    local token=$1
    echo "📝 Creating realm: $REALM_NAME"
    
    curl -s -X POST "$KEYCLOAK_URL/admin/realms" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d '{
            "realm": "'$REALM_NAME'",
            "enabled": true,
            "displayName": "SeaweedFS Test Realm",
            "accessTokenLifespan": 3600,
            "sslRequired": "none"
        }'
}

# Function to create client
create_client() {
    local token=$1
    echo "📝 Creating client: $CLIENT_ID"
    
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d '{
            "clientId": "'$CLIENT_ID'",
            "enabled": true,
            "publicClient": false,
            "secret": "'$CLIENT_SECRET'",
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "standardFlowEnabled": true,
            "implicitFlowEnabled": false,
            "redirectUris": ["*"],
            "webOrigins": ["*"]
        }'
}

# Function to create role
create_role() {
    local token=$1
    local role_name=$2
    local role_description=$3
    
    echo "📝 Creating role: $role_name"
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "'$role_name'",
            "description": "'$role_description'"
        }'
}

# Function to create user
create_user() {
    local token=$1
    local username=$2
    local password=$3
    local email=$4
    local first_name=$5
    local last_name=$6
    local roles=$7
    
    echo "📝 Creating user: $username"
    
    # Create user
    curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d '{
            "username": "'$username'",
            "email": "'$email'",
            "firstName": "'$first_name'",
            "lastName": "'$last_name'",
            "enabled": true,
            "emailVerified": true,
            "credentials": [{
                "type": "password",
                "value": "'$password'",
                "temporary": false
            }]
        }'
    
    # Get user ID
    local user_id=$(curl -s -H "Authorization: Bearer $token" \
        "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users?username=$username" | \
        jq -r '.[0].id')
    
    # Assign roles
    if [ -n "$roles" ]; then
        echo "📝 Assigning roles to $username: $roles"
        IFS=',' read -ra ROLE_ARRAY <<< "$roles"
        for role in "${ROLE_ARRAY[@]}"; do
            # Get role representation
            local role_rep=$(curl -s -H "Authorization: Bearer $token" \
                "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles/$role")
            
            # Assign role to user
            curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users/$user_id/role-mappings/realm" \
                -H "Authorization: Bearer $token" \
                -H "Content-Type: application/json" \
                -d "[$role_rep]"
        done
    fi
}

# Main setup process
main() {
    echo "🚀 Starting Keycloak setup..."
    
    # Wait for Keycloak to be ready
    echo "⏳ Waiting for Keycloak to be ready..."
    timeout 120 bash -c "until curl -s $KEYCLOAK_URL/realms/master > /dev/null; do sleep 2; done" || {
        echo "❌ Keycloak is not ready after 120 seconds"
        exit 1
    }
    
    # Get admin token
    echo "🔑 Getting admin access token..."
    ADMIN_TOKEN=$(get_admin_token)
    if [ -z "$ADMIN_TOKEN" ] || [ "$ADMIN_TOKEN" = "null" ]; then
        echo "❌ Failed to get admin access token"
        exit 1
    fi
    
    # Create realm if it doesn't exist
    if ! realm_exists "$ADMIN_TOKEN"; then
        create_realm "$ADMIN_TOKEN"
        sleep 2
    else
        echo "✅ Realm $REALM_NAME already exists"
    fi
    
    # Create client
    create_client "$ADMIN_TOKEN"
    sleep 1
    
    # Create roles
    create_role "$ADMIN_TOKEN" "s3-admin" "SeaweedFS S3 Administrator"
    create_role "$ADMIN_TOKEN" "s3-read-only" "SeaweedFS S3 Read-Only User"
    create_role "$ADMIN_TOKEN" "s3-write-only" "SeaweedFS S3 Write-Only User"
    sleep 1
    
    # Create test users
    create_user "$ADMIN_TOKEN" "admin-user" "admin123" "admin@seaweedfs.test" "Admin" "User" "s3-admin"
    create_user "$ADMIN_TOKEN" "read-user" "read123" "read@seaweedfs.test" "Read" "User" "s3-read-only"
    create_user "$ADMIN_TOKEN" "write-user" "write123" "write@seaweedfs.test" "Write" "User" "s3-write-only"
    
    echo "✅ Keycloak setup completed successfully!"
    echo "🔗 Realm: $KEYCLOAK_URL/realms/$REALM_NAME"
    echo "👥 Test users created:"
    echo "   - admin-user (password: admin123) - s3-admin role"
    echo "   - read-user (password: read123) - s3-read-only role"
    echo "   - write-user (password: write123) - s3-write-only role"
    echo "🔑 Client: $CLIENT_ID (secret: $CLIENT_SECRET)"
}

# Run main function
main "$@"
