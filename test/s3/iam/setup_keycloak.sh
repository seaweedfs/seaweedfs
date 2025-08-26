#!/bin/bash

# Keycloak Setup Script for CI/CD
# This script sets up a Keycloak realm with test users and roles for SeaweedFS S3 IAM testing

set -e

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
# Support both old and new Keycloak environment variable formats
ADMIN_USER="${KC_BOOTSTRAP_ADMIN_USERNAME:-${KEYCLOAK_ADMIN:-admin}}"
ADMIN_PASSWORD="${KC_BOOTSTRAP_ADMIN_PASSWORD:-${KEYCLOAK_ADMIN_PASSWORD:-admin}}"
REALM_NAME="seaweedfs-test"
CLIENT_ID="seaweedfs-s3"
CLIENT_SECRET="seaweedfs-s3-secret"

echo "🔧 Setting up Keycloak realm and users for SeaweedFS S3 IAM testing..."
echo "Keycloak URL: $KEYCLOAK_URL"

# Function to get admin access token with retry logic
get_admin_token() {
    local max_attempts=5
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        echo "🔑 Getting admin access token (attempt $attempt/$max_attempts)..."
        
        local response=$(curl -s -X POST "$KEYCLOAK_URL/realms/master/protocol/openid-connect/token" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            -d "username=$ADMIN_USER" \
            -d "password=$ADMIN_PASSWORD" \
            -d "grant_type=password" \
            -d "client_id=admin-cli" 2>/dev/null || echo '{"error":"curl_failed"}')
        
        local token=$(echo "$response" | jq -r '.access_token // empty' 2>/dev/null || echo "")
        
        if [ -n "$token" ] && [ "$token" != "null" ] && [ "$token" != "" ]; then
            echo "✅ Successfully obtained admin token"
            echo "$token"
            return 0
        fi
        
        echo "⚠️  Failed to get token (attempt $attempt). Response: $response"
        
        if [ $attempt -eq $max_attempts ]; then
            echo "❌ Failed to get admin access token after $max_attempts attempts"
            echo "🔍 Checking Keycloak status..."
            curl -s "$KEYCLOAK_URL/realms/master" || echo "Keycloak master realm not accessible"
            return 1
        fi
        
        echo "⏳ Waiting 5 seconds before retry..."
        sleep 5
        attempt=$((attempt + 1))
    done
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
    
    local payload=$(jq -n \
        --arg realm "$REALM_NAME" \
        '{
            "realm": $realm,
            "enabled": true,
            "displayName": "SeaweedFS Test Realm",
            "accessTokenLifespan": 3600,
            "sslRequired": "none"
        }')
    
    local response=$(curl -s -X POST "$KEYCLOAK_URL/admin/realms" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload")
    
    if [[ -n "$response" && "$response" != *"error"* ]]; then
        echo "✅ Realm created successfully"
    else
        echo "⚠️  Realm creation response: $response"
    fi
}

# Function to create client
create_client() {
    local token=$1
    echo "📝 Creating client: $CLIENT_ID"
    
    local payload=$(jq -n \
        --arg clientId "$CLIENT_ID" \
        --arg secret "$CLIENT_SECRET" \
        '{
            "clientId": $clientId,
            "enabled": true,
            "publicClient": false,
            "secret": $secret,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "standardFlowEnabled": true,
            "implicitFlowEnabled": false,
            "redirectUris": ["*"],
            "webOrigins": ["*"]
        }')
    
    local response=$(curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload")
    
    if [[ -n "$response" && "$response" != *"error"* ]]; then
        echo "✅ Client created successfully"
    else
        echo "⚠️  Client creation response: $response"
    fi
}

# Function to create role
create_role() {
    local token=$1
    local role_name=$2
    local role_description=$3
    
    echo "📝 Creating role: $role_name"
    
    local payload=$(jq -n \
        --arg name "$role_name" \
        --arg description "$role_description" \
        '{
            "name": $name,
            "description": $description
        }')
    
    local response=$(curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload")
    
    if [[ -n "$response" && "$response" != *"error"* ]]; then
        echo "✅ Role '$role_name' created successfully"
    else
        echo "⚠️  Role creation response: $response"
    fi
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
    local user_payload=$(jq -n \
        --arg username "$username" \
        --arg email "$email" \
        --arg firstName "$first_name" \
        --arg lastName "$last_name" \
        --arg password "$password" \
        '{
            "username": $username,
            "email": $email,
            "firstName": $firstName,
            "lastName": $lastName,
            "enabled": true,
            "emailVerified": true,
            "credentials": [{
                "type": "password",
                "value": $password,
                "temporary": false
            }]
        }')
    
    local user_response=$(curl -s -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$user_payload")
    
    if [[ -n "$user_response" && "$user_response" != *"error"* ]]; then
        echo "✅ User '$username' created successfully"
    else
        echo "⚠️  User creation response: $user_response"
    fi
    
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
    
    # Wait for Keycloak to be ready with better health checking
    echo "⏳ Waiting for Keycloak to be ready..."
    timeout 300 bash -c '
        while true; do
            # Try health endpoint first (if available)
            if curl -s http://localhost:8080/health/ready > /dev/null 2>&1; then
                echo "✅ Keycloak health check passed"
                break
            fi
            
            # Fallback to master realm check
            if curl -s $KEYCLOAK_URL/realms/master > /dev/null 2>&1; then
                echo "✅ Keycloak master realm accessible"
                break
            fi
            
            echo "Still waiting for Keycloak..."
            sleep 5
        done
    ' || {
        echo "❌ Keycloak is not ready after 300 seconds"
        exit 1
    }
    
    # Additional wait for admin user to be fully set up
    echo "⏳ Waiting for admin user to be fully initialized..."
    sleep 10
    
    # Get admin token
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
    create_role "$ADMIN_TOKEN" "s3-read-write" "SeaweedFS S3 Read-Write User"
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
