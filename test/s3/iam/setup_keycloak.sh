#!/usr/bin/env bash

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

KEYCLOAK_IMAGE="quay.io/keycloak/keycloak:26.0.7"
CONTAINER_NAME="keycloak-iam-test"
KEYCLOAK_PORT="8080"
KEYCLOAK_URL="http://localhost:${KEYCLOAK_PORT}"

# Realm and test fixtures expected by tests
REALM_NAME="seaweedfs-test"
CLIENT_ID="seaweedfs-s3"
CLIENT_SECRET="seaweedfs-s3-secret"
ROLE_ADMIN="s3-admin"
ROLE_READONLY="s3-read-only"
ROLE_READWRITE="s3-read-write"

declare -A USERS
USERS=(
  [admin-user]=admin123
  [read-user]=read123
  [write-user]=write123
)

echo -e "${BLUE}🔧 Setting up Keycloak realm and users for SeaweedFS S3 IAM testing...${NC}"
echo "Keycloak URL: ${KEYCLOAK_URL}"

ensure_container() {
  # Prefer any already running Keycloak container to avoid port conflicts
  if docker ps --format '{{.Names}}' | grep -q '^keycloak$'; then
    CONTAINER_NAME="keycloak"
    echo -e "${GREEN}✅ Using existing container '${CONTAINER_NAME}'${NC}"
    return 0
  fi
  if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${GREEN}✅ Using existing container '${CONTAINER_NAME}'${NC}"
    return 0
  fi
  echo -e "${YELLOW}🐳 Starting Keycloak container (${KEYCLOAK_IMAGE})...${NC}"
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker run -d --name "${CONTAINER_NAME}" -p "${KEYCLOAK_PORT}:8080" \
    -e KEYCLOAK_ADMIN=admin \
    -e KEYCLOAK_ADMIN_PASSWORD=admin \
    -e KC_HTTP_ENABLED=true \
    -e KC_HOSTNAME_STRICT=false \
    -e KC_HOSTNAME_STRICT_HTTPS=false \
    -e KC_HEALTH_ENABLED=true \
    "${KEYCLOAK_IMAGE}" start-dev >/dev/null
}

wait_ready() {
  echo -e "${YELLOW}⏳ Waiting for Keycloak to be ready...${NC}"
  for i in $(seq 1 120); do
    if curl -sf "${KEYCLOAK_URL}/health/ready" >/dev/null; then
      echo -e "${GREEN}✅ Keycloak health check passed${NC}"
      return 0
    fi
    if curl -sf "${KEYCLOAK_URL}/realms/master" >/dev/null; then
      echo -e "${GREEN}✅ Keycloak master realm accessible${NC}"
      return 0
    fi
    sleep 2
  done
  echo -e "${RED}❌ Keycloak did not become ready in time${NC}"
  exit 1
}

kcadm() {
  docker exec -i "${CONTAINER_NAME}" /opt/keycloak/bin/kcadm.sh "$@"
}

admin_login() {
  kcadm config credentials --server "${KEYCLOAK_URL}" --realm master --user admin --password admin >/dev/null
}

ensure_realm() {
  if kcadm get realms | grep -q '"realm": *"'${REALM_NAME}'"'; then
    echo -e "${GREEN}✅ Realm '${REALM_NAME}' already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating realm '${REALM_NAME}'...${NC}"
    kcadm create realms -s realm="${REALM_NAME}" -s enabled=true >/dev/null
    echo -e "${GREEN}✅ Realm created${NC}"
  fi
}

ensure_client() {
  local id
  id=$(kcadm get clients -r "${REALM_NAME}" -q clientId="${CLIENT_ID}" | jq -r '.[0].id // empty')
  if [[ -n "${id}" ]]; then
    echo -e "${GREEN}✅ Client '${CLIENT_ID}' already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating client '${CLIENT_ID}'...${NC}"
    kcadm create clients -r "${REALM_NAME}" \
      -s clientId="${CLIENT_ID}" \
      -s protocol=openid-connect \
      -s publicClient=false \
      -s serviceAccountsEnabled=false \
      -s directAccessGrantsEnabled=true \
      -s standardFlowEnabled=false \
      -s implicitFlowEnabled=false \
      -s secret="${CLIENT_SECRET}" >/dev/null
    echo -e "${GREEN}✅ Client created${NC}"
  fi
}

ensure_role() {
  local role="$1"
  if kcadm get roles -r "${REALM_NAME}" | jq -r '.[].name' | grep -qx "${role}"; then
    echo -e "${GREEN}✅ Role '${role}' exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating role '${role}'...${NC}"
    kcadm create roles -r "${REALM_NAME}" -s name="${role}" >/dev/null
  fi
}

ensure_user() {
  local username="$1" password="$2"
  local uid
  uid=$(kcadm get users -r "${REALM_NAME}" -q username="${username}" | jq -r '.[0].id // empty')
  if [[ -z "${uid}" ]]; then
    echo -e "${YELLOW}📝 Creating user '${username}'...${NC}"
    uid=$(kcadm create users -r "${REALM_NAME}" -s username="${username}" -s enabled=true -i)
  else
    echo -e "${GREEN}✅ User '${username}' exists${NC}"
  fi
  echo -e "${YELLOW}🔑 Setting password for '${username}'...${NC}"
  kcadm set-password -r "${REALM_NAME}" --userid "${uid}" --new-password "${password}" --temporary=false >/dev/null
}

assign_role() {
  local username="$1" role="$2"
  local uid rid
  uid=$(kcadm get users -r "${REALM_NAME}" -q username="${username}" | jq -r '.[0].id')
  rid=$(kcadm get roles -r "${REALM_NAME}" | jq -r ".[] | select(.name==\"${role}\") | .id")
  # Check if role already assigned
  if kcadm get "users/${uid}/role-mappings/realm" -r "${REALM_NAME}" | jq -r '.[].name' | grep -qx "${role}"; then
    echo -e "${GREEN}✅ User '${username}' already has role '${role}'${NC}"
    return 0
  fi
  echo -e "${YELLOW}➕ Assigning role '${role}' to '${username}'...${NC}"
  kcadm add-roles -r "${REALM_NAME}" --uid "${uid}" --rolename "${role}" >/dev/null
}

main() {
  command -v docker >/dev/null || { echo -e "${RED}❌ Docker is required${NC}"; exit 1; }
  command -v jq >/dev/null || { echo -e "${RED}❌ jq is required${NC}"; exit 1; }

  ensure_container
  wait_ready
  admin_login
  ensure_realm
  ensure_client
  ensure_role "${ROLE_ADMIN}"
  ensure_role "${ROLE_READONLY}"
  ensure_role "${ROLE_READWRITE}"

  for u in "${!USERS[@]}"; do
    ensure_user "$u" "${USERS[$u]}"
  done

  assign_role admin-user  "${ROLE_ADMIN}"
  assign_role read-user   "${ROLE_READONLY}"
  assign_role write-user  "${ROLE_READWRITE}"

  echo -e "${GREEN}✅ Keycloak test realm '${REALM_NAME}' configured${NC}"
}

main "$@"

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
    
    local http_code=$(curl -s -w "%{http_code}" -X POST "$KEYCLOAK_URL/admin/realms" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -o /tmp/realm_response.json)
    
    local response=$(cat /tmp/realm_response.json 2>/dev/null || echo "")
    
    if [[ "$http_code" == "201" ]]; then
        echo "✅ Realm created successfully"
        return 0
    else
        echo "❌ Realm creation failed with HTTP $http_code"
        echo "📋 Response: $response"
        return 1
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
    
    local http_code=$(curl -s -w "%{http_code}" -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/clients" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -o /tmp/client_response.json)
    
    local response=$(cat /tmp/client_response.json 2>/dev/null || echo "")
    
    if [[ "$http_code" == "201" ]]; then
        echo "✅ Client created successfully"
        return 0
    else
        echo "❌ Client creation failed with HTTP $http_code"
        echo "📋 Response: $response"
        return 1
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
    
    local http_code=$(curl -s -w "%{http_code}" -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/roles" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -o /tmp/role_response_$role_name.json)
    
    local response=$(cat /tmp/role_response_$role_name.json 2>/dev/null || echo "")
    
    if [[ "$http_code" == "201" ]]; then
        echo "✅ Role '$role_name' created successfully"
        return 0
    else
        echo "❌ Role '$role_name' creation failed with HTTP $http_code"
        echo "📋 Response: $response"
        return 1
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
    
    local http_code=$(curl -s -w "%{http_code}" -X POST "$KEYCLOAK_URL/admin/realms/$REALM_NAME/users" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$user_payload" \
        -o /tmp/user_response_$username.json)
    
    local user_response=$(cat /tmp/user_response_$username.json 2>/dev/null || echo "")
    
    if [[ "$http_code" == "201" ]]; then
        echo "✅ User '$username' created successfully"
    else
        echo "❌ User '$username' creation failed with HTTP $http_code"
        echo "📋 Response: $user_response"
        return 1
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
        if ! create_realm "$ADMIN_TOKEN"; then
            echo "❌ Failed to create realm $REALM_NAME"
            exit 1
        fi
        sleep 2
        
        # Wait for realm to be fully available
        echo "⏳ Waiting for realm to be fully initialized..."
        timeout 60 bash -c "
            while ! curl -fs $KEYCLOAK_URL/realms/$REALM_NAME/.well-known/openid-configuration >/dev/null 2>&1; do
                echo '   ... waiting for realm endpoint'
                sleep 3
            done
        " || {
            echo "❌ Realm $REALM_NAME not accessible after creation"
            exit 1
        }
        echo "✅ Realm is now accessible"
    else
        echo "✅ Realm $REALM_NAME already exists"
    fi
    
    # Create client
    if ! create_client "$ADMIN_TOKEN"; then
        echo "❌ Failed to create client $CLIENT_ID"
        exit 1
    fi
    sleep 1
    
    # Create roles
    if ! create_role "$ADMIN_TOKEN" "s3-admin" "SeaweedFS S3 Administrator"; then
        echo "❌ Failed to create s3-admin role"
        exit 1
    fi
    if ! create_role "$ADMIN_TOKEN" "s3-read-only" "SeaweedFS S3 Read-Only User"; then
        echo "❌ Failed to create s3-read-only role"
        exit 1
    fi
    if ! create_role "$ADMIN_TOKEN" "s3-write-only" "SeaweedFS S3 Write-Only User"; then
        echo "❌ Failed to create s3-write-only role"
        exit 1
    fi
    if ! create_role "$ADMIN_TOKEN" "s3-read-write" "SeaweedFS S3 Read-Write User"; then
        echo "❌ Failed to create s3-read-write role"
        exit 1
    fi
    sleep 1
    
    # Create test users
    if ! create_user "$ADMIN_TOKEN" "admin-user" "admin123" "admin@seaweedfs.test" "Admin" "User" "s3-admin"; then
        echo "❌ Failed to create admin-user"
        exit 1
    fi
    if ! create_user "$ADMIN_TOKEN" "read-user" "read123" "read@seaweedfs.test" "Read" "User" "s3-read-only"; then
        echo "❌ Failed to create read-user" 
        exit 1
    fi
    if ! create_user "$ADMIN_TOKEN" "write-user" "write123" "write@seaweedfs.test" "Write" "User" "s3-write-only"; then
        echo "❌ Failed to create write-user"
        exit 1
    fi
    
    echo "✅ Keycloak setup completed successfully!"
    echo "🔗 Realm: $KEYCLOAK_URL/realms/$REALM_NAME"
    echo "👥 Test users created:"
    echo "   - admin-user (password: admin123) - s3-admin role"
    echo "   - read-user (password: read123) - s3-read-only role"
    echo "   - write-user (password: write123) - s3-write-only role"
    echo "🔑 Client: $CLIENT_ID (secret: $CLIENT_SECRET)"
    
    # Cleanup temporary files
    rm -f /tmp/realm_response.json /tmp/client_response.json /tmp/role_response_*.json /tmp/user_response_*.json
}

# Run main function
main "$@"
