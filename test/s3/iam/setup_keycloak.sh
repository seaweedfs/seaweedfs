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
KEYCLOAK_PORT="8080"  # Default external port
KEYCLOAK_INTERNAL_PORT="8080"  # Internal container port (always 8080)
KEYCLOAK_URL="http://localhost:${KEYCLOAK_PORT}"

# Realm and test fixtures expected by tests
REALM_NAME="seaweedfs-test"
CLIENT_ID="seaweedfs-s3"
CLIENT_SECRET="seaweedfs-s3-secret"
ROLE_ADMIN="s3-admin"
ROLE_READONLY="s3-read-only"
ROLE_WRITEONLY="s3-write-only"
ROLE_READWRITE="s3-read-write"

# User credentials (matches Docker setup script logic: removes non-alphabetic chars + "123")
get_user_password() {
  case "$1" in
    "admin-user") echo "adminuser123" ;;        # "admin-user" -> "adminuser123"
    "read-user") echo "readuser123" ;;          # "read-user" -> "readuser123"
    "write-user") echo "writeuser123" ;;        # "write-user" -> "writeuser123"
    "write-only-user") echo "writeonlyuser123" ;;  # "write-only-user" -> "writeonlyuser123"
    *) echo "" ;;
  esac
}

# List of users to create
USERS="admin-user read-user write-user write-only-user"

echo -e "${BLUE}🔧 Setting up Keycloak realm and users for SeaweedFS S3 IAM testing...${NC}"

ensure_container() {
  # Check for any existing Keycloak container and detect its port
  local keycloak_containers=$(docker ps --format '{{.Names}}\t{{.Ports}}' | grep -E "(keycloak|quay.io/keycloak)")
  
  if [[ -n "$keycloak_containers" ]]; then
    # Parse the first available Keycloak container
    CONTAINER_NAME=$(echo "$keycloak_containers" | head -1 | awk '{print $1}')
    
    # Extract the external port from the port mapping using sed (compatible with older bash)
    local port_mapping=$(echo "$keycloak_containers" | head -1 | awk '{print $2}')
    local extracted_port=$(echo "$port_mapping" | sed -n 's/.*:\([0-9]*\)->8080.*/\1/p')
    if [[ -n "$extracted_port" ]]; then
      KEYCLOAK_PORT="$extracted_port"
      KEYCLOAK_URL="http://localhost:${KEYCLOAK_PORT}"
      echo -e "${GREEN}[OK] Using existing container '${CONTAINER_NAME}' on port ${KEYCLOAK_PORT}${NC}"
      return 0
    fi
  fi
  
  # Fallback: check for specific container names  
  if docker ps --format '{{.Names}}' | grep -q '^keycloak$'; then
    CONTAINER_NAME="keycloak"
    # Try to detect port for 'keycloak' container using docker port command
    local ports=$(docker port keycloak 8080 2>/dev/null | head -1)
    if [[ -n "$ports" ]]; then
      local extracted_port=$(echo "$ports" | sed -n 's/.*:\([0-9]*\)$/\1/p')
      if [[ -n "$extracted_port" ]]; then
        KEYCLOAK_PORT="$extracted_port"
        KEYCLOAK_URL="http://localhost:${KEYCLOAK_PORT}"
      fi
    fi
    echo -e "${GREEN}[OK] Using existing container '${CONTAINER_NAME}' on port ${KEYCLOAK_PORT}${NC}"
    return 0
  fi
  if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${GREEN}[OK] Using existing container '${CONTAINER_NAME}'${NC}"
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
      echo -e "${GREEN}[OK] Keycloak health check passed${NC}"
      return 0
    fi
    if curl -sf "${KEYCLOAK_URL}/realms/master" >/dev/null; then
      echo -e "${GREEN}[OK] Keycloak master realm accessible${NC}"
      return 0
    fi
    sleep 2
  done
  echo -e "${RED}[FAIL] Keycloak did not become ready in time${NC}"
  exit 1
}

kcadm() {
  # Always authenticate before each command to ensure context
  # Try different admin passwords that might be used in different environments
  # GitHub Actions uses "admin", local testing might use "admin123"
  local admin_passwords=("admin" "admin123" "password")
  local auth_success=false
  
  for pwd in "${admin_passwords[@]}"; do
    if docker exec -i "${CONTAINER_NAME}" /opt/keycloak/bin/kcadm.sh config credentials --server "http://localhost:${KEYCLOAK_INTERNAL_PORT}" --realm master --user admin --password "$pwd" >/dev/null 2>&1; then
      auth_success=true
      break
    fi
  done
  
  if [[ "$auth_success" == false ]]; then
    echo -e "${RED}[FAIL] Failed to authenticate with any known admin password${NC}"
    return 1
  fi
  
  docker exec -i "${CONTAINER_NAME}" /opt/keycloak/bin/kcadm.sh "$@"
}

admin_login() {
  # This is now handled by each kcadm() call  
  echo "Logging into http://localhost:${KEYCLOAK_INTERNAL_PORT} as user admin of realm master"
}

ensure_realm() {
  if kcadm get realms | grep -q "${REALM_NAME}"; then
    echo -e "${GREEN}[OK] Realm '${REALM_NAME}' already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating realm '${REALM_NAME}'...${NC}"
    if kcadm create realms -s realm="${REALM_NAME}" -s enabled=true 2>/dev/null; then
    echo -e "${GREEN}[OK] Realm created${NC}"
    else
      # Check if it exists now (might have been created by another process)
      if kcadm get realms | grep -q "${REALM_NAME}"; then
        echo -e "${GREEN}[OK] Realm '${REALM_NAME}' already exists (created concurrently)${NC}"
      else
        echo -e "${RED}[FAIL] Failed to create realm '${REALM_NAME}'${NC}"
        return 1
      fi
    fi
  fi
}

ensure_client() {
  local id
  id=$(kcadm get clients -r "${REALM_NAME}" -q clientId="${CLIENT_ID}" | jq -r '.[0].id // empty')
  if [[ -n "${id}" ]]; then
    echo -e "${GREEN}[OK] Client '${CLIENT_ID}' already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating client '${CLIENT_ID}'...${NC}"
    kcadm create clients -r "${REALM_NAME}" \
      -s clientId="${CLIENT_ID}" \
      -s protocol=openid-connect \
      -s publicClient=false \
      -s serviceAccountsEnabled=true \
      -s directAccessGrantsEnabled=true \
      -s standardFlowEnabled=true \
      -s implicitFlowEnabled=false \
      -s secret="${CLIENT_SECRET}" >/dev/null
    echo -e "${GREEN}[OK] Client created${NC}"
  fi
  
  # Create and configure role mapper for the client
  configure_role_mapper "${CLIENT_ID}"
}

ensure_role() {
  local role="$1"
  if kcadm get roles -r "${REALM_NAME}" | jq -r '.[].name' | grep -qx "${role}"; then
    echo -e "${GREEN}[OK] Role '${role}' exists${NC}"
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
    uid=$(kcadm create users -r "${REALM_NAME}" \
      -s username="${username}" \
      -s enabled=true \
      -s email="${username}@seaweedfs.test" \
      -s emailVerified=true \
      -s firstName="${username}" \
      -s lastName="User" \
      -i)
  else
    echo -e "${GREEN}[OK] User '${username}' exists${NC}"
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
    echo -e "${GREEN}[OK] User '${username}' already has role '${role}'${NC}"
    return 0
  fi
  echo -e "${YELLOW}➕ Assigning role '${role}' to '${username}'...${NC}"
  kcadm add-roles -r "${REALM_NAME}" --uid "${uid}" --rolename "${role}" >/dev/null
}

configure_role_mapper() {
  echo -e "${YELLOW}🔧 Configuring role mapper for client '${CLIENT_ID}'...${NC}"
  
  # Get client's internal ID
  local internal_id
  internal_id=$(kcadm get clients -r "${REALM_NAME}" -q clientId="${CLIENT_ID}" | jq -r '.[0].id // empty')
  
  if [[ -z "${internal_id}" ]]; then
    echo -e "${RED}[FAIL] Could not find client ${client_id} to configure role mapper${NC}"
    return 1
  fi
  
  # Check if a realm roles mapper already exists for this client
  local existing_mapper
  existing_mapper=$(kcadm get "clients/${internal_id}/protocol-mappers/models" -r "${REALM_NAME}" | jq -r '.[] | select(.name=="realm roles" and .protocolMapper=="oidc-usermodel-realm-role-mapper") | .id // empty')
  
  if [[ -n "${existing_mapper}" ]]; then
    echo -e "${GREEN}[OK] Realm roles mapper already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating realm roles mapper...${NC}"
    
    # Create protocol mapper for realm roles
    kcadm create "clients/${internal_id}/protocol-mappers/models" -r "${REALM_NAME}" \
      -s name="realm roles" \
      -s protocol="openid-connect" \
      -s protocolMapper="oidc-usermodel-realm-role-mapper" \
      -s consentRequired=false \
      -s 'config."multivalued"=true' \
      -s 'config."userinfo.token.claim"=true' \
      -s 'config."id.token.claim"=true' \
      -s 'config."access.token.claim"=true' \
      -s 'config."claim.name"=roles' \
      -s 'config."jsonType.label"=String' >/dev/null || {
        echo -e "${RED}[FAIL] Failed to create realm roles mapper${NC}"
        return 1
      }
    
    echo -e "${GREEN}[OK] Realm roles mapper created${NC}"
  fi
}

configure_audience_mapper() {
  echo -e "${YELLOW}🔧 Configuring audience mapper for client '${CLIENT_ID}'...${NC}"
  
  # Get client's internal ID
  local internal_id
  internal_id=$(kcadm get clients -r "${REALM_NAME}" -q clientId="${CLIENT_ID}" | jq -r '.[0].id // empty')
  
  if [[ -z "${internal_id}" ]]; then
    echo -e "${RED}[FAIL] Could not find client ${CLIENT_ID} to configure audience mapper${NC}"
    return 1
  fi
  
  # Check if an audience mapper already exists for this client
  local existing_mapper
  existing_mapper=$(kcadm get "clients/${internal_id}/protocol-mappers/models" -r "${REALM_NAME}" | jq -r '.[] | select(.name=="audience-mapper" and .protocolMapper=="oidc-audience-mapper") | .id // empty')
  
  if [[ -n "${existing_mapper}" ]]; then
    echo -e "${GREEN}[OK] Audience mapper already exists${NC}"
  else
    echo -e "${YELLOW}📝 Creating audience mapper...${NC}"
    
    # Create protocol mapper for audience
    kcadm create "clients/${internal_id}/protocol-mappers/models" -r "${REALM_NAME}" \
      -s name="audience-mapper" \
      -s protocol="openid-connect" \
      -s protocolMapper="oidc-audience-mapper" \
      -s consentRequired=false \
      -s 'config."included.client.audience"='"${CLIENT_ID}" \
      -s 'config."id.token.claim"=false' \
      -s 'config."access.token.claim"=true' >/dev/null || {
        echo -e "${RED}[FAIL] Failed to create audience mapper${NC}"
        return 1
      }
    
    echo -e "${GREEN}[OK] Audience mapper created${NC}"
  fi
}

main() {
  command -v docker >/dev/null || { echo -e "${RED}[FAIL] Docker is required${NC}"; exit 1; }
  command -v jq >/dev/null || { echo -e "${RED}[FAIL] jq is required${NC}"; exit 1; }

  ensure_container
  echo "Keycloak URL: ${KEYCLOAK_URL}"
  wait_ready
  admin_login
  ensure_realm
  ensure_client
  configure_role_mapper
  configure_audience_mapper
  ensure_role "${ROLE_ADMIN}"
  ensure_role "${ROLE_READONLY}"
  ensure_role "${ROLE_WRITEONLY}"
  ensure_role "${ROLE_READWRITE}"

  for u in $USERS; do
    ensure_user "$u" "$(get_user_password "$u")"
  done

  assign_role admin-user  "${ROLE_ADMIN}"
  assign_role read-user   "${ROLE_READONLY}"
  assign_role write-user  "${ROLE_READWRITE}"

  # Also create a dedicated write-only user for testing  
  ensure_user write-only-user "$(get_user_password write-only-user)"
  assign_role write-only-user "${ROLE_WRITEONLY}"
  
  # Copy the appropriate IAM configuration for this environment
  setup_iam_config

  # Validate the setup by testing authentication and role inclusion
  echo -e "${YELLOW}🔍 Validating setup by testing admin-user authentication and role mapping...${NC}"
  sleep 2
  
  local validation_result=$(curl -s -w "%{http_code}" -X POST "http://localhost:${KEYCLOAK_PORT}/realms/${REALM_NAME}/protocol/openid-connect/token" \
            -H "Content-Type: application/x-www-form-urlencoded" \
            -d "grant_type=password" \
    -d "client_id=${CLIENT_ID}" \
    -d "client_secret=${CLIENT_SECRET}" \
    -d "username=admin-user" \
    -d "password=adminuser123" \
    -d "scope=openid profile email" \
    -o /tmp/auth_test_response.json)
  
  if [[ "${validation_result: -3}" == "200" ]]; then
    echo -e "${GREEN}[OK] Authentication validation successful${NC}"
    
    # Extract and decode JWT token to check for roles
    local access_token=$(cat /tmp/auth_test_response.json | jq -r '.access_token // empty')
    if [[ -n "${access_token}" ]]; then
      # Decode JWT payload (second part) and check for roles
      local payload=$(echo "${access_token}" | cut -d'.' -f2)
      # Add padding if needed for base64 decode
      while [[ $((${#payload} % 4)) -ne 0 ]]; do
        payload="${payload}="
      done
      
      local decoded=$(echo "${payload}" | base64 -d 2>/dev/null || echo "{}")
      local roles=$(echo "${decoded}" | jq -r '.roles // empty' 2>/dev/null || echo "")
      
      if [[ -n "${roles}" && "${roles}" != "null" ]]; then
        echo -e "${GREEN}[OK] JWT token includes roles: ${roles}${NC}"
      else
        echo -e "${YELLOW}⚠️  JWT token does not include 'roles' claim${NC}"
        echo -e "${YELLOW}Decoded payload sample:${NC}"
        echo "${decoded}" | jq '.' 2>/dev/null || echo "${decoded}"
      fi
    fi
  else
    echo -e "${RED}[FAIL] Authentication validation failed with HTTP ${validation_result: -3}${NC}"
    echo -e "${YELLOW}Response body:${NC}"
    cat /tmp/auth_test_response.json 2>/dev/null || echo "No response body"
    echo -e "${YELLOW}This may indicate a setup issue that needs to be resolved${NC}"
  fi
  rm -f /tmp/auth_test_response.json
  
  echo -e "${GREEN}[OK] Keycloak test realm '${REALM_NAME}' configured${NC}"
}

setup_iam_config() {
  echo -e "${BLUE}🔧 Setting up IAM configuration for detected environment${NC}"
  
  # Change to script directory to ensure config files are found
  local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  cd "$script_dir"
  
  # Choose the appropriate config based on detected port
  local config_source
  if [[ "${KEYCLOAK_PORT}" == "8080" ]]; then
    config_source="iam_config.github.json"
    echo "   Using GitHub Actions configuration (port 8080)"
  else
    config_source="iam_config.local.json" 
    echo "   Using local development configuration (port ${KEYCLOAK_PORT})"
  fi
  
  # Verify source config exists
  if [[ ! -f "$config_source" ]]; then
    echo -e "${RED}[FAIL] Config file $config_source not found in $script_dir${NC}"
    exit 1
  fi
  
  # Copy the appropriate config
  cp "$config_source" "iam_config.json"
  
  local detected_issuer=$(cat iam_config.json | jq -r '.providers[] | select(.name=="keycloak") | .config.issuer')
  echo -e "${GREEN}[OK] IAM configuration set successfully${NC}"
  echo "   - Using config: $config_source"
  echo "   - Keycloak issuer: $detected_issuer"
}

main "$@"
