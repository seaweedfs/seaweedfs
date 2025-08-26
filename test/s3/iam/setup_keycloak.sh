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
