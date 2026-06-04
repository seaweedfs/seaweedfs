#!/usr/bin/env bash
# Generate mTLS material + security.toml with Terraform, then run a real
# master+volume+filer cluster with mTLS enabled and assert it works.
#   ./run_local_secure.sh        # render + run + assert + teardown
#   KEEP=1 ./run_local_secure.sh
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
export PATH="/opt/homebrew/bin:$PATH"
TOFU="${TOFU:-tofu}"
WEED="${WEED:-$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin/weed}"
WORKDIR="${WORKDIR:-/tmp/seaweedfs-tftest-secure}"
LOGDIR="$WORKDIR/logs"
RUNDIR="$WORKDIR/run"

PASS=0
FAIL=0
ok()   { echo "  PASS: $1"; PASS=$((PASS + 1)); }
bad()  { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
info() { echo "==> $1"; }

cleanup() {
  info "tearing down"
  if [ -d "$RUNDIR" ]; then
    for pf in "$RUNDIR"/*.pid; do [ -f "$pf" ] && kill "$(cat "$pf")" 2>/dev/null; done
    sleep 1
    for pf in "$RUNDIR"/*.pid; do [ -f "$pf" ] && kill -9 "$(cat "$pf")" 2>/dev/null; done
  fi
}

info "cleaning $WORKDIR"
case "$WORKDIR" in "" | "/" | "$HOME") echo "refusing to delete '$WORKDIR'" >&2; exit 2 ;; esac
rm -rf "$WORKDIR"
mkdir -p "$LOGDIR" "$RUNDIR" "$WORKDIR/.seaweedfs"
[ -x "$WEED" ] || { echo "weed not found at $WEED" >&2; exit 2; }

info "generating certs + rendering config with OpenTofu"
cd "$HERE"
"$TOFU" init -backend=false -input=false -no-color >/dev/null 2>&1 || { echo "tofu init failed"; exit 2; }
if ! "$TOFU" apply -auto-approve -input=false -no-color \
       -var "weed_binary=$WEED" -var "workdir=$WORKDIR" >"$LOGDIR/tofu.log" 2>&1; then
  echo "tofu apply failed"; tail -30 "$LOGDIR/tofu.log"; exit 2
fi

# write certs to their on-host paths
"$TOFU" output -json certs | jq -c '.[]' | while IFS= read -r c; do
  p="$(echo "$c" | jq -r '.path')"; m="$(echo "$c" | jq -r '.mode')"
  mkdir -p "$(dirname "$p")"
  echo "$c" | jq -r '.content' > "$p"
  chmod "$m" "$p"
done
# place security.toml where weed searches ($HOME/.seaweedfs)
"$TOFU" output -raw security_toml > "$WORKDIR/.seaweedfs/security.toml"
info "security.toml + $(ls "$WORKDIR"/certs | wc -l | tr -d ' ') cert dirs written"

OUT="$("$TOFU" output -json cluster)"

# derive ports from the rendered config (high range; never hardcoded)
port_of() { echo "$OUT" | jq -r --arg r "$1" '.[] | select(.role==$r) | .http_port' | head -1; }
MPORT="$(port_of master)"; VPORT="$(port_of volume)"; FPORT="$(port_of filer)"

busy=""
for p in $(echo "$OUT" | jq -r '.[].http_port'); do
  lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1 && busy="$busy $p"
done
[ -n "$busy" ] && { echo "Required port(s) already in use:$busy -- is another SeaweedFS running?" >&2; exit 3; }

[ "${KEEP:-0}" = "1" ] || trap cleanup EXIT INT TERM

# launch a node with HOME pointed at $WORKDIR so weed loads security.toml
launch() {
  n="$1"
  for d in $(echo "$OUT" | jq -r --arg n "$n" '.[$n].data_dirs[]?'); do mkdir -p "$d"; done
  ENVS=("HOME=$WORKDIR")
  while IFS= read -r e; do [ -n "$e" ] && ENVS+=("$e"); done \
    < <(echo "$OUT" | jq -r --arg n "$n" '.[$n].env | to_entries[] | "\(.key)=\(.value)"')
  ARGV=()
  while IFS= read -r a; do ARGV+=("$a"); done \
    < <(echo "$OUT" | jq -r --arg n "$n" '.[$n].argv[]')
  info "launching $n (mTLS)"
  env "${ENVS[@]}" "$WEED" "${ARGV[@]}" >"$LOGDIR/$n.log" 2>&1 &
  echo "$!" > "$RUNDIR/$n.pid"
}

wait_http() {
  url="$1"; t="${2:-30}"; i=0
  while [ "$i" -lt "$t" ]; do
    curl -fsS -o /dev/null --max-time 2 "$url" 2>/dev/null && return 0
    i=$((i + 1)); sleep 1
  done
  return 1
}

launch master-m0
QOK=0; i=0
while [ "$i" -lt 40 ]; do
  st="$(curl -fsS --max-time 2 "http://127.0.0.1:$MPORT/cluster/status" 2>/dev/null)" || { i=$((i+1)); sleep 1; continue; }
  [ "$(echo "$st" | jq -r '.IsLeader // false')" = "true" ] && { QOK=1; break; }
  i=$((i + 1)); sleep 1
done
[ "$QOK" -eq 1 ] && ok "master elected leader under mTLS" || bad "master leader (mTLS)"

launch volume-v0
# registration (master gRPC over mTLS) confirms the volume joined the cluster
AOK=0; i=0
while [ "$i" -lt 40 ]; do
  fid="$(curl -fsS --max-time 2 "http://127.0.0.1:$MPORT/dir/assign" 2>/dev/null | jq -r '.fid // ""')"
  [ -n "$fid" ] && { AOK=1; break; }
  i=$((i + 1)); sleep 1
done
[ "$AOK" -eq 1 ] && ok "volume registered via mTLS gRPC (fid $fid)" || bad "volume registration over mTLS"
# /healthz flips to 200 after the first heartbeat completes (slower under mTLS)
wait_http "http://127.0.0.1:$VPORT/healthz" 60 && ok "volume /healthz up (mTLS)" || bad "volume /healthz"

launch filer-f0
wait_http "http://127.0.0.1:$FPORT/" 30 && ok "filer / up (mTLS)" || bad "filer /"
# [jwt.filer_signing] is active, so the filer requires a signed JWT for writes.
# An unsigned write MUST be rejected with 401 -- this proves the JWT signing key
# rendered into security.toml is enforced (positive security assertion).
echo "smoke" > "$WORKDIR/hello.txt"
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -X POST \
  -F "file=@$WORKDIR/hello.txt" "http://127.0.0.1:$FPORT/smoke/hello.txt" 2>/dev/null)"
[ "$code" = "401" ] && ok "filer rejects unsigned write (HTTP 401) => JWT signing enforced" \
  || bad "filer JWT enforcement (expected 401, got HTTP $code)"

echo
info "RESULTS: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
