#!/usr/bin/env bash
# Render a SeaweedFS cluster with the Terraform core module and run it as real
# `weed` processes locally, then assert it actually works. No cloud, no docker.
#
#   ./run_local_cluster.sh            # render + run + assert + teardown
#   KEEP=1 ./run_local_cluster.sh     # leave the cluster running after asserts
#   WEED=/path/to/weed ./run_local_cluster.sh
#
# Ports come from the rendered config (high range, to avoid colliding with a
# SeaweedFS cluster already running on this machine). Exits non-zero on any
# failed assertion or if a required port is already in use.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
export PATH="/opt/homebrew/bin:$PATH"
TOFU="${TOFU:-tofu}"
WEED="${WEED:-$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin/weed}"
WORKDIR="${WORKDIR:-/tmp/seaweedfs-tftest}"
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
rm -rf "$WORKDIR"
mkdir -p "$LOGDIR" "$RUNDIR"
[ -x "$WEED" ] || { echo "weed binary not found/executable at $WEED" >&2; exit 2; }

info "rendering cluster config with OpenTofu"
cd "$HERE"
"$TOFU" init -backend=false -input=false -no-color >/dev/null 2>&1 || { echo "tofu init failed"; exit 2; }
if ! "$TOFU" apply -auto-approve -input=false -no-color \
       -var "weed_binary=$WEED" -var "workdir=$WORKDIR" >"$LOGDIR/tofu.log" 2>&1; then
  echo "tofu apply failed; see $LOGDIR/tofu.log"; tail -30 "$LOGDIR/tofu.log"; exit 2
fi
OUT="$("$TOFU" output -json cluster)"
[ -n "$OUT" ] || { echo "empty cluster output"; exit 2; }

# ---- derive ports by role (never hardcoded) ---------------------------------
port_of() { echo "$OUT" | jq -r --arg r "$1" '.[] | select(.role==$r) | .http_port' | head -1; }
mports()  { echo "$OUT" | jq -r '.[] | select(.role=="master") | .http_port' | sort; }
MPORT1="$(mports | head -1)"
VPORT="$(port_of volume)"; FPORT="$(port_of filer)"; SPORT="$(port_of s3)"
NMASTERS="$(mports | wc -l | tr -d ' ')"

# ---- refuse to run if a required port is already taken (foreign cluster) -----
busy=""
for p in $(echo "$OUT" | jq -r '.[].http_port'); do
  lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1 && busy="$busy $p"
done
[ -n "$busy" ] && { echo "Required port(s) already in use:$busy -- is another SeaweedFS running?" >&2; exit 3; }

[ "${KEEP:-0}" = "1" ] || trap cleanup EXIT INT TERM

node_names() { echo "$OUT" | jq -r 'keys[]'; }
node_field() { echo "$OUT" | jq -r --arg n "$1" --arg f "$2" '.[$n][$f]'; }

launch_node() {
  n="$1"
  for p in $(echo "$OUT" | jq -r --arg n "$n" '.[$n].config_files | keys[]?'); do
    mkdir -p "$(dirname "$p")"
    echo "$OUT" | jq -r --arg n "$n" --arg p "$p" '.[$n].config_files[$p]' > "$p"
  done
  for d in $(echo "$OUT" | jq -r --arg n "$n" '.[$n].data_dirs[]?'); do mkdir -p "$d"; done
  ENVS=()
  while IFS= read -r e; do [ -n "$e" ] && ENVS+=("$e"); done \
    < <(echo "$OUT" | jq -r --arg n "$n" '.[$n].env | to_entries[] | "\(.key)=\(.value)"')
  ARGV=()
  while IFS= read -r a; do ARGV+=("$a"); done \
    < <(echo "$OUT" | jq -r --arg n "$n" '.[$n].argv[]')
  info "launching $n"
  if [ "${#ENVS[@]}" -gt 0 ]; then
    env "${ENVS[@]}" "$WEED" "${ARGV[@]}" >"$LOGDIR/$n.log" 2>&1 &
  else
    "$WEED" "${ARGV[@]}" >"$LOGDIR/$n.log" 2>&1 &
  fi
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

# ---- launch masters and wait for quorum -------------------------------------
for n in $(node_names); do
  case "$(node_field "$n" role)" in master) launch_node "$n";; esac
done

info "waiting for master quorum across ports: $(mports | tr '\n' ' ')"
QUORUM_OK=0; i=0
while [ "$i" -lt 40 ]; do
  LEADERS=0; AGREED_LEADER=""; mismatch=0; reachable=0
  for port in $(mports); do
    st="$(curl -fsS --max-time 2 "http://127.0.0.1:$port/cluster/status" 2>/dev/null)" || continue
    reachable=$((reachable + 1))
    [ "$(echo "$st" | jq -r '.IsLeader // false')" = "true" ] && LEADERS=$((LEADERS + 1))
    ldr="$(echo "$st" | jq -r '.Leader // ""')"
    if [ -n "$ldr" ]; then
      if [ -z "$AGREED_LEADER" ]; then AGREED_LEADER="$ldr"; elif [ "$AGREED_LEADER" != "$ldr" ]; then mismatch=1; fi
    fi
  done
  if [ "$reachable" -eq "$NMASTERS" ] && [ "$LEADERS" -eq 1 ] && [ "$mismatch" -eq 0 ] && [ -n "$AGREED_LEADER" ]; then
    QUORUM_OK=1; break
  fi
  i=$((i + 1)); sleep 1
done
if [ "$QUORUM_OK" -eq 1 ]; then
  ok "$NMASTERS-master quorum: exactly one leader ($AGREED_LEADER), all agree"
else
  bad "master quorum (reachable=$reachable leaders=$LEADERS leader='$AGREED_LEADER')"
fi

# ---- volume -----------------------------------------------------------------
for n in $(node_names); do
  case "$(node_field "$n" role)" in volume) launch_node "$n";; esac
done
wait_http "http://127.0.0.1:$VPORT/healthz" 40 && ok "volume /healthz up" || bad "volume /healthz"
ASSIGN_OK=0; i=0
while [ "$i" -lt 20 ]; do
  fid="$(curl -fsS --max-time 2 "http://127.0.0.1:$MPORT1/dir/assign" 2>/dev/null | jq -r '.fid // ""')"
  [ -n "$fid" ] && { ASSIGN_OK=1; break; }
  i=$((i + 1)); sleep 1
done
[ "$ASSIGN_OK" -eq 1 ] && ok "master assigned fid ($fid) => volume registered" || bad "master /dir/assign (no volume?)"

# ---- filer ------------------------------------------------------------------
for n in $(node_names); do
  case "$(node_field "$n" role)" in filer) launch_node "$n";; esac
done
wait_http "http://127.0.0.1:$FPORT/" 30 && ok "filer / up" || bad "filer /"
PAYLOAD="seaweedfs-terraform-smoke-$$"
echo "$PAYLOAD" > "$WORKDIR/hello.txt"
curl -fsS -F "file=@$WORKDIR/hello.txt" "http://127.0.0.1:$FPORT/smoke/hello.txt" >/dev/null 2>&1
got="$(curl -fsS --max-time 5 "http://127.0.0.1:$FPORT/smoke/hello.txt" 2>/dev/null)"
[ "$got" = "$PAYLOAD" ] && ok "filer PUT/GET round-trip" || bad "filer round-trip (got '$got')"

# ---- s3 ---------------------------------------------------------------------
for n in $(node_names); do
  case "$(node_field "$n" role)" in s3) launch_node "$n";; esac
done
wait_http "http://127.0.0.1:$SPORT/status" 30 && ok "s3 /status up" || bad "s3 /status"
curl -fsS -X PUT "http://127.0.0.1:$SPORT/smoke-bucket" >/dev/null 2>&1
sleep 1
curl -fsS -X PUT --data-binary "$PAYLOAD" "http://127.0.0.1:$SPORT/smoke-bucket/obj.txt" >/dev/null 2>&1
s3got="$(curl -fsS --max-time 5 "http://127.0.0.1:$SPORT/smoke-bucket/obj.txt" 2>/dev/null)"
[ "$s3got" = "$PAYLOAD" ] && ok "s3 PUT/GET round-trip" || bad "s3 round-trip (got '$s3got')"

echo
info "RESULTS: $PASS passed, $FAIL failed"
[ "${KEEP:-0}" = "1" ] && info "KEEP=1: cluster left running. Logs in $LOGDIR."
[ "$FAIL" -eq 0 ]
