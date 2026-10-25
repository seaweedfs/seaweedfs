#!/usr/bin/env bash
#
# Integration test: git clone & pull on a SeaweedFS FUSE mount.
#
# Verifies that the mount correctly supports git's file operations by:
#   1. Creating a bare repo on the mount (acts as a remote)
#   2. Cloning it, making commits, and pushing back to the mount
#   3. Cloning from the mount into a working directory also on the mount
#   4. Pushing additional commits to the bare repo
#   5. Checking out an older revision in the on-mount clone
#   6. Running git pull to fast-forward with real changes
#   7. Verifying file content integrity at each step
#
# Usage:
#   bash test-git-on-mount.sh /path/to/mount/point
#
# The mount must already be running. All test artifacts are created under
# <mount>/git-test-<pid> and cleaned up on exit (unless TEST_KEEP=1).
#
set -euo pipefail

MOUNT_DIR="${1:?Usage: $0 <mount-dir>}"
TEST_DIR="$MOUNT_DIR/git-test-$$"
LOCAL_DIR=$(mktemp -d)
PASS=0
FAIL=0

cleanup() {
    if [[ "${TEST_KEEP:-}" == "1" ]]; then
        echo "TEST_KEEP=1 — leaving artifacts:"
        echo "  mount: $TEST_DIR"
        echo "  local: $LOCAL_DIR"
    else
        rm -rf "$TEST_DIR" 2>/dev/null || true
        rm -rf "$LOCAL_DIR" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# --- helpers ---------------------------------------------------------------

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

assert_file_contains() {
    local file=$1 expected=$2 label=$3
    if [[ -f "$file" ]] && grep -qF "$expected" "$file" 2>/dev/null; then
        pass "$label"
    else
        fail "$label (expected '$expected' in $file)"
    fi
}

assert_file_exists() {
    local file=$1 label=$2
    if [[ -f "$file" ]]; then
        pass "$label"
    else
        fail "$label ($file not found)"
    fi
}

assert_file_not_exists() {
    local file=$1 label=$2
    if [[ ! -f "$file" ]]; then
        pass "$label"
    else
        fail "$label ($file should not exist)"
    fi
}

assert_eq() {
    local actual=$1 expected=$2 label=$3
    if [[ "$actual" == "$expected" ]]; then
        pass "$label"
    else
        fail "$label (expected '$expected', got '$actual')"
    fi
}

# --- setup -----------------------------------------------------------------

echo "========================================"
echo "  Git-on-mount integration test"
echo "========================================"
echo "Mount:  $MOUNT_DIR"
echo "Test:   $TEST_DIR"
echo "Local:  $LOCAL_DIR"
echo ""

if ! mountpoint -q "$MOUNT_DIR" 2>/dev/null && [[ ! -d "$MOUNT_DIR" ]]; then
    echo "ERROR: $MOUNT_DIR is not a valid directory"
    exit 1
fi

mkdir -p "$TEST_DIR"

# --- Phase 1: Create bare repo on mount -----------------------------------

echo "--- Phase 1: Create bare repo on mount ---"

BARE_REPO="$TEST_DIR/repo.git"
git init --bare "$BARE_REPO" >/dev/null 2>&1
pass "bare repo created on mount"

# --- Phase 2: Clone locally, make initial commits, push -------------------

echo "--- Phase 2: Clone locally, make initial commits, push ---"

LOCAL_CLONE="$LOCAL_DIR/clone1"
git clone "$BARE_REPO" "$LOCAL_CLONE" >/dev/null 2>&1
cd "$LOCAL_CLONE"
git config user.email "test@seaweedfs.test"
git config user.name "SeaweedFS Test"

# Commit 1: initial files
echo "hello world" > README.md
mkdir -p src
echo 'package main; import "fmt"; func main() { fmt.Println("v1") }' > src/main.go
git add -A && git commit -m "initial commit" >/dev/null 2>&1
COMMIT1=$(git rev-parse HEAD)

# Commit 2: add more files
mkdir -p data
for i in $(seq 1 20); do
    printf "file-%03d: %s\n" "$i" "$(head -c 64 /dev/urandom | base64)" > "data/file-$(printf '%03d' $i).txt"
done
git add -A && git commit -m "add data files" >/dev/null 2>&1
COMMIT2=$(git rev-parse HEAD)

# Commit 3: modify and add
echo 'package main; import "fmt"; func main() { fmt.Println("v2") }' > src/main.go
echo "# Updated readme" >> README.md
mkdir -p docs
echo "documentation content" > docs/guide.md
git add -A && git commit -m "update src and add docs" >/dev/null 2>&1
COMMIT3=$(git rev-parse HEAD)

git push origin master >/dev/null 2>&1 || git push origin main >/dev/null 2>&1
BRANCH=$(git rev-parse --abbrev-ref HEAD)
pass "3 commits pushed to mount bare repo (branch=$BRANCH)"

# --- Phase 3: Clone from mount bare repo to mount working dir -------------

echo "--- Phase 3: Clone from mount bare repo to on-mount working dir ---"

MOUNT_CLONE="$TEST_DIR/working"
git clone "$BARE_REPO" "$MOUNT_CLONE" >/dev/null 2>&1

# Verify clone integrity
assert_file_exists "$MOUNT_CLONE/README.md" "README.md exists after clone"
assert_file_contains "$MOUNT_CLONE/README.md" "# Updated readme" "README.md has latest content"
assert_file_contains "$MOUNT_CLONE/src/main.go" 'v2' "src/main.go has v2"
assert_file_exists "$MOUNT_CLONE/docs/guide.md" "docs/guide.md exists"
assert_file_exists "$MOUNT_CLONE/data/file-001.txt" "data files exist"
assert_file_exists "$MOUNT_CLONE/data/file-020.txt" "data/file-020.txt exists"

CLONE_HEAD=$(cd "$MOUNT_CLONE" && git rev-parse HEAD)
assert_eq "$CLONE_HEAD" "$COMMIT3" "on-mount clone HEAD matches commit 3"

# Count files
FILE_COUNT=$(find "$MOUNT_CLONE/data" -name '*.txt' | wc -l | tr -d ' ')
assert_eq "$FILE_COUNT" "20" "data/ has 20 files"

# --- Phase 4: Push more commits from local clone --------------------------

echo "--- Phase 4: Push more commits from local clone ---"

cd "$LOCAL_CLONE"

# Commit 4: larger changes
for i in $(seq 21 50); do
    printf "file-%03d: %s\n" "$i" "$(head -c 128 /dev/urandom | base64)" > "data/file-$(printf '%03d' $i).txt"
done
echo 'package main; import "fmt"; func main() { fmt.Println("v3") }' > src/main.go
git add -A && git commit -m "expand data and update to v3" >/dev/null 2>&1
COMMIT4=$(git rev-parse HEAD)

# Commit 5: rename and delete
git mv docs/guide.md docs/manual.md
git rm data/file-001.txt >/dev/null 2>&1
git commit -m "rename guide, remove file-001" >/dev/null 2>&1
COMMIT5=$(git rev-parse HEAD)

git push origin "$BRANCH" >/dev/null 2>&1
pass "2 more commits pushed (5 total)"

# --- Phase 5: Checkout older revision in on-mount clone -------------------

echo "--- Phase 5: Checkout older revision in on-mount clone ---"

cd "$MOUNT_CLONE"
git checkout "$COMMIT2" >/dev/null 2>&1

# Verify we're at commit 2 state
DETACHED_HEAD=$(git rev-parse HEAD)
assert_eq "$DETACHED_HEAD" "$COMMIT2" "on-mount clone at commit 2 (detached)"
assert_file_not_exists "$MOUNT_CLONE/docs/guide.md" "docs/guide.md not in commit 2"
assert_file_contains "$MOUNT_CLONE/src/main.go" 'v1' "src/main.go has v1 at commit 2"

# --- Phase 6: Return to branch and pull -----------------------------------

echo "--- Phase 6: Return to branch and pull with real changes ---"

cd "$MOUNT_CLONE"
git checkout "$BRANCH" >/dev/null 2>&1
# At this point the on-mount clone is at commit 3, remote is at commit 5
OLD_HEAD=$(git rev-parse HEAD)
assert_eq "$OLD_HEAD" "$COMMIT3" "on-mount clone at commit 3 before pull"

git pull >/dev/null 2>&1
NEW_HEAD=$(git rev-parse HEAD)
assert_eq "$NEW_HEAD" "$COMMIT5" "HEAD matches commit 5 after pull"

# Verify commit 5 state
assert_file_contains "$MOUNT_CLONE/src/main.go" 'v3' "src/main.go has v3 after pull"
assert_file_exists "$MOUNT_CLONE/docs/manual.md" "docs/manual.md exists (renamed)"
assert_file_not_exists "$MOUNT_CLONE/docs/guide.md" "docs/guide.md gone (renamed)"
assert_file_not_exists "$MOUNT_CLONE/data/file-001.txt" "data/file-001.txt removed"
assert_file_exists "$MOUNT_CLONE/data/file-050.txt" "data/file-050.txt exists"

FINAL_COUNT=$(find "$MOUNT_CLONE/data" -name '*.txt' | wc -l | tr -d ' ')
assert_eq "$FINAL_COUNT" "49" "data/ has 49 files (50 added, 1 removed)"

# --- Phase 7: Verify git log integrity -----------------------------------

echo "--- Phase 7: Verify git log integrity ---"

cd "$MOUNT_CLONE"
GIT_LOG=$(git log --format=%s)
LOG_COUNT=$(echo "$GIT_LOG" | wc -l | tr -d ' ')
assert_eq "$LOG_COUNT" "5" "git log shows 5 commits"

# Verify commit messages
echo "$GIT_LOG" | grep -qF "initial commit" && pass "commit 1 message in log" || fail "commit 1 message missing"
echo "$GIT_LOG" | grep -qF "expand data" && pass "commit 4 message in log" || fail "commit 4 message missing"
echo "$GIT_LOG" | grep -qF "rename guide" && pass "commit 5 message in log" || fail "commit 5 message missing"

# --- Phase 8: Verify git status is clean ----------------------------------

echo "--- Phase 8: Verify git status is clean ---"

cd "$MOUNT_CLONE"
STATUS=$(git status --porcelain)
if [[ -z "$STATUS" ]]; then
    pass "git status is clean"
else
    fail "git status has changes: $STATUS"
fi

# --- Results ---------------------------------------------------------------

echo ""
echo "========================================"
echo "  Results: $PASS passed, $FAIL failed"
echo "========================================"

if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
