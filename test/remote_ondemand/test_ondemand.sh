#!/bin/sh
set -e
PASS=0; FAIL=0

assert_eq() {
  if [ "$1" = "$2" ]; then PASS=$((PASS+1)); echo "  PASS: $3"
  else FAIL=$((FAIL+1)); echo "  FAIL: $3 (expected '$1', got '$2')"; fi
}

# --- Test 1: Create bucket and upload file to central ---
echo "Test 1: Upload file to central cluster"
curl -s -X PUT "$CENTRAL_S3/testbucket" > /dev/null
dd if=/dev/urandom bs=1M count=10 2>/dev/null | \
  curl -s -X PUT -T - "$CENTRAL_S3/testbucket/file1.bin"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CENTRAL_S3/testbucket/file1.bin")
assert_eq "200" "$STATUS" "File uploaded to central"

# --- Test 2: On-demand GET via replica (exact path, triggers lazy fetch) ---
echo "Test 2: On-demand GET from replica (lazy fetch)"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA_S3/testbucket/file1.bin")
assert_eq "200" "$STATUS" "File accessible via replica on-demand"

# --- Test 3: LIST on replica shows the file ---
echo "Test 3: LIST on replica shows remote files"
LIST=$(curl -s "$REPLICA_S3/testbucket?list-type=2")
echo "$LIST" | grep -q "file1.bin"
assert_eq "0" "$?" "file1.bin appears in replica LIST"

# --- Test 4: Upload NEW file to central after mount ---
echo "Test 4: New file added to central after mount"
dd if=/dev/urandom bs=1M count=5 2>/dev/null | \
  curl -s -X PUT -T - "$CENTRAL_S3/testbucket/file2_new.bin"
sleep 2  # small delay for consistency

# GET by exact path (lazy fetch)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA_S3/testbucket/file2_new.bin")
assert_eq "200" "$STATUS" "New file accessible via replica lazy fetch"

# LIST should show it (hybrid listing)
LIST=$(curl -s "$REPLICA_S3/testbucket?list-type=2")
echo "$LIST" | grep -q "file2_new.bin"
assert_eq "0" "$?" "New file appears in replica LIST (hybrid)"

# --- Test 5: HEAD on replica returns correct size ---
echo "Test 5: HEAD returns correct metadata"
SIZE=$(curl -sI "$REPLICA_S3/testbucket/file1.bin" | grep -i content-length | tr -d '\r' | awk '{print $2}')
assert_eq "10485760" "$SIZE" "Content-Length matches 10MB"

# --- Test 6: Non-existent file returns 404 (not 500) ---
echo "Test 6: Non-existent file returns 404"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA_S3/testbucket/does_not_exist.bin")
assert_eq "404" "$STATUS" "Missing file returns 404"

# --- Test 7: Subdirectory listing ---
echo "Test 7: Subdirectory handling"
curl -s -X PUT -T /dev/null "$CENTRAL_S3/testbucket/subdir/nested.txt"
sleep 2
LIST=$(curl -s "$REPLICA_S3/testbucket?list-type=2&prefix=subdir/")
echo "$LIST" | grep -q "nested.txt"
assert_eq "0" "$?" "Nested file in subdir visible via replica"

# --- Summary ---
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
