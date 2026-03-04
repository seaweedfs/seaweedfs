#!/bin/sh
# On-demand remote storage functional tests
# Uses filer HTTP API for both central and replica clusters.
PASS=0; FAIL=0

CENTRAL="http://$CENTRAL_FILER"
REPLICA="http://$REPLICA_FILER"

assert_eq() {
  if [ "$1" = "$2" ]; then PASS=$((PASS+1)); echo "  PASS: $3"
  else FAIL=$((FAIL+1)); echo "  FAIL: $3 (expected '$1', got '$2')"; fi
}

assert_contains() {
  if echo "$1" | grep -q "$2" 2>/dev/null; then PASS=$((PASS+1)); echo "  PASS: $3"
  else FAIL=$((FAIL+1)); echo "  FAIL: $3 ('$2' not found in output)"; fi
}

# Helper: upload file to filer via multipart POST
upload_file() {
  local url="$1"
  local file="$2"
  curl -s -F "file=@$file" "$url" > /dev/null 2>&1
}

# --- Test 1: Upload file to central ---
echo "Test 1: Upload file to central cluster"
dd if=/dev/urandom of=/tmp/file1.bin bs=1M count=10 2>/dev/null
upload_file "$CENTRAL/buckets/testbucket/" /tmp/file1.bin
# Verify via filer
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CENTRAL/buckets/testbucket/file1.bin")
assert_eq "200" "$STATUS" "File uploaded to central"

# --- Test 2: On-demand GET via replica (triggers lazy fetch) ---
echo "Test 2: On-demand GET from replica (lazy fetch)"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA/buckets/testbucket/file1.bin")
assert_eq "200" "$STATUS" "File accessible via replica on-demand"

# --- Test 3: LIST on replica shows the file ---
echo "Test 3: LIST on replica shows remote files"
LIST=$(curl -s "$REPLICA/buckets/testbucket/?pretty=y")
assert_contains "$LIST" "file1.bin" "file1.bin appears in replica LIST"

# --- Test 4: Upload NEW file to central after mount ---
echo "Test 4: New file added to central after mount"
dd if=/dev/urandom of=/tmp/file2_new.bin bs=1M count=5 2>/dev/null
upload_file "$CENTRAL/buckets/testbucket/" /tmp/file2_new.bin
sleep 2  # small delay for consistency

# GET by exact path (lazy fetch)
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA/buckets/testbucket/file2_new.bin")
assert_eq "200" "$STATUS" "New file accessible via replica lazy fetch"

# LIST should show it (hybrid listing)
LIST=$(curl -s "$REPLICA/buckets/testbucket/?pretty=y")
assert_contains "$LIST" "file2_new.bin" "New file appears in replica LIST (hybrid)"

# --- Test 5: HEAD on replica returns correct size ---
echo "Test 5: HEAD returns correct metadata"
SIZE=$(curl -sI "$REPLICA/buckets/testbucket/file1.bin" | grep -i content-length | tr -d '\r' | awk '{print $2}')
assert_eq "10485760" "$SIZE" "Content-Length matches 10MB"

# --- Test 6: Non-existent file returns 404 (not 500) ---
echo "Test 6: Non-existent file returns 404"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA/buckets/testbucket/does_not_exist.bin")
assert_eq "404" "$STATUS" "Missing file returns 404"

# --- Test 7: Subdirectory on-demand listing ---
echo "Test 7: Subdirectory on-demand listing"
echo "test content" > /tmp/nested.txt
upload_file "$CENTRAL/buckets/testbucket/subdir/" /tmp/nested.txt
sleep 3
# Verify subdirectory listing works even when subdir doesn't exist locally
LIST=$(curl -s "$REPLICA/buckets/testbucket/subdir/?pretty=y")
assert_contains "$LIST" "nested.txt" "Subdirectory listing shows remote file"
# Also verify direct GET works
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$REPLICA/buckets/testbucket/subdir/nested.txt")
assert_eq "200" "$STATUS" "Nested file in subdir accessible via replica"

# Cleanup temp files
rm -f /tmp/file1.bin /tmp/file2_new.bin /tmp/nested.txt

# --- Summary ---
echo ""
echo "==============================="
echo "Results: $PASS passed, $FAIL failed"
echo "==============================="
[ "$FAIL" -eq 0 ] || exit 1
