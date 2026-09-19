#!/usr/bin/env bash
#
# Prepares the fixtures required by the Snowflake s3compat API test suite
# (https://github.com/snowflakedb/snowflake-s3compat-api-test-suite) against a
# running SeaweedFS S3 endpoint:
#
#   BUCKET_NAME_1           versioning-enabled bucket the suite writes to
#   NOT_ACCESSIBLE_BUCKET   bucket with a deny-all bucket policy; the suite
#                           expects 403 AccessDenied for every operation on it
#   PREFIX_FOR_PAGE_LISTING prefix under BUCKET_NAME_1 holding more than 1000
#                           objects (the suite asserts paged listing works)
#
# Requires: aws CLI on PATH, AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set.

set -euo pipefail

ENDPOINT_URL="${ENDPOINT_URL:-http://127.0.0.1:8333}"
BUCKET_NAME_1="${BUCKET_NAME_1:-sf-snowflake-test}"
NOT_ACCESSIBLE_BUCKET="${NOT_ACCESSIBLE_BUCKET:-sf-denied-bucket}"
PREFIX_FOR_PAGE_LISTING="${PREFIX_FOR_PAGE_LISTING:-test-suite/page-listing/}"
PAGE_LISTING_TOTAL_SIZE="${PAGE_LISTING_TOTAL_SIZE:-1100}"

aws="aws --endpoint-url $ENDPOINT_URL"

echo "Creating test bucket $BUCKET_NAME_1 with versioning enabled"
$aws s3api create-bucket --bucket "$BUCKET_NAME_1"
$aws s3api put-bucket-versioning --bucket "$BUCKET_NAME_1" --versioning-configuration Status=Enabled
$aws s3api get-bucket-versioning --bucket "$BUCKET_NAME_1"

echo "Creating not-accessible bucket $NOT_ACCESSIBLE_BUCKET with a deny-all bucket policy"
$aws s3api create-bucket --bucket "$NOT_ACCESSIBLE_BUCKET"
POLICY_FILE="$(mktemp)"
cat > "$POLICY_FILE" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyAll",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:*",
      "Resource": [
        "arn:aws:s3:::${NOT_ACCESSIBLE_BUCKET}",
        "arn:aws:s3:::${NOT_ACCESSIBLE_BUCKET}/*"
      ]
    }
  ]
}
EOF
$aws s3api put-bucket-policy --bucket "$NOT_ACCESSIBLE_BUCKET" --policy "file://$POLICY_FILE"
rm -f "$POLICY_FILE"

if OUT="$($aws s3api get-bucket-location --bucket "$NOT_ACCESSIBLE_BUCKET" 2>&1)"; then
  echo "ERROR: expected AccessDenied on $NOT_ACCESSIBLE_BUCKET, got success" >&2
  exit 1
elif ! echo "$OUT" | grep -q "AccessDenied"; then
  echo "ERROR: expected AccessDenied on $NOT_ACCESSIBLE_BUCKET, got: $OUT" >&2
  exit 1
fi
echo "Verified $NOT_ACCESSIBLE_BUCKET denies access"

if [ "$PAGE_LISTING_TOTAL_SIZE" -le 1000 ]; then
  echo "ERROR: PAGE_LISTING_TOTAL_SIZE must be > 1000, got $PAGE_LISTING_TOTAL_SIZE" >&2
  exit 1
fi

echo "Uploading $PAGE_LISTING_TOTAL_SIZE objects to s3://$BUCKET_NAME_1/$PREFIX_FOR_PAGE_LISTING"
WORKDIR="$(mktemp -d)"
for i in $(seq 1 "$PAGE_LISTING_TOTAL_SIZE"); do
  echo "object-$i" > "$WORKDIR/file_$(printf %05d "$i").txt"
done
$aws s3 sync "$WORKDIR" "s3://$BUCKET_NAME_1/$PREFIX_FOR_PAGE_LISTING" --quiet
rm -rf "$WORKDIR"

COUNT=$($aws s3 ls "s3://$BUCKET_NAME_1/$PREFIX_FOR_PAGE_LISTING" | wc -l | tr -d ' ')
if [ "$COUNT" != "$PAGE_LISTING_TOTAL_SIZE" ]; then
  echo "ERROR: expected $PAGE_LISTING_TOTAL_SIZE objects under $PREFIX_FOR_PAGE_LISTING, found $COUNT" >&2
  exit 1
fi
echo "Verified $COUNT objects under s3://$BUCKET_NAME_1/$PREFIX_FOR_PAGE_LISTING"
echo "Fixtures ready."
