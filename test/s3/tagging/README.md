# S3 Object Tagging Tests

This directory contains tests for S3 object tagging functionality.

## Issue Reference

These tests were created to verify the fix for [GitHub Issue #7589](https://github.com/seaweedfs/seaweedfs/issues/7589):
**S3 object Tags query comes back empty**

## Problem Description

When uploading an object with tags using the `X-Amz-Tagging` header, the tags were not being stored. 
When querying the object tagging with `GetObjectTagging`, the response was empty.

This was a regression between SeaweedFS 4.00 and 4.01.

## Root Cause

The `putToFiler` function in `s3api_object_handlers_put.go` was not parsing the `X-Amz-Tagging` header
and storing the tags in the entry's Extended metadata. The code was only copying user metadata 
(headers starting with `X-Amz-Meta-`) but not object tags.

## Fix

Added tag parsing logic to `putToFiler` that:
1. Reads the `X-Amz-Tagging` header
2. Parses it using `url.ParseQuery()` for proper URL decoding
3. Stores each tag with the prefix `X-Amz-Tagging-` in the entry's Extended metadata

## Running Tests

```bash
# Run all tagging tests
cd test/s3/tagging
make test

# Run specific test
make test-upload

# Or using go test directly
go test -v ./...
```

## Test Cases

1. **TestObjectTaggingOnUpload** - Basic test for tags sent during object upload
2. **TestObjectTaggingOnUploadWithSpecialCharacters** - Tests URL-encoded tag values
3. **TestObjectTaggingOnUploadWithEmptyValue** - Tests tags with empty values
4. **TestPutObjectTaggingAPI** - Tests the PutObjectTagging API separately
5. **TestDeleteObjectTagging** - Tests tag deletion
6. **TestTagsNotPreservedAfterObjectOverwrite** - Verifies AWS S3 behavior on overwrite
7. **TestMaximumNumberOfTags** - Tests storing the maximum 10 tags
8. **TestTagCountHeader** - Tests the x-amz-tagging-count header in HeadObject
