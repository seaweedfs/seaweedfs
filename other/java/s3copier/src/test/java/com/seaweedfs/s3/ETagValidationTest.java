package com.seaweedfs.s3;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.*;

/**
 * AWS SDK v2 Integration Tests for S3 ETag Format Validation.
 * 
 * These tests verify that SeaweedFS returns correct ETag formats that are
 * compatible with AWS SDK v2's checksum validation.
 * 
 * Background (GitHub Issue #7768):
 * AWS SDK v2 for Java validates ETags as hexadecimal MD5 hashes for PutObject
 * responses. If the ETag contains non-hex characters (like '-' in composite
 * format), the SDK fails with "Invalid base 16 character: '-'".
 * 
 * Per AWS S3 specification:
 * - Regular PutObject: ETag is always a pure MD5 hex string (32 chars)
 * - CompleteMultipartUpload: ETag is composite format "<md5>-<partcount>"
 * 
 * To run these tests:
 *   mvn test -Dtest=ETagValidationTest -DS3_ENDPOINT=http://localhost:8333
 * 
 * Or set environment variable:
 *   export S3_ENDPOINT=http://localhost:8333
 *   mvn test -Dtest=ETagValidationTest
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("S3 ETag Format Validation Tests (AWS SDK v2)")
class ETagValidationTest {

    // Configuration - can be overridden via system properties or environment variables
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:8333";
    private static final String DEFAULT_ACCESS_KEY = "some_access_key1";
    private static final String DEFAULT_SECRET_KEY = "some_secret_key1";
    private static final String DEFAULT_REGION = "us-east-1";
    
    // Auto-chunking threshold in SeaweedFS (must match s3api_object_handlers_put.go)
    private static final int AUTO_CHUNK_SIZE = 8 * 1024 * 1024; // 8MB
    
    // Test sizes
    private static final int SMALL_FILE_SIZE = 1024; // 1KB
    private static final int LARGE_FILE_SIZE = 10 * 1024 * 1024; // 10MB (triggers auto-chunking)
    private static final int XL_FILE_SIZE = 25 * 1024 * 1024; // 25MB (multiple chunks)
    private static final int MULTIPART_PART_SIZE = 5 * 1024 * 1024; // 5MB per part
    
    // ETag format patterns
    private static final Pattern PURE_MD5_PATTERN = Pattern.compile("^\"?[a-f0-9]{32}\"?$");
    private static final Pattern COMPOSITE_PATTERN = Pattern.compile("^\"?[a-f0-9]{32}-\\d+\"?$");
    
    private S3Client s3Client;
    private String testBucketName;
    private final SecureRandom random = new SecureRandom();

    @BeforeAll
    void setUp() {
        String endpoint = getConfig("S3_ENDPOINT", DEFAULT_ENDPOINT);
        String accessKey = getConfig("S3_ACCESS_KEY", DEFAULT_ACCESS_KEY);
        String secretKey = getConfig("S3_SECRET_KEY", DEFAULT_SECRET_KEY);
        String region = getConfig("S3_REGION", DEFAULT_REGION);
        
        System.out.println("Connecting to S3 endpoint: " + endpoint);
        
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .forcePathStyle(true) // Required for SeaweedFS
                .build();
        
        // Create test bucket
        testBucketName = "test-etag-" + UUID.randomUUID().toString().substring(0, 8);
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(testBucketName)
                .build());
        
        System.out.println("Created test bucket: " + testBucketName);
    }

    @AfterAll
    void tearDown() {
        if (s3Client != null && testBucketName != null) {
            try {
                // Delete all objects with pagination
                String continuationToken = null;
                do {
                    ListObjectsV2Response listResp = s3Client.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(testBucketName)
                                    .continuationToken(continuationToken)
                                    .build());
                    for (S3Object obj : listResp.contents()) {
                        s3Client.deleteObject(DeleteObjectRequest.builder()
                                .bucket(testBucketName)
                                .key(obj.key())
                                .build());
                    }
                    continuationToken = listResp.nextContinuationToken();
                } while (continuationToken != null);
                
                // Abort any multipart uploads
                ListMultipartUploadsResponse mpResp = s3Client.listMultipartUploads(
                        ListMultipartUploadsRequest.builder().bucket(testBucketName).build());
                for (MultipartUpload upload : mpResp.uploads()) {
                    s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(testBucketName)
                            .key(upload.key())
                            .uploadId(upload.uploadId())
                            .build());
                }
                
                // Delete bucket
                s3Client.deleteBucket(DeleteBucketRequest.builder()
                        .bucket(testBucketName)
                        .build());
                
                System.out.println("Cleaned up test bucket: " + testBucketName);
            } catch (Exception e) {
                System.err.println("Warning: Failed to cleanup test bucket: " + e.getMessage());
            }
            s3Client.close();
        }
    }

    @Test
    @DisplayName("Small file PutObject should return pure MD5 ETag")
    void testSmallFilePutObject() throws Exception {
        byte[] testData = generateRandomData(SMALL_FILE_SIZE);
        String expectedMD5 = calculateMD5Hex(testData);
        String objectKey = "small-file-" + UUID.randomUUID() + ".bin";
        
        PutObjectResponse response = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build(),
                RequestBody.fromBytes(testData));
        
        String etag = response.eTag();
        System.out.println("Small file ETag: " + etag + " (expected MD5: " + expectedMD5 + ")");
        
        assertThat(etag)
                .describedAs("Small file ETag should be pure MD5")
                .matches(PURE_MD5_PATTERN);
        assertThat(cleanETag(etag))
                .describedAs("ETag should match calculated MD5")
                .isEqualTo(expectedMD5);
        assertThat(etag)
                .describedAs("ETag should not contain hyphen")
                .doesNotContain("-");
    }

    /**
     * Critical test for GitHub Issue #7768.
     * 
     * This test uploads a file larger than the auto-chunking threshold (8MB),
     * which triggers SeaweedFS to split the file into multiple internal chunks.
     * 
     * Previously, this caused SeaweedFS to return a composite ETag like
     * "d41d8cd98f00b204e9800998ecf8427e-2", which AWS SDK v2 rejected because
     * it validates the ETag as hexadecimal and '-' is not a valid hex character.
     * 
     * The fix ensures that regular PutObject always returns a pure MD5 ETag,
     * regardless of internal chunking.
     */
    @Test
    @DisplayName("Large file PutObject (>8MB) should return pure MD5 ETag - Issue #7768")
    void testLargeFilePutObject_Issue7768() throws Exception {
        byte[] testData = generateRandomData(LARGE_FILE_SIZE);
        String expectedMD5 = calculateMD5Hex(testData);
        String objectKey = "large-file-" + UUID.randomUUID() + ".bin";
        
        System.out.println("Uploading large file (" + LARGE_FILE_SIZE + " bytes, " +
                "> " + AUTO_CHUNK_SIZE + " byte auto-chunk threshold)...");
        
        // This is where Issue #7768 would manifest - SDK v2 validates ETag
        PutObjectResponse response = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build(),
                RequestBody.fromBytes(testData));
        
        String etag = response.eTag();
        int expectedChunks = (LARGE_FILE_SIZE / AUTO_CHUNK_SIZE) + 1;
        System.out.println("Large file ETag: " + etag + 
                " (expected MD5: " + expectedMD5 + ", internal chunks: ~" + expectedChunks + ")");
        
        // These assertions would fail before the fix
        assertThat(etag)
                .describedAs("Large file PutObject ETag MUST be pure MD5 (not composite)")
                .matches(PURE_MD5_PATTERN);
        assertThat(etag)
                .describedAs("Large file ETag should NOT be composite format")
                .doesNotMatch(COMPOSITE_PATTERN);
        assertThat(etag)
                .describedAs("ETag should not contain hyphen for regular PutObject")
                .doesNotContain("-");
        assertThat(cleanETag(etag))
                .describedAs("ETag should match calculated MD5")
                .isEqualTo(expectedMD5);
        
        // Verify hex decoding works (this is what fails in Issue #7768)
        assertThatCode(() -> hexToBytes(cleanETag(etag)))
                .describedAs("ETag should be valid hexadecimal (AWS SDK v2 validation)")
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Extra large file PutObject (>24MB) should return pure MD5 ETag")
    void testExtraLargeFilePutObject() throws Exception {
        byte[] testData = generateRandomData(XL_FILE_SIZE);
        String expectedMD5 = calculateMD5Hex(testData);
        String objectKey = "xl-file-" + UUID.randomUUID() + ".bin";
        
        int expectedChunks = (XL_FILE_SIZE / AUTO_CHUNK_SIZE) + 1;
        System.out.println("Uploading XL file (" + XL_FILE_SIZE + " bytes, ~" + 
                expectedChunks + " internal chunks)...");
        
        PutObjectResponse response = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build(),
                RequestBody.fromBytes(testData));
        
        String etag = response.eTag();
        System.out.println("XL file ETag: " + etag);
        
        assertThat(etag)
                .describedAs("XL file PutObject ETag MUST be pure MD5")
                .matches(PURE_MD5_PATTERN);
        assertThat(cleanETag(etag))
                .describedAs("ETag should match calculated MD5")
                .isEqualTo(expectedMD5);
    }

    @Test
    @DisplayName("Multipart upload should return composite ETag")
    void testMultipartUploadETag() throws Exception {
        int totalSize = 15 * 1024 * 1024; // 15MB = 3 parts
        byte[] testData = generateRandomData(totalSize);
        String objectKey = "multipart-file-" + UUID.randomUUID() + ".bin";
        
        System.out.println("Performing multipart upload (" + totalSize + " bytes)...");
        
        // Initiate multipart upload
        CreateMultipartUploadResponse createResp = s3Client.createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build());
        String uploadId = createResp.uploadId();
        
        List<CompletedPart> completedParts = new ArrayList<>();
        int partNumber = 1;
        
        // Upload parts
        for (int offset = 0; offset < totalSize; offset += MULTIPART_PART_SIZE) {
            int end = Math.min(offset + MULTIPART_PART_SIZE, totalSize);
            byte[] partData = new byte[end - offset];
            System.arraycopy(testData, offset, partData, 0, partData.length);
            
            UploadPartResponse uploadResp = s3Client.uploadPart(
                    UploadPartRequest.builder()
                            .bucket(testBucketName)
                            .key(objectKey)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .build(),
                    RequestBody.fromBytes(partData));
            
            completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadResp.eTag())
                    .build());
            partNumber++;
        }
        
        // Complete multipart upload
        CompleteMultipartUploadResponse completeResp = s3Client.completeMultipartUpload(
                CompleteMultipartUploadRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());
        
        String etag = completeResp.eTag();
        System.out.println("Multipart upload ETag: " + etag + " (" + completedParts.size() + " parts)");
        
        // Multipart uploads SHOULD have composite ETag
        assertThat(etag)
                .describedAs("Multipart upload ETag SHOULD be composite format")
                .matches(COMPOSITE_PATTERN);
        assertThat(etag)
                .describedAs("Multipart ETag should contain hyphen")
                .contains("-");
        
        // Verify part count in ETag
        String[] parts = cleanETag(etag).split("-");
        assertThat(parts).hasSize(2);
        assertThat(parts[1])
                .describedAs("Part count in ETag should match uploaded parts")
                .isEqualTo(String.valueOf(completedParts.size()));
    }

    @Test
    @DisplayName("ETag should be consistent across PUT, HEAD, and GET")
    void testETagConsistency() throws Exception {
        byte[] testData = generateRandomData(LARGE_FILE_SIZE);
        String objectKey = "consistency-test-" + UUID.randomUUID() + ".bin";
        
        // PUT
        PutObjectResponse putResp = s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build(),
                RequestBody.fromBytes(testData));
        String putETag = putResp.eTag();
        
        // HEAD
        HeadObjectResponse headResp = s3Client.headObject(
                HeadObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build());
        String headETag = headResp.eTag();
        
        // GET
        GetObjectResponse getResp = s3Client.getObject(
                GetObjectRequest.builder()
                        .bucket(testBucketName)
                        .key(objectKey)
                        .build())
                .response();
        String getETag = getResp.eTag();
        
        System.out.println("PUT ETag: " + putETag + ", HEAD ETag: " + headETag + ", GET ETag: " + getETag);
        
        assertThat(putETag).isEqualTo(headETag);
        assertThat(putETag).isEqualTo(getETag);
    }

    @Test
    @DisplayName("Multiple large file uploads should all return pure MD5 ETags")
    void testMultipleLargeFileUploads() throws Exception {
        int numFiles = 3;
        
        for (int i = 0; i < numFiles; i++) {
            byte[] testData = generateRandomData(LARGE_FILE_SIZE);
            String expectedMD5 = calculateMD5Hex(testData);
            String objectKey = "multi-large-" + i + "-" + UUID.randomUUID() + ".bin";
            
            PutObjectResponse response = s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(testBucketName)
                            .key(objectKey)
                            .build(),
                    RequestBody.fromBytes(testData));
            
            String etag = response.eTag();
            System.out.println("File " + i + " ETag: " + etag);
            
            assertThat(etag)
                    .describedAs("File " + i + " ETag should be pure MD5")
                    .matches(PURE_MD5_PATTERN);
            assertThat(cleanETag(etag))
                    .describedAs("File " + i + " ETag should match MD5")
                    .isEqualTo(expectedMD5);
            
            // Validate hex decoding
            assertThatCode(() -> hexToBytes(cleanETag(etag)))
                    .doesNotThrowAnyException();
        }
    }

    // Helper methods
    
    private String getConfig(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            value = System.getenv(key);
        }
        return value != null ? value : defaultValue;
    }
    
    private byte[] generateRandomData(int size) {
        byte[] data = new byte[size];
        random.nextBytes(data);
        return data;
    }
    
    private String calculateMD5Hex(byte[] data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(data);
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    private String cleanETag(String etag) {
        if (etag == null) return null;
        return etag.replace("\"", "");
    }
    
    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }
}

