package seaweedfs.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Integration tests for SeaweedInputStream and SeaweedOutputStream.
 * 
 * These tests verify stream operations against a running SeaweedFS instance.
 * 
 * Prerequisites:
 * - SeaweedFS master, volume server, and filer must be running
 * - Default ports: filer HTTP 8888, filer gRPC 18888
 * 
 * To run tests:
 * export SEAWEEDFS_TEST_ENABLED=true
 * mvn test -Dtest=SeaweedStreamIntegrationTest
 */
public class SeaweedStreamIntegrationTest {

    private FilerClient filerClient;
    private static final String TEST_ROOT = "/test-stream-integration";
    private static final boolean TESTS_ENABLED = 
        "true".equalsIgnoreCase(System.getenv("SEAWEEDFS_TEST_ENABLED"));

    @Before
    public void setUp() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        
        filerClient = new FilerClient("localhost", 18888);
        
        // Clean up any existing test directory
        if (filerClient.exists(TEST_ROOT)) {
            filerClient.rm(TEST_ROOT, true, true);
        }
        
        // Create test root directory
        filerClient.mkdirs(TEST_ROOT, 0755);
    }

    @After
    public void tearDown() throws Exception {
        if (!TESTS_ENABLED || filerClient == null) {
            return;
        }
        
        try {
            // Clean up test directory
            if (filerClient.exists(TEST_ROOT)) {
                filerClient.rm(TEST_ROOT, true, true);
            }
        } finally {
            filerClient.shutdown();
        }
    }

    @Test
    public void testWriteAndReadSmallFile() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/small.txt";
        String testContent = "Hello, SeaweedFS!";
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Verify file exists
        assertTrue("File should exist", filerClient.exists(testPath));
        
        // Read file
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        assertNotNull("Entry should not be null", entry);
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] buffer = new byte[testContent.length()];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        assertEquals("Should read all bytes", testContent.length(), bytesRead);
        assertEquals("Content should match", testContent, new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void testWriteAndReadLargeFile() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/large.bin";
        int fileSize = 10 * 1024 * 1024; // 10 MB
        
        // Generate random data
        byte[] originalData = new byte[fileSize];
        new Random(42).nextBytes(originalData); // Use seed for reproducibility
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(originalData);
        outputStream.close();
        
        // Verify file exists
        assertTrue("File should exist", filerClient.exists(testPath));
        
        // Read file
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        assertNotNull("Entry should not be null", entry);
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        
        // Read file in chunks to handle large files properly
        byte[] readData = new byte[fileSize];
        int totalRead = 0;
        int bytesRead;
        byte[] buffer = new byte[8192]; // Read in 8KB chunks
        
        while ((bytesRead = inputStream.read(buffer)) > 0) {
            System.arraycopy(buffer, 0, readData, totalRead, bytesRead);
            totalRead += bytesRead;
        }
        inputStream.close();
        
        assertEquals("Should read all bytes", fileSize, totalRead);
        assertArrayEquals("Content should match", originalData, readData);
    }

    @Test
    public void testWriteInChunks() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/chunked.txt";
        String[] chunks = {"First chunk. ", "Second chunk. ", "Third chunk."};
        
        // Write file in chunks
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        for (String chunk : chunks) {
            outputStream.write(chunk.getBytes(StandardCharsets.UTF_8));
        }
        outputStream.close();
        
        // Read and verify
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] buffer = new byte[1024];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        String expected = String.join("", chunks);
        String actual = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
        
        assertEquals("Content should match", expected, actual);
    }

    @Test
    public void testReadWithOffset() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/offset.txt";
        String testContent = "0123456789ABCDEFGHIJ";
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Read with offset
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        inputStream.seek(10); // Skip first 10 bytes
        
        byte[] buffer = new byte[10];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        assertEquals("Should read 10 bytes", 10, bytesRead);
        assertEquals("Should read from offset", "ABCDEFGHIJ", 
            new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void testReadPartial() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/partial.txt";
        String testContent = "The quick brown fox jumps over the lazy dog";
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Read partial
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        
        // Read only "quick brown"
        inputStream.seek(4);
        byte[] buffer = new byte[11];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        assertEquals("Should read 11 bytes", 11, bytesRead);
        assertEquals("Should read partial content", "quick brown", 
            new String(buffer, StandardCharsets.UTF_8));
    }

    @Test
    public void testEmptyFile() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/empty.txt";
        
        // Write empty file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.close();
        
        // Verify file exists
        assertTrue("File should exist", filerClient.exists(testPath));
        
        // Read empty file
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        assertNotNull("Entry should not be null", entry);
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] buffer = new byte[100];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        assertEquals("Should read 0 bytes from empty file", -1, bytesRead);
    }

    @Test
    public void testOverwriteFile() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/overwrite.txt";
        String originalContent = "Original content";
        String newContent = "New content that overwrites the original";
        
        // Write original file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(originalContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Overwrite file
        outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(newContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Read and verify
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] buffer = new byte[1024];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        String actual = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
        assertEquals("Should have new content", newContent, actual);
    }

    @Test
    public void testMultipleReads() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/multireads.txt";
        String testContent = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testContent.getBytes(StandardCharsets.UTF_8));
        outputStream.close();
        
        // Read in multiple small chunks
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        
        StringBuilder result = new StringBuilder();
        byte[] buffer = new byte[5];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) > 0) {
            result.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
        }
        inputStream.close();
        
        assertEquals("Should read entire content", testContent, result.toString());
    }

    @Test
    public void testBinaryData() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/binary.bin";
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }
        
        // Write binary file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(binaryData);
        outputStream.close();
        
        // Read and verify
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] readData = new byte[256];
        int bytesRead = inputStream.read(readData);
        inputStream.close();
        
        assertEquals("Should read all bytes", 256, bytesRead);
        assertArrayEquals("Binary data should match", binaryData, readData);
    }

    @Test
    public void testFlush() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/flush.txt";
        String testContent = "Content to flush";
        
        // Write file with flush
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testContent.getBytes(StandardCharsets.UTF_8));
        outputStream.flush(); // Explicitly flush
        outputStream.close();
        
        // Verify file was written
        assertTrue("File should exist after flush", filerClient.exists(testPath));
        
        // Read and verify
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] buffer = new byte[testContent.length()];
        int bytesRead = inputStream.read(buffer);
        inputStream.close();
        
        assertEquals("Content should match", testContent, 
            new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
    }

    /**
     * Tests range reads similar to how Parquet reads column chunks.
     * This simulates:
     * 1. Seeking to specific offsets
     * 2. Reading specific byte ranges
     * 3. Verifying each read() call returns the correct number of bytes
     * 
     * This test specifically addresses the bug where read() was returning 0
     * for inline content or -1 prematurely for chunked reads.
     */
    @Test
    public void testRangeReads() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        String testPath = TEST_ROOT + "/rangereads.dat";
        
        // Create a 1275-byte file (similar to the Parquet file size that was failing)
        byte[] testData = new byte[1275];
        Random random = new Random(42); // Fixed seed for reproducibility
        random.nextBytes(testData);
        
        // Write file
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);
        outputStream.write(testData);
        outputStream.close();
        
        // Read file entry
        FilerProto.Entry entry = filerClient.lookupEntry(
            SeaweedOutputStream.getParentDirectory(testPath),
            SeaweedOutputStream.getFileName(testPath)
        );
        
        // Test 1: Read last 8 bytes (like reading Parquet footer length)
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        inputStream.seek(1267);
        byte[] buffer = new byte[8];
        int bytesRead = inputStream.read(buffer, 0, 8);
        assertEquals("Should read 8 bytes at offset 1267", 8, bytesRead);
        assertArrayEquals("Content at offset 1267 should match", 
            Arrays.copyOfRange(testData, 1267, 1275), buffer);
        inputStream.close();
        
        // Test 2: Read large chunk in middle (like reading column data)
        inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        inputStream.seek(383);
        buffer = new byte[884]; // Read bytes 383-1267
        bytesRead = inputStream.read(buffer, 0, 884);
        assertEquals("Should read 884 bytes at offset 383", 884, bytesRead);
        assertArrayEquals("Content at offset 383 should match", 
            Arrays.copyOfRange(testData, 383, 1267), buffer);
        inputStream.close();
        
        // Test 3: Read from beginning (like reading Parquet magic bytes)
        inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        buffer = new byte[4];
        bytesRead = inputStream.read(buffer, 0, 4);
        assertEquals("Should read 4 bytes at offset 0", 4, bytesRead);
        assertArrayEquals("Content at offset 0 should match", 
            Arrays.copyOfRange(testData, 0, 4), buffer);
        inputStream.close();
        
        // Test 4: Multiple sequential reads without seeking (like H2SeekableInputStream.readFully)
        // This is the critical test case that was failing!
        inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        inputStream.seek(1197); // Position where EOF was being returned prematurely
        
        byte[] fullBuffer = new byte[78]; // Try to read the "missing" 78 bytes
        int totalRead = 0;
        int offset = 0;
        int remaining = 78;
        
        // Simulate Parquet's H2SeekableInputStream.readFully() loop
        while (remaining > 0) {
            int read = inputStream.read(fullBuffer, offset, remaining);
            if (read == -1) {
                fail(String.format(
                    "Got EOF after reading %d bytes, but expected to read %d more bytes (total requested: 78)",
                    totalRead, remaining));
            }
            assertTrue("Each read() should return positive bytes", read > 0);
            totalRead += read;
            offset += read;
            remaining -= read;
        }
        
        assertEquals("Should read all 78 bytes in readFully loop", 78, totalRead);
        assertArrayEquals("Content at offset 1197 should match", 
            Arrays.copyOfRange(testData, 1197, 1275), fullBuffer);
        inputStream.close();
        
        // Test 5: Read entire file in one go
        inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        byte[] allData = new byte[1275];
        bytesRead = inputStream.read(allData, 0, 1275);
        assertEquals("Should read entire 1275 bytes", 1275, bytesRead);
        assertArrayEquals("Entire content should match", testData, allData);
        inputStream.close();
    }
}

