package seaweedfs.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * Unit test to reproduce the Parquet EOF issue.
 * 
 * The issue: When Parquet writes column chunks, it calls getPos() to record
 * offsets.
 * If getPos() returns a position that doesn't include buffered (unflushed)
 * data,
 * the footer metadata will have incorrect offsets.
 * 
 * This test simulates Parquet's behavior:
 * 1. Write some data (column chunk 1)
 * 2. Call getPos() - Parquet records this as the END of chunk 1
 * 3. Write more data (column chunk 2)
 * 4. Call getPos() - Parquet records this as the END of chunk 2
 * 5. Close the file
 * 6. Verify that the recorded positions match the actual file content
 * 
 * Prerequisites:
 * - SeaweedFS master, volume server, and filer must be running
 * - Default ports: filer HTTP 8888, filer gRPC 18888
 * 
 * To run:
 * export SEAWEEDFS_TEST_ENABLED=true
 * cd other/java/client
 * mvn test -Dtest=GetPosBufferTest
 */
public class GetPosBufferTest {

    private FilerClient filerClient;
    private static final String TEST_ROOT = "/test-getpos-buffer";
    private static final boolean TESTS_ENABLED = "true".equalsIgnoreCase(System.getenv("SEAWEEDFS_TEST_ENABLED"));

    @Before
    public void setUp() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }

        String filerHost = System.getenv().getOrDefault("SEAWEEDFS_FILER_HOST", "localhost");
        String filerGrpcPort = System.getenv().getOrDefault("SEAWEEDFS_FILER_GRPC_PORT", "18888");

        filerClient = new FilerClient(filerHost, Integer.parseInt(filerGrpcPort));

        // Clean up any existing test directory
        if (filerClient.exists(TEST_ROOT)) {
            filerClient.rm(TEST_ROOT, true, true);
        }

        // Create test root directory
        filerClient.mkdirs(TEST_ROOT, 0755);
    }

    @After
    public void tearDown() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        if (filerClient != null) {
            filerClient.rm(TEST_ROOT, true, true);
            filerClient.shutdown();
        }
    }

    @Test
    public void testGetPosWithBufferedData() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        System.out.println("\n=== Testing getPos() with buffered data ===");

        String testPath = TEST_ROOT + "/getpos-test.bin";

        // Simulate what Parquet does when writing column chunks
        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);

        // Write "column chunk 1" - 100 bytes
        byte[] chunk1 = new byte[100];
        for (int i = 0; i < 100; i++) {
            chunk1[i] = (byte) i;
        }
        outputStream.write(chunk1);

        // Parquet calls getPos() here to record end of chunk 1
        long posAfterChunk1 = outputStream.getPos();
        System.out.println("Position after chunk 1 (100 bytes): " + posAfterChunk1);
        assertEquals("getPos() should return 100 after writing 100 bytes", 100, posAfterChunk1);

        // Write "column chunk 2" - 200 bytes
        byte[] chunk2 = new byte[200];
        for (int i = 0; i < 200; i++) {
            chunk2[i] = (byte) (i + 100);
        }
        outputStream.write(chunk2);

        // Parquet calls getPos() here to record end of chunk 2
        long posAfterChunk2 = outputStream.getPos();
        System.out.println("Position after chunk 2 (200 more bytes): " + posAfterChunk2);
        assertEquals("getPos() should return 300 after writing 300 bytes total", 300, posAfterChunk2);

        // Write "column chunk 3" - small chunk of 78 bytes (the problematic size!)
        byte[] chunk3 = new byte[78];
        for (int i = 0; i < 78; i++) {
            chunk3[i] = (byte) (i + 50);
        }
        outputStream.write(chunk3);

        // Parquet calls getPos() here to record end of chunk 3
        long posAfterChunk3 = outputStream.getPos();
        System.out.println("Position after chunk 3 (78 more bytes): " + posAfterChunk3);
        assertEquals("getPos() should return 378 after writing 378 bytes total", 378, posAfterChunk3);

        // Close to flush everything
        outputStream.close();
        System.out.println("File closed successfully");

        // Now read the file and verify its actual size matches what getPos() reported
        FilerProto.Entry entry = filerClient.lookupEntry(
                SeaweedOutputStream.getParentDirectory(testPath),
                SeaweedOutputStream.getFileName(testPath));

        long actualFileSize = SeaweedRead.fileSize(entry);
        System.out.println("Actual file size on disk: " + actualFileSize);

        assertEquals("File size should match the last getPos() value", 378, actualFileSize);

        // Now read the file and verify we can read all the data
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);

        byte[] readBuffer = new byte[500]; // Larger buffer to read everything
        int totalRead = 0;
        int bytesRead;
        while ((bytesRead = inputStream.read(readBuffer, totalRead, readBuffer.length - totalRead)) > 0) {
            totalRead += bytesRead;
        }
        inputStream.close();

        System.out.println("Total bytes read: " + totalRead);
        assertEquals("Should read exactly 378 bytes", 378, totalRead);

        // Verify the data is correct
        for (int i = 0; i < 100; i++) {
            assertEquals("Chunk 1 data mismatch at byte " + i, (byte) i, readBuffer[i]);
        }
        for (int i = 0; i < 200; i++) {
            assertEquals("Chunk 2 data mismatch at byte " + (100 + i), (byte) (i + 100), readBuffer[100 + i]);
        }
        for (int i = 0; i < 78; i++) {
            assertEquals("Chunk 3 data mismatch at byte " + (300 + i), (byte) (i + 50), readBuffer[300 + i]);
        }

        System.out.println("SUCCESS: All data verified correctly!\n");
    }

    @Test
    public void testGetPosWithSmallWrites() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        System.out.println("\n=== Testing getPos() with many small writes (Parquet pattern) ===");

        String testPath = TEST_ROOT + "/small-writes-test.bin";

        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);

        // Parquet writes column data in small chunks and frequently calls getPos()
        String[] columnData = { "Alice", "Bob", "Charlie", "David" };
        long[] recordedPositions = new long[columnData.length];

        for (int i = 0; i < columnData.length; i++) {
            byte[] data = columnData[i].getBytes(StandardCharsets.UTF_8);
            outputStream.write(data);

            // Parquet calls getPos() after each value to track offsets
            recordedPositions[i] = outputStream.getPos();
            System.out.println("After writing '" + columnData[i] + "': pos=" + recordedPositions[i]);
        }

        long finalPos = outputStream.getPos();
        System.out.println("Final position before close: " + finalPos);

        outputStream.close();

        // Verify file size
        FilerProto.Entry entry = filerClient.lookupEntry(
                SeaweedOutputStream.getParentDirectory(testPath),
                SeaweedOutputStream.getFileName(testPath));
        long actualFileSize = SeaweedRead.fileSize(entry);

        System.out.println("Actual file size: " + actualFileSize);
        assertEquals("File size should match final getPos()", finalPos, actualFileSize);

        // Verify we can read using the recorded positions
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);

        long currentPos = 0;
        for (int i = 0; i < columnData.length; i++) {
            long nextPos = recordedPositions[i];
            int length = (int) (nextPos - currentPos);

            byte[] buffer = new byte[length];
            int bytesRead = inputStream.read(buffer, 0, length);

            assertEquals("Should read " + length + " bytes for '" + columnData[i] + "'", length, bytesRead);

            String readData = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
            System.out.println("Read at offset " + currentPos + ": '" + readData + "'");
            assertEquals("Data mismatch", columnData[i], readData);

            currentPos = nextPos;
        }

        inputStream.close();

        System.out.println("SUCCESS: Small writes with getPos() tracking work correctly!\n");
    }

    @Test
    public void testGetPosWithExactly78BytesBuffered() throws IOException {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        System.out.println("\n=== Testing getPos() with EXACTLY 78 bytes buffered (the bug size!) ===");

        String testPath = TEST_ROOT + "/78-bytes-test.bin";

        SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, testPath);

        // Write some initial data
        byte[] initial = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            initial[i] = (byte) i;
        }
        outputStream.write(initial);
        outputStream.flush(); // Ensure this is flushed

        long posAfterFlush = outputStream.getPos();
        System.out.println("Position after 1000 bytes + flush: " + posAfterFlush);
        assertEquals("Should be at position 1000 after flush", 1000, posAfterFlush);

        // Now write EXACTLY 78 bytes (the problematic buffer size in our bug)
        byte[] problematicChunk = new byte[78];
        for (int i = 0; i < 78; i++) {
            problematicChunk[i] = (byte) (i + 50);
        }
        outputStream.write(problematicChunk);

        // DO NOT FLUSH - this is the bug scenario!
        // Parquet calls getPos() here while the 78 bytes are still buffered
        long posWithBufferedData = outputStream.getPos();
        System.out.println("Position with 78 bytes BUFFERED (not flushed): " + posWithBufferedData);

        // This MUST return 1078, not 1000!
        assertEquals("getPos() MUST include buffered data", 1078, posWithBufferedData);

        // Now close (which will flush)
        outputStream.close();

        // Verify actual file size
        FilerProto.Entry entry = filerClient.lookupEntry(
                SeaweedOutputStream.getParentDirectory(testPath),
                SeaweedOutputStream.getFileName(testPath));
        long actualFileSize = SeaweedRead.fileSize(entry);

        System.out.println("Actual file size: " + actualFileSize);
        assertEquals("File size must be 1078", 1078, actualFileSize);

        // Try to read at position 1000 for 78 bytes (what Parquet would try)
        SeaweedInputStream inputStream = new SeaweedInputStream(filerClient, testPath, entry);
        inputStream.seek(1000);

        byte[] readBuffer = new byte[78];
        int bytesRead = inputStream.read(readBuffer, 0, 78);

        System.out.println("Bytes read at position 1000: " + bytesRead);
        assertEquals("Should successfully read 78 bytes at position 1000", 78, bytesRead);

        // Verify the data matches
        for (int i = 0; i < 78; i++) {
            assertEquals("Data mismatch at byte " + i, problematicChunk[i], readBuffer[i]);
        }

        inputStream.close();

        System.out.println("SUCCESS: getPos() correctly includes buffered data!\n");
    }
}
