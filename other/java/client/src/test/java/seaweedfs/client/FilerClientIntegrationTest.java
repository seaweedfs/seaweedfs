package seaweedfs.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Integration tests for FilerClient.
 * 
 * These tests verify FilerClient operations against a running SeaweedFS filer
 * instance.
 * 
 * Prerequisites:
 * - SeaweedFS master, volume server, and filer must be running
 * - Default ports: filer HTTP 8888, filer gRPC 18888
 * 
 * To run tests:
 * export SEAWEEDFS_TEST_ENABLED=true
 * mvn test -Dtest=FilerClientIntegrationTest
 */
public class FilerClientIntegrationTest {

    private FilerClient filerClient;
    private static final String TEST_ROOT = "/test-client-integration";
    private static final boolean TESTS_ENABLED = "true".equalsIgnoreCase(System.getenv("SEAWEEDFS_TEST_ENABLED"));

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
    public void testMkdirs() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testDir = TEST_ROOT + "/testdir";
        boolean success = filerClient.mkdirs(testDir, 0755);

        assertTrue("Directory creation should succeed", success);
        assertTrue("Directory should exist", filerClient.exists(testDir));
    }

    @Test
    public void testTouch() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testFile = TEST_ROOT + "/testfile.txt";
        boolean success = filerClient.touch(testFile, 0644);

        assertTrue("Touch should succeed", success);
        assertTrue("File should exist", filerClient.exists(testFile));
    }

    @Test
    public void testExists() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        assertTrue("Root should exist", filerClient.exists("/"));
        assertTrue("Test root should exist", filerClient.exists(TEST_ROOT));
        assertFalse("Non-existent path should not exist",
                filerClient.exists(TEST_ROOT + "/nonexistent"));
    }

    @Test
    public void testListEntries() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        // Create some test files and directories
        filerClient.touch(TEST_ROOT + "/file1.txt", 0644);
        filerClient.touch(TEST_ROOT + "/file2.txt", 0644);
        filerClient.mkdirs(TEST_ROOT + "/subdir", 0755);

        List<FilerProto.Entry> entries = filerClient.listEntries(TEST_ROOT);

        assertNotNull("Entries should not be null", entries);
        assertEquals("Should have 3 entries", 3, entries.size());
    }

    @Test
    public void testListEntriesWithPrefix() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        // Create test files
        filerClient.touch(TEST_ROOT + "/test1.txt", 0644);
        filerClient.touch(TEST_ROOT + "/test2.txt", 0644);
        filerClient.touch(TEST_ROOT + "/other.txt", 0644);

        List<FilerProto.Entry> entries = filerClient.listEntries(TEST_ROOT, "test", "", 100, false);

        assertNotNull("Entries should not be null", entries);
        assertEquals("Should have 2 entries starting with 'test'", 2, entries.size());
    }

    @Test
    public void testDeleteFile() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testFile = TEST_ROOT + "/deleteme.txt";
        filerClient.touch(testFile, 0644);

        assertTrue("File should exist before delete", filerClient.exists(testFile));

        boolean success = filerClient.rm(testFile, false, true);

        assertTrue("Delete should succeed", success);
        assertFalse("File should not exist after delete", filerClient.exists(testFile));
    }

    @Test
    public void testDeleteDirectoryRecursive() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testDir = TEST_ROOT + "/deletedir";
        filerClient.mkdirs(testDir, 0755);
        filerClient.touch(testDir + "/file.txt", 0644);

        assertTrue("Directory should exist", filerClient.exists(testDir));
        assertTrue("File should exist", filerClient.exists(testDir + "/file.txt"));

        boolean success = filerClient.rm(testDir, true, true);

        assertTrue("Delete should succeed", success);
        assertFalse("Directory should not exist after delete", filerClient.exists(testDir));
    }

    @Test
    public void testRename() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String srcFile = TEST_ROOT + "/source.txt";
        String dstFile = TEST_ROOT + "/destination.txt";

        filerClient.touch(srcFile, 0644);
        assertTrue("Source file should exist", filerClient.exists(srcFile));

        boolean success = filerClient.mv(srcFile, dstFile);

        assertTrue("Rename should succeed", success);
        assertFalse("Source file should not exist after rename", filerClient.exists(srcFile));
        assertTrue("Destination file should exist after rename", filerClient.exists(dstFile));
    }

    @Test
    public void testGetEntry() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testFile = TEST_ROOT + "/getentry.txt";
        filerClient.touch(testFile, 0644);

        FilerProto.Entry entry = filerClient.lookupEntry(TEST_ROOT, "getentry.txt");

        assertNotNull("Entry should not be null", entry);
        assertEquals("Entry name should match", "getentry.txt", entry.getName());
        assertFalse("Entry should not be a directory", entry.getIsDirectory());
    }

    @Test
    public void testGetEntryForDirectory() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testDir = TEST_ROOT + "/testsubdir";
        filerClient.mkdirs(testDir, 0755);

        FilerProto.Entry entry = filerClient.lookupEntry(TEST_ROOT, "testsubdir");

        assertNotNull("Entry should not be null", entry);
        assertEquals("Entry name should match", "testsubdir", entry.getName());
        assertTrue("Entry should be a directory", entry.getIsDirectory());
    }

    @Test
    public void testCreateAndListNestedDirectories() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String nestedPath = TEST_ROOT + "/level1/level2/level3";
        boolean success = filerClient.mkdirs(nestedPath, 0755);

        assertTrue("Nested directory creation should succeed", success);
        assertTrue("Nested directory should exist", filerClient.exists(nestedPath));

        // Verify each level exists
        assertTrue("Level 1 should exist", filerClient.exists(TEST_ROOT + "/level1"));
        assertTrue("Level 2 should exist", filerClient.exists(TEST_ROOT + "/level1/level2"));
        assertTrue("Level 3 should exist", filerClient.exists(nestedPath));
    }

    @Test
    public void testMultipleFilesInDirectory() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String testDir = TEST_ROOT + "/multifiles";
        filerClient.mkdirs(testDir, 0755);

        // Create 10 files
        for (int i = 0; i < 10; i++) {
            filerClient.touch(testDir + "/file" + i + ".txt", 0644);
        }

        List<FilerProto.Entry> entries = filerClient.listEntries(testDir);

        assertNotNull("Entries should not be null", entries);
        assertEquals("Should have 10 files", 10, entries.size());
    }

    @Test
    public void testRenameDirectory() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String srcDir = TEST_ROOT + "/sourcedir";
        String dstDir = TEST_ROOT + "/destdir";

        filerClient.mkdirs(srcDir, 0755);
        filerClient.touch(srcDir + "/file.txt", 0644);

        boolean success = filerClient.mv(srcDir, dstDir);

        assertTrue("Directory rename should succeed", success);
        assertFalse("Source directory should not exist", filerClient.exists(srcDir));
        assertTrue("Destination directory should exist", filerClient.exists(dstDir));
        assertTrue("File should exist in destination", filerClient.exists(dstDir + "/file.txt"));
    }

    @Test
    public void testLookupNonExistentEntry() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        FilerProto.Entry entry = filerClient.lookupEntry(TEST_ROOT, "nonexistent.txt");

        assertNull("Entry for non-existent file should be null", entry);
    }

    @Test
    public void testEmptyDirectory() {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        String emptyDir = TEST_ROOT + "/emptydir";
        filerClient.mkdirs(emptyDir, 0755);

        List<FilerProto.Entry> entries = filerClient.listEntries(emptyDir);

        assertNotNull("Entries should not be null", entries);
        assertTrue("Empty directory should have no entries", entries.isEmpty());
    }
}
