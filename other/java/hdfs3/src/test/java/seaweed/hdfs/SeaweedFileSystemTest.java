package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Unit tests for SeaweedFileSystem.
 * 
 * These tests verify basic FileSystem operations against a SeaweedFS backend.
 * Note: These tests require a running SeaweedFS filer instance.
 * 
 * To run tests, ensure SeaweedFS is running with default ports:
 * - Filer HTTP: 8888
 * - Filer gRPC: 18888
 * 
 * Set environment variable SEAWEEDFS_TEST_ENABLED=true to enable these tests.
 */
public class SeaweedFileSystemTest {

    private SeaweedFileSystem fs;
    private Configuration conf;
    private static final String TEST_ROOT = "/test-hdfs3";
    private static final boolean TESTS_ENABLED = 
        "true".equalsIgnoreCase(System.getenv("SEAWEEDFS_TEST_ENABLED"));

    @Before
    public void setUp() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        
        conf = new Configuration();
        conf.set("fs.seaweed.filer.host", "localhost");
        conf.setInt("fs.seaweed.filer.port", 8888);
        conf.setInt("fs.seaweed.filer.port.grpc", 18888);
        
        fs = new SeaweedFileSystem();
        URI uri = new URI("seaweedfs://localhost:8888/");
        fs.initialize(uri, conf);
        
        // Clean up any existing test directory
        Path testPath = new Path(TEST_ROOT);
        if (fs.exists(testPath)) {
            fs.delete(testPath, true);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (!TESTS_ENABLED || fs == null) {
            return;
        }
        
        // Clean up test directory
        Path testPath = new Path(TEST_ROOT);
        if (fs.exists(testPath)) {
            fs.delete(testPath, true);
        }
        
        fs.close();
    }

    @Test
    public void testInitialization() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        assertNotNull(fs);
        assertEquals("seaweedfs", fs.getScheme());
        assertNotNull(fs.getUri());
        assertEquals("/", fs.getWorkingDirectory().toUri().getPath());
    }

    @Test
    public void testMkdirs() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testDir = new Path(TEST_ROOT + "/testdir");
        assertTrue("Failed to create directory", fs.mkdirs(testDir));
        assertTrue("Directory should exist", fs.exists(testDir));
        
        FileStatus status = fs.getFileStatus(testDir);
        assertTrue("Path should be a directory", status.isDirectory());
    }

    @Test
    public void testCreateAndReadFile() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/testfile.txt");
        String testContent = "Hello, SeaweedFS!";
        
        // Create and write to file
        FSDataOutputStream out = fs.create(testFile, FsPermission.getDefault(), 
            false, 4096, (short) 1, 4 * 1024 * 1024, null);
        assertNotNull("Output stream should not be null", out);
        out.write(testContent.getBytes());
        out.close();
        
        // Verify file exists
        assertTrue("File should exist", fs.exists(testFile));
        
        // Read and verify content
        FSDataInputStream in = fs.open(testFile, 4096);
        assertNotNull("Input stream should not be null", in);
        byte[] buffer = new byte[testContent.length()];
        int bytesRead = in.read(buffer);
        in.close();
        
        assertEquals("Should read all bytes", testContent.length(), bytesRead);
        assertEquals("Content should match", testContent, new String(buffer));
    }

    @Test
    public void testFileStatus() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/statustest.txt");
        String content = "test content";
        
        FSDataOutputStream out = fs.create(testFile);
        out.write(content.getBytes());
        out.close();
        
        FileStatus status = fs.getFileStatus(testFile);
        assertNotNull("FileStatus should not be null", status);
        assertFalse("Should not be a directory", status.isDirectory());
        assertTrue("Should be a file", status.isFile());
        assertEquals("File length should match", content.length(), status.getLen());
        assertNotNull("Path should not be null", status.getPath());
    }

    @Test
    public void testListStatus() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testDir = new Path(TEST_ROOT + "/listtest");
        fs.mkdirs(testDir);
        
        // Create multiple files
        for (int i = 0; i < 3; i++) {
            Path file = new Path(testDir, "file" + i + ".txt");
            FSDataOutputStream out = fs.create(file);
            out.write(("content" + i).getBytes());
            out.close();
        }
        
        FileStatus[] statuses = fs.listStatus(testDir);
        assertNotNull("List should not be null", statuses);
        assertEquals("Should have 3 files", 3, statuses.length);
    }

    @Test
    public void testRename() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path srcFile = new Path(TEST_ROOT + "/source.txt");
        Path dstFile = new Path(TEST_ROOT + "/destination.txt");
        String content = "rename test";
        
        // Create source file
        FSDataOutputStream out = fs.create(srcFile);
        out.write(content.getBytes());
        out.close();
        
        assertTrue("Source file should exist", fs.exists(srcFile));
        
        // Rename
        assertTrue("Rename should succeed", fs.rename(srcFile, dstFile));
        
        // Verify
        assertFalse("Source file should not exist", fs.exists(srcFile));
        assertTrue("Destination file should exist", fs.exists(dstFile));
        
        // Verify content preserved
        FSDataInputStream in = fs.open(dstFile);
        byte[] buffer = new byte[content.length()];
        in.read(buffer);
        in.close();
        assertEquals("Content should be preserved", content, new String(buffer));
    }

    @Test
    public void testDelete() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/deletetest.txt");
        
        // Create file
        FSDataOutputStream out = fs.create(testFile);
        out.write("delete me".getBytes());
        out.close();
        
        assertTrue("File should exist before delete", fs.exists(testFile));
        
        // Delete
        assertTrue("Delete should succeed", fs.delete(testFile, false));
        assertFalse("File should not exist after delete", fs.exists(testFile));
    }

    @Test
    public void testDeleteDirectory() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testDir = new Path(TEST_ROOT + "/deletedir");
        Path testFile = new Path(testDir, "file.txt");
        
        // Create directory with file
        fs.mkdirs(testDir);
        FSDataOutputStream out = fs.create(testFile);
        out.write("content".getBytes());
        out.close();
        
        assertTrue("Directory should exist", fs.exists(testDir));
        assertTrue("File should exist", fs.exists(testFile));
        
        // Recursive delete
        assertTrue("Recursive delete should succeed", fs.delete(testDir, true));
        assertFalse("Directory should not exist after delete", fs.exists(testDir));
        assertFalse("File should not exist after delete", fs.exists(testFile));
    }

    @Test
    public void testAppend() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/appendtest.txt");
        String initialContent = "initial";
        String appendContent = " appended";
        
        // Create initial file
        FSDataOutputStream out = fs.create(testFile);
        out.write(initialContent.getBytes());
        out.close();
        
        // Append
        FSDataOutputStream appendOut = fs.append(testFile, 4096, null);
        assertNotNull("Append stream should not be null", appendOut);
        appendOut.write(appendContent.getBytes());
        appendOut.close();
        
        // Verify combined content
        FSDataInputStream in = fs.open(testFile);
        byte[] buffer = new byte[initialContent.length() + appendContent.length()];
        int bytesRead = in.read(buffer);
        in.close();
        
        String expected = initialContent + appendContent;
        assertEquals("Should read all bytes", expected.length(), bytesRead);
        assertEquals("Content should match", expected, new String(buffer));
    }

    @Test
    public void testSetWorkingDirectory() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path originalWd = fs.getWorkingDirectory();
        assertEquals("Original working directory should be /", "/", originalWd.toUri().getPath());
        
        Path newWd = new Path(TEST_ROOT);
        fs.mkdirs(newWd);
        fs.setWorkingDirectory(newWd);
        
        Path currentWd = fs.getWorkingDirectory();
        assertTrue("Working directory should be updated", 
            currentWd.toUri().getPath().contains(TEST_ROOT));
    }

    @Test
    public void testSetPermission() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/permtest.txt");
        
        // Create file
        FSDataOutputStream out = fs.create(testFile);
        out.write("permission test".getBytes());
        out.close();
        
        // Set permission
        FsPermission newPerm = new FsPermission((short) 0644);
        fs.setPermission(testFile, newPerm);
        
        FileStatus status = fs.getFileStatus(testFile);
        assertNotNull("Permission should not be null", status.getPermission());
    }

    @Test
    public void testSetOwner() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path testFile = new Path(TEST_ROOT + "/ownertest.txt");
        
        // Create file
        FSDataOutputStream out = fs.create(testFile);
        out.write("owner test".getBytes());
        out.close();
        
        // Set owner - this may not fail even if not fully implemented
        fs.setOwner(testFile, "testuser", "testgroup");
        
        // Just verify the call doesn't throw an exception
        FileStatus status = fs.getFileStatus(testFile);
        assertNotNull("FileStatus should not be null", status);
    }

    @Test
    public void testRenameToExistingDirectory() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        Path srcFile = new Path(TEST_ROOT + "/movefile.txt");
        Path dstDir = new Path(TEST_ROOT + "/movedir");
        
        // Create source file and destination directory
        FSDataOutputStream out = fs.create(srcFile);
        out.write("move test".getBytes());
        out.close();
        fs.mkdirs(dstDir);
        
        // Rename file to existing directory (should move file into directory)
        assertTrue("Rename to directory should succeed", fs.rename(srcFile, dstDir));
        
        // File should be moved into the directory
        Path expectedLocation = new Path(dstDir, srcFile.getName());
        assertTrue("File should exist in destination directory", fs.exists(expectedLocation));
        assertFalse("Source file should not exist", fs.exists(srcFile));
    }
}

