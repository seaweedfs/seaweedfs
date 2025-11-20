package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for SeaweedFileSystem configuration that don't require a running SeaweedFS instance.
 * 
 * These tests verify basic properties and constants.
 */
public class SeaweedFileSystemConfigTest {

    private SeaweedFileSystem fs;
    private Configuration conf;

    @Before
    public void setUp() {
        fs = new SeaweedFileSystem();
        conf = new Configuration();
    }

    @Test
    public void testScheme() {
        assertEquals("seaweedfs", fs.getScheme());
    }

    @Test
    public void testConstants() {
        // Test that constants are defined correctly
        assertEquals("fs.seaweed.filer.host", SeaweedFileSystem.FS_SEAWEED_FILER_HOST);
        assertEquals("fs.seaweed.filer.port", SeaweedFileSystem.FS_SEAWEED_FILER_PORT);
        assertEquals("fs.seaweed.filer.port.grpc", SeaweedFileSystem.FS_SEAWEED_FILER_PORT_GRPC);
        assertEquals(8888, SeaweedFileSystem.FS_SEAWEED_DEFAULT_PORT);
        assertEquals("fs.seaweed.buffer.size", SeaweedFileSystem.FS_SEAWEED_BUFFER_SIZE);
        assertEquals(4 * 1024 * 1024, SeaweedFileSystem.FS_SEAWEED_DEFAULT_BUFFER_SIZE);
        assertEquals("fs.seaweed.replication", SeaweedFileSystem.FS_SEAWEED_REPLICATION);
        assertEquals("fs.seaweed.volume.server.access", SeaweedFileSystem.FS_SEAWEED_VOLUME_SERVER_ACCESS);
        assertEquals("fs.seaweed.filer.cn", SeaweedFileSystem.FS_SEAWEED_FILER_CN);
    }

    @Test
    public void testWorkingDirectoryPathOperations() {
        // Test path operations that don't require initialization
        Path testPath = new Path("/test/path");
        assertTrue("Path should be absolute", testPath.isAbsolute());
        assertEquals("/test/path", testPath.toUri().getPath());
        
        Path childPath = new Path(testPath, "child");
        assertEquals("/test/path/child", childPath.toUri().getPath());
    }

    @Test
    public void testConfigurationProperties() {
        // Test that configuration can be set and read
        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_HOST, "testhost");
        assertEquals("testhost", conf.get(SeaweedFileSystem.FS_SEAWEED_FILER_HOST));
        
        conf.setInt(SeaweedFileSystem.FS_SEAWEED_FILER_PORT, 9999);
        assertEquals(9999, conf.getInt(SeaweedFileSystem.FS_SEAWEED_FILER_PORT, 0));
        
        conf.setInt(SeaweedFileSystem.FS_SEAWEED_BUFFER_SIZE, 8 * 1024 * 1024);
        assertEquals(8 * 1024 * 1024, conf.getInt(SeaweedFileSystem.FS_SEAWEED_BUFFER_SIZE, 0));
        
        conf.set(SeaweedFileSystem.FS_SEAWEED_REPLICATION, "001");
        assertEquals("001", conf.get(SeaweedFileSystem.FS_SEAWEED_REPLICATION));
        
        conf.set(SeaweedFileSystem.FS_SEAWEED_VOLUME_SERVER_ACCESS, "publicUrl");
        assertEquals("publicUrl", conf.get(SeaweedFileSystem.FS_SEAWEED_VOLUME_SERVER_ACCESS));
        
        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_CN, "test-cn");
        assertEquals("test-cn", conf.get(SeaweedFileSystem.FS_SEAWEED_FILER_CN));
    }

    @Test
    public void testDefaultBufferSize() {
        // Test default buffer size constant
        int expected = 4 * 1024 * 1024; // 4MB
        assertEquals(expected, SeaweedFileSystem.FS_SEAWEED_DEFAULT_BUFFER_SIZE);
    }

    @Test
    public void testDefaultPort() {
        // Test default port constant
        assertEquals(8888, SeaweedFileSystem.FS_SEAWEED_DEFAULT_PORT);
    }
}
