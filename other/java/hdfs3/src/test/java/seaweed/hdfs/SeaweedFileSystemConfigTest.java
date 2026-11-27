package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import seaweedfs.client.FilerCredentials;

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

    @Test
    public void testFilerCredentialsFromConfiguration() throws Exception {
        FilerCredentials credentials = new FilerCredentials();
        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_BASIC_AUTH_USER, "alice");
        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_BASIC_AUTH_PASSWORD, "s3cret");
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);
        String basic = "Basic " + java.util.Base64.getEncoder()
                .encodeToString("alice:s3cret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(basic, credentials.getAuthorizationHeaderValue(true));

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_KEY, "write-key");
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);
        assertTrue(credentials.getAuthorizationHeaderValue(true).startsWith("BEARER "));
        assertEquals(basic, credentials.getAuthorizationHeaderValue(false));

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_READ_KEY, "read-key");
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);
        assertTrue(credentials.getAuthorizationHeaderValue(false).startsWith("BEARER "));
    }

    @Test
    public void testFilerCredentialOptionsOverrideOneValueEach() throws Exception {
        FilerCredentials credentials = new FilerCredentials();
        credentials.setBasicAuth("toml-user", "toml-pass");
        credentials.setJwtReadSigningKey("toml-read-key", 0);

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_BASIC_AUTH_PASSWORD, "new-pass");
        conf.setLong(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_READ_EXPIRES, 300);
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);

        assertEquals("Basic " + java.util.Base64.getEncoder()
                        .encodeToString("toml-user:new-pass".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                credentials.getAuthorizationHeaderValue(true));
        String[] read = credentials.getAuthorizationHeaderValue(false).substring("BEARER ".length()).split("\\.");
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
        mac.init(new javax.crypto.spec.SecretKeySpec(
                "toml-read-key".getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
        assertEquals(java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(mac.doFinal(
                (read[0] + "." + read[1]).getBytes(java.nio.charset.StandardCharsets.UTF_8))), read[2]);
        String payload = new String(java.util.Base64.getUrlDecoder().decode(read[1]),
                java.nio.charset.StandardCharsets.UTF_8);
        long exp = Long.parseLong(payload.replaceAll("[^0-9]", ""));
        assertTrue(exp - System.currentTimeMillis() / 1000 >= 299);
    }
}
