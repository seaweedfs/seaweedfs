package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import seaweedfs.client.FilerCredentials;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
        String basic = "Basic " + Base64.getEncoder().encodeToString("alice:s3cret".getBytes(StandardCharsets.UTF_8));
        assertEquals(basic, credentials.getAuthorizationHeaderValue(true));

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_KEY, "write-key");
        conf.setLong(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_EXPIRES, 120);
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);
        long now = System.currentTimeMillis() / 1000;
        assertTrue(signedExp(credentials.getAuthorizationHeaderValue(true), "write-key") - now >= 119);
        assertEquals(basic, credentials.getAuthorizationHeaderValue(false));

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_READ_KEY, "read-key");
        conf.setLong(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_READ_EXPIRES, 240);
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);
        long readExp = signedExp(credentials.getAuthorizationHeaderValue(false), "read-key") - now;
        assertTrue(readExp >= 239 && readExp <= 245);
        long writeExp = signedExp(credentials.getAuthorizationHeaderValue(true), "write-key") - now;
        assertTrue(writeExp >= 119 && writeExp <= 125);
    }

    @Test
    public void testFilerCredentialOptionsOverrideOneValueEach() throws Exception {
        FilerCredentials credentials = new FilerCredentials();
        credentials.setBasicAuth("toml-user", "toml-pass");
        credentials.setJwtReadSigningKey("toml-read-key", 0);

        conf.set(SeaweedFileSystem.FS_SEAWEED_FILER_BASIC_AUTH_PASSWORD, "new-pass");
        conf.setLong(SeaweedFileSystem.FS_SEAWEED_FILER_JWT_SIGNING_READ_EXPIRES, 300);
        SeaweedFileSystem.applyFilerCredentials(conf, credentials);

        assertEquals("Basic " + Base64.getEncoder().encodeToString("toml-user:new-pass".getBytes(StandardCharsets.UTF_8)),
                credentials.getAuthorizationHeaderValue(true));
        long exp = signedExp(credentials.getAuthorizationHeaderValue(false), "toml-read-key");
        assertTrue(exp - System.currentTimeMillis() / 1000 >= 299);
    }

    // Asserts the header is a filer JWT signed with key and returns its exp claim.
    private static long signedExp(String header, String key) throws Exception {
        assertTrue(header, header.startsWith("BEARER "));
        String[] parts = header.substring("BEARER ".length()).split("\\.");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        assertEquals(Base64.getUrlEncoder().withoutPadding().encodeToString(
                mac.doFinal((parts[0] + "." + parts[1]).getBytes(StandardCharsets.UTF_8))), parts[2]);
        String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
        return Long.parseLong(payload.replaceAll("[^0-9]", ""));
    }
}
