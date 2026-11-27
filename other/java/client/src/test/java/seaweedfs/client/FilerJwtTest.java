package seaweedfs.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FilerJwtTest {

    private FilerCredentials credentials;

    @Before
    public void clearCredentials() {
        FilerSecurityContext.setBasicAuth(null, null);
        credentials = new FilerCredentials();
    }

    @After
    public void clearProxyBasicAuth() {
        FilerSecurityContext.setBasicAuth(null, null);
    }

    @Test
    public void filerJwtIsSignedHs256WithExp() throws Exception {
        long before = System.currentTimeMillis() / 1000;
        String token = FilerCredentials.genJwtForFilerServer("write-key".getBytes(StandardCharsets.UTF_8), 10);
        long after = System.currentTimeMillis() / 1000;

        String[] parts = token.split("\\.");
        assertEquals(3, parts.length);
        assertEquals("{\"alg\":\"HS256\",\"typ\":\"JWT\"}", decode(parts[0]));
        long exp = exp(decode(parts[1]));
        assertTrue(exp >= before + 10 && exp <= after + 10);
        assertEquals(sign("write-key", parts[0] + "." + parts[1]), parts[2]);
        assertFalse(token.contains("="));
    }

    @Test
    public void noCredentialsMeansNoFilerHeader() {
        assertNull(credentials.getAuthorizationHeaderValue(true));
        assertNull(credentials.getAuthorizationHeaderValue(false));
    }

    @Test
    public void accessLevelPicksItsSigningKey() throws Exception {
        credentials.setJwtSigningKey("write-key", 0);
        credentials.setJwtReadSigningKey("read-key", 0);
        long now = System.currentTimeMillis() / 1000;

        String[] write = bearer(credentials.getAuthorizationHeaderValue(true));
        assertEquals(sign("write-key", write[0] + "." + write[1]), write[2]);
        assertTrue(exp(decode(write[1])) - now <= 11);

        String[] read = bearer(credentials.getAuthorizationHeaderValue(false));
        assertEquals(sign("read-key", read[0] + "." + read[1]), read[2]);
        assertTrue(exp(decode(read[1])) - now >= 60);
    }

    @Test
    public void basicAuthCoversAccessLevelsWithoutKey() {
        FilerSecurityContext.setBasicAuth("proxy", "proxy-pass");
        assertEquals(FilerSecurityContext.getBasicAuthHeaderValue(),
                credentials.getAuthorizationHeaderValue(false));

        credentials.setBasicAuth("filer", "filer-pass");
        String filerBasic = "Basic " + Base64.getEncoder()
                .encodeToString("filer:filer-pass".getBytes(StandardCharsets.UTF_8));
        assertEquals(filerBasic, credentials.getAuthorizationHeaderValue(false));

        credentials.setJwtSigningKey("write-key", 5);
        assertTrue(credentials.getAuthorizationHeaderValue(true).startsWith("BEARER "));
        assertEquals(filerBasic, credentials.getAuthorizationHeaderValue(false));
    }

    @Test
    public void filerBasicAuthFollowsThePassword() {
        credentials.setBasicAuth("filer", "");
        assertNull(credentials.getAuthorizationHeaderValue(false));

        credentials.setBasicAuth("", "only-pass");
        assertEquals("Basic " + Base64.getEncoder().encodeToString(":only-pass".getBytes(StandardCharsets.UTF_8)),
                credentials.getAuthorizationHeaderValue(false));
    }

    @Test
    public void nullKeepsTheCurrentValue() throws Exception {
        credentials.setBasicAuth("filer", "old-pass");
        credentials.setBasicAuth(null, "new-pass");
        assertEquals("Basic " + Base64.getEncoder().encodeToString("filer:new-pass".getBytes(StandardCharsets.UTF_8)),
                credentials.getAuthorizationHeaderValue(false));

        credentials.setJwtSigningKey("write-key", 0);
        credentials.setJwtSigningKey(null, 300);
        long now = System.currentTimeMillis() / 1000;
        String[] write = bearer(credentials.getAuthorizationHeaderValue(true));
        assertEquals(sign("write-key", write[0] + "." + write[1]), write[2]);
        assertTrue(exp(decode(write[1])) - now >= 300);

        credentials.setJwtSigningKey("", 0);
        assertTrue(credentials.getAuthorizationHeaderValue(true).startsWith("Basic "));
    }

    @Test
    public void copiesSignIndependently() throws Exception {
        credentials.setJwtSigningKey("shared-key", 0);
        FilerCredentials a = new FilerCredentials(credentials);
        FilerCredentials b = new FilerCredentials(credentials);
        b.setJwtSigningKey("b-key", 0);

        String[] fromA = bearer(a.getAuthorizationHeaderValue(true));
        String[] fromB = bearer(b.getAuthorizationHeaderValue(true));
        assertEquals(sign("shared-key", fromA[0] + "." + fromA[1]), fromA[2]);
        assertEquals(sign("b-key", fromB[0] + "." + fromB[1]), fromB[2]);
        String[] fromDefaults = bearer(credentials.getAuthorizationHeaderValue(true));
        assertEquals(sign("shared-key", fromDefaults[0] + "." + fromDefaults[1]), fromDefaults[2]);
    }

    @Test
    public void onlyFilerProxyUrlsAreFilerRequests() {
        assertTrue(SeaweedUtil.isProxyChunkUrl("http://filer:8888/?proxyChunkId=3,01637037d6"));
        assertFalse(SeaweedUtil.isProxyChunkUrl("http://volume:8080/3,01637037d6"));
        assertFalse(SeaweedUtil.isProxyChunkUrl(null));
    }

    private static String[] bearer(String header) {
        assertTrue(header.startsWith("BEARER "));
        return header.substring("BEARER ".length()).split("\\.");
    }

    private static String decode(String part) {
        return new String(Base64.getUrlDecoder().decode(part), StandardCharsets.UTF_8);
    }

    private static long exp(String payload) {
        Matcher m = Pattern.compile("^\\{\"exp\":(\\d+)\\}$").matcher(payload);
        assertTrue(payload, m.matches());
        return Long.parseLong(m.group(1));
    }

    private static String sign(String key, String signingInput) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
    }
}
