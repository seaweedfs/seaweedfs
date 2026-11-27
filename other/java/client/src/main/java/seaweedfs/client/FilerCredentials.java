package seaweedfs.client;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;

// Credentials for a filer's own HTTP port: [filer.basic_auth], [jwt.filer_signing] and
// [jwt.filer_signing.read]. Each FilerClient starts from its own copy of the security.toml values.
public final class FilerCredentials {

    private static final long DEFAULT_SIGNING_EXPIRES_AFTER_SECONDS = 10;
    private static final long DEFAULT_READ_SIGNING_EXPIRES_AFTER_SECONDS = 60;

    private volatile String basicAuthUser = "";
    private volatile String basicAuthPassword = "";
    private volatile String basicAuthHeaderValue;
    private volatile String signingKey = "";
    private volatile long signingExpiresAfterSeconds = DEFAULT_SIGNING_EXPIRES_AFTER_SECONDS;
    private volatile String readSigningKey = "";
    private volatile long readSigningExpiresAfterSeconds = DEFAULT_READ_SIGNING_EXPIRES_AFTER_SECONDS;

    public FilerCredentials() {
    }

    public FilerCredentials(FilerCredentials other) {
        synchronized (other) {
            basicAuthUser = other.basicAuthUser;
            basicAuthPassword = other.basicAuthPassword;
            basicAuthHeaderValue = other.basicAuthHeaderValue;
            signingKey = other.signingKey;
            signingExpiresAfterSeconds = other.signingExpiresAfterSeconds;
            readSigningKey = other.readSigningKey;
            readSigningExpiresAfterSeconds = other.readSigningExpiresAfterSeconds;
        }
    }

    // A null argument keeps the current value. Like the filer, an empty password turns
    // Basic Auth off and an empty user is allowed.
    public synchronized void setBasicAuth(String user, String password) {
        if (user != null) {
            basicAuthUser = user;
        }
        if (password != null) {
            basicAuthPassword = password;
        }
        basicAuthHeaderValue = basicAuthPassword.isEmpty() ? null
                : FilerSecurityContext.encodeBasicAuth(basicAuthUser, basicAuthPassword);
    }

    // A null key or expiresAfterSeconds <= 0 keeps the current value; an empty key turns signing off.
    public synchronized void setJwtSigningKey(String key, long expiresAfterSeconds) {
        if (key != null) {
            signingKey = key;
        }
        if (expiresAfterSeconds > 0) {
            signingExpiresAfterSeconds = expiresAfterSeconds;
        }
    }

    // Same as setJwtSigningKey, for the read key.
    public synchronized void setJwtReadSigningKey(String key, long expiresAfterSeconds) {
        if (key != null) {
            readSigningKey = key;
        }
        if (expiresAfterSeconds > 0) {
            readSigningExpiresAfterSeconds = expiresAfterSeconds;
        }
    }

    // Returns the Authorization header value for an HTTP request addressed to the filer:
    // a filer JWT for the access level when its key is set, else [filer.basic_auth], else
    // [basic_auth]; null when none is configured. The filer reads GET and HEAD with the read key.
    public String getAuthorizationHeaderValue(boolean isWrite) {
        String key = isWrite ? signingKey : readSigningKey;
        if (!key.isEmpty()) {
            return "BEARER " + genJwtForFilerServer(key.getBytes(StandardCharsets.UTF_8),
                    isWrite ? signingExpiresAfterSeconds : readSigningExpiresAfterSeconds);
        }
        String basicAuth = basicAuthHeaderValue;
        if (basicAuth != null) {
            return basicAuth;
        }
        return FilerSecurityContext.getBasicAuthHeaderValue();
    }

    // Mints an HS256 token with only an "exp" claim, like security.GenJwtForFilerServer.
    static String genJwtForFilerServer(byte[] signingKey, long expiresAfterSeconds) {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        long exp = System.currentTimeMillis() / 1000 + expiresAfterSeconds;
        String signingInput = encoder.encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8))
                + "." + encoder.encodeToString(("{\"exp\":" + exp + "}").getBytes(StandardCharsets.UTF_8));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(signingKey, "HmacSHA256"));
            return signingInput + "." + encoder.encodeToString(mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("cannot sign filer jwt", e);
        }
    }
}
