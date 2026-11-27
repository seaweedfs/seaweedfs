package seaweedfs.client;

import com.google.common.base.Strings;
import com.moandjiezana.toml.Toml;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public abstract class FilerSecurityContext extends SslContext {
//extends Netty SslContext to access its protected static utility methods in
//buildHttpSslContext()

    private static final Logger logger = LoggerFactory.getLogger(FilerSecurityContext.class);
    private static boolean grpcSecurityEnabled;
    private static boolean httpSecurityEnabled;
    private static SslContext grpcSslContext;
    private static SSLContext httpSslContext;

    private static String grpcTrustCertCollectionFilePath;
    private static String grpcClientCertChainFilePath;
    private static String grpcClientPrivateKeyFilePath;

    private static String httpTrustCertCollectionFilePath;
    private static String httpClientCertChainFilePath;
    private static String httpClientPrivateKeyFilePath;

    // Optional HTTP Basic Auth for reaching SeaweedFS through an Nginx reverse proxy
    // that has auth_basic enabled. Applied to both the filer gRPC channel and the
    // volume/proxy HTTP requests. Precomputed so per-chunk requests don't re-encode.
    private static volatile String basicAuthHeaderValue;

    // Credentials for the filer's own HTTP port ([filer.basic_auth], [jwt.filer_signing],
    // [jwt.filer_signing.read]), sent only on requests addressed to the filer.
    private static volatile String filerBasicAuthHeaderValue;
    private static volatile FilerSigningKey filerSigningKey;
    private static volatile FilerSigningKey filerReadSigningKey;
    private static final long DEFAULT_FILER_SIGNING_EXPIRES_AFTER_SECONDS = 10;
    private static final long DEFAULT_FILER_READ_SIGNING_EXPIRES_AFTER_SECONDS = 60;


    static {
        String securityFileName = "security.toml";
        String home = System.getProperty("user.home");
        File f1 = new File("./"+securityFileName);
        File f2 = new File(home + "/.seaweedfs/"+securityFileName);
        File f3 = new File("/etc/seaweedfs/"+securityFileName);

        File securityFile = f1.exists()? f1 : f2.exists() ? f2 : f3.exists()? f3 : null;

        if (securityFile==null){
            logger.debug("Security file not found");
            grpcSecurityEnabled = false;
            httpSecurityEnabled = false;
        } else {

            Toml toml = new Toml().read(securityFile);
            logger.debug("reading ssl setup from {}", securityFile);

            basicAuthHeaderValue = computeBasicAuthHeaderValue(
                    toml.getString("basic_auth.username"), toml.getString("basic_auth.password"));
            setFilerBasicAuth(toml.getString("filer.basic_auth.user"), toml.getString("filer.basic_auth.password"));
            setFilerJwtSigningKey(toml.getString("jwt.filer_signing.key"),
                    toml.getLong("jwt.filer_signing.expires_after_seconds", 0L));
            setFilerJwtReadSigningKey(toml.getString("jwt.filer_signing.read.key"),
                    toml.getLong("jwt.filer_signing.read.expires_after_seconds", 0L));

            grpcTrustCertCollectionFilePath = toml.getString("grpc.ca");
            logger.debug("loading gRPC ca from {}", grpcTrustCertCollectionFilePath);
            grpcClientCertChainFilePath = toml.getString("grpc.client.cert");
            logger.debug("loading gRPC client ca from {}", grpcClientCertChainFilePath);
            grpcClientPrivateKeyFilePath = toml.getString("grpc.client.key");
            logger.debug("loading gRPC client key from {}", grpcClientPrivateKeyFilePath);

            if (Strings.isNullOrEmpty(grpcClientCertChainFilePath) && Strings.isNullOrEmpty(grpcClientPrivateKeyFilePath)) {
                logger.debug("gRPC private key file locations not set");
                grpcSecurityEnabled = false;
            } else {
                try {
                    grpcSslContext = buildGrpcSslContext();
                    grpcSecurityEnabled = true;
                } catch (Exception e) {
                    logger.warn("Couldn't initialize gRPC security context, filer operations are likely to fail!", e);
                    grpcSslContext = null;
                    grpcSecurityEnabled = false;
                }
            }

            if (toml.getBoolean("https.client.enabled", false)) {
                httpTrustCertCollectionFilePath = toml.getString("https.client.ca");
                logger.debug("loading HTTP ca from {}", httpTrustCertCollectionFilePath);
                httpClientCertChainFilePath = toml.getString("https.client.cert");
                logger.debug("loading HTTP client ca from {}", httpClientCertChainFilePath);
                httpClientPrivateKeyFilePath = toml.getString("https.client.key");
                logger.debug("loading HTTP client key from {}", httpClientPrivateKeyFilePath);

                if (Strings.isNullOrEmpty(httpClientCertChainFilePath) && Strings.isNullOrEmpty(httpClientPrivateKeyFilePath)) {
                    logger.debug("HTTP private key file locations not set");
                    httpSecurityEnabled = false;
                } else {
                    try {
                        httpSslContext = buildHttpSslContext();
                        httpSecurityEnabled = true;
                    } catch (Exception e) {
                        logger.warn("Couldn't initialize HTTP security context, volume operations are likely to fail!", e);
                        httpSslContext = null;
                        httpSecurityEnabled = false;
                    }
                }
            } else {
                httpSecurityEnabled = false;
            }
        }
        // possibly fix the format https://netty.io/wiki/sslcontextbuilder-and-private-key.html
    }

    public static boolean isGrpcSecurityEnabled() {
        return grpcSecurityEnabled;
    }

    public static boolean isHttpSecurityEnabled() {
        return httpSecurityEnabled;
    }

    public static SslContext getGrpcSslContext() {
        return grpcSslContext;
    }

    public static SSLContext getHttpSslContext() {
        return httpSslContext;
    }

    // Configure HTTP Basic Auth programmatically instead of via security.toml. Must be
    // called before constructing a FilerClient so the credentials reach the gRPC channel.
    public static void setBasicAuth(String username, String password) {
        basicAuthHeaderValue = computeBasicAuthHeaderValue(username, password);
    }

    public static boolean isBasicAuthEnabled() {
        return basicAuthHeaderValue != null;
    }

    // Returns the "Basic <base64(user:password)>" Authorization header value, or null
    // when no Basic Auth credentials are configured.
    public static String getBasicAuthHeaderValue() {
        return basicAuthHeaderValue;
    }

    // Configure the filer's [filer.basic_auth] credentials programmatically. Like the
    // filer, an empty password turns them off and an empty user is allowed.
    public static void setFilerBasicAuth(String username, String password) {
        filerBasicAuthHeaderValue = Strings.isNullOrEmpty(password) ? null
                : encodeBasicAuth(Strings.nullToEmpty(username), password);
    }

    // Configure jwt.filer_signing.key programmatically; expiresAfterSeconds <= 0 uses 10.
    public static void setFilerJwtSigningKey(String key, long expiresAfterSeconds) {
        filerSigningKey = FilerSigningKey.of(key, expiresAfterSeconds, DEFAULT_FILER_SIGNING_EXPIRES_AFTER_SECONDS);
    }

    // Configure jwt.filer_signing.read.key programmatically; expiresAfterSeconds <= 0 uses 60.
    public static void setFilerJwtReadSigningKey(String key, long expiresAfterSeconds) {
        filerReadSigningKey = FilerSigningKey.of(key, expiresAfterSeconds, DEFAULT_FILER_READ_SIGNING_EXPIRES_AFTER_SECONDS);
    }

    // Returns the Authorization header value for an HTTP request addressed to the filer:
    // a filer JWT for the access level when its key is set, else [filer.basic_auth], else
    // [basic_auth]; null when none is configured. The filer reads GET and HEAD with the read key.
    public static String getFilerAuthorizationHeaderValue(boolean isWrite) {
        FilerSigningKey key = isWrite ? filerSigningKey : filerReadSigningKey;
        if (key != null) {
            return "BEARER " + genJwtForFilerServer(key.key, key.expiresAfterSeconds);
        }
        if (filerBasicAuthHeaderValue != null) {
            return filerBasicAuthHeaderValue;
        }
        return basicAuthHeaderValue;
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

    private static final class FilerSigningKey {
        final byte[] key;
        final long expiresAfterSeconds;

        private FilerSigningKey(byte[] key, long expiresAfterSeconds) {
            this.key = key;
            this.expiresAfterSeconds = expiresAfterSeconds;
        }

        static FilerSigningKey of(String key, long expiresAfterSeconds, long defaultExpiresAfterSeconds) {
            if (Strings.isNullOrEmpty(key)) {
                return null;
            }
            return new FilerSigningKey(key.getBytes(StandardCharsets.UTF_8),
                    expiresAfterSeconds > 0 ? expiresAfterSeconds : defaultExpiresAfterSeconds);
        }
    }

    private static String computeBasicAuthHeaderValue(String username, String password) {
        if (Strings.isNullOrEmpty(username)) {
            return null;
        }
        return encodeBasicAuth(username, password == null ? "" : password);
    }

    private static String encodeBasicAuth(String username, String password) {
        String credentials = username + ":" + password;
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private static SslContext buildGrpcSslContext() throws SSLException {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        if (grpcTrustCertCollectionFilePath != null) {
            builder.trustManager(new File(grpcTrustCertCollectionFilePath));
        }
        if (grpcClientCertChainFilePath != null && grpcClientPrivateKeyFilePath != null) {
            builder.keyManager(new File(grpcClientCertChainFilePath), new File(grpcClientPrivateKeyFilePath));
        }

        return builder.build();
    }

    private static SSLContext buildHttpSslContext() throws GeneralSecurityException, IOException {
        SSLContextBuilder builder = SSLContexts.custom();

        if (httpTrustCertCollectionFilePath != null) {
            final X509Certificate[] trustCerts = toX509Certificates(new File(httpTrustCertCollectionFilePath));
            final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);

            int i = 0;
            for (X509Certificate cert: trustCerts) {
                String alias = Integer.toString(++i);
                ks.setCertificateEntry(alias, cert);
            }

            builder.loadTrustMaterial(ks, null);
        }

        if (httpClientCertChainFilePath != null && httpClientPrivateKeyFilePath != null) {
            final X509Certificate[] keyCerts = toX509Certificates(new File(httpClientCertChainFilePath));
            final PrivateKey key = toPrivateKey(new File(httpClientPrivateKeyFilePath), null);
            char[] emptyPassword = new char[0];
            final KeyStore ks = buildKeyStore(keyCerts, key, emptyPassword, null);
            logger.debug("Loaded {} key certificates", ks.size());
            builder.loadKeyMaterial(ks, emptyPassword);
        }

        return builder.build();
    }
}
