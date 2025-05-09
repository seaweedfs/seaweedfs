package seaweedfs.client;

import com.google.common.base.Strings;
import com.moandjiezana.toml.Toml;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

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

            if (toml.getBoolean("https.client.enabled")) {
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
