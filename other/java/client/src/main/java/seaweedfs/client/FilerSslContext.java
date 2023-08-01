package seaweedfs.client;

import com.google.common.base.Strings;
import com.moandjiezana.toml.Toml;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;

public class FilerSslContext {

    private static final Logger logger = LoggerFactory.getLogger(FilerSslContext.class);

    public static SslContext loadSslContext() throws SSLException {
        String securityFileName = "security.toml";
        String home = System.getProperty("user.home");
        File f1 = new File("./"+securityFileName);
        File f2 = new File(home + "/.seaweedfs/"+securityFileName);
        File f3 = new File("/etc/seaweedfs/"+securityFileName);

        File securityFile = f1.exists()? f1 : f2.exists() ? f2 : f3.exists()? f3 : null;

        if (securityFile==null){
            return null;
        }

        Toml toml = new Toml().read(securityFile);
        logger.debug("reading ssl setup from {}", securityFile);

        String trustCertCollectionFilePath = toml.getString("grpc.ca");
        logger.debug("loading ca from {}", trustCertCollectionFilePath);
        String clientCertChainFilePath = toml.getString("grpc.client.cert");
        logger.debug("loading client ca from {}", clientCertChainFilePath);
        String clientPrivateKeyFilePath = toml.getString("grpc.client.key");
        logger.debug("loading client key from {}", clientPrivateKeyFilePath);

        if (Strings.isNullOrEmpty(clientPrivateKeyFilePath) && Strings.isNullOrEmpty(clientPrivateKeyFilePath)){
            return null;
        }

        // possibly fix the format https://netty.io/wiki/sslcontextbuilder-and-private-key.html

        return buildSslContext(trustCertCollectionFilePath, clientCertChainFilePath, clientPrivateKeyFilePath);
    }


    private static SslContext buildSslContext(String trustCertCollectionFilePath,
                                             String clientCertChainFilePath,
                                             String clientPrivateKeyFilePath) throws SSLException {
        SslContextBuilder builder = GrpcSslContexts.forClient();
        if (trustCertCollectionFilePath != null) {
            builder.trustManager(new File(trustCertCollectionFilePath));
        }
        if (clientCertChainFilePath != null && clientPrivateKeyFilePath != null) {
            builder.keyManager(new File(clientCertChainFilePath), new File(clientPrivateKeyFilePath));
        }
        return builder.trustManager(InsecureTrustManagerFactory.INSTANCE).build();
    }
}
