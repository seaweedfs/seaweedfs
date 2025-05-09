package seaweedfs.client;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

public class SeaweedUtil {

    private static final Logger logger = LoggerFactory.getLogger(SeaweedUtil.class);
    static PoolingHttpClientConnectionManager cm;
    static CloseableHttpClient httpClient;

    static {
        //Apache HTTP client has a terrible API that makes you configure everything twice
        //NoopHostnameVerifier is required because SeaweedFS doesn't verify hostnames
        //and the servers are likely to have TLS certificates that do not match their hosts
        if (FilerSecurityContext.isHttpSecurityEnabled()) {
            SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
                    FilerSecurityContext.getHttpSslContext(),
                    NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslSocketFactory)
                            .register("http", new PlainConnectionSocketFactory())
                            .build();
            cm = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        } else {
            cm = new PoolingHttpClientConnectionManager();
        }

        // Increase max total connection to 200
        cm.setMaxTotal(200);
        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(20);

        HttpClientBuilder builder = HttpClientBuilder.create()
                .setConnectionManager(cm)
                .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE);

        if (FilerSecurityContext.isHttpSecurityEnabled()) {
            builder.setSSLContext(FilerSecurityContext.getHttpSslContext());
            builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
        }

        httpClient = builder.build();
    }

    public static CloseableHttpClient getClosableHttpClient() {
        return httpClient;
    }

    public static String[] toDirAndName(String fullpath) {
        if (fullpath == null) {
            return new String[]{"/", ""};
        }
        if (fullpath.endsWith("/")) {
            fullpath = fullpath.substring(0, fullpath.length() - 1);
        }
        if (fullpath.length() == 0) {
            return new String[]{"/", ""};
        }
        int sep = fullpath.lastIndexOf("/");
        String parent = sep == 0 ? "/" : fullpath.substring(0, sep);
        String name = fullpath.substring(sep + 1);
        return new String[]{parent, name};
    }

    public static String joinHostPort(String host, int port) {
        if (host.startsWith("[") && host.endsWith("]")) {
            return host + ":" + port;
        }
        if (host.indexOf(':')>=0) {
            return "[" + host + "]:" + port;
        }
        return host + ":" + port;
    }
}
