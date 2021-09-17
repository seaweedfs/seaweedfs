package seaweedfs.client;

import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class SeaweedUtil {

    static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    static CloseableHttpClient httpClient;

    static {
        // Increase max total connection to 200
        cm.setMaxTotal(200);
        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(20);

        httpClient = HttpClientBuilder.create()
                .setConnectionManager(cm)
                .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                .build();
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
