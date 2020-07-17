package seaweedfs.client;

import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class SeaweedUtil {

    static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

    static {
        // Increase max total connection to 200
        cm.setMaxTotal(200);
        // Increase default max connection per route to 20
        cm.setDefaultMaxPerRoute(20);
    }

    public static CloseableHttpClient getClosableHttpClient() {
        return HttpClientBuilder.create()
                .setConnectionManager(cm)
                .setConnectionReuseStrategy(DefaultConnectionReuseStrategy.INSTANCE)
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                .build();
    }
}
