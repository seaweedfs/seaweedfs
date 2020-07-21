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
}
