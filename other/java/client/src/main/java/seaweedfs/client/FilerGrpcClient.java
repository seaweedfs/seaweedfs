package seaweedfs.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FilerGrpcClient {

    private static final Logger logger = LoggerFactory.getLogger(FilerGrpcClient.class);
    static SslContext sslContext;

    static {
        try {
            sslContext = FilerSslContext.loadSslContext();
        } catch (SSLException e) {
            logger.warn("failed to load ssl context", e);
        }
    }

    public final int VOLUME_SERVER_ACCESS_DIRECT = 0;
    public final int VOLUME_SERVER_ACCESS_PUBLIC_URL = 1;
    public final int VOLUME_SERVER_ACCESS_FILER_PROXY = 2;
    public final Map<String, FilerProto.Locations> vidLocations = new HashMap<>();
    protected int randomClientId;
    private final ManagedChannel channel;
    private final SeaweedFilerGrpc.SeaweedFilerBlockingStub blockingStub;
    private final SeaweedFilerGrpc.SeaweedFilerStub asyncStub;
    private final SeaweedFilerGrpc.SeaweedFilerFutureStub futureStub;
    private boolean cipher = false;
    private String collection = "";
    private String replication = "";
    private int volumeServerAccess = VOLUME_SERVER_ACCESS_DIRECT;
    private String filerAddress;

    public FilerGrpcClient(String host, int port, int grpcPort) {
        this(host, port, grpcPort, sslContext);
    }

    public FilerGrpcClient(String host, int port, int grpcPort, SslContext sslContext) {

        this(sslContext == null ?
                ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext()
                        .maxInboundMessageSize(1024 * 1024 * 1024) :
                NettyChannelBuilder.forAddress(host, grpcPort)
                        .maxInboundMessageSize(1024 * 1024 * 1024)
                        .negotiationType(NegotiationType.TLS)
                        .sslContext(sslContext));

        filerAddress = SeaweedUtil.joinHostPort(host, port);

        FilerProto.GetFilerConfigurationResponse filerConfigurationResponse =
                this.getBlockingStub().getFilerConfiguration(
                        FilerProto.GetFilerConfigurationRequest.newBuilder().build());
        cipher = filerConfigurationResponse.getCipher();
        collection = filerConfigurationResponse.getCollection();
        replication = filerConfigurationResponse.getReplication();
        randomClientId = new Random().nextInt();

    }

    private FilerGrpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = SeaweedFilerGrpc.newBlockingStub(channel);
        asyncStub = SeaweedFilerGrpc.newStub(channel);
        futureStub = SeaweedFilerGrpc.newFutureStub(channel);
    }

    public boolean isCipher() {
        return cipher;
    }

    public String getCollection() {
        return collection;
    }

    public String getReplication() {
        return replication;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public SeaweedFilerGrpc.SeaweedFilerBlockingStub getBlockingStub() {
        return blockingStub;
    }

    public SeaweedFilerGrpc.SeaweedFilerStub getAsyncStub() {
        return asyncStub;
    }

    public SeaweedFilerGrpc.SeaweedFilerFutureStub getFutureStub() {
        return futureStub;
    }

    public void setAccessVolumeServerDirectly() {
        this.volumeServerAccess = VOLUME_SERVER_ACCESS_DIRECT;
    }

    public boolean isAccessVolumeServerDirectly() {
        return this.volumeServerAccess == VOLUME_SERVER_ACCESS_DIRECT;
    }

    public void setAccessVolumeServerByPublicUrl() {
        this.volumeServerAccess = VOLUME_SERVER_ACCESS_PUBLIC_URL;
    }

    public boolean isAccessVolumeServerByPublicUrl() {
        return this.volumeServerAccess == VOLUME_SERVER_ACCESS_PUBLIC_URL;
    }

    public void setAccessVolumeServerByFilerProxy() {
        this.volumeServerAccess = VOLUME_SERVER_ACCESS_FILER_PROXY;
    }

    public boolean isAccessVolumeServerByFilerProxy() {
        return this.volumeServerAccess == VOLUME_SERVER_ACCESS_FILER_PROXY;
    }

    public String getChunkUrl(String chunkId, String url, String publicUrl) {
        switch (this.volumeServerAccess) {
            case VOLUME_SERVER_ACCESS_PUBLIC_URL:
                return String.format("http://%s/%s", publicUrl, chunkId);
            case VOLUME_SERVER_ACCESS_FILER_PROXY:
                return String.format("http://%s/?proxyChunkId=%s", this.filerAddress, chunkId);
            default:
                return String.format("http://%s/%s", url, chunkId);
        }
    }

}
