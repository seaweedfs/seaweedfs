package seaweedfs.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FilerGrpcClient {

    private static final Logger logger = LoggerFactory.getLogger(FilerGrpcClient.class);
    private static final SslContext sslContext;
    private static final String protocol;

    // gRPC keepalive settings - must be consistent with server-side settings in grpc_client_server.go
    private static final long KEEP_ALIVE_TIME_SECONDS = 60L;
    private static final long KEEP_ALIVE_TIMEOUT_SECONDS = 20L;

    static {
        sslContext = FilerSecurityContext.getGrpcSslContext();
        protocol = FilerSecurityContext.isHttpSecurityEnabled() ? "https" : "http";
    }

    public final int VOLUME_SERVER_ACCESS_DIRECT = 0;
    public final int VOLUME_SERVER_ACCESS_PUBLIC_URL = 1;
    public final int VOLUME_SERVER_ACCESS_FILER_PROXY = 2;
    public final Map<String, FilerProto.Locations> vidLocations = new HashMap<>();
    protected int randomClientId;

    // Connection pool to handle concurrent requests
    private static final int CHANNEL_POOL_SIZE = 4;
    private final List<ManagedChannel> channelPool;
    private final AtomicInteger channelIndex = new AtomicInteger(0);

    private boolean cipher = false;
    private String collection = "";
    private String replication = "";
    private int volumeServerAccess = VOLUME_SERVER_ACCESS_DIRECT;
    private String filerAddress;

    public FilerGrpcClient(String host, int port, int grpcPort, String cn) {
        this(host, port, grpcPort, cn, sslContext);
    }

    public FilerGrpcClient(String host, int port, int grpcPort, String cn, SslContext sslContext) {

        filerAddress = SeaweedUtil.joinHostPort(host, port);

        // Create a pool of channels for better concurrency handling
        channelPool = new ArrayList<>(CHANNEL_POOL_SIZE);

        for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
            channelPool.add(createChannelBuilder(host, grpcPort, sslContext, cn).build());
        }

        // Get filer configuration using first channel
        FilerProto.GetFilerConfigurationResponse filerConfigurationResponse = SeaweedFilerGrpc
                .newBlockingStub(channelPool.get(0)).getFilerConfiguration(
                        FilerProto.GetFilerConfigurationRequest.newBuilder().build());
        cipher = filerConfigurationResponse.getCipher();
        collection = filerConfigurationResponse.getCollection();
        replication = filerConfigurationResponse.getReplication();
        randomClientId = new Random().nextInt();

    }

    /**
     * Creates a NettyChannelBuilder with common gRPC configuration.
     * Supports plaintext and TLS modes with optional authority override.
     */
    private NettyChannelBuilder createChannelBuilder(String host, int grpcPort, SslContext sslContext, String cn) {
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, grpcPort)
                .maxInboundMessageSize(1024 * 1024 * 1024)
                .maxInboundMetadataSize(1024 * 1024)
                .flowControlWindow(16 * 1024 * 1024)
                .initialFlowControlWindow(16 * 1024 * 1024)
                .maxHeaderListSize(16 * 1024 * 1024)
                .keepAliveTime(KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(false)
                .withOption(io.grpc.netty.shaded.io.netty.channel.ChannelOption.SO_RCVBUF, 16 * 1024 * 1024)
                .withOption(io.grpc.netty.shaded.io.netty.channel.ChannelOption.SO_SNDBUF, 16 * 1024 * 1024);

        if (sslContext == null) {
            builder.usePlaintext();
        } else {
            builder.negotiationType(NegotiationType.TLS).sslContext(sslContext);
            if (!cn.isEmpty()) {
                builder.overrideAuthority(cn);
            }
        }
        return builder;
    }

    // Get a channel from the pool using round-robin
    private ManagedChannel getChannel() {
        int raw = channelIndex.getAndIncrement();
        int index = Math.floorMod(raw, CHANNEL_POOL_SIZE);
        return channelPool.get(index);
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
        for (ManagedChannel channel : channelPool) {
            channel.shutdown();
        }
        for (ManagedChannel channel : channelPool) {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public SeaweedFilerGrpc.SeaweedFilerBlockingStub getBlockingStub() {
        // Return a new stub using a channel from the pool (round-robin)
        return SeaweedFilerGrpc.newBlockingStub(getChannel());
    }

    public SeaweedFilerGrpc.SeaweedFilerStub getAsyncStub() {
        return SeaweedFilerGrpc.newStub(getChannel());
    }

    public SeaweedFilerGrpc.SeaweedFilerFutureStub getFutureStub() {
        return SeaweedFilerGrpc.newFutureStub(getChannel());
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
                return String.format("%s://%s/%s", protocol, publicUrl, chunkId);
            case VOLUME_SERVER_ACCESS_FILER_PROXY:
                return String.format("%s://%s/?proxyChunkId=%s", protocol, this.filerAddress, chunkId);
            default:
                return String.format("%s://%s/%s", protocol, url, chunkId);
        }
    }
}
