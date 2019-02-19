package seaweedfs.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FilerGrpcClient {

    private static final Logger logger = Logger.getLogger(FilerGrpcClient.class.getName());

    private final ManagedChannel channel;
    private final SeaweedFilerGrpc.SeaweedFilerBlockingStub blockingStub;
    private final SeaweedFilerGrpc.SeaweedFilerStub asyncStub;
    private final SeaweedFilerGrpc.SeaweedFilerFutureStub futureStub;


    public FilerGrpcClient(String host, int grpcPort) {
        this(ManagedChannelBuilder.forAddress(host, grpcPort).usePlaintext());
    }

    public FilerGrpcClient(String host, int grpcPort,
                           String caFilePath,
                           String clientCertFilePath,
                           String clientPrivateKeyFilePath) throws SSLException {

        this(NettyChannelBuilder.forAddress(host, grpcPort)
                .negotiationType(NegotiationType.TLS)
                .sslContext(buildSslContext(caFilePath,clientCertFilePath,clientPrivateKeyFilePath)));
    }

    public FilerGrpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = SeaweedFilerGrpc.newBlockingStub(channel);
        asyncStub = SeaweedFilerGrpc.newStub(channel);
        futureStub = SeaweedFilerGrpc.newFutureStub(channel);
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
        return builder.build();
    }

}
