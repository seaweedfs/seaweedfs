package seaweedfs.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.After;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BasicAuthTest {

    @After
    public void clearCredentials() {
        FilerSecurityContext.setBasicAuth(null, null);
    }

    @Test
    public void noCredentialsMeansNoHeader() {
        FilerSecurityContext.setBasicAuth(null, null);
        assertNull(FilerSecurityContext.getBasicAuthHeaderValue());
        assertFalse(FilerSecurityContext.isBasicAuthEnabled());
    }

    @Test
    public void encodesUserAndPassword() {
        FilerSecurityContext.setBasicAuth("alice", "s3cret");
        String expected = "Basic " + Base64.getEncoder()
                .encodeToString("alice:s3cret".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, FilerSecurityContext.getBasicAuthHeaderValue());
        assertTrue(FilerSecurityContext.isBasicAuthEnabled());
    }

    @Test
    public void interceptorAddsAuthorizationHeader() {
        String headerValue = "Basic dGVzdDp0ZXN0";
        final Metadata[] captured = new Metadata[1];

        Channel channel = new Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
                return new ClientCall<ReqT, RespT>() {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        captured[0] = headers;
                    }

                    @Override
                    public void request(int numMessages) {
                    }

                    @Override
                    public void cancel(String message, Throwable cause) {
                    }

                    @Override
                    public void halfClose() {
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                    }
                };
            }

            @Override
            public String authority() {
                return "test";
            }
        };

        BasicAuthInterceptor interceptor = new BasicAuthInterceptor(headerValue);
        ClientCall<Object, Object> call =
                interceptor.interceptCall(null, CallOptions.DEFAULT, channel);
        call.start(null, new Metadata());

        Metadata.Key<String> key = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
        assertNotNull(captured[0]);
        assertEquals(headerValue, captured[0].get(key));
    }
}
