import java.net.*;
import java.nio.*;
import java.nio.channels.*;

public class TestSocketReadiness {
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9093;

        System.out.println("Testing socket readiness with " + host + ":" + port);

        // Test 1: Simple blocking connect
        System.out.println("\n=== Test 1: Blocking Socket ===");
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            System.out.println("Blocking socket connected");
            System.out.println("   Available bytes: " + socket.getInputStream().available());
            Thread.sleep(100);
            System.out.println("   Available bytes after 100ms: " + socket.getInputStream().available());
        } catch (Exception e) {
            System.err.println("Blocking socket failed: " + e.getMessage());
        }

        // Test 2: Non-blocking NIO socket (like Kafka client uses)
        System.out.println("\n=== Test 2: Non-blocking NIO Socket ===");
        Selector selector = Selector.open();
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);

        try {
            boolean connected = channel.connect(new InetSocketAddress(host, port));
            System.out.println("   connect() returned: " + connected);

            SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT);

            int ready = selector.select(5000);
            System.out.println("   selector.select() returned: " + ready);

            if (ready > 0) {
                for (SelectionKey k : selector.selectedKeys()) {
                    if (k.isConnectable()) {
                        System.out.println("   isConnectable: true");
                        boolean finished = channel.finishConnect();
                        System.out.println("   finishConnect() returned: " + finished);

                        if (finished) {
                            k.interestOps(SelectionKey.OP_READ);

                            // Now check if immediately readable (THIS is what might be wrong)
                            selector.selectedKeys().clear();
                            int readReady = selector.selectNow();
                            System.out.println("   Immediately after connect, selectNow() = " + readReady);

                            if (readReady > 0) {
                                System.out.println("   Socket is IMMEDIATELY readable (unexpected!)");
                                ByteBuffer buf = ByteBuffer.allocate(1);
                                int bytesRead = channel.read(buf);
                                System.out.println("   read() returned: " + bytesRead);
                            } else {
                                System.out.println("   Socket is NOT immediately readable (correct)");
                            }
                        }
                    }
                }
            }

            System.out.println("NIO socket test completed");
        } catch (Exception e) {
            System.err.println("NIO socket failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            channel.close();
            selector.close();
        }

        System.out.println("\nAll tests completed");
    }
}
