import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientDebugger {

    public static void main(String[] args) throws Exception {
        String broker = args.length > 0 ? args[0] : "localhost:9093";

        System.out.println("=".repeat(80));
        System.out.println("KAFKA ADMINCLIENT DEBUGGER");
        System.out.println("=".repeat(80));
        System.out.println("Target broker: " + broker);

        // Test 1: Raw socket - capture exact bytes
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 1: Raw Socket - Capture ApiVersions Exchange");
        System.out.println("=".repeat(80));
        testRawSocket(broker);

        // Test 2: AdminClient with detailed logging
        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST 2: AdminClient with Logging");
        System.out.println("=".repeat(80));
        testAdminClient(broker);
    }

    private static void testRawSocket(String broker) {
        String[] parts = broker.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(10000);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            System.out.println("Connected to " + broker);

            // Build ApiVersions request (v4)
            // Format:
            // [Size][ApiKey=18][ApiVersion=4][CorrelationId=0][ClientId][TaggedFields]
            ByteArrayOutputStream requestBody = new ByteArrayOutputStream();

            // ApiKey (2 bytes) = 18
            requestBody.write(0);
            requestBody.write(18);

            // ApiVersion (2 bytes) = 4
            requestBody.write(0);
            requestBody.write(4);

            // CorrelationId (4 bytes) = 0
            requestBody.write(new byte[] { 0, 0, 0, 0 });

            // ClientId (compact string) = "debug-client"
            String clientId = "debug-client";
            writeCompactString(requestBody, clientId);

            // Tagged fields (empty)
            requestBody.write(0x00);

            byte[] request = requestBody.toByteArray();

            // Write size
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
            sizeBuffer.putInt(request.length);
            out.write(sizeBuffer.array());

            // Write request
            out.write(request);
            out.flush();

            System.out.println("\nSENT ApiVersions v4 Request:");
            System.out.println("   Size: " + request.length + " bytes");
            hexDump("   Request", request, Math.min(64, request.length));

            // Read response size
            byte[] sizeBytes = new byte[4];
            int read = in.read(sizeBytes);
            if (read != 4) {
                System.out.println("Failed to read response size (got " + read + " bytes)");
                return;
            }

            int responseSize = ByteBuffer.wrap(sizeBytes).getInt();
            System.out.println("\nRECEIVED Response:");
            System.out.println("   Size: " + responseSize + " bytes");

            // Read response body
            byte[] responseBytes = new byte[responseSize];
            int totalRead = 0;
            while (totalRead < responseSize) {
                int n = in.read(responseBytes, totalRead, responseSize - totalRead);
                if (n == -1) {
                    System.out.println("Unexpected EOF after " + totalRead + " bytes");
                    return;
                }
                totalRead += n;
            }

            System.out.println("   Read complete response: " + totalRead + " bytes");

            // Decode response
            System.out.println("\nRESPONSE STRUCTURE:");
            decodeApiVersionsResponse(responseBytes);

            // Try to read more (should timeout or get EOF)
            System.out.println("\n⏱️  Waiting for any additional data (10s timeout)...");
            socket.setSoTimeout(10000);
            try {
                int nextByte = in.read();
                if (nextByte == -1) {
                    System.out.println("   Server closed connection (EOF)");
                } else {
                    System.out.println("   Unexpected data: " + nextByte);
                }
            } catch (SocketTimeoutException e) {
                System.out.println("   Timeout - no additional data");
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testAdminClient(String broker) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-client-debugger");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);

        System.out.println("Creating AdminClient with config:");
        props.forEach((k, v) -> System.out.println("  " + k + " = " + v));

        try (AdminClient adminClient = AdminClient.create(props)) {
            System.out.println("AdminClient created");

            // Give the thread time to start
            Thread.sleep(1000);

            System.out.println("\nCalling describeCluster()...");
            DescribeClusterResult result = adminClient.describeCluster();

            System.out.println("   Waiting for nodes...");
            Collection<Node> nodes = result.nodes().get();

            System.out.println("Cluster description retrieved:");
            System.out.println("   Nodes: " + nodes.size());
            for (Node node : nodes) {
                System.out.println("     - Node " + node.id() + ": " + node.host() + ":" + node.port());
            }

            System.out.println("\n   Cluster ID: " + result.clusterId().get());

            Node controller = result.controller().get();
            if (controller != null) {
                System.out.println("   Controller: Node " + controller.id());
            }

        } catch (ExecutionException e) {
            System.out.println("Execution error: " + e.getCause().getMessage());
            e.getCause().printStackTrace();
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void decodeApiVersionsResponse(byte[] data) {
        int offset = 0;

        try {
            // Correlation ID (4 bytes)
            int correlationId = ByteBuffer.wrap(data, offset, 4).getInt();
            System.out.println("   [Offset " + offset + "] Correlation ID: " + correlationId);
            offset += 4;

            // Header tagged fields (varint - should be 0x00 for flexible v3+)
            int taggedFieldsLength = readUnsignedVarint(data, offset);
            System.out.println("   [Offset " + offset + "] Header Tagged Fields Length: " + taggedFieldsLength);
            offset += varintSize(data[offset]);

            // Error code (2 bytes)
            short errorCode = ByteBuffer.wrap(data, offset, 2).getShort();
            System.out.println("   [Offset " + offset + "] Error Code: " + errorCode);
            offset += 2;

            // API Keys array (compact array - varint length)
            int apiKeysLength = readUnsignedVarint(data, offset) - 1; // Compact array: length+1
            System.out.println("   [Offset " + offset + "] API Keys Count: " + apiKeysLength);
            offset += varintSize(data[offset]);

            // Show first few API keys
            System.out.println("   First 5 API Keys:");
            for (int i = 0; i < Math.min(5, apiKeysLength); i++) {
                short apiKey = ByteBuffer.wrap(data, offset, 2).getShort();
                offset += 2;
                short minVersion = ByteBuffer.wrap(data, offset, 2).getShort();
                offset += 2;
                short maxVersion = ByteBuffer.wrap(data, offset, 2).getShort();
                offset += 2;
                // Per-element tagged fields
                int perElementTagged = readUnsignedVarint(data, offset);
                offset += varintSize(data[offset]);

                System.out.println("     " + (i + 1) + ". API " + apiKey + ": v" + minVersion + "-v" + maxVersion);
            }

            System.out.println("   ... (showing first 5 of " + apiKeysLength + " APIs)");
            System.out.println("   Response structure is valid!");

            // Hex dump of first 64 bytes
            hexDump("\n   First 64 bytes", data, Math.min(64, data.length));

        } catch (Exception e) {
            System.out.println("   Failed to decode at offset " + offset + ": " + e.getMessage());
            hexDump("   Raw bytes", data, Math.min(128, data.length));
        }
    }

    private static int readUnsignedVarint(byte[] data, int offset) {
        int value = 0;
        int shift = 0;
        while (true) {
            byte b = data[offset++];
            value |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                break;
            shift += 7;
        }
        return value;
    }

    private static int varintSize(byte firstByte) {
        int size = 1;
        byte b = firstByte;
        while ((b & 0x80) != 0) {
            size++;
            b = (byte) (b << 1);
        }
        return size;
    }

    private static void writeCompactString(ByteArrayOutputStream out, String str) {
        byte[] bytes = str.getBytes();
        writeUnsignedVarint(out, bytes.length + 1); // Compact string: length+1
        out.write(bytes, 0, bytes.length);
    }

    private static void writeUnsignedVarint(ByteArrayOutputStream out, int value) {
        while ((value & ~0x7F) != 0) {
            out.write((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((byte) value);
    }

    private static void hexDump(String label, byte[] data, int length) {
        System.out.println(label + " (hex dump):");
        for (int i = 0; i < length; i += 16) {
            System.out.printf("      %04x  ", i);
            for (int j = 0; j < 16; j++) {
                if (i + j < length) {
                    System.out.printf("%02x ", data[i + j] & 0xFF);
                } else {
                    System.out.print("   ");
                }
                if (j == 7)
                    System.out.print(" ");
            }
            System.out.print(" |");
            for (int j = 0; j < 16 && i + j < length; j++) {
                byte b = data[i + j];
                System.out.print((b >= 32 && b < 127) ? (char) b : '.');
            }
            System.out.println("|");
        }
    }
}
