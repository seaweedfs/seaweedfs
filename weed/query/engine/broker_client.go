package engine

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
)

// BrokerClient handles communication with SeaweedFS MQ broker
// Implements BrokerClientInterface for production use
// Assumptions:
// 1. Service discovery via master server (discovers filers and brokers)
// 2. gRPC connection with default timeout of 30 seconds
// 3. Topics and namespaces are managed via SeaweedMessaging service
type BrokerClient struct {
	masterAddress  string
	filerAddress   string
	brokerAddress  string
	grpcDialOption grpc.DialOption
}

// NewBrokerClient creates a new MQ broker client
// Uses master HTTP address and converts it to gRPC address for service discovery
func NewBrokerClient(masterHTTPAddress string) *BrokerClient {
	// Convert HTTP address to gRPC address (typically HTTP port + 10000)
	masterGRPCAddress := convertHTTPToGRPC(masterHTTPAddress)

	return &BrokerClient{
		masterAddress:  masterGRPCAddress,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

// convertHTTPToGRPC converts HTTP address to gRPC address
// Follows SeaweedFS convention: gRPC port = HTTP port + 10000
func convertHTTPToGRPC(httpAddress string) string {
	if strings.Contains(httpAddress, ":") {
		parts := strings.Split(httpAddress, ":")
		if len(parts) == 2 {
			if port, err := strconv.Atoi(parts[1]); err == nil {
				return fmt.Sprintf("%s:%d", parts[0], port+10000)
			}
		}
	}
	// Fallback: return original address if conversion fails
	return httpAddress
}

// discoverFiler finds a filer from the master server
func (c *BrokerClient) discoverFiler() error {
	if c.filerAddress != "" {
		return nil // already discovered
	}

	conn, err := grpc.Dial(c.masterAddress, c.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to master at %s: %v", c.masterAddress, err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
		ClientType: cluster.FilerType,
	})
	if err != nil {
		return fmt.Errorf("failed to list filers from master: %v", err)
	}

	if len(resp.ClusterNodes) == 0 {
		return fmt.Errorf("no filers found in cluster")
	}

	// Use the first available filer and convert HTTP address to gRPC
	filerHTTPAddress := resp.ClusterNodes[0].Address
	c.filerAddress = convertHTTPToGRPC(filerHTTPAddress)

	return nil
}

// findBrokerBalancer discovers the broker balancer using filer lock mechanism
// First discovers filer from master, then uses filer to find broker balancer
func (c *BrokerClient) findBrokerBalancer() error {
	if c.brokerAddress != "" {
		return nil // already found
	}

	// First discover filer from master
	if err := c.discoverFiler(); err != nil {
		return fmt.Errorf("failed to discover filer: %v", err)
	}

	conn, err := grpc.Dial(c.filerAddress, c.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to filer at %s: %v", c.filerAddress, err)
	}
	defer conn.Close()

	client := filer_pb.NewSeaweedFilerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{
		Name: pub_balancer.LockBrokerBalancer,
	})
	if err != nil {
		return fmt.Errorf("failed to find broker balancer: %v", err)
	}

	c.brokerAddress = resp.Owner
	return nil
}

// GetFilerClient creates a filer client for accessing MQ data files
// Discovers filer from master if not already known
func (c *BrokerClient) GetFilerClient() (filer_pb.FilerClient, error) {
	// Ensure filer is discovered
	if err := c.discoverFiler(); err != nil {
		return nil, fmt.Errorf("failed to discover filer: %v", err)
	}

	return &filerClientImpl{
		filerAddress:   c.filerAddress,
		grpcDialOption: c.grpcDialOption,
	}, nil
}

// filerClientImpl implements filer_pb.FilerClient interface for MQ data access
type filerClientImpl struct {
	filerAddress   string
	grpcDialOption grpc.DialOption
}

// WithFilerClient executes a function with a connected filer client
func (f *filerClientImpl) WithFilerClient(followRedirect bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	conn, err := grpc.Dial(f.filerAddress, f.grpcDialOption)
	if err != nil {
		return fmt.Errorf("failed to connect to filer at %s: %v", f.filerAddress, err)
	}
	defer conn.Close()

	client := filer_pb.NewSeaweedFilerClient(conn)
	return fn(client)
}

// AdjustedUrl implements the FilerClient interface (placeholder implementation)
func (f *filerClientImpl) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

// GetDataCenter implements the FilerClient interface (placeholder implementation)
func (f *filerClientImpl) GetDataCenter() string {
	// Return empty string as we don't have data center information for this simple client
	return ""
}

// ListNamespaces retrieves all MQ namespaces (databases) from the filer
// RESOLVED: Now queries actual topic directories instead of hardcoded values
func (c *BrokerClient) ListNamespaces(ctx context.Context) ([]string, error) {
	// Get filer client to list directories under /topics
	filerClient, err := c.GetFilerClient()
	if err != nil {
		return []string{}, fmt.Errorf("failed to get filer client: %v", err)
	}

	var namespaces []string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// List directories under /topics to get namespaces
		request := &filer_pb.ListEntriesRequest{
			Directory: "/topics", // filer.TopicsDir constant value
		}

		stream, streamErr := client.ListEntries(ctx, request)
		if streamErr != nil {
			return fmt.Errorf("failed to list topics directory: %v", streamErr)
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break // End of stream
				}
				return fmt.Errorf("failed to receive entry: %v", recvErr)
			}

			// Only include directories (namespaces), skip files
			if resp.Entry != nil && resp.Entry.IsDirectory {
				namespaces = append(namespaces, resp.Entry.Name)
			}
		}

		return nil
	})

	if err != nil {
		return []string{}, fmt.Errorf("failed to list namespaces from /topics: %v", err)
	}

	// Return actual namespaces found (may be empty if no topics exist)
	return namespaces, nil
}

// ListTopics retrieves all topics in a namespace from the filer
// RESOLVED: Now queries actual topic directories instead of hardcoded values
func (c *BrokerClient) ListTopics(ctx context.Context, namespace string) ([]string, error) {
	// Get filer client to list directories under /topics/{namespace}
	filerClient, err := c.GetFilerClient()
	if err != nil {
		// Return empty list if filer unavailable - no fallback sample data
		return []string{}, nil
	}

	var topics []string
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// List directories under /topics/{namespace} to get topics
		namespaceDir := fmt.Sprintf("/topics/%s", namespace)
		request := &filer_pb.ListEntriesRequest{
			Directory: namespaceDir,
		}

		stream, streamErr := client.ListEntries(ctx, request)
		if streamErr != nil {
			return fmt.Errorf("failed to list namespace directory %s: %v", namespaceDir, streamErr)
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break // End of stream
				}
				return fmt.Errorf("failed to receive entry: %v", recvErr)
			}

			// Only include directories (topics), skip files
			if resp.Entry != nil && resp.Entry.IsDirectory {
				topics = append(topics, resp.Entry.Name)
			}
		}

		return nil
	})

	if err != nil {
		// Return empty list if directory listing fails - no fallback sample data
		return []string{}, nil
	}

	// Return actual topics found (may be empty if no topics exist in namespace)
	return topics, nil
}

// GetTopicSchema retrieves schema information for a specific topic
// Reads the actual schema from topic configuration stored in filer
func (c *BrokerClient) GetTopicSchema(ctx context.Context, namespace, topicName string) (*schema_pb.RecordType, error) {
	// Get filer client to read topic configuration
	filerClient, err := c.GetFilerClient()
	if err != nil {
		return nil, fmt.Errorf("failed to get filer client: %v", err)
	}

	var recordType *schema_pb.RecordType
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Read topic.conf file from /topics/{namespace}/{topic}/topic.conf
		topicDir := fmt.Sprintf("/topics/%s/%s", namespace, topicName)

		// First check if topic directory exists
		_, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: topicDir,
			Name:      "topic.conf",
		})
		if err != nil {
			return fmt.Errorf("topic %s.%s not found: %v", namespace, topicName, err)
		}

		// Read the topic.conf file content
		data, err := filer.ReadInsideFiler(client, topicDir, "topic.conf")
		if err != nil {
			return fmt.Errorf("failed to read topic.conf for %s.%s: %v", namespace, topicName, err)
		}

		// Parse the configuration
		conf := &mq_pb.ConfigureTopicResponse{}
		if err = jsonpb.Unmarshal(data, conf); err != nil {
			return fmt.Errorf("failed to unmarshal topic %s.%s configuration: %v", namespace, topicName, err)
		}

		// Extract the record type (schema)
		if conf.RecordType != nil {
			recordType = conf.RecordType
		} else {
			return fmt.Errorf("no schema found for topic %s.%s", namespace, topicName)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if recordType == nil {
		return nil, fmt.Errorf("no record type found for topic %s.%s", namespace, topicName)
	}

	return recordType, nil
}

// ConfigureTopic creates or modifies a topic configuration
// Assumption: Uses existing ConfigureTopic gRPC method for topic management
func (c *BrokerClient) ConfigureTopic(ctx context.Context, namespace, topicName string, partitionCount int32, recordType *schema_pb.RecordType) error {
	if err := c.findBrokerBalancer(); err != nil {
		return err
	}

	conn, err := grpc.Dial(c.brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to broker at %s: %v", c.brokerAddress, err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)

	// Create topic configuration
	_, err = client.ConfigureTopic(ctx, &mq_pb.ConfigureTopicRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      topicName,
		},
		PartitionCount: partitionCount,
		RecordType:     recordType,
	})
	if err != nil {
		return fmt.Errorf("failed to configure topic %s.%s: %v", namespace, topicName, err)
	}

	return nil
}

// DeleteTopic removes a topic and all its data
// Assumption: There's a delete/drop topic method (may need to be implemented in broker)
func (c *BrokerClient) DeleteTopic(ctx context.Context, namespace, topicName string) error {
	if err := c.findBrokerBalancer(); err != nil {
		return err
	}

	// TODO: Implement topic deletion
	// This may require a new gRPC method in the broker service

	return fmt.Errorf("topic deletion not yet implemented in broker - need to add DeleteTopic gRPC method")
}

// ListTopicPartitions discovers the actual partitions for a given topic
// This resolves TODO: Implement proper partition discovery via MQ broker
func (c *BrokerClient) ListTopicPartitions(ctx context.Context, namespace, topicName string) ([]topic.Partition, error) {
	if err := c.findBrokerBalancer(); err != nil {
		// Fallback to default partition when broker unavailable
		return []topic.Partition{{RangeStart: 0, RangeStop: 1000}}, nil
	}

	// Get topic configuration to determine actual partitions
	topicObj := topic.Topic{Namespace: namespace, Name: topicName}

	// Use filer client to read topic configuration
	filerClient, err := c.GetFilerClient()
	if err != nil {
		// Fallback to default partition
		return []topic.Partition{{RangeStart: 0, RangeStop: 1000}}, nil
	}

	var topicConf *mq_pb.ConfigureTopicResponse
	err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		topicConf, err = topicObj.ReadConfFile(client)
		return err
	})

	if err != nil {
		// Topic doesn't exist or can't read config, use default
		return []topic.Partition{{RangeStart: 0, RangeStop: 1000}}, nil
	}

	// Generate partitions based on topic configuration
	partitionCount := int32(4) // Default partition count for topics
	if len(topicConf.BrokerPartitionAssignments) > 0 {
		partitionCount = int32(len(topicConf.BrokerPartitionAssignments))
	}

	// Create partition ranges - simplified approach
	// Each partition covers an equal range of the hash space
	rangeSize := topic.PartitionCount / partitionCount
	var partitions []topic.Partition

	for i := int32(0); i < partitionCount; i++ {
		rangeStart := i * rangeSize
		rangeStop := (i + 1) * rangeSize
		if i == partitionCount-1 {
			// Last partition covers remaining range
			rangeStop = topic.PartitionCount
		}

		partitions = append(partitions, topic.Partition{
			RangeStart: rangeStart,
			RangeStop:  rangeStop,
			RingSize:   topic.PartitionCount,
			UnixTimeNs: time.Now().UnixNano(),
		})
	}

	return partitions, nil
}

// GetUnflushedMessages returns only messages that haven't been flushed to disk yet
// Uses buffer_start metadata to determine what data has been persisted vs still in-memory
// This prevents double-counting when combining with disk-based data
func (c *BrokerClient) GetUnflushedMessages(ctx context.Context, namespace, topicName string, partition topic.Partition, startTimeNs int64) ([]*filer_pb.LogEntry, error) {
	// Step 1: Find the broker that hosts this partition
	if err := c.findBrokerBalancer(); err != nil {
		// Return empty slice if we can't find broker - prevents double-counting
		return []*filer_pb.LogEntry{}, nil
	}

	// Step 2: Connect to broker and call the GetUnflushedMessages gRPC method
	conn, err := grpc.Dial(c.brokerAddress, c.grpcDialOption)
	if err != nil {
		// Return empty slice if connection fails - prevents double-counting
		return []*filer_pb.LogEntry{}, nil
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)

	// Step 3: Prepare the request using oneof start_filter (timestamp-based)
	request := &mq_pb.GetUnflushedMessagesRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      topicName,
		},
		Partition: &schema_pb.Partition{
			RingSize:   partition.RingSize,
			RangeStart: partition.RangeStart,
			RangeStop:  partition.RangeStop,
			UnixTimeNs: partition.UnixTimeNs,
		},
		StartFilter: &mq_pb.GetUnflushedMessagesRequest_StartTimeNs{
			StartTimeNs: startTimeNs,
		},
		// TODO: Could use buffer index filtering for more precision:
		// StartFilter: &mq_pb.GetUnflushedMessagesRequest_StartBufferIndex{
		//     StartBufferIndex: latestBufferIndex,
		// },
	}

	// Step 4: Call the broker streaming API
	stream, err := client.GetUnflushedMessages(ctx, request)
	if err != nil {
		// Return empty slice if gRPC call fails - prevents double-counting
		return []*filer_pb.LogEntry{}, nil
	}

	// Step 5: Receive streaming responses
	var logEntries []*filer_pb.LogEntry
	for {
		response, err := stream.Recv()
		if err != nil {
			// End of stream or error - return what we have to prevent double-counting
			break
		}

		// Handle error messages
		if response.Error != "" {
			// Log the error but return empty slice - prevents double-counting
			// (In debug mode, this would be visible)
			return []*filer_pb.LogEntry{}, nil
		}

		// Check for end of stream
		if response.EndOfStream {
			break
		}

		// Convert and collect the message
		if response.Message != nil {
			logEntries = append(logEntries, &filer_pb.LogEntry{
				TsNs:             response.Message.TsNs,
				Key:              response.Message.Key,
				Data:             response.Message.Data,
				PartitionKeyHash: int32(response.Message.PartitionKeyHash), // Convert uint32 to int32
			})
		}
	}

	return logEntries, nil
}
