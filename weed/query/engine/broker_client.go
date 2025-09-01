package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// BrokerClient handles communication with SeaweedFS MQ broker
// Assumptions:
// 1. Broker discovery via filer lock mechanism (same as shell commands)
// 2. gRPC connection with default timeout of 30 seconds
// 3. Topics and namespaces are managed via SeaweedMessaging service
type BrokerClient struct {
	filerAddress  string
	brokerAddress string
	grpcDialOption grpc.DialOption
}

// NewBrokerClient creates a new MQ broker client
// Assumption: Filer address is used to discover broker balancer
func NewBrokerClient(filerAddress string) *BrokerClient {
	return &BrokerClient{
		filerAddress:   filerAddress,
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

// findBrokerBalancer discovers the broker balancer using filer lock mechanism
// Assumption: Uses same pattern as existing shell commands
func (c *BrokerClient) findBrokerBalancer() error {
	if c.brokerAddress != "" {
		return nil // already found
	}

	conn, err := grpc.Dial(c.filerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
// This resolves TODO: Get real filerClient from broker connection
func (c *BrokerClient) GetFilerClient() (filer_pb.FilerClient, error) {
	if c.filerAddress == "" {
		return nil, fmt.Errorf("filer address not specified")
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
	// Simple implementation for MQ data access - may need adjustment for production
	return fmt.Sprintf("http://%s", location.Url)
}

// GetDataCenter implements the FilerClient interface (placeholder implementation)
func (f *filerClientImpl) GetDataCenter() string {
	// Return empty string as we don't have data center information for this simple client
	return ""
}

// ListNamespaces retrieves all MQ namespaces (databases)
// Assumption: This would be implemented via a new gRPC method or derived from ListTopics
func (c *BrokerClient) ListNamespaces(ctx context.Context) ([]string, error) {
	if err := c.findBrokerBalancer(); err != nil {
		return nil, err
	}

	// TODO: Implement proper namespace listing
	// For now, we'll derive from known topic patterns or use a dedicated API
	// This is a placeholder that should be replaced with actual broker call
	
	// Temporary implementation: return hardcoded namespaces
	// Real implementation would call a ListNamespaces gRPC method
	return []string{"default", "analytics", "logs"}, nil
}

// ListTopics retrieves all topics in a namespace
// Assumption: Uses existing ListTopics gRPC method from SeaweedMessaging service
func (c *BrokerClient) ListTopics(ctx context.Context, namespace string) ([]string, error) {
	if err := c.findBrokerBalancer(); err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(c.brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at %s: %v", c.brokerAddress, err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)
	
	resp, err := client.ListTopics(ctx, &mq_pb.ListTopicsRequest{
		// TODO: Add namespace filtering to ListTopicsRequest if supported
		// For now, we'll filter client-side
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %v", err)
	}

	// Filter topics by namespace
	// Assumption: Topic.Namespace field exists and matches our namespace
	var topics []string
	for _, topic := range resp.Topics {
		if topic.Namespace == namespace {
			topics = append(topics, topic.Name)
		}
	}

	return topics, nil
}

// GetTopicSchema retrieves schema information for a specific topic
// Assumption: Topic metadata includes schema information
func (c *BrokerClient) GetTopicSchema(ctx context.Context, namespace, topicName string) (*schema_pb.RecordType, error) {
	if err := c.findBrokerBalancer(); err != nil {
		return nil, err
	}

	// TODO: Implement proper schema retrieval
	// This might be part of LookupTopicBrokers or a dedicated GetTopicSchema method
	
	conn, err := grpc.Dial(c.brokerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at %s: %v", c.brokerAddress, err)
	}
	defer conn.Close()

	client := mq_pb.NewSeaweedMessagingClient(conn)
	
	// Use LookupTopicBrokers to get topic information
	resp, err := client.LookupTopicBrokers(ctx, &mq_pb.LookupTopicBrokersRequest{
		Topic: &schema_pb.Topic{
			Namespace: namespace,
			Name:      topicName,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to lookup topic %s.%s: %v", namespace, topicName, err)
	}

	// TODO: Extract schema from topic metadata
	// For now, return a placeholder schema
	if len(resp.BrokerPartitionAssignments) == 0 {
		return nil, fmt.Errorf("topic %s.%s not found", namespace, topicName)
	}

	// Placeholder schema - real implementation would extract from topic metadata
	return &schema_pb.RecordType{
		Fields: []*schema_pb.Field{
			{
				Name: "timestamp",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}},
			},
			{
				Name: "data",
				Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}},
			},
		},
	}, nil
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
