package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// NewSeaweedMQBrokerHandler creates a new handler with SeaweedMQ broker integration
func NewSeaweedMQBrokerHandler(masters string, filerGroup string, clientHost string) (*SeaweedMQHandler, error) {
	if masters == "" {
		return nil, fmt.Errorf("masters required - SeaweedMQ infrastructure must be configured")
	}

	// Parse master addresses using SeaweedFS utilities
	masterServerAddresses := pb.ServerAddresses(masters).ToAddresses()
	if len(masterServerAddresses) == 0 {
		return nil, fmt.Errorf("no valid master addresses provided")
	}

	// Load security configuration for gRPC connections
	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.mq")
	masterDiscovery := pb.ServerAddresses(masters).ToServiceDiscovery()

	// Use provided client host for proper gRPC connection
	// This is critical for MasterClient to establish streaming connections
	clientHostAddr := pb.ServerAddress(clientHost)

	masterClient := wdclient.NewMasterClient(grpcDialOption, filerGroup, "kafka-gateway", clientHostAddr, "", "", *masterDiscovery)

	glog.V(1).Infof("Created MasterClient with clientHost=%s, masters=%s", clientHost, masters)

	// Start KeepConnectedToMaster in background to maintain connection
	glog.V(1).Infof("Starting KeepConnectedToMaster background goroutine...")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		masterClient.KeepConnectedToMaster(ctx)
	}()

	// Give the connection a moment to establish
	time.Sleep(2 * time.Second)
	glog.V(1).Infof("Initial connection delay completed")

	// Discover brokers from masters using master client
	glog.V(1).Infof("About to call discoverBrokersWithMasterClient...")
	brokerAddresses, err := discoverBrokersWithMasterClient(masterClient, filerGroup)
	if err != nil {
		glog.Errorf("Broker discovery failed: %v", err)
		return nil, fmt.Errorf("failed to discover brokers: %v", err)
	}
	glog.V(1).Infof("Broker discovery returned: %v", brokerAddresses)

	if len(brokerAddresses) == 0 {
		return nil, fmt.Errorf("no brokers discovered from masters")
	}

	// Discover filers from masters using master client
	filerAddresses, err := discoverFilersWithMasterClient(masterClient, filerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to discover filers: %v", err)
	}

	// Create shared filer client accessor for all components
	sharedFilerAccessor := filer_client.NewFilerClientAccessor(
		filerAddresses,
		grpcDialOption,
	)

	// For now, use the first broker (can be enhanced later for load balancing)
	brokerAddress := brokerAddresses[0]

	// Create broker client with shared filer accessor
	brokerClient, err := NewBrokerClientWithFilerAccessor(brokerAddress, sharedFilerAccessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %v", err)
	}

	// Test the connection
	if err := brokerClient.HealthCheck(); err != nil {
		brokerClient.Close()
		return nil, fmt.Errorf("broker health check failed: %v", err)
	}

	return &SeaweedMQHandler{
		filerClientAccessor: sharedFilerAccessor,
		brokerClient:        brokerClient,
		masterClient:        masterClient,
		// topics map removed - always read from filer directly
		// ledgers removed - SMQ broker handles all offset management
		brokerAddresses:     brokerAddresses, // Store all discovered broker addresses
		hwmCache:            make(map[string]*hwmCacheEntry),
		hwmCacheTTL:         100 * time.Millisecond, // 100ms cache TTL for fresh HWM reads (critical for Schema Registry)
		topicExistsCache:    make(map[string]*topicExistsCacheEntry),
		topicExistsCacheTTL: 5 * time.Second, // 5 second cache TTL for topic existence
	}, nil
}

// discoverBrokersWithMasterClient queries masters for available brokers using reusable master client
func discoverBrokersWithMasterClient(masterClient *wdclient.MasterClient, filerGroup string) ([]string, error) {
	var brokers []string

	err := masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		glog.V(1).Infof("Inside MasterClient.WithClient callback - client obtained successfully")
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: filerGroup,
			Limit:      1000,
		})
		if err != nil {
			return err
		}

		glog.V(1).Infof("list cluster nodes successful - found %d cluster nodes", len(resp.ClusterNodes))

		// Extract broker addresses from response
		for _, node := range resp.ClusterNodes {
			if node.Address != "" {
				brokers = append(brokers, node.Address)
				glog.V(1).Infof("discovered broker: %s", node.Address)
			}
		}

		return nil
	})

	if err != nil {
		glog.Errorf("MasterClient.WithClient failed: %v", err)
	} else {
		glog.V(1).Infof("Broker discovery completed successfully - found %d brokers: %v", len(brokers), brokers)
	}

	return brokers, err
}

// discoverFilersWithMasterClient queries masters for available filers using reusable master client
func discoverFilersWithMasterClient(masterClient *wdclient.MasterClient, filerGroup string) ([]pb.ServerAddress, error) {
	var filers []pb.ServerAddress

	err := masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.FilerType,
			FilerGroup: filerGroup,
			Limit:      1000,
		})
		if err != nil {
			return err
		}

		// Extract filer addresses from response - return as HTTP addresses (pb.ServerAddress)
		for _, node := range resp.ClusterNodes {
			if node.Address != "" {
				// Return HTTP address as pb.ServerAddress (no pre-conversion to gRPC)
				httpAddr := pb.ServerAddress(node.Address)
				filers = append(filers, httpAddr)
			}
		}

		return nil
	})

	return filers, err
}

// GetFilerClientAccessor returns the shared filer client accessor
func (h *SeaweedMQHandler) GetFilerClientAccessor() *filer_client.FilerClientAccessor {
	return h.filerClientAccessor
}

// SetProtocolHandler sets the protocol handler reference for accessing connection context
func (h *SeaweedMQHandler) SetProtocolHandler(handler ProtocolHandler) {
	h.protocolHandler = handler
}

// GetBrokerAddresses returns the discovered SMQ broker addresses
func (h *SeaweedMQHandler) GetBrokerAddresses() []string {
	return h.brokerAddresses
}

// Close shuts down the handler and all connections
func (h *SeaweedMQHandler) Close() error {
	if h.brokerClient != nil {
		return h.brokerClient.Close()
	}
	return nil
}

// CreatePerConnectionBrokerClient creates a new BrokerClient instance for a specific connection
// CRITICAL: Each Kafka TCP connection gets its own BrokerClient to prevent gRPC stream interference
// This fixes the deadlock where CreateFreshSubscriber would block all connections
func (h *SeaweedMQHandler) CreatePerConnectionBrokerClient() (*BrokerClient, error) {
	// Use the same broker addresses as the shared client
	if len(h.brokerAddresses) == 0 {
		return nil, fmt.Errorf("no broker addresses available")
	}

	// Use the first broker address (in production, could use load balancing)
	brokerAddress := h.brokerAddresses[0]

	// Create a new client with the shared filer accessor
	client, err := NewBrokerClientWithFilerAccessor(brokerAddress, h.filerClientAccessor)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker client: %w", err)
	}

	return client, nil
}
