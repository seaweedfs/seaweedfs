package credential

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ CredentialStore = &PropagatingCredentialStore{}
var _ PolicyManager = &PropagatingCredentialStore{}

type PropagatingCredentialStore struct {
	CredentialStore
	masterClient   *wdclient.MasterClient
	grpcDialOption grpc.DialOption
}

func NewPropagatingCredentialStore(upstream CredentialStore, masterClient *wdclient.MasterClient, grpcDialOption grpc.DialOption) *PropagatingCredentialStore {
	return &PropagatingCredentialStore{
		CredentialStore: upstream,
		masterClient:    masterClient,
		grpcDialOption:  grpcDialOption,
	}
}

func (s *PropagatingCredentialStore) propagateChange(ctx context.Context, fn func(context.Context, s3_pb.SeaweedS3Client) error) {
	if s.masterClient == nil {
		return
	}

	// List S3 servers
	var s3Servers []string
	err := s.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: cluster.S3Type,
			FilerGroup: s.masterClient.FilerGroup,
		})
		if err != nil {
			return err
		}
		for _, node := range resp.ClusterNodes {
			s3Servers = append(s3Servers, node.Address)
		}
		return nil
	})
	if err != nil {
		glog.V(0).Infof("failed to list s3 servers: %v", err)
		return
	}

	// Create context with timeout for the propagation process
	propagateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	// Add propagation metadata to context
	propagateCtx = metadata.AppendToOutgoingContext(propagateCtx, "is-propagation", "true")
	defer cancel()

	var wg sync.WaitGroup
	for _, server := range s3Servers {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			err := pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
				client := s3_pb.NewSeaweedS3Client(conn)
				return fn(propagateCtx, client)
			}, server, false, s.grpcDialOption)
			if err != nil {
				glog.V(0).Infof("failed to propagate change to s3 server %s: %v", server, err)
			}
		}(server)
	}
	wg.Wait()
}

func (s *PropagatingCredentialStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	if err := s.CredentialStore.CreateUser(ctx, identity); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.CreateUser(tx, &iam_pb.CreateUserRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	if err := s.CredentialStore.UpdateUser(ctx, username, identity); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.UpdateUser(tx, &iam_pb.UpdateUserRequest{Username: username, Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteUser(ctx context.Context, username string) error {
	if err := s.CredentialStore.DeleteUser(ctx, username); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.DeleteUser(tx, &iam_pb.DeleteUserRequest{Username: username})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	if err := s.CredentialStore.CreateAccessKey(ctx, username, credential); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.CreateAccessKey(tx, &iam_pb.CreateAccessKeyRequest{Username: username, Credential: credential})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	if err := s.CredentialStore.DeleteAccessKey(ctx, username, accessKey); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.DeleteAccessKey(tx, &iam_pb.DeleteAccessKeyRequest{Username: username, AccessKey: accessKey})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if err := s.CredentialStore.PutPolicy(ctx, name, document); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		content, err := json.Marshal(document)
		if err != nil {
			return err
		}
		_, err = client.PutPolicy(tx, &iam_pb.PutPolicyRequest{Name: name, Content: string(content)})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeletePolicy(ctx context.Context, name string) error {
	if err := s.CredentialStore.DeletePolicy(ctx, name); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.DeletePolicy(tx, &iam_pb.DeletePolicyRequest{Name: name})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if pm, ok := s.CredentialStore.(PolicyManager); ok {
		if err := pm.CreatePolicy(ctx, name, document); err != nil {
			return err
		}
	} else {
		if err := s.CredentialStore.PutPolicy(ctx, name, document); err != nil {
			return err
		}
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		content, err := json.Marshal(document)
		if err != nil {
			return err
		}
		_, err = client.PutPolicy(tx, &iam_pb.PutPolicyRequest{Name: name, Content: string(content)})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	if pm, ok := s.CredentialStore.(PolicyManager); ok {
		if err := pm.UpdatePolicy(ctx, name, document); err != nil {
			return err
		}
	} else {
		if err := s.CredentialStore.PutPolicy(ctx, name, document); err != nil {
			return err
		}
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		content, err := json.Marshal(document)
		if err != nil {
			return err
		}
		_, err = client.PutPolicy(tx, &iam_pb.PutPolicyRequest{Name: name, Content: string(content)})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	if err := s.CredentialStore.CreateServiceAccount(ctx, sa); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.CreateServiceAccount(tx, &iam_pb.CreateServiceAccountRequest{ServiceAccount: sa})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	if err := s.CredentialStore.UpdateServiceAccount(ctx, id, sa); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.UpdateServiceAccount(tx, &iam_pb.UpdateServiceAccountRequest{Id: id, ServiceAccount: sa})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteServiceAccount(ctx context.Context, id string) error {
	if err := s.CredentialStore.DeleteServiceAccount(ctx, id); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3Client) error {
		_, err := client.DeleteServiceAccount(tx, &iam_pb.DeleteServiceAccountRequest{Id: id})
		return err
	})
	return nil
}
