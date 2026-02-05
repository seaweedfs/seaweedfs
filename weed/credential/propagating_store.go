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

func (s *PropagatingCredentialStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	if setter, ok := s.CredentialStore.(FilerAddressSetter); ok {
		setter.SetFilerAddressFunc(getFiler, grpcDialOption)
	}
}

func (s *PropagatingCredentialStore) propagateChange(ctx context.Context, fn func(context.Context, s3_pb.SeaweedS3IamCacheClient) error) {
	if s.masterClient == nil {
		return
	}

	// List S3 servers
	var s3Servers []string
	err := s.masterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		glog.V(4).Infof("IAM: listing S3 servers (FilerGroup: '%s')", s.masterClient.FilerGroup)
		resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: cluster.S3Type,
			FilerGroup: s.masterClient.FilerGroup,
		})
		if err != nil {
			glog.V(1).Infof("failed to list S3 servers: %v", err)
			return err
		}
		for _, node := range resp.ClusterNodes {
			s3Servers = append(s3Servers, node.Address)
		}

		return nil
	})
	if err != nil {
		glog.V(1).Infof("failed to list s3 servers via master client: %v", err)
		return
	}
	glog.V(1).Infof("IAM: propagating change to %d S3 servers: %v", len(s3Servers), s3Servers)

	// Create context with timeout for the propagation process
	propagateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, server := range s3Servers {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			err := pb.WithGrpcClient(false, 0, func(conn *grpc.ClientConn) error {
				glog.V(4).Infof("IAM: successfully connected to S3 server %s for propagation", server)
				client := s3_pb.NewSeaweedS3IamCacheClient(conn)
				return fn(propagateCtx, client)
			}, server, false, s.grpcDialOption)
			if err != nil {
				glog.V(1).Infof("failed to propagate change to s3 server %s: %v", server, err)
			}
		}(server)
	}
	wg.Wait()
}

func (s *PropagatingCredentialStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.CreateUser %s", identity.Name)
	if err := s.CredentialStore.CreateUser(ctx, identity); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.UpdateUser %s", username)
	if err := s.CredentialStore.UpdateUser(ctx, username, identity); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		if _, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity}); err != nil {
			return err
		}
		if username != identity.Name {
			if _, err := client.RemoveIdentity(tx, &iam_pb.RemoveIdentityRequest{Username: username}); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteUser(ctx context.Context, username string) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.DeleteUser %s", username)
	if err := s.CredentialStore.DeleteUser(ctx, username); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.RemoveIdentity(tx, &iam_pb.RemoveIdentityRequest{Username: username})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error {
	if err := s.CredentialStore.CreateAccessKey(ctx, username, credential); err != nil {
		return err
	}
	// Fetch updated identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, username)
	if err != nil {
		glog.Warningf("failed to get user %s after creating access key: %v", username, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteAccessKey(ctx context.Context, username string, accessKey string) error {
	if err := s.CredentialStore.DeleteAccessKey(ctx, username, accessKey); err != nil {
		return err
	}
	// Fetch updated identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, username)
	if err != nil {
		glog.Warningf("failed to get user %s after deleting access key: %v", username, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.PutPolicy %s", name)
	if err := s.CredentialStore.PutPolicy(ctx, name, document); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
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
	glog.V(4).Infof("IAM: PropagatingCredentialStore.DeletePolicy %s", name)
	if err := s.CredentialStore.DeletePolicy(ctx, name); err != nil {
		return err
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
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
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
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
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
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
	glog.V(4).Infof("IAM: PropagatingCredentialStore.CreateServiceAccount %s (parent: %s)", sa.Id, sa.ParentUser)
	if err := s.CredentialStore.CreateServiceAccount(ctx, sa); err != nil {
		return err
	}
	// Fetch parent identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, sa.ParentUser)
	if err != nil {
		glog.Warningf("failed to get parent user %s after creating service account: %v", sa.ParentUser, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	if err := s.CredentialStore.UpdateServiceAccount(ctx, id, sa); err != nil {
		return err
	}
	// Fetch parent identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, sa.ParentUser)
	if err != nil {
		glog.Warningf("failed to get parent user %s after updating service account: %v", sa.ParentUser, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DeleteServiceAccount(ctx context.Context, id string) error {
	// Retrieve SA first to get ParentUser
	sa, err := s.CredentialStore.GetServiceAccount(ctx, id)
	if err != nil {
		// If accessing non-existent SA, just proceed to delete (idempotency)
		// But we can't propagate to parent...
		if err := s.CredentialStore.DeleteServiceAccount(ctx, id); err != nil {
			return err
		}
		return nil
	}

	if err := s.CredentialStore.DeleteServiceAccount(ctx, id); err != nil {
		return err
	}

	// Fetch parent identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, sa.ParentUser)
	if err != nil {
		glog.Warningf("failed to get parent user %s after deleting service account: %v", sa.ParentUser, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) AttachUserPolicy(ctx context.Context, username string, policyName string) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.AttachUserPolicy %s -> %s", username, policyName)
	if err := s.CredentialStore.AttachUserPolicy(ctx, username, policyName); err != nil {
		return err
	}
	// Fetch updated identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, username)
	if err != nil {
		glog.Warningf("failed to get user %s after attaching policy: %v", username, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) DetachUserPolicy(ctx context.Context, username string, policyName string) error {
	glog.V(4).Infof("IAM: PropagatingCredentialStore.DetachUserPolicy %s -> %s", username, policyName)
	if err := s.CredentialStore.DetachUserPolicy(ctx, username, policyName); err != nil {
		return err
	}
	// Fetch updated identity to propagate
	identity, err := s.CredentialStore.GetUser(ctx, username)
	if err != nil {
		glog.Warningf("failed to get user %s after detaching policy: %v", username, err)
		return nil
	}
	s.propagateChange(ctx, func(tx context.Context, client s3_pb.SeaweedS3IamCacheClient) error {
		_, err := client.PutIdentity(tx, &iam_pb.PutIdentityRequest{Identity: identity})
		return err
	})
	return nil
}

func (s *PropagatingCredentialStore) ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error) {
	return s.CredentialStore.ListAttachedUserPolicies(ctx, username)
}
