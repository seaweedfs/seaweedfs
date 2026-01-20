package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	. "github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

type IamS3ApiConfig interface {
	GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	GetPolicies(policies *Policies) (err error)
	GetPolicy(name string) (policy *policy_engine.PolicyDocument, err error)
	PutPolicy(name string, policy *policy_engine.PolicyDocument) (err error)
	DeletePolicy(name string) (err error)
	CreateUser(user *iam_pb.Identity) (err error)
	GetUser(name string) (user *iam_pb.Identity, err error)
	UpdateUser(name string, user *iam_pb.Identity) (err error)
	DeleteUser(name string) (err error)
	ListUsers() (usernames []string, err error)
	CreateAccessKey(username string, cred *iam_pb.Credential) (err error)
	DeleteAccessKey(username string, accessKey string) (err error)
}

type IamS3ApiConfigure struct {
	option            *IamServerOption
	masterClient      *wdclient.MasterClient
	credentialManager *credential.CredentialManager
}

type IamServerOption struct {
	Masters        map[string]pb.ServerAddress
	Filers         []pb.ServerAddress
	Port           int
	GrpcDialOption grpc.DialOption
}

type IamApiServer struct {
	s3ApiConfig      IamS3ApiConfig
	iam              *s3api.IdentityAccessManagement
	shutdownContext  context.Context
	shutdownCancel   context.CancelFunc
	masterClient     *wdclient.MasterClient
}

var s3ApiConfigure IamS3ApiConfig

func NewIamApiServer(router *mux.Router, option *IamServerOption) (iamApiServer *IamApiServer, err error) {
	return NewIamApiServerWithStore(router, option, "")
}

func NewIamApiServerWithStore(router *mux.Router, option *IamServerOption, explicitStore string) (iamApiServer *IamApiServer, err error) {
	if len(option.Filers) == 0 {
		return nil, fmt.Errorf("at least one filer address is required")
	}
	
	masterClient := wdclient.NewMasterClient(option.GrpcDialOption, "", "iam", "", "", "", *pb.NewServiceDiscoveryFromMap(option.Masters))
	
	// Create a cancellable context for the master client connection
	// This allows graceful shutdown via Shutdown() method
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	
	// Start KeepConnectedToMaster for volume location lookups
	// IAM config files are typically small and inline, but if they ever have chunks,
	// ReadEntry→StreamContent needs masterClient for volume lookups
	glog.V(0).Infof("IAM API starting master client connection for volume location lookups")
	go masterClient.KeepConnectedToMaster(shutdownCtx)
	
	configure := &IamS3ApiConfigure{
		option:       option,
		masterClient: masterClient,
	}

	s3ApiConfigure = configure

	s3Option := s3api.S3ApiServerOption{
		Filers:         option.Filers,
		GrpcDialOption: option.GrpcDialOption,
	}

	iam := s3api.NewIdentityAccessManagementWithStore(&s3Option, explicitStore)
	configure.credentialManager = iam.GetCredentialManager()

	iamApiServer = &IamApiServer{
		s3ApiConfig:     s3ApiConfigure,
		iam:             iam,
		shutdownContext: shutdownCtx,
		shutdownCancel:  shutdownCancel,
		masterClient:    masterClient,
	}

	iamApiServer.registerRouter(router)

	return iamApiServer, nil
}

func (iama *IamApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	// ListBuckets

	// apiRouter.Methods("GET").Path("/").HandlerFunc(track(s3a.iam.Auth(s3a.ListBucketsHandler, ACTION_ADMIN), "LIST"))
	apiRouter.Methods(http.MethodPost).Path("/").HandlerFunc(iama.iam.Auth(iama.DoActions, ACTION_ADMIN))
	//
	// NotFound
	apiRouter.NotFoundHandler = http.HandlerFunc(s3err.NotFoundHandler)
}

// Shutdown gracefully stops the IAM API server and releases resources.
// It cancels the master client connection goroutine and closes gRPC connections.
// This method is safe to call multiple times.
//
// Note: This method is called via defer in weed/command/iam.go for best-effort cleanup.
// For proper graceful shutdown on SIGTERM/SIGINT, signal handling should be added to
// the command layer to call this method before process exit.
func (iama *IamApiServer) Shutdown() {
	if iama.shutdownCancel != nil {
		glog.V(0).Infof("IAM API server shutting down, stopping master client connection")
		iama.shutdownCancel()
	}
}

func (iama *IamS3ApiConfigure) GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.GetS3ApiConfigurationFromCredentialManager(s3cfg)
}

func (iama *IamS3ApiConfigure) PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.PutS3ApiConfigurationToCredentialManager(s3cfg)
}

func (iama *IamS3ApiConfigure) GetS3ApiConfigurationFromCredentialManager(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	config, err := iama.credentialManager.LoadConfiguration(context.Background())
	if err != nil {
		return fmt.Errorf("failed to load configuration from credential manager: %w", err)
	}
	*s3cfg = *config
	return nil
}

func (iama *IamS3ApiConfigure) PutS3ApiConfigurationToCredentialManager(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	return iama.credentialManager.SaveConfiguration(context.Background(), s3cfg)
}

func (iama *IamS3ApiConfigure) GetS3ApiConfigurationFromFiler(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	var buf bytes.Buffer
	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err = filer.ReadEntry(iama.masterClient, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if buf.Len() > 0 {
		if err = filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg); err != nil {
			return err
		}
	}
	return nil
}

func (iama *IamS3ApiConfigure) PutS3ApiConfigurationToFiler(s3cfg *iam_pb.S3ApiConfiguration) (err error) {
	buf := bytes.Buffer{}
	if err := filer.ProtoToText(&buf, s3cfg); err != nil {
		return fmt.Errorf("ProtoToText: %s", err)
	}
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		err = util.Retry("saveIamIdentity", func() error {
			return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) GetPolicies(policies *Policies) (err error) {
	policies.Policies = make(map[string]policy_engine.PolicyDocument)

	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// List files in the policies directory
		entries, err := filer.ListEntry(iama.masterClient, client, filer.IamPoliciesDirectory, "", 100000, "")
		if err != nil {
			if err == filer_pb.ErrNotFound {
				// Directory doesn't exist, which is fine = empty policies
				return nil
			}
			return err
		}

		// Read each file
		for _, entry := range entries {
			if entry.IsDirectory {
				continue
			}
			if !strings.HasSuffix(entry.Name, ".json") {
				continue
			}

			// Read content
			content, err := filer.ReadInsideFiler(client, filer.IamPoliciesDirectory, entry.Name)
			if err != nil {
				glog.Warningf("Failed to read policy file %s: %v", entry.Name, err)
				continue
			}

			if len(content) == 0 {
				continue
			}

			var policyDoc policy_engine.PolicyDocument
			if err := json.Unmarshal(content, &policyDoc); err != nil {
				glog.Warningf("Failed to parse policy file %s: %v", entry.Name, err)
				continue
			}
			
			// Use filename as policy name (without extension) if name not in metadata
			policyName := strings.TrimSuffix(entry.Name, ".json")
			policies.Policies[policyName] = policyDoc
		}
		return nil
	})
	return err
}

func (iama *IamS3ApiConfigure) GetPolicy(name string) (policy *policy_engine.PolicyDocument, err error) {
	filename := fmt.Sprintf("%s.json", name)
	var policyDoc policy_engine.PolicyDocument
	err = pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		content, err := filer.ReadInsideFiler(client, filer.IamPoliciesDirectory, filename)
		if err != nil {
			return err
		}
		if len(content) == 0 {
			return filer_pb.ErrNotFound
		}
		return json.Unmarshal(content, &policyDoc)
	})
	if err != nil {
		return nil, err
	}
	return &policyDoc, nil
}

func (iama *IamS3ApiConfigure) PutPolicy(name string, policy *policy_engine.PolicyDocument) (err error) {
	var b []byte
	if b, err = json.Marshal(policy); err != nil {
		return err
	}
	filename := fmt.Sprintf("%s.json", name)
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		if err := filer.SaveInsideFiler(client, filer.IamPoliciesDirectory, filename, b); err != nil {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) DeletePolicy(name string) (err error) {
	filename := fmt.Sprintf("%s.json", name)
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		// Check existence first to match AWS behavior (optional but good practice)
		// But deletion is idempotent usually, so we just try to delete
		err = filer.DeleteInsideFiler(client, filer.IamPoliciesDirectory, filename)
		if err != nil && err != filer_pb.ErrNotFound {
			return err
		}
		return nil
	})
}

func (iama *IamS3ApiConfigure) CreateUser(user *iam_pb.Identity) (err error) {
	return iama.credentialManager.CreateUser(context.Background(), user)
}

func (iama *IamS3ApiConfigure) GetUser(name string) (user *iam_pb.Identity, err error) {
	return iama.credentialManager.GetUser(context.Background(), name)
}

func (iama *IamS3ApiConfigure) UpdateUser(name string, user *iam_pb.Identity) (err error) {
	return iama.credentialManager.UpdateUser(context.Background(), name, user)
}

func (iama *IamS3ApiConfigure) DeleteUser(name string) (err error) {
	return iama.credentialManager.DeleteUser(context.Background(), name)
}

func (iama *IamS3ApiConfigure) ListUsers() (usernames []string, err error) {
	return iama.credentialManager.ListUsers(context.Background())
}

func (iama *IamS3ApiConfigure) CreateAccessKey(username string, cred *iam_pb.Credential) (err error) {
	return iama.credentialManager.CreateAccessKey(context.Background(), username, cred)
}

func (iama *IamS3ApiConfigure) DeleteAccessKey(username string, accessKey string) (err error) {
	return iama.credentialManager.DeleteAccessKey(context.Background(), username, accessKey)
}
