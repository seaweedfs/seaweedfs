package iamapi

// https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

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
	"github.com/seaweedfs/seaweedfs/weed/util/request_id"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type IamS3ApiConfig interface {
	GetS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	PutS3ApiConfiguration(s3cfg *iam_pb.S3ApiConfiguration) (err error)
	GetPolicies(policies *Policies) (err error)
	PutPolicies(policies *Policies) (err error)
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
	s3ApiConfig     IamS3ApiConfig
	iam             *s3api.IdentityAccessManagement
	shutdownContext context.Context
	shutdownCancel  context.CancelFunc
	masterClient    *wdclient.MasterClient
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

	// Initialize FilerClient for IAM - explicit filers only (no discovery as FilerGroup unspecified)
	filerClient := wdclient.NewFilerClient(option.Filers, option.GrpcDialOption, "")
	iam := s3api.NewIdentityAccessManagementWithStore(&s3Option, filerClient, explicitStore)
	configure.credentialManager = iam.GetCredentialManager()

	iamApiServer = &IamApiServer{
		s3ApiConfig:     s3ApiConfigure,
		iam:             iam,
		shutdownContext: shutdownCtx,
		shutdownCancel:  shutdownCancel,
		masterClient:    masterClient,
	}

	// Keep attempting to load configuration from filer now that we have a client
	go func() {
		if err := iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
			glog.Warningf("Failed to load IAM config from credential manager after client update: %v", err)
		}
	}()

	iamApiServer.registerRouter(router)

	return iamApiServer, nil
}

func (iama *IamApiServer) registerRouter(router *mux.Router) {
	// API Router
	apiRouter := router.PathPrefix("/").Subrouter()
	apiRouter.Use(request_id.Middleware)
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
	if iama.iam != nil {
		iama.iam.Shutdown()
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
	// Use proto.Merge to avoid copying the sync.Mutex embedded in the message
	proto.Merge(s3cfg, config)
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
			return filer.SaveInsideFiler(context.Background(), client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
		})
		if err != nil {
			return err
		}
		return nil
	})
}

// GetPolicies returns the merged view of managed policies (from the configured
// credential store, so postgres/grpc/filer_etc are all honored), plus the
// legacy filer-side inline and group-inline policies. Reading managed policies
// from the credential manager keeps the IAM API in sync with the Admin UI,
// which writes through the same store. The legacy policies.json file is still
// consulted as a fallback so existing deployments don't lose managed policies
// that were written before this change.
func (iama *IamS3ApiConfigure) GetPolicies(policies *Policies) (err error) {
	ctx := context.Background()

	if policies.Policies == nil {
		policies.Policies = make(map[string]policy_engine.PolicyDocument)
	}

	// Managed policies from the credential store (canonical source).
	if iama.credentialManager != nil {
		managed, mErr := iama.credentialManager.GetPolicies(ctx)
		if mErr != nil {
			return fmt.Errorf("load managed policies from credential store: %w", mErr)
		}
		for name, doc := range managed {
			policies.Policies[name] = doc
		}
	}

	// Inline (user/group) policies still live in the legacy policies.json
	// file on the filer. Legacy managed policies stored there before this
	// change are merged in as a fallback (credential-store wins on conflict).
	var buf bytes.Buffer
	rErr := pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer.ReadEntry(iama.masterClient, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf)
	})
	if rErr != nil && rErr != filer_pb.ErrNotFound {
		return rErr
	}
	if rErr == filer_pb.ErrNotFound || buf.Len() == 0 {
		return nil
	}
	var legacy Policies
	if err := json.Unmarshal(buf.Bytes(), &legacy); err != nil {
		return err
	}
	for name, doc := range legacy.Policies {
		if _, exists := policies.Policies[name]; !exists {
			policies.Policies[name] = doc
		}
	}
	if len(legacy.InlinePolicies) > 0 {
		if policies.InlinePolicies == nil {
			policies.InlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument, len(legacy.InlinePolicies))
		}
		for user, byName := range legacy.InlinePolicies {
			policies.InlinePolicies[user] = byName
		}
	}
	if len(legacy.GroupInlinePolicies) > 0 {
		if policies.GroupInlinePolicies == nil {
			policies.GroupInlinePolicies = make(map[string]map[string]policy_engine.PolicyDocument, len(legacy.GroupInlinePolicies))
		}
		for group, byName := range legacy.GroupInlinePolicies {
			policies.GroupInlinePolicies[group] = byName
		}
	}
	return nil
}

// PutPolicies persists managed policies via the credential store (Create/
// Update/Delete deltas relative to the store's current contents) and writes
// inline/group inline policies to the filer policies.json file. Routing
// managed policies through the credential manager fixes the split-brain
// where the IAM API wrote to the filer while the Admin UI wrote to the
// configured store. Legacy managed policies that GetPolicies merged in from
// the filer file are upserted into the store on the next PutPolicies, which
// also wipes them from the rewritten legacy file.
func (iama *IamS3ApiConfigure) PutPolicies(policies *Policies) (err error) {
	ctx := context.Background()

	legacy := Policies{
		InlinePolicies:      policies.InlinePolicies,
		GroupInlinePolicies: policies.GroupInlinePolicies,
	}

	if iama.credentialManager == nil {
		// Defensive fallback: with no credential store available, persist
		// everything to the legacy file like the pre-refactor code did.
		legacy.Policies = policies.Policies
	} else {
		current, gErr := iama.credentialManager.GetPolicies(ctx)
		if gErr != nil {
			return fmt.Errorf("read managed policies from credential store: %w", gErr)
		}
		desired := policies.Policies
		for name, doc := range desired {
			existing, found := current[name]
			if !found {
				if cErr := iama.credentialManager.CreatePolicy(ctx, name, doc); cErr != nil {
					return fmt.Errorf("create policy %s in credential store: %w", name, cErr)
				}
				continue
			}
			if !reflect.DeepEqual(existing, doc) {
				if uErr := iama.credentialManager.UpdatePolicy(ctx, name, doc); uErr != nil {
					return fmt.Errorf("update policy %s in credential store: %w", name, uErr)
				}
			}
		}
		for name := range current {
			if _, keep := desired[name]; !keep {
				if dErr := iama.credentialManager.DeletePolicy(ctx, name); dErr != nil {
					return fmt.Errorf("delete policy %s from credential store: %w", name, dErr)
				}
			}
		}
	}

	if len(legacy.Policies) == 0 && len(legacy.InlinePolicies) == 0 && len(legacy.GroupInlinePolicies) == 0 {
		return nil
	}

	b, err := json.Marshal(&legacy)
	if err != nil {
		return err
	}
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(ctx, client, filer.IamConfigDirectory, filer.IamPoliciesFile, b)
	})
}
