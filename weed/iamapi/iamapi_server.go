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

// GetPolicies returns the merged view of managed, user-inline, and group-inline
// policies from the configured credential store, plus a one-shot fallback read
// of the legacy /etc/iam/policies.json bundle. Routing reads through the
// credential manager keeps the IAM API in sync with the Admin UI; the legacy
// file fallback lets deployments that wrote policies before this change still
// see them until the next PutPolicies migrates them into the store.
func (iama *IamS3ApiConfigure) GetPolicies(policies *Policies) (err error) {
	ctx := context.Background()

	if policies.Policies == nil {
		policies.Policies = make(map[string]policy_engine.PolicyDocument)
	}

	if iama.credentialManager != nil {
		managed, mErr := iama.credentialManager.GetPolicies(ctx)
		if mErr != nil {
			return fmt.Errorf("load managed policies from credential store: %w", mErr)
		}
		for name, doc := range managed {
			policies.Policies[name] = doc
		}

		inline, iErr := iama.credentialManager.LoadAllInlinePolicies(ctx)
		if iErr != nil {
			return fmt.Errorf("load user inline policies from credential store: %w", iErr)
		}
		mergeInline(&policies.InlinePolicies, inline)

		groupInline, gErr := iama.credentialManager.LoadAllGroupInlinePolicies(ctx)
		if gErr != nil {
			return fmt.Errorf("load group inline policies from credential store: %w", gErr)
		}
		mergeInline(&policies.GroupInlinePolicies, groupInline)
	}

	// Legacy /etc/iam/policies.json fallback. Credential store wins on
	// conflict (per name / per (user, name) / per (group, name)) so we never
	// produce duplicates.
	var buf bytes.Buffer
	rErr := pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer.ReadEntry(iama.masterClient, client, filer.IamConfigDirectory, filer.IamPoliciesFile, &buf)
	})
	if rErr != nil && rErr != filer_pb.ErrNotFound {
		return fmt.Errorf("read legacy policies from filer: %w", rErr)
	}
	if rErr == filer_pb.ErrNotFound || buf.Len() == 0 {
		return nil
	}
	var legacy Policies
	if err := json.Unmarshal(buf.Bytes(), &legacy); err != nil {
		return fmt.Errorf("unmarshal legacy policies: %w", err)
	}
	for name, doc := range legacy.Policies {
		if _, exists := policies.Policies[name]; !exists {
			policies.Policies[name] = doc
		}
	}
	mergeInlineFallback(&policies.InlinePolicies, legacy.InlinePolicies)
	mergeInlineFallback(&policies.GroupInlinePolicies, legacy.GroupInlinePolicies)
	return nil
}

// PutPolicies persists managed, user-inline, and group-inline policies through
// the credential store as deltas relative to its current contents. When the
// credential store is something other than filer_etc (postgres, grpc, memory),
// the legacy /etc/iam/policies.json bundle is cleared after a successful write
// so that data merged in by the GetPolicies fallback during migration cannot
// resurface — otherwise a later DeletePolicy would be undone by the next read.
// For filer_etc the bundle is left alone because filer_etc owns it as its own
// inline-policy backing store (see filer_etc.saveLegacyPoliciesCollection).
// With no credential manager available (a test path), everything is written
// back to the bundle, preserving the pre-refactor behavior.
func (iama *IamS3ApiConfigure) PutPolicies(policies *Policies) (err error) {
	ctx := context.Background()

	if iama.credentialManager == nil {
		legacy := Policies{
			Policies:            policies.Policies,
			InlinePolicies:      policies.InlinePolicies,
			GroupInlinePolicies: policies.GroupInlinePolicies,
		}
		return iama.writeLegacyPolicies(ctx, &legacy)
	}

	// Managed delta.
	currentManaged, gErr := iama.credentialManager.GetPolicies(ctx)
	if gErr != nil {
		return fmt.Errorf("read managed policies from credential store: %w", gErr)
	}
	for name, doc := range policies.Policies {
		existing, found := currentManaged[name]
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
	for name := range currentManaged {
		if _, keep := policies.Policies[name]; !keep {
			if dErr := iama.credentialManager.DeletePolicy(ctx, name); dErr != nil {
				return fmt.Errorf("delete policy %s from credential store: %w", name, dErr)
			}
		}
	}

	// User-inline delta.
	currentInline, iErr := iama.credentialManager.LoadAllInlinePolicies(ctx)
	if iErr != nil {
		return fmt.Errorf("read user inline policies from credential store: %w", iErr)
	}
	for user, desired := range policies.InlinePolicies {
		existing := currentInline[user]
		for name, doc := range desired {
			if prior, found := existing[name]; found && reflect.DeepEqual(prior, doc) {
				continue
			}
			if pErr := iama.credentialManager.PutUserInlinePolicy(ctx, user, name, doc); pErr != nil {
				return fmt.Errorf("put inline policy %s/%s in credential store: %w", user, name, pErr)
			}
		}
	}
	for user, existing := range currentInline {
		desired := policies.InlinePolicies[user]
		for name := range existing {
			if _, keep := desired[name]; keep {
				continue
			}
			if dErr := iama.credentialManager.DeleteUserInlinePolicy(ctx, user, name); dErr != nil {
				return fmt.Errorf("delete inline policy %s/%s from credential store: %w", user, name, dErr)
			}
		}
	}

	// Group-inline delta.
	currentGroupInline, ggErr := iama.credentialManager.LoadAllGroupInlinePolicies(ctx)
	if ggErr != nil {
		return fmt.Errorf("read group inline policies from credential store: %w", ggErr)
	}
	for group, desired := range policies.GroupInlinePolicies {
		existing := currentGroupInline[group]
		for name, doc := range desired {
			if prior, found := existing[name]; found && reflect.DeepEqual(prior, doc) {
				continue
			}
			if pErr := iama.credentialManager.PutGroupInlinePolicy(ctx, group, name, doc); pErr != nil {
				return fmt.Errorf("put group inline policy %s/%s in credential store: %w", group, name, pErr)
			}
		}
	}
	for group, existing := range currentGroupInline {
		desired := policies.GroupInlinePolicies[group]
		for name := range existing {
			if _, keep := desired[name]; keep {
				continue
			}
			if dErr := iama.credentialManager.DeleteGroupInlinePolicy(ctx, group, name); dErr != nil {
				return fmt.Errorf("delete group inline policy %s/%s from credential store: %w", group, name, dErr)
			}
		}
	}

	// filer_etc owns the legacy bundle itself (it's how filer_etc persists
	// inline policies). For every other store the bundle is at most a one-
	// shot migration source — empty it so deleted legacy-only entries don't
	// reappear via the GetPolicies fallback on the next read.
	if iama.credentialManager.GetStoreName() == string(credential.StoreTypeFilerEtc) {
		return nil
	}
	return iama.writeLegacyPolicies(ctx, &Policies{})
}

// writeLegacyPolicies serializes the given Policies struct to the legacy
// /etc/iam/policies.json bundle. Called both for the no-credential-manager
// fallback (full bundle) and for the credential-manager path (empty bundle,
// to drain legacy data after migration).
func (iama *IamS3ApiConfigure) writeLegacyPolicies(ctx context.Context, p *Policies) error {
	b, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal legacy policies: %w", err)
	}
	return pb.WithOneOfGrpcFilerClients(false, iama.option.Filers, iama.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(ctx, client, filer.IamConfigDirectory, filer.IamPoliciesFile, b)
	})
}

// mergeInline overwrites entries in dst with src (credential-store data taking
// precedence over anything already in dst).
func mergeInline(dst *map[string]map[string]policy_engine.PolicyDocument, src map[string]map[string]policy_engine.PolicyDocument) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]map[string]policy_engine.PolicyDocument, len(src))
	}
	for outer, byName := range src {
		bucket := (*dst)[outer]
		if bucket == nil {
			bucket = make(map[string]policy_engine.PolicyDocument, len(byName))
			(*dst)[outer] = bucket
		}
		for name, doc := range byName {
			bucket[name] = doc
		}
	}
}

// mergeInlineFallback fills only entries that are missing in dst — the legacy
// file never overrides whatever the credential store returned.
func mergeInlineFallback(dst *map[string]map[string]policy_engine.PolicyDocument, src map[string]map[string]policy_engine.PolicyDocument) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]map[string]policy_engine.PolicyDocument, len(src))
	}
	for outer, byName := range src {
		bucket := (*dst)[outer]
		if bucket == nil {
			bucket = make(map[string]policy_engine.PolicyDocument, len(byName))
			(*dst)[outer] = bucket
		}
		for name, doc := range byName {
			if _, exists := bucket[name]; !exists {
				bucket[name] = doc
			}
		}
	}
}
