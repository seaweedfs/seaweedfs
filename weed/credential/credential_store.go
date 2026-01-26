package credential

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
	ErrAccessKeyNotFound = errors.New("access key not found")
)

// CredentialStoreTypeName represents the type name of a credential store
type CredentialStoreTypeName string

// Credential store name constants
const (
	StoreTypeMemory        CredentialStoreTypeName = "memory"
	StoreTypeFilerEtc      CredentialStoreTypeName = "filer_etc"
	StoreTypeFilerMultiple CredentialStoreTypeName = "filer_multiple"
	StoreTypePostgres      CredentialStoreTypeName = "postgres"
	StoreTypeGrpc          CredentialStoreTypeName = "grpc"
)

// CredentialStore defines the interface for user credential storage and retrieval
type CredentialStore interface {
	// GetName returns the name of the credential store implementation
	GetName() CredentialStoreTypeName

	// Initialize initializes the credential store with configuration
	Initialize(configuration util.Configuration, prefix string) error

	// LoadConfiguration loads the entire S3 API configuration
	LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error)

	// SaveConfiguration saves the entire S3 API configuration
	SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error

	// CreateUser creates a new user with the given identity
	CreateUser(ctx context.Context, identity *iam_pb.Identity) error

	// GetUser retrieves a user by username
	GetUser(ctx context.Context, username string) (*iam_pb.Identity, error)

	// UpdateUser updates an existing user
	UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error

	// DeleteUser removes a user by username
	DeleteUser(ctx context.Context, username string) error

	// ListUsers returns all usernames
	ListUsers(ctx context.Context) ([]string, error)

	// GetUserByAccessKey retrieves a user by access key
	GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error)

	// CreateAccessKey creates a new access key for a user
	CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error

	// DeleteAccessKey removes an access key for a user
	DeleteAccessKey(ctx context.Context, username string, accessKey string) error

	// Policy Management
	GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error)
	// PutPolicy creates or replaces a policy document.
	PutPolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error
	DeletePolicy(ctx context.Context, name string) error
	GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error)

	// Service Account Management
	CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error
	UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error
	DeleteServiceAccount(ctx context.Context, id string) error
	GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error)
	ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error)

	// Shutdown performs cleanup when the store is being shut down
	Shutdown()
}

// AccessKeyInfo represents access key information with metadata
type AccessKeyInfo struct {
	AccessKey string    `json:"accessKey"`
	SecretKey string    `json:"secretKey"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"createdAt"`
}

// UserCredentials represents a user's credentials and metadata
type UserCredentials struct {
	Username    string               `json:"username"`
	Email       string               `json:"email"`
	Account     *iam_pb.Account      `json:"account,omitempty"`
	Credentials []*iam_pb.Credential `json:"credentials"`
	Actions     []string             `json:"actions"`
	CreatedAt   time.Time            `json:"createdAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
}

// PolicyManager interface for managing IAM policies
type PolicyManager interface {
	GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error)
	CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error
	UpdatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error
	DeletePolicy(ctx context.Context, name string) error
	GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error)
}

// Stores holds all available credential store implementations
var Stores []CredentialStore
