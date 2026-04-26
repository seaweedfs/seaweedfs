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
	ErrUserNotFound           = errors.New("user not found")
	ErrUserAlreadyExists      = errors.New("user already exists")
	ErrAccessKeyNotFound      = errors.New("access key not found")
	ErrServiceAccountNotFound = errors.New("service account not found")
	ErrPolicyNotFound         = errors.New("policy not found")
	ErrPolicyAlreadyAttached  = errors.New("policy already attached")
	ErrPolicyNotAttached      = errors.New("policy not attached to user")
	ErrGroupNotFound          = errors.New("group not found")
	ErrGroupAlreadyExists     = errors.New("group already exists")
	ErrGroupNotEmpty          = errors.New("group is not empty")
	ErrUserNotInGroup         = errors.New("user is not a member of the group")
)

// CredentialStoreTypeName represents the type name of a credential store
type CredentialStoreTypeName string

// Credential store name constants
const (
	StoreTypeMemory   CredentialStoreTypeName = "memory"
	StoreTypeFilerEtc CredentialStoreTypeName = "filer_etc"
	StoreTypePostgres CredentialStoreTypeName = "postgres"
	StoreTypeGrpc     CredentialStoreTypeName = "grpc"
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
	// ListPolicyNames returns the names of all policies
	ListPolicyNames(ctx context.Context) ([]string, error)
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
	GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error)

	// User Policy Attachment Management
	// AttachUserPolicy attaches a managed policy to a user by policy name
	AttachUserPolicy(ctx context.Context, username string, policyName string) error
	// DetachUserPolicy detaches a managed policy from a user
	DetachUserPolicy(ctx context.Context, username string, policyName string) error
	// ListAttachedUserPolicies returns the list of policy names attached to a user
	ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error)

	// Group Management
	CreateGroup(ctx context.Context, group *iam_pb.Group) error
	GetGroup(ctx context.Context, groupName string) (*iam_pb.Group, error)
	DeleteGroup(ctx context.Context, groupName string) error
	ListGroups(ctx context.Context) ([]string, error)
	UpdateGroup(ctx context.Context, group *iam_pb.Group) error

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

// InlinePolicyStore is an optional interface for credential stores that support
// per-user inline policy storage. Stores that implement this interface preserve
// the exact policy document submitted via PutUserPolicy, enabling lossless
// round-trips through GetUserPolicy.
type InlinePolicyStore interface {
	PutUserInlinePolicy(ctx context.Context, userName, policyName string, document policy_engine.PolicyDocument) error
	GetUserInlinePolicy(ctx context.Context, userName, policyName string) (*policy_engine.PolicyDocument, error)
	DeleteUserInlinePolicy(ctx context.Context, userName, policyName string) error
	ListUserInlinePolicies(ctx context.Context, userName string) ([]string, error)
}

// UserRenamer is an optional interface for credential stores that can
// atomically rename a user along with all rows that reference the old
// username (credentials, inline policies, etc). Backends with referential
// integrity (e.g. PostgreSQL with ON DELETE CASCADE foreign keys on
// user_inline_policies) MUST implement this so the IAM rename path can
// move the dependents within a single transaction; copying them via
// per-row Put / Get / Delete calls would violate the FK at statement
// time because the new user row does not yet exist.
type UserRenamer interface {
	RenameUser(ctx context.Context, oldName, newName string) error
}

// Stores holds all available credential store implementations
var Stores []CredentialStore
