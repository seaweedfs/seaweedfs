package s3tables

import (
	"fmt"
	"strings"
)

// Permission represents a specific action permission
type Permission string

const (
	// Table bucket permissions
	PermCreateTableBucket Permission = "s3tables:CreateTableBucket"
	PermDeleteTableBucket Permission = "s3tables:DeleteTableBucket"
	PermGetTableBucket    Permission = "s3tables:GetTableBucket"
	PermListTableBuckets  Permission = "s3tables:ListTableBuckets"

	// Namespace permissions
	PermCreateNamespace Permission = "s3tables:CreateNamespace"
	PermDeleteNamespace Permission = "s3tables:DeleteNamespace"
	PermGetNamespace    Permission = "s3tables:GetNamespace"
	PermListNamespaces  Permission = "s3tables:ListNamespaces"

	// Table permissions
	PermCreateTable Permission = "s3tables:CreateTable"
	PermDeleteTable Permission = "s3tables:DeleteTable"
	PermGetTable    Permission = "s3tables:GetTable"
	PermListTables  Permission = "s3tables:ListTables"

	// Policy permissions
	PermPutTableBucketPolicy    Permission = "s3tables:PutTableBucketPolicy"
	PermGetTableBucketPolicy    Permission = "s3tables:GetTableBucketPolicy"
	PermDeleteTableBucketPolicy Permission = "s3tables:DeleteTableBucketPolicy"
	PermPutTablePolicy          Permission = "s3tables:PutTablePolicy"
	PermGetTablePolicy          Permission = "s3tables:GetTablePolicy"
	PermDeleteTablePolicy       Permission = "s3tables:DeleteTablePolicy"

	// Tagging permissions
	PermTagResource         Permission = "s3tables:TagResource"
	PermListTagsForResource Permission = "s3tables:ListTagsForResource"
	PermUntagResource       Permission = "s3tables:UntagResource"
)

// PermissionSet represents a set of allowed permissions for a principal
type PermissionSet map[Permission]bool

// PermissionPolicy defines access control rules
type PermissionPolicy struct {
	// Owner has full access to all operations
	Owner string

	// Permissions map principal (account ID) to allowed permissions
	Permissions map[string]PermissionSet
}

// OperationPermissions maps S3 Tables operations to required permissions
var OperationPermissions = map[string]Permission{
	"CreateTableBucket":       PermCreateTableBucket,
	"DeleteTableBucket":       PermDeleteTableBucket,
	"GetTableBucket":          PermGetTableBucket,
	"ListTableBuckets":        PermListTableBuckets,
	"CreateNamespace":         PermCreateNamespace,
	"DeleteNamespace":         PermDeleteNamespace,
	"GetNamespace":            PermGetNamespace,
	"ListNamespaces":          PermListNamespaces,
	"CreateTable":             PermCreateTable,
	"DeleteTable":             PermDeleteTable,
	"GetTable":                PermGetTable,
	"ListTables":              PermListTables,
	"PutTableBucketPolicy":    PermPutTableBucketPolicy,
	"GetTableBucketPolicy":    PermGetTableBucketPolicy,
	"DeleteTableBucketPolicy": PermDeleteTableBucketPolicy,
	"PutTablePolicy":          PermPutTablePolicy,
	"GetTablePolicy":          PermGetTablePolicy,
	"DeleteTablePolicy":       PermDeleteTablePolicy,
	"TagResource":             PermTagResource,
	"ListTagsForResource":     PermListTagsForResource,
	"UntagResource":           PermUntagResource,
}

// CheckPermission checks if a principal has permission to perform an operation
func CheckPermission(operation, principal, owner string) bool {
	// Owner always has permission
	if principal == owner {
		return true
	}

	// For now, only the owner can perform operations
	// This can be extended to support more granular permissions via policies
	return false
}

// CanCreateTableBucket checks if principal can create table buckets
func CanCreateTableBucket(principal, owner string) bool {
	return CheckPermission("CreateTableBucket", principal, owner)
}

// CanDeleteTableBucket checks if principal can delete table buckets
func CanDeleteTableBucket(principal, owner string) bool {
	return CheckPermission("DeleteTableBucket", principal, owner)
}

// CanGetTableBucket checks if principal can read table bucket details
func CanGetTableBucket(principal, owner string) bool {
	return CheckPermission("GetTableBucket", principal, owner)
}

// CanListTableBuckets checks if principal can list table buckets
func CanListTableBuckets(principal, owner string) bool {
	return CheckPermission("ListTableBuckets", principal, owner)
}

// CanCreateNamespace checks if principal can create namespaces
func CanCreateNamespace(principal, owner string) bool {
	return CheckPermission("CreateNamespace", principal, owner)
}

// CanDeleteNamespace checks if principal can delete namespaces
func CanDeleteNamespace(principal, owner string) bool {
	return CheckPermission("DeleteNamespace", principal, owner)
}

// CanGetNamespace checks if principal can read namespace details
func CanGetNamespace(principal, owner string) bool {
	return CheckPermission("GetNamespace", principal, owner)
}

// CanListNamespaces checks if principal can list namespaces
func CanListNamespaces(principal, owner string) bool {
	return CheckPermission("ListNamespaces", principal, owner)
}

// CanCreateTable checks if principal can create tables
func CanCreateTable(principal, owner string) bool {
	return CheckPermission("CreateTable", principal, owner)
}

// CanDeleteTable checks if principal can delete tables
func CanDeleteTable(principal, owner string) bool {
	return CheckPermission("DeleteTable", principal, owner)
}

// CanGetTable checks if principal can read table details
func CanGetTable(principal, owner string) bool {
	return CheckPermission("GetTable", principal, owner)
}

// CanListTables checks if principal can list tables
func CanListTables(principal, owner string) bool {
	return CheckPermission("ListTables", principal, owner)
}

// CanManagePolicy checks if principal can manage policies
func CanManagePolicy(principal, owner string) bool {
	// Policy management requires owner permissions
	return principal == owner
}

// CanManageTags checks if principal can manage tags
func CanManageTags(principal, owner string) bool {
	return CheckPermission("TagResource", principal, owner)
}

// ExtractPrincipalFromContext extracts the principal (account ID) from request context
// For now, this returns the owner/creator, but can be extended to parse from request headers/certs
func ExtractPrincipalFromContext(contextID string) string {
	// Extract from context, e.g., "user123" or "account-id"
	// This is a simplified version - in production, this would parse AWS auth headers
	if strings.Contains(contextID, ":") {
		parts := strings.Split(contextID, ":")
		return parts[0]
	}
	return contextID
}

// AuthError represents an authorization error
type AuthError struct {
	Operation string
	Principal string
	Message   string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("unauthorized: %s is not permitted to perform %s: %s", e.Principal, e.Operation, e.Message)
}

// NewAuthError creates a new authorization error
func NewAuthError(operation, principal, message string) *AuthError {
	return &AuthError{
		Operation: operation,
		Principal: principal,
		Message:   message,
	}
}
