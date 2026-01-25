package iam

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

type PostgresIamStorage struct {
	// db *sql.DB
}

func NewPostgresIamStorage(config map[string]interface{}) (*PostgresIamStorage, error) {
	return &PostgresIamStorage{}, nil
}

func (s *PostgresIamStorage) CreateIdentity(ctx context.Context, identity *iam_pb.Identity) error {
	return fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) GetIdentity(ctx context.Context, name string) (*iam_pb.Identity, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) UpdateIdentity(ctx context.Context, identity *iam_pb.Identity) error {
	return fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) DeleteIdentity(ctx context.Context, name string) error {
	return fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) ListIdentities(ctx context.Context, limit int, offset string) ([]*iam_pb.Identity, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) CreatePolicy(ctx context.Context, name string, policy *policy.PolicyDocument) error {
	return fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) GetPolicy(ctx context.Context, name string) (*policy.PolicyDocument, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) DeletePolicy(ctx context.Context, name string) error {
	return fmt.Errorf("not implemented")
}

func (s *PostgresIamStorage) ListPolicies(ctx context.Context, limit int, offset string) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}
