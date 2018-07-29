package wdclient

import (
	"context"
)

type SeaweedClient struct {
	*MasterClient
}

func NewSeaweedClient(ctx context.Context, clientName string, masters []string) *SeaweedClient {
	return &SeaweedClient{
		MasterClient: NewMasterClient(ctx, clientName, masters),
	}
}
