package wdclient

import (
	"context"
)

type SeaweedClient struct {
	ctx        context.Context
	Master     string
	ClientName string
}

func NewSeaweedClient(ctx context.Context, clientName string, masters []string) *SeaweedClient {
	return &SeaweedClient{
		ctx:        ctx,
		ClientName: clientName,

	}
}
