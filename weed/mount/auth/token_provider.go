package auth

import "context"

type TokenProvider interface {
	GetToken(ctx context.Context) (string, error)
}
