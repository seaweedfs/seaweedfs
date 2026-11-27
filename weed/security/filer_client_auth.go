package security

import (
	"context"
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// FilerTokenSource resolves a logical client's token for one RPC attempt.
// An empty token leaves the call unchanged; an error prevents the call.
// Sources must be safe for concurrent use and must not log token material.
type FilerTokenSource func(context.Context) (EncodedJwt, error)

// FilerAdminToken resolves the current filesystem gRPC signing key and mints
// a ten-second admin token. No key means no change to existing client behavior.
// grpc_require_token is a receiver policy, not a switch for presenting a key
// the client already holds. Previous keys are for verification, not signing.
func FilerAdminToken(context.Context) (EncodedJwt, error) {
	v, _, err := util.ReadConfigurationWithError("security", "")
	if err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return "", errors.New("filer: cannot read client security configuration")
		}
	}
	return filerAdminToken(v), nil
}

// FilerAdminTokenFromFile selects an endpoint's existing security file instead
// of the process-wide configuration. It re-reads that file for every attempt;
// an unreadable file fails closed without using another endpoint's key.
// An empty path selects FilerAdminToken.
func FilerAdminTokenFromFile(path string) FilerTokenSource {
	if path == "" {
		return FilerAdminToken
	}
	return func(context.Context) (EncodedJwt, error) {
		v := viper.New()
		v.SetConfigFile(path)
		v.AutomaticEnv()
		v.SetEnvPrefix("weed")
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		if err := v.ReadInConfig(); err != nil {
			// Parser errors can contain configuration values. Do not expose them.
			return "", errors.New("filer: cannot read client security configuration")
		}
		return filerAdminToken(v), nil
	}
}

func filerAdminToken(v interface{ GetString(string) string }) EncodedJwt {
	key := v.GetString("jwt.filer_signing.grpc_key")
	if key == "" {
		key = v.GetString("jwt.filer_signing.key")
	}
	return GenJwtForFilerAdmin(SigningKey(key), 10)
}

// WithFilerToken supplies outgoing authorization only when the caller has not
// already selected an identity. In particular, it never replaces a scoped
// bearer with admin authority. It preserves deadlines, cancellation and all
// unrelated metadata, without modifying the original context's metadata.
func WithFilerToken(ctx context.Context, source FilerTokenSource) (context.Context, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	if _, supplied := md["authorization"]; supplied || source == nil {
		return ctx, nil
	}
	token, err := source(ctx)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return ctx, nil
	}
	md = md.Copy()
	md.Set("authorization", BearerPrefix+string(token))
	return metadata.NewOutgoingContext(ctx, md), nil
}

// WithFilerClientAuth wraps a connection interface for one logical client,
// without changing the underlying (possibly pooled) connection. Generated
// filer clients call Invoke/NewStream on this wrapper, so sources are resolved
// for each invocation even when callers construct their own contexts. Other
// gRPC services are left unchanged. Stream messages retain the opening identity.
func WithFilerClientAuth(conn grpc.ClientConnInterface, source FilerTokenSource) grpc.ClientConnInterface {
	return &filerAuthConn{ClientConnInterface: conn, source: source}
}

type filerAuthConn struct {
	grpc.ClientConnInterface
	source FilerTokenSource
}

func (c *filerAuthConn) requestContext(ctx context.Context, method string) (context.Context, error) {
	if !strings.HasPrefix(method, "/filer_pb.SeaweedFiler/") {
		return ctx, nil
	}
	return WithFilerToken(ctx, c.source)
}

func (c *filerAuthConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	ctx, err := c.requestContext(ctx, method)
	if err != nil {
		return err
	}
	return c.ClientConnInterface.Invoke(ctx, method, args, reply, opts...)
}

func (c *filerAuthConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := c.requestContext(ctx, method)
	if err != nil {
		return nil, err
	}
	return c.ClientConnInterface.NewStream(ctx, desc, method, opts...)
}
