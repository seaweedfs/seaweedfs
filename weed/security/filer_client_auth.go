package security

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// FilerTokenSource resolves a logical client's token for one RPC attempt.
// An empty token leaves the call unchanged; an error prevents the call.
// Sources must be safe for concurrent use and must not log token material.
type FilerTokenSource func(context.Context) (EncodedJwt, error)

// filerClientKeyTTL bounds how often a token source re-reads its security
// configuration. Reading and parsing the file costs far more than signing, and
// sources run on every RPC; a rotation is picked up within this interval.
var filerClientKeyTTL = time.Second

var errFilerClientConfig = errors.New("filer: cannot read client security configuration")

// filerClientKeys caches the signing keys read from one security
// configuration. A read failure is not cached, so the next call retries.
type filerClientKeys struct {
	mu     sync.Mutex
	readAt time.Time
	read   bool
	known  bool // grpc and shared hold the last keys read successfully
	grpc   SigningKey
	shared SigningKey
	load   func() (interface{ GetString(string) string }, error)
	// fallback, when set, supplies keys for a failed read that has no last
	// successful read to fall back to; without it a failed read fails closed.
	fallback func() interface{ GetString(string) string }
	// stamp identifies the inputs besides the file contents, such as the
	// WEED_ environment overrides; a change invalidates the cached keys.
	stamp   func() string
	stamped string
}

// filerClientKeyEnvStamp covers the environment overrides of the two keys.
func filerClientKeyEnvStamp() string {
	return os.Getenv("WEED_JWT_FILER_SIGNING_GRPC_KEY") + "\x00" + os.Getenv("WEED_JWT_FILER_SIGNING_KEY")
}

func (k *filerClientKeys) get() (grpcKey, sharedKey SigningKey, err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	stamp := ""
	if k.stamp != nil {
		stamp = k.stamp()
	}
	if k.read && stamp == k.stamped && time.Since(k.readAt) < filerClientKeyTTL {
		return k.grpc, k.shared, nil
	}
	v, err := k.load()
	if err == nil {
		grpcKey, sharedKey, err = filerClientKeysFrom(v)
	}
	if err != nil {
		k.read = false
		if k.fallback == nil {
			return nil, nil, err
		}
		if k.known {
			return k.grpc, k.shared, nil
		}
		return filerClientKeysFrom(k.fallback())
	}
	k.grpc, k.shared = grpcKey, sharedKey
	k.readAt, k.read, k.known, k.stamped = time.Now(), true, true, stamp
	return k.grpc, k.shared, nil
}

// filerClientKeyCheck refuses keys that cannot sign in this process.
var filerClientKeyCheck = util.CheckFips140HmacKeys

// filerClientKeysFrom reads the keys a client signs with. A key too short for
// GODEBUG=fips140=only is an error, as at startup, because signing with it
// would panic; a reloaded file is checked the same way.
func filerClientKeysFrom(v interface{ GetString(string) string }) (grpcKey, sharedKey SigningKey, err error) {
	settings := []string{"jwt.filer_signing.grpc_key", "jwt.filer_signing.key"}
	keys := []string{v.GetString(settings[0]), v.GetString(settings[1])}
	if err := filerClientKeyCheck(settings, keys); err != nil {
		return nil, nil, err
	}
	return SigningKey(keys[0]), SigningKey(keys[1]), nil
}

func (k *filerClientKeys) token(ctx context.Context) (EncodedJwt, error) {
	grpcKey, sharedKey, err := k.get()
	if err != nil {
		return "", err
	}
	return filerAdminTokenForKeys(ctx, grpcKey, sharedKey), nil
}

// globalFilerClientKeys follows security.toml as the process finds it now.
// When the file exists but cannot be read (for example a root-only file in a
// non-root process, or a file being replaced), it keeps the last keys read,
// else the keys loaded at startup, as FilerAdminKeys does: a process that
// could never read the file keeps its existing tokenless behavior.
var globalFilerClientKeys = &filerClientKeys{
	load: func() (interface{ GetString(string) string }, error) {
		v, _, err := util.ReadConfigurationWithError("security", "")
		if err != nil {
			var notFound viper.ConfigFileNotFoundError
			if !errors.As(err, &notFound) {
				return nil, errFilerClientConfig
			}
		}
		return v, nil
	},
	fallback: func() interface{ GetString(string) string } { return util.GetViper() },
	stamp: func() string {
		return filerClientKeyEnvStamp() + "\x00" + util.ConfigurationFileDirectory.String()
	},
}

// FilerAdminToken resolves the current filesystem gRPC signing key and mints
// a ten-second admin token. No key means no change to existing client behavior.
// grpc_require_token is a receiver policy, not a switch for presenting a key
// the client already holds. Previous keys are for verification, not signing.
func FilerAdminToken(ctx context.Context) (EncodedJwt, error) {
	return globalFilerClientKeys.token(ctx)
}

// FilerAdminTokenFromFile selects an endpoint's existing security file instead
// of the process-wide configuration. An unreadable file fails closed without
// using another endpoint's key. WEED_ environment overrides do not apply: they
// are process-wide, so honoring them would sign every endpoint's admin token
// with one key and hand one cluster a token valid on the other. An empty path
// selects FilerAdminToken.
func FilerAdminTokenFromFile(path string) FilerTokenSource {
	if path == "" {
		return FilerAdminToken
	}
	keys := &filerClientKeys{load: func() (interface{ GetString(string) string }, error) {
		v := viper.New()
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			// Parser errors can contain configuration values. Do not expose them.
			return nil, errFilerClientConfig
		}
		return v, nil
	}}
	return keys.token
}

type filerSubscriptionKey struct{}

// withFilerSubscription marks a context as opening a metadata subscription.
func withFilerSubscription(ctx context.Context) context.Context {
	return context.WithValue(ctx, filerSubscriptionKey{}, true)
}

func isFilerSubscriptionMethod(method string) bool {
	return method == "/filer_pb.SeaweedFiler/SubscribeMetadata" ||
		method == "/filer_pb.SeaweedFiler/SubscribeLocalMetadata"
}

// filerAdminTokenForKeys signs with the key filesystem gRPC verifies:
// grpc_key when set, else the shared key. Metadata subscriptions prefer the
// shared key, as filer peers do, because older filers verify subscription
// admin tokens only with it.
func filerAdminTokenForKeys(ctx context.Context, grpcKey, sharedKey SigningKey) EncodedJwt {
	key := grpcKey
	if subscription, _ := ctx.Value(filerSubscriptionKey{}).(bool); (subscription && len(sharedKey) > 0) || len(key) == 0 {
		key = sharedKey
	}
	return GenJwtForFilerAdmin(key, 10)
}

func filerAdminToken(v interface{ GetString(string) string }) EncodedJwt {
	return filerAdminTokenForKeys(context.Background(), SigningKey(v.GetString("jwt.filer_signing.grpc_key")), SigningKey(v.GetString("jwt.filer_signing.key")))
}

// WithFilerSubscriptionToken is WithFilerToken for opening a metadata
// subscription; admin token sources then prefer the shared signing key.
func WithFilerSubscriptionToken(ctx context.Context, source FilerTokenSource) (context.Context, error) {
	return WithFilerToken(withFilerSubscription(ctx), source)
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
	if isFilerSubscriptionMethod(method) {
		return WithFilerSubscriptionToken(ctx, c.source)
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
