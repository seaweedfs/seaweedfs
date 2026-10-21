package credential

import (
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/kms/local"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeStore is a minimal in-memory CredentialStore for exercising the
// encryption wrapper; unneeded methods panic via the embedded nil interface.
type fakeStore struct {
	CredentialStore
	config *iam_pb.S3ApiConfiguration
	users  map[string]*iam_pb.Identity
	sas    map[string]*iam_pb.ServiceAccount
}

func newFakeStore() *fakeStore {
	return &fakeStore{users: map[string]*iam_pb.Identity{}, sas: map[string]*iam_pb.ServiceAccount{}}
}

func (f *fakeStore) GetName() CredentialStoreTypeName { return "fake" }

func (f *fakeStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	f.users[identity.Name] = proto.Clone(identity).(*iam_pb.Identity)
	return nil
}

func (f *fakeStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	if u, ok := f.users[username]; ok {
		return proto.Clone(u).(*iam_pb.Identity), nil
	}
	return nil, ErrUserNotFound
}

func (f *fakeStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	return f.CreateUser(ctx, identity)
}

func (f *fakeStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	return proto.Clone(f.config).(*iam_pb.S3ApiConfiguration), nil
}

func (f *fakeStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	f.config = proto.Clone(config).(*iam_pb.S3ApiConfiguration)
	return nil
}

func (f *fakeStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	f.users[username].Credentials = append(f.users[username].Credentials, proto.Clone(cred).(*iam_pb.Credential))
	return nil
}

func (f *fakeStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	f.sas[sa.Id] = proto.Clone(sa).(*iam_pb.ServiceAccount)
	return nil
}

func (f *fakeStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	return proto.Clone(f.sas[id]).(*iam_pb.ServiceAccount), nil
}

func setupKMS(t *testing.T) {
	t.Helper()
	provider, err := local.NewLocalKMSProvider(nil)
	require.NoError(t, err)
	kms.SetGlobalKMSProvider(provider)
	t.Cleanup(func() { kms.SetGlobalKMSProvider(nil) })
}

func testIdentity() *iam_pb.Identity {
	return &iam_pb.Identity{
		Name: "alice",
		Credentials: []*iam_pb.Credential{
			{AccessKey: "AKIA_TEST", SecretKey: "secret-one"},
		},
		Actions: []string{"Read"},
	}
}

func TestEncryptedStoreRoundTrip(t *testing.T) {
	setupKMS(t)
	fake := newFakeStore()
	store := NewEncryptedCredentialStore(fake, "")
	ctx := context.Background()

	require.NoError(t, store.CreateUser(ctx, testIdentity()))

	stored := fake.users["alice"].Credentials[0].SecretKey
	assert.NotEqual(t, "secret-one", stored)
	assert.True(t, strings.HasPrefix(stored, "{"), "stored secret should be an envelope: %s", stored)
	envelope, err := kms.ParseEnvelope([]byte(stored))
	require.NoError(t, err)
	assert.NotEmpty(t, envelope.Ciphertext)
	assert.NotEmpty(t, envelope.ProviderSpecific[encryptedDataKeyField])

	got, err := store.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, "secret-one", got.Credentials[0].SecretKey)
	assert.Equal(t, "AKIA_TEST", got.Credentials[0].AccessKey)
}

func TestEncryptedStorePlaintextPassthrough(t *testing.T) {
	fake := newFakeStore()
	fake.users["bob"] = testIdentity()
	fake.users["bob"].Name = "bob"
	store := NewEncryptedCredentialStore(fake, "")

	got, err := store.GetUser(context.Background(), "bob")
	require.NoError(t, err)
	assert.Equal(t, "secret-one", got.Credentials[0].SecretKey)
}

func TestEncryptedStoreNoKMSFailsWrite(t *testing.T) {
	kms.SetGlobalKMSProvider(nil)
	fake := newFakeStore()
	store := NewEncryptedCredentialStore(fake, "")

	err := store.CreateUser(context.Background(), testIdentity())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "KMS provider")
	assert.Empty(t, fake.users, "failed writes must not persist plaintext secrets")
}

func TestEncryptedStoreSaveLoadConfiguration(t *testing.T) {
	setupKMS(t)
	fake := newFakeStore()
	store := NewEncryptedCredentialStore(fake, "")
	ctx := context.Background()

	config := &iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{testIdentity()}}
	require.NoError(t, store.SaveConfiguration(ctx, config))
	assert.Equal(t, "secret-one", config.Identities[0].Credentials[0].SecretKey,
		"the caller's config object must not be mutated")

	loaded, err := store.LoadConfiguration(ctx)
	require.NoError(t, err)
	assert.Equal(t, "secret-one", loaded.Identities[0].Credentials[0].SecretKey)

	stored := fake.config.Identities[0].Credentials[0].SecretKey
	assert.NotEqual(t, "secret-one", stored)
}

func TestEncryptedStoreServiceAccount(t *testing.T) {
	setupKMS(t)
	fake := newFakeStore()
	store := NewEncryptedCredentialStore(fake, "")
	ctx := context.Background()

	sa := &iam_pb.ServiceAccount{
		Id:         "sa-1",
		ParentUser: "alice",
		Credential: &iam_pb.Credential{AccessKey: "SA_KEY", SecretKey: "sa-secret"},
	}
	require.NoError(t, store.CreateServiceAccount(ctx, sa))

	assert.NotEqual(t, "sa-secret", fake.sas["sa-1"].Credential.SecretKey)

	got, err := store.GetServiceAccount(ctx, "sa-1")
	require.NoError(t, err)
	assert.Equal(t, "sa-secret", got.Credential.SecretKey)
}

func TestEncryptedStoreNoDoubleWrap(t *testing.T) {
	setupKMS(t)
	fake := newFakeStore()
	store := NewEncryptedCredentialStore(fake, "")
	ctx := context.Background()

	require.NoError(t, store.CreateUser(ctx, testIdentity()))
	storedOnce := fake.users["alice"].Credentials[0].SecretKey

	// Saving an already-enveloped value must not wrap it again.
	identity := testIdentity()
	identity.Credentials[0].SecretKey = storedOnce
	require.NoError(t, store.UpdateUser(ctx, "alice", identity))
	assert.Equal(t, storedOnce, fake.users["alice"].Credentials[0].SecretKey)

	got, err := store.GetUser(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, "secret-one", got.Credentials[0].SecretKey)
}
