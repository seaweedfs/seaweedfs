package credential

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// CredentialEncryptionKeyID is the default KMS key used to wrap secret keys
// when credential.toml enables encryption without naming one.
const CredentialEncryptionKeyID = "seaweedfs-credentials"

// encryptedDataKeyField carries the KMS-wrapped data key inside the envelope.
const encryptedDataKeyField = "encrypted_data_key"

// credentialEncryptionContext authenticates the data key wrapping so a blob
// cannot be replayed from a different subsystem.
var credentialEncryptionContext = map[string]string{"seaweedfs": "credential-store"}

// EncryptedCredentialStore wraps another credential store and encrypts secret
// keys before they are persisted, decrypting them on reads. Secrets stored
// without an envelope pass through unchanged, so existing plaintext
// configurations keep working while new writes are encrypted.
type EncryptedCredentialStore struct {
	CredentialStore
	keyID string
}

var _ CredentialStore = &EncryptedCredentialStore{}

// NewEncryptedCredentialStore wraps an upstream store with secret encryption
// under the named KMS key. An empty keyID uses CredentialEncryptionKeyID.
func NewEncryptedCredentialStore(upstream CredentialStore, keyID string) CredentialStore {
	if keyID == "" {
		keyID = CredentialEncryptionKeyID
	}
	wrapped := &EncryptedCredentialStore{CredentialStore: upstream, keyID: keyID}
	if _, ok := upstream.(UserRenamer); ok {
		return &encryptedRenamerStore{wrapped}
	}
	return wrapped
}

// encryptedRenamerStore adds RenameUser to the wrapper only when the upstream
// store supports it, so wrapped stores keep their atomic rename capability.
type encryptedRenamerStore struct {
	*EncryptedCredentialStore
}

func (s *encryptedRenamerStore) RenameUser(ctx context.Context, oldName, newName string) error {
	return s.CredentialStore.(UserRenamer).RenameUser(ctx, oldName, newName)
}

// SetFilerAddressFunc forwards a dynamic filer address to stores that need it.
func (s *EncryptedCredentialStore) SetFilerAddressFunc(getFiler func() pb.ServerAddress, grpcDialOption grpc.DialOption) {
	if setter, ok := s.CredentialStore.(FilerAddressSetter); ok {
		setter.SetFilerAddressFunc(getFiler, grpcDialOption)
	}
}

// The optional interface forwarders below return the same neutral results the
// CredentialManager produces for stores without the capability, so wrapping a
// store never changes which calls succeed.

func (s *EncryptedCredentialStore) PutUserInlinePolicy(ctx context.Context, userName, policyName string, document policy_engine.PolicyDocument) error {
	if store, ok := s.CredentialStore.(InlinePolicyStore); ok {
		return store.PutUserInlinePolicy(ctx, userName, policyName, document)
	}
	return nil
}

func (s *EncryptedCredentialStore) GetUserInlinePolicy(ctx context.Context, userName, policyName string) (*policy_engine.PolicyDocument, error) {
	if store, ok := s.CredentialStore.(InlinePolicyStore); ok {
		return store.GetUserInlinePolicy(ctx, userName, policyName)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) DeleteUserInlinePolicy(ctx context.Context, userName, policyName string) error {
	if store, ok := s.CredentialStore.(InlinePolicyStore); ok {
		return store.DeleteUserInlinePolicy(ctx, userName, policyName)
	}
	return nil
}

func (s *EncryptedCredentialStore) ListUserInlinePolicies(ctx context.Context, userName string) ([]string, error) {
	if store, ok := s.CredentialStore.(InlinePolicyStore); ok {
		return store.ListUserInlinePolicies(ctx, userName)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) LoadInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	if loader, ok := s.CredentialStore.(InlinePoliciesLoader); ok {
		return loader.LoadInlinePolicies(ctx)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) PutGroupInlinePolicy(ctx context.Context, groupName, policyName string, document policy_engine.PolicyDocument) error {
	if store, ok := s.CredentialStore.(GroupInlinePolicyStore); ok {
		return store.PutGroupInlinePolicy(ctx, groupName, policyName, document)
	}
	return nil
}

func (s *EncryptedCredentialStore) GetGroupInlinePolicy(ctx context.Context, groupName, policyName string) (*policy_engine.PolicyDocument, error) {
	if store, ok := s.CredentialStore.(GroupInlinePolicyStore); ok {
		return store.GetGroupInlinePolicy(ctx, groupName, policyName)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) DeleteGroupInlinePolicy(ctx context.Context, groupName, policyName string) error {
	if store, ok := s.CredentialStore.(GroupInlinePolicyStore); ok {
		return store.DeleteGroupInlinePolicy(ctx, groupName, policyName)
	}
	return nil
}

func (s *EncryptedCredentialStore) ListGroupInlinePolicies(ctx context.Context, groupName string) ([]string, error) {
	if store, ok := s.CredentialStore.(GroupInlinePolicyStore); ok {
		return store.ListGroupInlinePolicies(ctx, groupName)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) LoadGroupInlinePolicies(ctx context.Context) (map[string]map[string]policy_engine.PolicyDocument, error) {
	if loader, ok := s.CredentialStore.(GroupInlinePoliciesLoader); ok {
		return loader.LoadGroupInlinePolicies(ctx)
	}
	return nil, nil
}

func (s *EncryptedCredentialStore) provider() (kms.KMSProvider, error) {
	provider := kms.GetGlobalKMS()
	if provider == nil {
		return nil, fmt.Errorf("credential encryption is enabled but no KMS provider is configured (see the [kms] section)")
	}
	return provider, nil
}

func (s *EncryptedCredentialStore) encryptSecret(ctx context.Context, plaintext string) (string, error) {
	if plaintext == "" {
		return plaintext, nil
	}
	if envelope, err := kms.ParseEnvelope([]byte(plaintext)); err == nil && envelope.ProviderSpecific[encryptedDataKeyField] != nil {
		return plaintext, nil
	}
	provider, err := s.provider()
	if err != nil {
		return "", err
	}
	keyID, err := provider.GetKeyID(ctx, s.keyID)
	if err != nil {
		return "", fmt.Errorf("resolve credential encryption key: %w", err)
	}
	dataKey, err := provider.GenerateDataKey(ctx, &kms.GenerateDataKeyRequest{
		KeyID:             keyID,
		KeySpec:           kms.KeySpecAES256,
		EncryptionContext: credentialEncryptionContext,
	})
	if err != nil {
		return "", fmt.Errorf("generate credential data key: %w", err)
	}
	defer kms.ClearSensitiveData(dataKey.Plaintext)

	aesgcm, err := newGCM(dataKey.Plaintext)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	blob := aesgcm.Seal(nonce, nonce, []byte(plaintext), []byte(keyID))

	envelope, err := kms.CreateEnvelope("kms", keyID, base64.StdEncoding.EncodeToString(blob), map[string]interface{}{
		encryptedDataKeyField: base64.StdEncoding.EncodeToString(dataKey.CiphertextBlob),
	})
	if err != nil {
		return "", err
	}
	return string(envelope), nil
}

func (s *EncryptedCredentialStore) decryptSecret(ctx context.Context, stored string) (string, error) {
	envelope, err := kms.ParseEnvelope([]byte(stored))
	if err != nil {
		return stored, nil
	}
	blob, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return "", fmt.Errorf("decode credential ciphertext: %w", err)
	}
	encDataKey, ok := envelope.ProviderSpecific[encryptedDataKeyField].(string)
	if !ok || encDataKey == "" {
		return "", fmt.Errorf("credential envelope missing %s", encryptedDataKeyField)
	}
	encDataKeyBytes, err := base64.StdEncoding.DecodeString(encDataKey)
	if err != nil {
		return "", fmt.Errorf("decode credential data key: %w", err)
	}

	provider, err := s.provider()
	if err != nil {
		return "", err
	}
	dataKey, err := provider.Decrypt(ctx, &kms.DecryptRequest{
		CiphertextBlob:    encDataKeyBytes,
		EncryptionContext: credentialEncryptionContext,
	})
	if err != nil {
		return "", fmt.Errorf("decrypt credential data key: %w", err)
	}
	defer kms.ClearSensitiveData(dataKey.Plaintext)

	aesgcm, err := newGCM(dataKey.Plaintext)
	if err != nil {
		return "", err
	}
	if len(blob) < aesgcm.NonceSize() {
		return "", fmt.Errorf("credential ciphertext too short")
	}
	plain, err := aesgcm.Open(nil, blob[:aesgcm.NonceSize()], blob[aesgcm.NonceSize():], []byte(envelope.KeyID))
	if err != nil {
		return "", fmt.Errorf("decrypt credential secret: %w", err)
	}
	return string(plain), nil
}

func newGCM(dataKey []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(dataKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (s *EncryptedCredentialStore) encryptIdentity(ctx context.Context, identity *iam_pb.Identity) (*iam_pb.Identity, error) {
	cloned := proto.Clone(identity).(*iam_pb.Identity)
	for _, cred := range cloned.Credentials {
		encrypted, err := s.encryptSecret(ctx, cred.SecretKey)
		if err != nil {
			return nil, err
		}
		cred.SecretKey = encrypted
	}
	return cloned, nil
}

func (s *EncryptedCredentialStore) decryptIdentity(ctx context.Context, identity *iam_pb.Identity) (*iam_pb.Identity, error) {
	if identity == nil {
		return nil, nil
	}
	for _, cred := range identity.Credentials {
		decrypted, err := s.decryptSecret(ctx, cred.SecretKey)
		if err != nil {
			return nil, err
		}
		cred.SecretKey = decrypted
	}
	return identity, nil
}

func (s *EncryptedCredentialStore) encryptServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) (*iam_pb.ServiceAccount, error) {
	cloned := proto.Clone(sa).(*iam_pb.ServiceAccount)
	if cloned.Credential != nil {
		encrypted, err := s.encryptSecret(ctx, cloned.Credential.SecretKey)
		if err != nil {
			return nil, err
		}
		cloned.Credential.SecretKey = encrypted
	}
	return cloned, nil
}

func (s *EncryptedCredentialStore) decryptServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) (*iam_pb.ServiceAccount, error) {
	if sa == nil {
		return nil, nil
	}
	if sa.Credential != nil {
		decrypted, err := s.decryptSecret(ctx, sa.Credential.SecretKey)
		if err != nil {
			return nil, err
		}
		sa.Credential.SecretKey = decrypted
	}
	return sa, nil
}

func (s *EncryptedCredentialStore) LoadConfiguration(ctx context.Context) (*iam_pb.S3ApiConfiguration, error) {
	config, err := s.CredentialStore.LoadConfiguration(ctx)
	if err != nil || config == nil {
		return config, err
	}
	for _, identity := range config.Identities {
		if _, err := s.decryptIdentity(ctx, identity); err != nil {
			return nil, err
		}
	}
	return config, nil
}

func (s *EncryptedCredentialStore) SaveConfiguration(ctx context.Context, config *iam_pb.S3ApiConfiguration) error {
	cloned := proto.Clone(config).(*iam_pb.S3ApiConfiguration)
	for _, identity := range cloned.Identities {
		for _, cred := range identity.Credentials {
			encrypted, err := s.encryptSecret(ctx, cred.SecretKey)
			if err != nil {
				return err
			}
			cred.SecretKey = encrypted
		}
	}
	return s.CredentialStore.SaveConfiguration(ctx, cloned)
}

func (s *EncryptedCredentialStore) CreateUser(ctx context.Context, identity *iam_pb.Identity) error {
	encrypted, err := s.encryptIdentity(ctx, identity)
	if err != nil {
		return err
	}
	return s.CredentialStore.CreateUser(ctx, encrypted)
}

func (s *EncryptedCredentialStore) GetUser(ctx context.Context, username string) (*iam_pb.Identity, error) {
	identity, err := s.CredentialStore.GetUser(ctx, username)
	if err != nil {
		return nil, err
	}
	return s.decryptIdentity(ctx, identity)
}

func (s *EncryptedCredentialStore) UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error {
	encrypted, err := s.encryptIdentity(ctx, identity)
	if err != nil {
		return err
	}
	return s.CredentialStore.UpdateUser(ctx, username, encrypted)
}

func (s *EncryptedCredentialStore) GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error) {
	identity, err := s.CredentialStore.GetUserByAccessKey(ctx, accessKey)
	if err != nil {
		return nil, err
	}
	return s.decryptIdentity(ctx, identity)
}

func (s *EncryptedCredentialStore) CreateAccessKey(ctx context.Context, username string, cred *iam_pb.Credential) error {
	cloned := proto.Clone(cred).(*iam_pb.Credential)
	encrypted, err := s.encryptSecret(ctx, cloned.SecretKey)
	if err != nil {
		return err
	}
	cloned.SecretKey = encrypted
	return s.CredentialStore.CreateAccessKey(ctx, username, cloned)
}

func (s *EncryptedCredentialStore) CreateServiceAccount(ctx context.Context, sa *iam_pb.ServiceAccount) error {
	encrypted, err := s.encryptServiceAccount(ctx, sa)
	if err != nil {
		return err
	}
	return s.CredentialStore.CreateServiceAccount(ctx, encrypted)
}

func (s *EncryptedCredentialStore) UpdateServiceAccount(ctx context.Context, id string, sa *iam_pb.ServiceAccount) error {
	encrypted, err := s.encryptServiceAccount(ctx, sa)
	if err != nil {
		return err
	}
	return s.CredentialStore.UpdateServiceAccount(ctx, id, encrypted)
}

func (s *EncryptedCredentialStore) GetServiceAccount(ctx context.Context, id string) (*iam_pb.ServiceAccount, error) {
	sa, err := s.CredentialStore.GetServiceAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.decryptServiceAccount(ctx, sa)
}

func (s *EncryptedCredentialStore) ListServiceAccounts(ctx context.Context) ([]*iam_pb.ServiceAccount, error) {
	accounts, err := s.CredentialStore.ListServiceAccounts(ctx)
	if err != nil {
		return nil, err
	}
	for _, sa := range accounts {
		if _, err := s.decryptServiceAccount(ctx, sa); err != nil {
			return nil, err
		}
	}
	return accounts, nil
}

func (s *EncryptedCredentialStore) GetServiceAccountByAccessKey(ctx context.Context, accessKey string) (*iam_pb.ServiceAccount, error) {
	sa, err := s.CredentialStore.GetServiceAccountByAccessKey(ctx, accessKey)
	if err != nil {
		return nil, err
	}
	return s.decryptServiceAccount(ctx, sa)
}
