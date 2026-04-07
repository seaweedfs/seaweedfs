package shell

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	iamgrpc "github.com/seaweedfs/seaweedfs/weed/credential/grpc"
	weediam "github.com/seaweedfs/seaweedfs/weed/iam"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var errS3AccessKeyInUse = errors.New("access key already in use")

type s3ShellCommand struct {
	name string
	help string
	do   func([]string, *CommandEnv, io.Writer) error
}

func (c *s3ShellCommand) Name() string { return c.name }

func (c *s3ShellCommand) Help() string { return c.help }

func (c *s3ShellCommand) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return c.do(args, commandEnv, writer)
}

func (c *s3ShellCommand) HasTag(CommandTag) bool { return false }

type s3ShellStore interface {
	CreateUser(ctx context.Context, identity *iam_pb.Identity) error
	GetUser(ctx context.Context, username string) (*iam_pb.Identity, error)
	UpdateUser(ctx context.Context, username string, identity *iam_pb.Identity) error
	DeleteUser(ctx context.Context, username string) error
	ListUsers(ctx context.Context) ([]string, error)
	GetUserByAccessKey(ctx context.Context, accessKey string) (*iam_pb.Identity, error)
	CreateAccessKey(ctx context.Context, username string, credential *iam_pb.Credential) error
	DeleteAccessKey(ctx context.Context, username string, accessKey string) error
	GetPolicy(ctx context.Context, name string) (*policy_engine.PolicyDocument, error)
	GetPolicies(ctx context.Context) (map[string]policy_engine.PolicyDocument, error)
	CreatePolicy(ctx context.Context, name string, document policy_engine.PolicyDocument) error
	DeletePolicy(ctx context.Context, name string) error
	AttachUserPolicy(ctx context.Context, username string, policyName string) error
	DetachUserPolicy(ctx context.Context, username string, policyName string) error
	ListAttachedUserPolicies(ctx context.Context, username string) ([]string, error)
}

type s3UserCreateOptions struct {
	name                string
	accessKey           string
	secretKey           string
	generateCredentials bool
	accountID           string
	displayName         string
	email               string
}

type s3AccessKeyCreateOptions struct {
	username            string
	accessKey           string
	secretKey           string
	generateCredentials bool
}

func init() {
	Commands = append(Commands,
		&s3ShellCommand{
			name: "s3.user.list",
			help: `list S3 users with source and status`,
			do:   runS3UserListCommand,
		},
		&s3ShellCommand{
			name: "s3.user.show",
			help: `show one S3 user's details

	s3.user.show -name alice`,
			do: runS3UserShowCommand,
		},
		&s3ShellCommand{
			name: "s3.user.create",
			help: `create a filer-backed S3 user

	s3.user.create -name alice -generate_credentials
	s3.user.create -name alice -access_key AKIA... -secret_key secret
	s3.user.create -name alice -email alice@example.com -display_name "Alice"`,
			do: runS3UserCreateCommand,
		},
		&s3ShellCommand{
			name: "s3.user.delete",
			help: `delete a filer-backed S3 user

	s3.user.delete -name alice`,
			do: runS3UserDeleteCommand,
		},
		&s3ShellCommand{
			name: "s3.user.enable",
			help: `enable a filer-backed S3 user

	s3.user.enable -name alice`,
			do: runS3UserEnableCommand,
		},
		&s3ShellCommand{
			name: "s3.user.disable",
			help: `disable a filer-backed S3 user

	s3.user.disable -name alice`,
			do: runS3UserDisableCommand,
		},
		&s3ShellCommand{
			name: "s3.user.accesskey.list",
			help: `list access keys for a filer-backed S3 user

	s3.user.accesskey.list -user alice`,
			do: runS3UserAccessKeyListCommand,
		},
		&s3ShellCommand{
			name: "s3.user.accesskey.create",
			help: `create an access key for a filer-backed S3 user

	s3.user.accesskey.create -user alice -generate_credentials
	s3.user.accesskey.create -user alice -access_key AKIA... -secret_key secret`,
			do: runS3UserAccessKeyCreateCommand,
		},
		&s3ShellCommand{
			name: "s3.user.accesskey.delete",
			help: `delete an access key for a filer-backed S3 user

	s3.user.accesskey.delete -user alice -access_key AKIA...`,
			do: runS3UserAccessKeyDeleteCommand,
		},
	)
}

func runS3UserListCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.list", flag.ContinueOnError)
	fs.SetOutput(writer)
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserList(ctx, store, writer)
	})
}

func runS3UserShowCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.show", flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "user name")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserShow(ctx, store, *name, writer)
	})
}

func runS3UserCreateCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.create", flag.ContinueOnError)
	fs.SetOutput(writer)
	opts := s3UserCreateOptions{}
	fs.StringVar(&opts.name, "name", "", "user name")
	fs.StringVar(&opts.accessKey, "access_key", "", "access key to create")
	fs.StringVar(&opts.secretKey, "secret_key", "", "secret key to create")
	fs.BoolVar(&opts.generateCredentials, "generate_credentials", false, "generate an initial access key pair")
	fs.StringVar(&opts.accountID, "account_id", "", "account id")
	fs.StringVar(&opts.displayName, "display_name", "", "account display name")
	fs.StringVar(&opts.email, "email", "", "account email")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if opts.name == "" {
		return fmt.Errorf("-name is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserCreate(ctx, store, opts, writer)
	})
}

func runS3UserDeleteCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.delete", flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "user name")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserDelete(ctx, store, *name, writer)
	})
}

func runS3UserEnableCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return runS3UserStatusCommand("s3.user.enable", false, args, commandEnv, writer)
}

func runS3UserDisableCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return runS3UserStatusCommand("s3.user.disable", true, args, commandEnv, writer)
}

func runS3UserStatusCommand(commandName string, disabled bool, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(commandName, flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "user name")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserSetDisabled(ctx, store, *name, disabled, writer)
	})
}

func runS3UserAccessKeyListCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.accesskey.list", flag.ContinueOnError)
	fs.SetOutput(writer)
	user := fs.String("user", "", "user name")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *user == "" {
		return fmt.Errorf("-user is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserAccessKeyList(ctx, store, *user, writer)
	})
}

func runS3UserAccessKeyCreateCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.accesskey.create", flag.ContinueOnError)
	fs.SetOutput(writer)
	opts := s3AccessKeyCreateOptions{}
	fs.StringVar(&opts.username, "user", "", "user name")
	fs.StringVar(&opts.accessKey, "access_key", "", "access key to create")
	fs.StringVar(&opts.secretKey, "secret_key", "", "secret key to create")
	fs.BoolVar(&opts.generateCredentials, "generate_credentials", false, "generate an access key pair")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if opts.username == "" {
		return fmt.Errorf("-user is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserAccessKeyCreate(ctx, store, opts, writer)
	})
}

func runS3UserAccessKeyDeleteCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.user.accesskey.delete", flag.ContinueOnError)
	fs.SetOutput(writer)
	user := fs.String("user", "", "user name")
	accessKey := fs.String("access_key", "", "access key to delete")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *user == "" {
		return fmt.Errorf("-user is required")
	}
	if *accessKey == "" {
		return fmt.Errorf("-access_key is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3UserAccessKeyDelete(ctx, store, *user, *accessKey, writer)
	})
}

func runS3UserList(ctx context.Context, store s3ShellStore, writer io.Writer) error {
	usernames, err := store.ListUsers(ctx)
	if err != nil {
		return err
	}
	sort.Strings(usernames)

	tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tSOURCE\tSTATUS\tACCESS KEYS\tPOLICIES")
	for _, username := range usernames {
		identity, err := store.GetUser(ctx, username)
		if err != nil {
			return err
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\n",
			identity.Name,
			s3IdentitySource(identity),
			s3IdentityStatus(identity),
			len(identity.Credentials),
			len(identity.PolicyNames),
		)
	}
	return tw.Flush()
}

func runS3UserShow(ctx context.Context, store s3ShellStore, username string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	writeS3IdentityDetails(writer, identity)
	return nil
}

func runS3UserCreate(ctx context.Context, store s3ShellStore, opts s3UserCreateOptions, writer io.Writer) error {
	existing, err := store.GetUser(ctx, opts.name)
	switch {
	case err == nil && existing != nil:
		if existing.IsStatic {
			return fmt.Errorf("user %q already exists in -s3.config; edit the static config for bootstrap users", opts.name)
		}
		return fmt.Errorf("user %q already exists", opts.name)
	case err != nil && !s3ShellIsNotFound(err):
		return err
	}

	identity := &iam_pb.Identity{Name: opts.name}
	if opts.accountID != "" || opts.displayName != "" || opts.email != "" {
		identity.Account = &iam_pb.Account{
			Id:           opts.accountID,
			DisplayName:  opts.displayName,
			EmailAddress: opts.email,
		}
	}

	createdCredential, err := s3BuildCredentialForCreate(ctx, store, opts.accessKey, opts.secretKey, opts.generateCredentials || opts.accessKey != "" || opts.secretKey != "")
	if err != nil {
		return err
	}
	if createdCredential != nil {
		identity.Credentials = []*iam_pb.Credential{createdCredential}
	}

	if err := store.CreateUser(ctx, identity); err != nil {
		if s3ShellIsAlreadyExists(err) {
			return fmt.Errorf("user %q already exists", opts.name)
		}
		return err
	}

	fmt.Fprintf(writer, "Created user %q.\n", opts.name)
	if createdCredential != nil {
		writeCreatedCredential(writer, createdCredential)
	}
	return nil
}

func runS3UserDelete(ctx context.Context, store s3ShellStore, username string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, "delete"); err != nil {
		return err
	}
	if err := store.DeleteUser(ctx, username); err != nil {
		if s3ShellIsNotFound(err) {
			return fmt.Errorf("user %q not found", username)
		}
		return err
	}
	fmt.Fprintf(writer, "Deleted user %q.\n", username)
	return nil
}

func runS3UserSetDisabled(ctx context.Context, store s3ShellStore, username string, disabled bool, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, ternary(disabled, "disable", "enable")); err != nil {
		return err
	}
	if identity.Disabled == disabled {
		fmt.Fprintf(writer, "User %q is already %s.\n", username, s3IdentityStatus(identity))
		return nil
	}

	updated := proto.Clone(identity).(*iam_pb.Identity)
	updated.Disabled = disabled
	if err := store.UpdateUser(ctx, username, updated); err != nil {
		return err
	}
	if disabled {
		fmt.Fprintf(writer, "Disabled user %q.\n", username)
	} else {
		fmt.Fprintf(writer, "Enabled user %q.\n", username)
	}
	return nil
}

func runS3UserAccessKeyList(ctx context.Context, store s3ShellStore, username string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}

	tw := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "USER\tACCESS KEY\tSTATUS")
	for _, credential := range sortedCredentials(identity.Credentials) {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", username, credential.AccessKey, s3CredentialStatus(credential))
	}
	if len(identity.Credentials) == 0 {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", username, "-", "-")
	}
	return tw.Flush()
}

func runS3UserAccessKeyCreate(ctx context.Context, store s3ShellStore, opts s3AccessKeyCreateOptions, writer io.Writer) error {
	identity, err := getS3User(ctx, store, opts.username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, "create access keys for"); err != nil {
		return err
	}

	createdCredential, err := s3BuildCredentialForCreate(ctx, store, opts.accessKey, opts.secretKey, opts.generateCredentials || opts.accessKey != "" || opts.secretKey != "")
	if err != nil {
		return err
	}
	if createdCredential == nil {
		return fmt.Errorf("set -generate_credentials or provide -access_key and/or -secret_key")
	}

	if err := store.CreateAccessKey(ctx, opts.username, createdCredential); err != nil {
		if s3ShellIsAlreadyExists(err) {
			return fmt.Errorf("access key %q is already in use", createdCredential.AccessKey)
		}
		return err
	}

	fmt.Fprintf(writer, "Created access key for user %q.\n", opts.username)
	writeCreatedCredential(writer, createdCredential)
	return nil
}

func runS3UserAccessKeyDelete(ctx context.Context, store s3ShellStore, username string, accessKey string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, "delete access keys for"); err != nil {
		return err
	}
	if err := store.DeleteAccessKey(ctx, username, accessKey); err != nil {
		if s3ShellIsNotFound(err) {
			return fmt.Errorf("access key %q not found for user %q", accessKey, username)
		}
		return err
	}
	fmt.Fprintf(writer, "Deleted access key %q for user %q.\n", accessKey, username)
	return nil
}

func withS3ShellStore(commandEnv *CommandEnv, fn func(ctx context.Context, store s3ShellStore) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store := &iamgrpc.IamGrpcStore{}
	store.SetFilerAddressFunc(func() pb.ServerAddress {
		return commandEnv.option.FilerAddress
	}, commandEnv.option.GrpcDialOption)

	return fn(ctx, store)
}

func ensureMutableIdentity(identity *iam_pb.Identity, action string) error {
	if identity != nil && identity.IsStatic {
		return fmt.Errorf("cannot %s user %q because it comes from -s3.config; edit the static config for bootstrap users", action, identity.Name)
	}
	return nil
}

func getS3User(ctx context.Context, store s3ShellStore, username string) (*iam_pb.Identity, error) {
	identity, err := store.GetUser(ctx, username)
	if err != nil {
		if s3ShellIsNotFound(err) {
			return nil, fmt.Errorf("user %q not found", username)
		}
		return nil, err
	}
	return identity, nil
}

func s3BuildCredentialForCreate(ctx context.Context, store s3ShellStore, accessKey, secretKey string, shouldCreate bool) (*iam_pb.Credential, error) {
	if !shouldCreate {
		return nil, nil
	}

	if accessKey == "" {
		var err error
		accessKey, err = generateUniqueAccessKey(ctx, store)
		if err != nil {
			return nil, err
		}
	} else {
		if err := ensureAccessKeyAvailable(ctx, store, accessKey); err != nil {
			return nil, err
		}
	}
	if secretKey == "" {
		var err error
		secretKey, err = weediam.GenerateSecretAccessKey()
		if err != nil {
			return nil, fmt.Errorf("generate secret key: %w", err)
		}
	}

	return &iam_pb.Credential{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Status:    weediam.AccessKeyStatusActive,
	}, nil
}

func ensureAccessKeyAvailable(ctx context.Context, store s3ShellStore, accessKey string) error {
	_, err := store.GetUserByAccessKey(ctx, accessKey)
	if err == nil {
		return fmt.Errorf("%w: %s", errS3AccessKeyInUse, accessKey)
	}
	if s3ShellIsNotFound(err) {
		return nil
	}
	return err
}

func generateUniqueAccessKey(ctx context.Context, store s3ShellStore) (string, error) {
	for range 16 {
		accessKey, err := generateAccessKey()
		if err != nil {
			return "", err
		}
		if err := ensureAccessKeyAvailable(ctx, store, accessKey); err == nil {
			return accessKey, nil
		} else if !errors.Is(err, errS3AccessKeyInUse) {
			return "", err
		}
	}
	return "", fmt.Errorf("failed to generate a unique access key")
}

func generateAccessKey() (string, error) {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var raw [20]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate access key: %w", err)
	}
	for i := range raw {
		raw[i] = charset[int(raw[i])%len(charset)]
	}
	return string(raw[:]), nil
}

func s3IdentitySource(identity *iam_pb.Identity) string {
	if identity != nil && identity.IsStatic {
		return "static"
	}
	return "dynamic"
}

func s3IdentityStatus(identity *iam_pb.Identity) string {
	if identity != nil && identity.Disabled {
		return "disabled"
	}
	return "enabled"
}

func s3CredentialStatus(credential *iam_pb.Credential) string {
	if credential == nil || credential.Status == "" {
		return weediam.AccessKeyStatusActive
	}
	return credential.Status
}

func sortedCredentials(credentials []*iam_pb.Credential) []*iam_pb.Credential {
	out := append([]*iam_pb.Credential(nil), credentials...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].AccessKey < out[j].AccessKey
	})
	return out
}

func writeCreatedCredential(writer io.Writer, credential *iam_pb.Credential) {
	fmt.Fprintf(writer, "Access Key: %s\n", credential.AccessKey)
	fmt.Fprintf(writer, "Secret Key: %s\n", credential.SecretKey)
}

func writeS3IdentityDetails(writer io.Writer, identity *iam_pb.Identity) {
	fmt.Fprintf(writer, "Name: %s\n", identity.Name)
	fmt.Fprintf(writer, "Source: %s\n", s3IdentitySource(identity))
	fmt.Fprintf(writer, "Status: %s\n", s3IdentityStatus(identity))
	if identity.Account != nil {
		if identity.Account.Id != "" {
			fmt.Fprintf(writer, "Account ID: %s\n", identity.Account.Id)
		}
		if identity.Account.DisplayName != "" {
			fmt.Fprintf(writer, "Display Name: %s\n", identity.Account.DisplayName)
		}
		if identity.Account.EmailAddress != "" {
			fmt.Fprintf(writer, "Email: %s\n", identity.Account.EmailAddress)
		}
	}

	if len(identity.Credentials) == 0 {
		fmt.Fprintln(writer, "Access Keys: none")
	} else {
		fmt.Fprintln(writer, "Access Keys:")
		for _, credential := range sortedCredentials(identity.Credentials) {
			fmt.Fprintf(writer, "  %s (%s)\n", credential.AccessKey, s3CredentialStatus(credential))
		}
	}

	if len(identity.PolicyNames) == 0 {
		fmt.Fprintln(writer, "Policies: none")
	} else {
		policies := append([]string(nil), identity.PolicyNames...)
		sort.Strings(policies)
		fmt.Fprintf(writer, "Policies: %s\n", strings.Join(policies, ", "))
	}

	if len(identity.Actions) == 0 {
		fmt.Fprintln(writer, "Actions: none")
	} else {
		actions := append([]string(nil), identity.Actions...)
		sort.Strings(actions)
		fmt.Fprintf(writer, "Actions: %s\n", strings.Join(actions, ", "))
	}
}

func s3ShellIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, credential.ErrUserNotFound) ||
		errors.Is(err, credential.ErrAccessKeyNotFound) ||
		errors.Is(err, credential.ErrPolicyNotFound) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return true
	}
	return false
}

func s3ShellIsAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, credential.ErrUserAlreadyExists) {
		return true
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
		return true
	}
	return false
}

func ternary[T any](cond bool, onTrue, onFalse T) T {
	if cond {
		return onTrue
	}
	return onFalse
}
