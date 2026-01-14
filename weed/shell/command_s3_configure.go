package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func init() {
	Commands = append(Commands, &commandS3Configure{})
}

type commandS3Configure struct {
}

func (c *commandS3Configure) Name() string {
	return "s3.configure"
}

func (c *commandS3Configure) Help() string {
	return `configure and apply s3 options for each bucket

	# see the current configuration file content
	s3.configure

	# create a new identity with account information
	s3.configure -user=username -actions=Read,Write,List,Tagging -buckets=bucket-name -access_key=key -secret_key=secret -account_id=id -account_display_name=name -account_email=email@example.com -apply
	`
}

func (c *commandS3Configure) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3Configure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	s3ConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	actions := s3ConfigureCommand.String("actions", "", "comma separated actions names: Read,Write,List,Tagging,Admin")
	user := s3ConfigureCommand.String("user", "", "user name")
	buckets := s3ConfigureCommand.String("buckets", "", "bucket name")
	accessKey := s3ConfigureCommand.String("access_key", "", "specify the access key")
	secretKey := s3ConfigureCommand.String("secret_key", "", "specify the secret key")
	accountId := s3ConfigureCommand.String("account_id", "", "specify the account id")
	accountDisplayName := s3ConfigureCommand.String("account_display_name", "", "specify the account display name")
	accountEmail := s3ConfigureCommand.String("account_email", "", "specify the account email address")
	isDelete := s3ConfigureCommand.Bool("delete", false, "delete users, actions or access keys")
	apply := s3ConfigureCommand.Bool("apply", false, "update and apply s3 configuration")
	if err = s3ConfigureCommand.Parse(args); err != nil {
		return nil
	}

	// Try to detect if a credential backend is configured
	credConfig, err := credential.LoadCredentialConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load credential configuration: %w", err)
	}

	// Use credential manager if a backend is configured
	var credentialManager *credential.CredentialManager
	useCredentialManager := false
	if credConfig != nil {
		glog.V(0).Infof("Credential backend detected: %s. Using credential manager for s3.configure", credConfig.Store)
		credentialManager, err = credential.NewCredentialManager(
			credential.CredentialStoreTypeName(credConfig.Store),
			credConfig.Config,
			credConfig.Prefix,
		)
		if err != nil {
			return fmt.Errorf("failed to initialize credential manager: %w", err)
		}
		defer credentialManager.Shutdown()
		useCredentialManager = true
	} else {
		glog.V(1).Info("No credential backend configured. Using filer-based storage for s3.configure")
	}

	// Check which account flags were provided and build update functions
	var accountUpdates []func(*iam_pb.Account)
	s3ConfigureCommand.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "account_id":
			accountUpdates = append(accountUpdates, func(a *iam_pb.Account) { a.Id = *accountId })
		case "account_display_name":
			accountUpdates = append(accountUpdates, func(a *iam_pb.Account) { a.DisplayName = *accountDisplayName })
		case "account_email":
			accountUpdates = append(accountUpdates, func(a *iam_pb.Account) { a.EmailAddress = *accountEmail })
		}
	})

	// Helper function to update account information on an identity
	updateAccountInfo := func(account **iam_pb.Account) {
		if len(accountUpdates) > 0 {
			if *account == nil {
				*account = &iam_pb.Account{}
			}
			for _, update := range accountUpdates {
				update(*account)
			}
		}
	}

	// Load configuration from appropriate source
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if useCredentialManager {
		// Load from credential manager
		ctx := context.Background()
		config, err := credentialManager.LoadConfiguration(ctx)
		if err != nil {
			return fmt.Errorf("failed to load configuration from credential manager: %w", err)
		}
		if config != nil {
			s3cfg = config
		}
	} else {
		// Load from filer (legacy behavior)
		var buf bytes.Buffer
		if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.ReadEntry(commandEnv.MasterClient, client, filer.IamConfigDirectory, filer.IamIdentityFile, &buf)
		}); err != nil && err != filer_pb.ErrNotFound {
			return err
		}

		if buf.Len() > 0 {
			if err = filer.ParseS3ConfigurationFromBytes(buf.Bytes(), s3cfg); err != nil {
				return err
			}
		}
	}

	idx := 0
	changed := false
	if *user != "" {
		for i, identity := range s3cfg.Identities {
			if *user == identity.Name {
				idx = i
				changed = true
				break
			}
		}
	}
	var cmdActions []string
	for _, action := range strings.Split(*actions, ",") {
		if *buckets == "" {
			cmdActions = append(cmdActions, action)
		} else {
			for _, bucket := range strings.Split(*buckets, ",") {
				cmdActions = append(cmdActions, fmt.Sprintf("%s:%s", action, bucket))
			}
		}
	}
	if changed {
		infoAboutSimulationMode(writer, *apply, "-apply")
		if *isDelete {
			var exists []int
			for _, cmdAction := range cmdActions {
				for i, currentAction := range s3cfg.Identities[idx].Actions {
					if cmdAction == currentAction {
						exists = append(exists, i)
					}
				}
			}
			sort.Sort(sort.Reverse(sort.IntSlice(exists)))
			for _, i := range exists {
				s3cfg.Identities[idx].Actions = append(
					s3cfg.Identities[idx].Actions[:i],
					s3cfg.Identities[idx].Actions[i+1:]...,
				)
			}
			if *accessKey != "" {
				exists = []int{}
				for i, credential := range s3cfg.Identities[idx].Credentials {
					if credential.AccessKey == *accessKey {
						exists = append(exists, i)
					}
				}
				sort.Sort(sort.Reverse(sort.IntSlice(exists)))
				for _, i := range exists {
					s3cfg.Identities[idx].Credentials = append(
						s3cfg.Identities[idx].Credentials[:i],
						s3cfg.Identities[idx].Credentials[i+1:]...,
					)
				}

			}
			if *actions == "" && *accessKey == "" && *buckets == "" {
				s3cfg.Identities = append(s3cfg.Identities[:idx], s3cfg.Identities[idx+1:]...)
			}
		} else {
			if *actions != "" {
				for _, cmdAction := range cmdActions {
					found := false
					for _, action := range s3cfg.Identities[idx].Actions {
						if cmdAction == action {
							found = true
							break
						}
					}
					if !found {
						s3cfg.Identities[idx].Actions = append(s3cfg.Identities[idx].Actions, cmdAction)
					}
				}
			}
			if *accessKey != "" && *user != "anonymous" {
				found := false
				for _, credential := range s3cfg.Identities[idx].Credentials {
					if credential.AccessKey == *accessKey {
						found = true
						credential.SecretKey = *secretKey
						break
					}
				}
				if !found {
					s3cfg.Identities[idx].Credentials = append(s3cfg.Identities[idx].Credentials, &iam_pb.Credential{
						AccessKey: *accessKey,
						SecretKey: *secretKey,
					})
				}
			}
			// Update account information if provided
			updateAccountInfo(&s3cfg.Identities[idx].Account)
		}
	} else if *user != "" && *actions != "" {
		infoAboutSimulationMode(writer, *apply, "-apply")
		identity := iam_pb.Identity{
			Name:        *user,
			Actions:     cmdActions,
			Credentials: []*iam_pb.Credential{},
		}
		if *user != "anonymous" {
			identity.Credentials = append(identity.Credentials,
				&iam_pb.Credential{AccessKey: *accessKey, SecretKey: *secretKey})
		}
		// Add account information if provided
		updateAccountInfo(&identity.Account)
		s3cfg.Identities = append(s3cfg.Identities, &identity)
	}

	if err = filer.CheckDuplicateAccessKey(s3cfg); err != nil {
		return err
	}

	// Display configuration
	var buf bytes.Buffer
	filer.ProtoToText(&buf, s3cfg)
	fmt.Fprint(writer, buf.String())
	fmt.Fprintln(writer)

	// Save configuration if apply flag is set
	if *apply {
		if useCredentialManager {
			// Save to credential manager
			ctx := context.Background()
			if err := credentialManager.SaveConfiguration(ctx, s3cfg); err != nil {
				return fmt.Errorf("failed to save configuration to credential manager: %w", err)
			}
			glog.V(0).Infof("Configuration saved to credential backend: %s", credConfig.Store)
		} else {
			// Save to filer (legacy behavior)
			if err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				return filer.SaveInsideFiler(client, filer.IamConfigDirectory, filer.IamIdentityFile, buf.Bytes())
			}); err != nil {
				return err
			}
		}
	}

	return nil
}
