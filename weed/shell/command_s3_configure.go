package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/s3api"
	"io"
	"sort"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
	`
}

func (c *commandS3Configure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	s3ConfigureCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	actions := s3ConfigureCommand.String("actions", "", "actions names")
	user := s3ConfigureCommand.String("user", "", "user name")
	buckets := s3ConfigureCommand.String("buckets", "", "bucket name")
	accessKey := s3ConfigureCommand.String("access_key", "", "specify the access key")
	secretKey := s3ConfigureCommand.String("secret_key", "", "specify the secret key")
	isDelete := s3ConfigureCommand.Bool("delete", false, "delete users, actions or access keys")
	apply := s3ConfigureCommand.Bool("apply", false, "update and apply s3 configuration")

	if err = s3ConfigureCommand.Parse(args); err != nil {
		return nil
	}

	var identities []*s3api.Identity
	if err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: filer.DirectoryEtc,
			Name:      s3api.S3ConfName,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}
		if err = s3api.LoadS3configFromEntryExtended(&respLookupEntry.Entry.Extended, &identities); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	idx := 0
	changed := false
	if *user != "" && *buckets != "" {
		for i, identity := range identities {
			if *user == identity.Name {
				idx = i
				changed = true
				break
			}
		}
	}
	cmdActions := []s3api.Action{}
	for _, bucket := range strings.Split(*buckets, ",") {
		for _, action := range strings.Split(*actions, ",") {
			cmdActions = append(cmdActions, s3api.Action(fmt.Sprintf("%s:%s", action, bucket)))
		}
	}
	cmdCredential := &s3api.Credential{
		AccessKey: *accessKey,
		SecretKey: *secretKey,
	}

	if changed {
		if *isDelete {
			exists := []int{}
			for _, cmdAction := range cmdActions {
				for i, currentAction := range identities[idx].Actions {
					if cmdAction == currentAction {
						exists = append(exists, i)
					}
				}
			}
			sort.Sort(sort.Reverse(sort.IntSlice(exists)))
			for _, i := range exists {
				identities[idx].Actions = append(identities[idx].Actions[:i], identities[idx].Actions[i+1:]...)
			}
			if *accessKey != "" {
				exists = []int{}
				for i, credential := range identities[idx].Credentials {
					if credential.AccessKey == *accessKey {
						exists = append(exists, i)
					}
				}
				sort.Sort(sort.Reverse(sort.IntSlice(exists)))
				for _, i := range exists {
					identities[idx].Credentials = append(identities[idx].Credentials[:i], identities[idx].Credentials[:i+1]...)
				}

			}
			if *actions == "" && *accessKey == "" {
				identities = append(identities[:idx], identities[idx+1:]...)
			}
		} else {
			identities[idx].Actions = append(identities[idx].Actions, cmdActions...)
			identities[idx].Credentials = append(identities[idx].Credentials, &s3api.Credential{
				AccessKey: *accessKey,
				SecretKey: *secretKey,
			})
		}
	} else {
		identity := s3api.Identity{
			Name:    *user,
			Actions: cmdActions,
		}
		identity.Credentials = append(identity.Credentials, &s3api.Credential{
			AccessKey: *accessKey,
			SecretKey: *secretKey,
		})
		identities = append(identities, &identity)
	}

	fmt.Fprintf(writer, fmt.Sprintf("%+v\n", identities))
	fmt.Fprintln(writer)

	if !*apply {
		return nil
	}

	if err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: filer.DirectoryEtc,
			Name:      s3api.S3ConfName,
		}
		respLookupEntry, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			return err
		}
		if err = s3api.SaveS3configToEntryExtended(&respLookupEntry.Entry.Extended, &identities); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
