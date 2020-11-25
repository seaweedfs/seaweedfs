package shell

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/iam_pb"
	"github.com/chrislusf/seaweedfs/weed/s3iam"
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

	s3cfg := &iam_pb.S3ApiConfiguration{}
	ifs := &s3iam.IAMFilerStore{}
	if err = commandEnv.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		ifs = s3iam.NewIAMFilerStore(&client)
		if err := ifs.LoadIAMConfig(s3cfg); err != nil {
			return nil
		}
		return nil
	}); err != nil {
		return err
	}

	idx := 0
	changed := false
	if *user != "" && *buckets != "" {
		for i, identity := range s3cfg.Identities {
			if *user == identity.Name {
				idx = i
				changed = true
				break
			}
		}
	}
	var cmdActions []string
	for _, bucket := range strings.Split(*buckets, ",") {
		for _, action := range strings.Split(*actions, ",") {
			cmdActions = append(cmdActions, fmt.Sprintf("%s:%s", action, bucket))
		}
	}
	if changed {
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
						s3cfg.Identities[idx].Credentials[:i+1]...,
					)
				}

			}
			if *actions == "" && *accessKey == "" {
				s3cfg.Identities = append(s3cfg.Identities[:idx], s3cfg.Identities[idx+1:]...)
			}
		} else {
			s3cfg.Identities[idx].Actions = append(s3cfg.Identities[idx].Actions, cmdActions...)
			s3cfg.Identities[idx].Credentials = append(s3cfg.Identities[idx].Credentials, &iam_pb.Credential{
				AccessKey: *accessKey,
				SecretKey: *secretKey,
			})
		}
	} else {
		identity := iam_pb.Identity{
			Name:    *user,
			Actions: cmdActions,
		}
		identity.Credentials = append(identity.Credentials, &iam_pb.Credential{
			AccessKey: *accessKey,
			SecretKey: *secretKey,
		})
		s3cfg.Identities = append(s3cfg.Identities, &identity)
	}

	fmt.Fprintf(writer, fmt.Sprintf("%+v\n", s3cfg.Identities))
	fmt.Fprintln(writer)

	if *apply {
		if err := ifs.SaveIAMConfig(s3cfg); err != nil {
			return err
		}
	}

	return nil
}
