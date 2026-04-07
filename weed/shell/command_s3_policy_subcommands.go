package shell

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func init() {
	Commands = append(Commands,
		&s3ShellCommand{
			name: "s3.policy.create",
			help: `create or update a managed S3 policy

	s3.policy.create -name photos-rw -file policy.json`,
			do: runS3PolicyCreateCommand,
		},
		&s3ShellCommand{
			name: "s3.policy.show",
			help: `show one managed S3 policy

	s3.policy.show -name photos-rw`,
			do: runS3PolicyShowCommand,
		},
		&s3ShellCommand{
			name: "s3.policy.list",
			help: `list managed S3 policies`,
			do:   runS3PolicyListCommand,
		},
		&s3ShellCommand{
			name: "s3.policy.delete",
			help: `delete a managed S3 policy

	s3.policy.delete -name photos-rw`,
			do: runS3PolicyDeleteCommand,
		},
		&s3ShellCommand{
			name: "s3.policy.attach",
			help: `attach a managed S3 policy to a filer-backed user

	s3.policy.attach -user alice -name photos-rw`,
			do: runS3PolicyAttachCommand,
		},
		&s3ShellCommand{
			name: "s3.policy.detach",
			help: `detach a managed S3 policy from a filer-backed user

	s3.policy.detach -user alice -name photos-rw`,
			do: runS3PolicyDetachCommand,
		},
	)
}

func runS3PolicyCreateCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.policy.create", flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "policy name")
	file := fs.String("file", "", "policy file (json)")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}
	if *file == "" {
		return fmt.Errorf("-file is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3PolicyCreate(ctx, store, *name, *file, writer)
	})
}

func runS3PolicyShowCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.policy.show", flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "policy name")
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
		return runS3PolicyShow(ctx, store, *name, writer)
	})
}

func runS3PolicyListCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.policy.list", flag.ContinueOnError)
	fs.SetOutput(writer)
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		return runS3PolicyList(ctx, store, writer)
	})
}

func runS3PolicyDeleteCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet("s3.policy.delete", flag.ContinueOnError)
	fs.SetOutput(writer)
	name := fs.String("name", "", "policy name")
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
		return runS3PolicyDelete(ctx, store, *name, writer)
	})
}

func runS3PolicyAttachCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return runS3PolicyAttachDetachCommand("s3.policy.attach", true, args, commandEnv, writer)
}

func runS3PolicyDetachCommand(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	return runS3PolicyAttachDetachCommand("s3.policy.detach", false, args, commandEnv, writer)
}

func runS3PolicyAttachDetachCommand(commandName string, attach bool, args []string, commandEnv *CommandEnv, writer io.Writer) error {
	fs := flag.NewFlagSet(commandName, flag.ContinueOnError)
	fs.SetOutput(writer)
	user := fs.String("user", "", "user name")
	name := fs.String("name", "", "policy name")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if *user == "" {
		return fmt.Errorf("-user is required")
	}
	if *name == "" {
		return fmt.Errorf("-name is required")
	}

	return withS3ShellStore(commandEnv, func(ctx context.Context, store s3ShellStore) error {
		if attach {
			return runS3PolicyAttach(ctx, store, *user, *name, writer)
		}
		return runS3PolicyDetach(ctx, store, *user, *name, writer)
	})
}

func runS3PolicyCreate(ctx context.Context, store s3ShellStore, name, file string, writer io.Writer) error {
	document, err := loadPolicyDocumentFromFile(file)
	if err != nil {
		return err
	}
	if err := store.CreatePolicy(ctx, name, *document); err != nil {
		return err
	}
	fmt.Fprintf(writer, "Saved policy %q.\n", name)
	return nil
}

func runS3PolicyShow(ctx context.Context, store s3ShellStore, name string, writer io.Writer) error {
	document, err := store.GetPolicy(ctx, name)
	if err != nil {
		return err
	}
	if document == nil {
		return fmt.Errorf("policy %q not found", name)
	}
	formatted, err := marshalPolicyDocument(document)
	if err != nil {
		return err
	}
	fmt.Fprintln(writer, formatted)
	return nil
}

func runS3PolicyList(ctx context.Context, store s3ShellStore, writer io.Writer) error {
	policies, err := store.GetPolicies(ctx)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(policies))
	for name := range policies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintln(writer, name)
	}
	return nil
}

func runS3PolicyDelete(ctx context.Context, store s3ShellStore, name string, writer io.Writer) error {
	if err := store.DeletePolicy(ctx, name); err != nil {
		if s3ShellIsNotFound(err) {
			return fmt.Errorf("policy %q not found", name)
		}
		return err
	}
	fmt.Fprintf(writer, "Deleted policy %q.\n", name)
	return nil
}

func runS3PolicyAttach(ctx context.Context, store s3ShellStore, username, policyName string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, "attach policies to"); err != nil {
		return err
	}
	if err := store.AttachUserPolicy(ctx, username, policyName); err != nil {
		if errors.Is(err, credential.ErrPolicyNotFound) {
			return fmt.Errorf("policy %q not found", policyName)
		}
		return err
	}
	fmt.Fprintf(writer, "Attached policy %q to user %q.\n", policyName, username)
	return nil
}

func runS3PolicyDetach(ctx context.Context, store s3ShellStore, username, policyName string, writer io.Writer) error {
	identity, err := getS3User(ctx, store, username)
	if err != nil {
		return err
	}
	if err := ensureMutableIdentity(identity, "detach policies from"); err != nil {
		return err
	}
	if err := store.DetachUserPolicy(ctx, username, policyName); err != nil {
		if errors.Is(err, credential.ErrPolicyNotAttached) {
			return fmt.Errorf("policy %q is not attached to user %q", policyName, username)
		}
		return err
	}
	fmt.Fprintf(writer, "Detached policy %q from user %q.\n", policyName, username)
	return nil
}

func loadPolicyDocumentFromFile(file string) (*policy_engine.PolicyDocument, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read policy file: %v", err)
	}
	var document policy_engine.PolicyDocument
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("invalid policy json: %v", err)
	}
	return &document, nil
}

func marshalPolicyDocument(document *policy_engine.PolicyDocument) (string, error) {
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
