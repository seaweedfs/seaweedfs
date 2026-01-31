package shell

import (
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func init() {
	Commands = append(Commands, &commandS3TablesNamespace{})
}

type commandS3TablesNamespace struct{}

func (c *commandS3TablesNamespace) Name() string {
	return "s3tables.namespace"
}

func (c *commandS3TablesNamespace) Help() string {
	return `manage s3tables namespaces

# create a namespace
s3tables.namespace -create -bucket <bucket> -account <account_id> -name <namespace>

# list namespaces
s3tables.namespace -list -bucket <bucket> -account <account_id> [-prefix <prefix>] [-limit <n>] [-continuation <token>]

# get namespace details
s3tables.namespace -get -bucket <bucket> -account <account_id> -name <namespace>

# delete namespace
s3tables.namespace -delete -bucket <bucket> -account <account_id> -name <namespace>
`
}

func (c *commandS3TablesNamespace) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3TablesNamespace) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	cmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	create := cmd.Bool("create", false, "create namespace")
	list := cmd.Bool("list", false, "list namespaces")
	get := cmd.Bool("get", false, "get namespace")
	deleteNamespace := cmd.Bool("delete", false, "delete namespace")

	bucketName := cmd.String("bucket", "", "table bucket name")
	account := cmd.String("account", "", "owner account id")
	name := cmd.String("name", "", "namespace name")
	prefix := cmd.String("prefix", "", "namespace prefix")
	limit := cmd.Int("limit", 100, "max namespaces to return")
	continuation := cmd.String("continuation", "", "continuation token")

	if err := cmd.Parse(args); err != nil {
		return err
	}

	actions := []*bool{create, list, get, deleteNamespace}
	count := 0
	for _, action := range actions {
		if *action {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("exactly one action must be specified")
	}
	if *bucketName == "" {
		return fmt.Errorf("-bucket is required")
	}
	if *account == "" {
		return fmt.Errorf("-account is required")
	}

	bucketArn, err := buildS3TablesBucketARN(*bucketName, *account)
	if err != nil {
		return err
	}

	namespace := strings.TrimSpace(*name)
	if (namespace == "" || namespace == "-") && (*create || *get || *deleteNamespace) {
		return fmt.Errorf("-name is required")
	}

	switch {
	case *create:
		req := &s3tables.CreateNamespaceRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}}
		var resp s3tables.CreateNamespaceResponse
		if err := executeS3Tables(commandEnv, "CreateNamespace", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "Namespace: %s\n", strings.Join(resp.Namespace, "/"))
	case *list:
		req := &s3tables.ListNamespacesRequest{TableBucketARN: bucketArn, Prefix: *prefix, ContinuationToken: *continuation, MaxNamespaces: *limit}
		var resp s3tables.ListNamespacesResponse
		if err := executeS3Tables(commandEnv, "ListNamespaces", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		if len(resp.Namespaces) == 0 {
			fmt.Fprintln(writer, "No namespaces found")
			return nil
		}
		for _, ns := range resp.Namespaces {
			fmt.Fprintf(writer, "Namespace: %s\n", strings.Join(ns.Namespace, "/"))
			fmt.Fprintf(writer, "CreatedAt: %s\n", ns.CreatedAt.Format(timeFormat))
			fmt.Fprintln(writer, "---")
		}
		if resp.ContinuationToken != "" {
			fmt.Fprintf(writer, "ContinuationToken: %s\n", resp.ContinuationToken)
		}
	case *get:
		req := &s3tables.GetNamespaceRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}}
		var resp s3tables.GetNamespaceResponse
		if err := executeS3Tables(commandEnv, "GetNamespace", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "Namespace: %s\n", strings.Join(resp.Namespace, "/"))
		fmt.Fprintf(writer, "OwnerAccountID: %s\n", resp.OwnerAccountID)
		fmt.Fprintf(writer, "CreatedAt: %s\n", resp.CreatedAt.Format(timeFormat))
	case *deleteNamespace:
		req := &s3tables.DeleteNamespaceRequest{TableBucketARN: bucketArn, Namespace: []string{namespace}}
		if err := executeS3Tables(commandEnv, "DeleteNamespace", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Namespace deleted")
	}
	return nil
}
