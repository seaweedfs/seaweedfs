package shell

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func init() {
	Commands = append(Commands, &commandS3TablesTable{})
}

type commandS3TablesTable struct{}

func (c *commandS3TablesTable) Name() string {
	return "s3tables.table"
}

func (c *commandS3TablesTable) Help() string {
	return `manage s3tables tables

# create a table
s3tables.table -create -bucket <bucket> -account <account_id> -namespace <namespace> -name <table> -format ICEBERG [-metadata metadata.json] [-tags key=value]

# list tables
s3tables.table -list -bucket <bucket> -account <account_id> [-namespace <namespace>] [-prefix <prefix>] [-limit <n>] [-continuation <token>]

# get table details
s3tables.table -get -bucket <bucket> -account <account_id> -namespace <namespace> -name <table>

# delete table
s3tables.table -delete -bucket <bucket> -account <account_id> -namespace <namespace> -name <table> [-version <token>]

# manage table policy
s3tables.table -put-policy -bucket <bucket> -account <account_id> -namespace <namespace> -name <table> -file policy.json
s3tables.table -get-policy -bucket <bucket> -account <account_id> -namespace <namespace> -name <table>
s3tables.table -delete-policy -bucket <bucket> -account <account_id> -namespace <namespace> -name <table>
`
}

func (c *commandS3TablesTable) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3TablesTable) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	cmd := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	create := cmd.Bool("create", false, "create table")
	list := cmd.Bool("list", false, "list tables")
	get := cmd.Bool("get", false, "get table")
	deleteTable := cmd.Bool("delete", false, "delete table")
	putPolicy := cmd.Bool("put-policy", false, "put table policy")
	getPolicy := cmd.Bool("get-policy", false, "get table policy")
	deletePolicy := cmd.Bool("delete-policy", false, "delete table policy")

	bucketName := cmd.String("bucket", "", "table bucket name")
	account := cmd.String("account", "", "owner account id")
	namespace := cmd.String("namespace", "", "namespace")
	name := cmd.String("name", "", "table name")
	format := cmd.String("format", "ICEBERG", "table format")
	metadataFile := cmd.String("metadata", "", "table metadata json file")
	tags := cmd.String("tags", "", "comma separated tags key=value")
	prefix := cmd.String("prefix", "", "table name prefix")
	limit := cmd.Int("limit", 100, "max tables to return")
	continuation := cmd.String("continuation", "", "continuation token")
	version := cmd.String("version", "", "version token")
	policyFile := cmd.String("file", "", "policy file (json)")

	if err := cmd.Parse(args); err != nil {
		return err
	}

	actions := []*bool{create, list, get, deleteTable, putPolicy, getPolicy, deletePolicy}
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

	ns := strings.TrimSpace(*namespace)
	if (*create || *get || *deleteTable || *putPolicy || *getPolicy || *deletePolicy) && ns == "" {
		return fmt.Errorf("-namespace is required")
	}
	if (*create || *get || *deleteTable || *putPolicy || *getPolicy || *deletePolicy) && *name == "" {
		return fmt.Errorf("-name is required")
	}

	switch {
	case *create:
		var metadata *s3tables.TableMetadata
		if *metadataFile != "" {
			content, err := os.ReadFile(*metadataFile)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(content, &metadata); err != nil {
				return err
			}
		}
		req := &s3tables.CreateTableRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name, Format: *format, Metadata: metadata}
		if *tags != "" {
			parsed, err := parseS3TablesTags(*tags)
			if err != nil {
				return err
			}
			req.Tags = parsed
		}
		var resp s3tables.CreateTableResponse
		if err := executeS3Tables(commandEnv, "CreateTable", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "TableARN: %s\n", resp.TableARN)
		fmt.Fprintf(writer, "VersionToken: %s\n", resp.VersionToken)
	case *list:
		var nsList []string
		if ns != "" {
			nsList = []string{ns}
		}
		req := &s3tables.ListTablesRequest{TableBucketARN: bucketArn, Namespace: nsList, Prefix: *prefix, ContinuationToken: *continuation, MaxTables: *limit}
		var resp s3tables.ListTablesResponse
		if err := executeS3Tables(commandEnv, "ListTables", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		if len(resp.Tables) == 0 {
			fmt.Fprintln(writer, "No tables found")
			return nil
		}
		for _, table := range resp.Tables {
			fmt.Fprintf(writer, "Name: %s\n", table.Name)
			fmt.Fprintf(writer, "TableARN: %s\n", table.TableARN)
			fmt.Fprintf(writer, "Namespace: %s\n", strings.Join(table.Namespace, "/"))
			fmt.Fprintf(writer, "CreatedAt: %s\n", table.CreatedAt.Format(timeFormat))
			fmt.Fprintf(writer, "ModifiedAt: %s\n", table.ModifiedAt.Format(timeFormat))
			fmt.Fprintln(writer, "---")
		}
		if resp.ContinuationToken != "" {
			fmt.Fprintf(writer, "ContinuationToken: %s\n", resp.ContinuationToken)
		}
	case *get:
		req := &s3tables.GetTableRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name}
		var resp s3tables.GetTableResponse
		if err := executeS3Tables(commandEnv, "GetTable", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintf(writer, "Name: %s\n", resp.Name)
		fmt.Fprintf(writer, "TableARN: %s\n", resp.TableARN)
		fmt.Fprintf(writer, "Namespace: %s\n", strings.Join(resp.Namespace, "/"))
		fmt.Fprintf(writer, "OwnerAccountID: %s\n", resp.OwnerAccountID)
		fmt.Fprintf(writer, "CreatedAt: %s\n", resp.CreatedAt.Format(timeFormat))
		fmt.Fprintf(writer, "ModifiedAt: %s\n", resp.ModifiedAt.Format(timeFormat))
		fmt.Fprintf(writer, "VersionToken: %s\n", resp.VersionToken)
	case *deleteTable:
		req := &s3tables.DeleteTableRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name, VersionToken: *version}
		if err := executeS3Tables(commandEnv, "DeleteTable", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Table deleted")
	case *putPolicy:
		if *policyFile == "" {
			return fmt.Errorf("-file is required")
		}
		content, err := os.ReadFile(*policyFile)
		if err != nil {
			return err
		}
		req := &s3tables.PutTablePolicyRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name, ResourcePolicy: string(content)}
		if err := executeS3Tables(commandEnv, "PutTablePolicy", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Table policy updated")
	case *getPolicy:
		req := &s3tables.GetTablePolicyRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name}
		var resp s3tables.GetTablePolicyResponse
		if err := executeS3Tables(commandEnv, "GetTablePolicy", req, &resp, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, resp.ResourcePolicy)
	case *deletePolicy:
		req := &s3tables.DeleteTablePolicyRequest{TableBucketARN: bucketArn, Namespace: []string{ns}, Name: *name}
		if err := executeS3Tables(commandEnv, "DeleteTablePolicy", req, nil, *account); err != nil {
			return parseS3TablesError(err)
		}
		fmt.Fprintln(writer, "Table policy deleted")
	}
	return nil
}
