package command

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/lance"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
	"github.com/seaweedfs/seaweedfs/weed/shell"
)

func init() { shell.Commands = append(shell.Commands, &commandS3Vectors{}) }

type commandS3Vectors struct{}

func (c *commandS3Vectors) Name() string                 { return "s3vectors" }
func (c *commandS3Vectors) HasTag(shell.CommandTag) bool { return false }
func (c *commandS3Vectors) Help() string {
	return `manage vector buckets, indexes, policies, tags and vectors

s3vectors -operation CreateVectorBucket -request '{"vectorBucketName":"embeddings"}'
s3vectors -operation ListVectorBuckets
s3vectors -operation CreateIndex -file index.json
s3vectors -operation QueryVectors -file query.json

Requests use the S3 Vectors API JSON shape. The shell uses trusted cluster
credentials. Index and vector operations require a registered vector engine.
`
}

func (c *commandS3Vectors) Do(args []string, env *shell.CommandEnv, writer io.Writer) error {
	flags := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	operation := flags.String("operation", "", "S3 Vectors operation")
	request := flags.String("request", "", "request JSON")
	file := flags.String("file", "", "request JSON file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if !slices.Contains(s3vectors.Operations(), *operation) {
		return fmt.Errorf("unknown operation %q; supported: %v", *operation, s3vectors.Operations())
	}
	if *request != "" && *file != "" {
		return fmt.Errorf("use either -request or -file")
	}
	body := []byte(*request)
	if *file != "" {
		reader, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer reader.Close()
		body, err = io.ReadAll(io.LimitReader(reader, s3_constants.S3VectorsMaxRequestBodyBytes+1))
		if err != nil {
			return err
		}
	}
	if len(body) == 0 {
		body = []byte(`{}`)
	}
	if len(body) > s3_constants.S3VectorsMaxRequestBodyBytes {
		return fmt.Errorf("request is too large")
	}
	if !json.Valid(body) {
		return fmt.Errorf("request must be valid JSON")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend := s3vectors.NewVectorDiscovery(env.MasterClient.ListCachedMasters, env.GrpcDialOption(), env.MasterClient.FilerGroup)
	defer backend.Close()
	server := s3vectors.NewServer(env, nil, backend)
	server.SetRegion(s3tables.DefaultRegion)
	server.SetAccountID(s3tables.DefaultAccountID)
	catalog := lance.NewServer(env, nil)
	server.SetDescribe(func(ctx context.Context, id []string) (string, map[string]string, error) {
		r := (&http.Request{Method: http.MethodPost, Header: make(http.Header)}).WithContext(ctx)
		return catalog.DescribeTableInternal(r, id)
	})
	var response json.RawMessage
	if err := server.Execute(ctx, *operation, json.RawMessage(body), &response, s3_constants.AccountAdminId); err != nil {
		return err
	}
	if len(response) > 0 {
		var value interface{}
		if err := json.Unmarshal(response, &value); err != nil {
			return err
		}
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	}
	return nil
}
