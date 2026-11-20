package command

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/shell"
)

func init() { shell.Commands = append(shell.Commands, &commandS3VectorsIndexMetadata{}) }

type commandS3VectorsIndexMetadata struct{}

func (c *commandS3VectorsIndexMetadata) Name() string                 { return "s3vectors.index.metadata" }
func (c *commandS3VectorsIndexMetadata) HasTag(shell.CommandTag) bool { return false }
func (c *commandS3VectorsIndexMetadata) Help() string {
	return `list, add or remove the indexed metadata keys of a vector index

s3vectors.index.metadata -bucket embeddings -index articles
s3vectors.index.metadata -bucket embeddings -index articles -add author
s3vectors.index.metadata -bucket embeddings -index articles -remove author

An indexed key keeps a scalar index of the string values a filter's $eq and
$in compare, so those filters stop reading every row's metadata. Query
results do not change. A key is first "requested"; the lance_metadata_index
worker job builds it ("building") and then "ready" queries use it. Removing
sets it "off" at once and the job drops it. Non-filterable keys cannot be
indexed, an index has at most 8 indexed keys, and adding is refused while a
registered vector engine does not fill indexed key columns on its writes.
`
}

func (c *commandS3VectorsIndexMetadata) Do(args []string, env *shell.CommandEnv, writer io.Writer) error {
	flags := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	bucket := flags.String("bucket", "", "vector bucket name")
	index := flags.String("index", "", "index name")
	add := flags.String("add", "", "metadata key to index")
	remove := flags.String("remove", "", "metadata key to stop indexing")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *bucket == "" || *index == "" {
		return fmt.Errorf("-bucket and -index are required")
	}
	if *add != "" && *remove != "" {
		return fmt.Errorf("use either -add or -remove")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	server, closeServer := shellVectorsServer(env)
	defer closeServer()
	var keys *vector_pb.MetadataKeysResponse
	var err error
	switch {
	case *add != "":
		keys, err = server.UpdateMetadataKey(ctx, *bucket, *index, *add, false)
	case *remove != "":
		keys, err = server.UpdateMetadataKey(ctx, *bucket, *index, *remove, true)
	default:
		keys, err = server.MetadataKeys(ctx, *bucket, *index)
	}
	if err != nil {
		return err
	}
	return printMetadataKeys(writer, keys)
}

func printMetadataKeys(writer io.Writer, keys *vector_pb.MetadataKeysResponse) error {
	if len(keys.Keys) == 0 {
		_, err := fmt.Fprintln(writer, "no indexed metadata keys")
		return err
	}
	w := tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tSTATE\tCOVERAGE\tSEGMENTS\tSIZE")
	for _, k := range keys.Keys {
		coverage := "-"
		if k.Rows > 0 && k.State != "requested" {
			coverage = fmt.Sprintf("%d%%", k.IndexedRows*100/k.Rows)
		}
		size := "-"
		if k.IndexBytes > 0 {
			size = fmt.Sprintf("%.1f MiB", float64(k.IndexBytes)/(1<<20))
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", k.Key, k.State, coverage, k.Segments, size)
	}
	return w.Flush()
}
