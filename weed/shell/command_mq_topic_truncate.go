package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	Commands = append(Commands, &commandMqTopicTruncate{})
}

type commandMqTopicTruncate struct {
}

func (c *commandMqTopicTruncate) Name() string {
	return "mq.topic.truncate"
}

func (c *commandMqTopicTruncate) Help() string {
	return `clear all data from a topic while preserving topic structure

	Example:
		mq.topic.truncate -namespace <namespace> -topic <topic_name>

	This command removes all log files and parquet files from all partitions
	of the specified topic, while keeping the topic configuration intact.
`
}

func (c *commandMqTopicTruncate) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqTopicTruncate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	// parse parameters
	mqCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	namespace := mqCommand.String("namespace", "", "namespace name")
	topicName := mqCommand.String("topic", "", "topic name")
	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	if *namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if *topicName == "" {
		return fmt.Errorf("topic name is required")
	}

	// Verify topic exists by trying to read its configuration
	t := topic.NewTopic(*namespace, *topicName)

	err := commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		_, err := t.ReadConfFile(client)
		if err != nil {
			return fmt.Errorf("topic %s.%s does not exist or cannot be read: %v", *namespace, *topicName, err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "Truncating topic %s.%s...\n", *namespace, *topicName)

	// Discover and clear all partitions using centralized logic
	partitions, err := t.DiscoverPartitions(context.Background(), commandEnv)
	if err != nil {
		return fmt.Errorf("failed to discover topic partitions: %v", err)
	}

	if len(partitions) == 0 {
		fmt.Fprintf(writer, "No partitions found for topic %s.%s\n", *namespace, *topicName)
		return nil
	}

	fmt.Fprintf(writer, "Found %d partitions, clearing data...\n", len(partitions))

	// Clear data from each partition
	totalFilesDeleted := 0
	for _, partitionPath := range partitions {
		filesDeleted, err := c.clearPartitionData(commandEnv, partitionPath, writer)
		if err != nil {
			fmt.Fprintf(writer, "Warning: failed to clear partition %s: %v\n", partitionPath, err)
			continue
		}
		totalFilesDeleted += filesDeleted
		fmt.Fprintf(writer, "Cleared partition: %s (%d files)\n", partitionPath, filesDeleted)
	}

	fmt.Fprintf(writer, "Successfully truncated topic %s.%s - deleted %d files from %d partitions\n",
		*namespace, *topicName, totalFilesDeleted, len(partitions))

	return nil
}

// clearPartitionData deletes all data files (log files, parquet files) from a partition directory
// Returns the number of files deleted
func (c *commandMqTopicTruncate) clearPartitionData(commandEnv *CommandEnv, partitionPath string, writer io.Writer) (int, error) {
	filesDeleted := 0

	err := filer_pb.ReadDirAllEntries(context.Background(), commandEnv, util.FullPath(partitionPath), "", func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			return nil // Skip subdirectories
		}

		fileName := entry.Name

		// Preserve configuration files
		if strings.HasSuffix(fileName, ".conf") ||
			strings.HasSuffix(fileName, ".config") ||
			fileName == "topic.conf" ||
			fileName == "partition.conf" {
			fmt.Fprintf(writer, "  Preserving config file: %s\n", fileName)
			return nil
		}

		// Delete all data files (log files, parquet files, offset files, etc.)
		deleteErr := filer_pb.Remove(context.Background(), commandEnv, partitionPath, fileName, false, true, true, false, nil)

		if deleteErr != nil {
			fmt.Fprintf(writer, "  Warning: failed to delete %s/%s: %v\n", partitionPath, fileName, deleteErr)
			// Continue with other files rather than failing entirely
		} else {
			fmt.Fprintf(writer, "  Deleted: %s\n", fileName)
			filesDeleted++
		}

		return nil
	})

	return filesDeleted, err
}
