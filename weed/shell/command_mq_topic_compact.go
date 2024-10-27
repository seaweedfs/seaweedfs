package shell

import (
	"flag"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/logstore"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"io"
	"time"
)

func init() {
	Commands = append(Commands, &commandMqTopicCompact{})
}

type commandMqTopicCompact struct {
}

func (c *commandMqTopicCompact) Name() string {
	return "mq.topic.compact"
}

func (c *commandMqTopicCompact) Help() string {
	return `compact the topic storage into parquet format

	Example:
		mq.topic.compact -namespace <namespace> -topic <topic_name> -timeAgo <time_ago>

`
}

func (c *commandMqTopicCompact) HasTag(tag CommandTag) bool {
	return ResourceHeavy == tag
}

func (c *commandMqTopicCompact) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	// parse parameters
	mqCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	namespace := mqCommand.String("namespace", "", "namespace name")
	topicName := mqCommand.String("topic", "", "topic name")
	timeAgo := mqCommand.Duration("timeAgo", 2*time.Minute, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	replication := mqCommand.String("replication", "", "replication type")
	collection := mqCommand.String("collection", "", "optional collection name")
	dataCenter := mqCommand.String("dataCenter", "", "optional data center name")
	diskType := mqCommand.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	maxMB := mqCommand.Int("maxMB", 4, "split files larger than the limit")

	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	storagePreference := &operation.StoragePreference{
		Replication: *replication,
		Collection:  *collection,
		DataCenter:  *dataCenter,
		DiskType:    *diskType,
		MaxMB:       *maxMB,
	}

	// read topic configuration
	fca := &filer_client.FilerClientAccessor{
		GetFiler: func() pb.ServerAddress {
			return commandEnv.option.FilerAddress
		},
		GetGrpcDialOption: func() grpc.DialOption {
			return commandEnv.option.GrpcDialOption
		},
	}
	t := topic.NewTopic(*namespace, *topicName)
	topicConf, err := fca.ReadTopicConfFromFiler(t)
	if err != nil {
		return err
	}

	// get record type
	recordType := topicConf.GetRecordType()
	recordType = schema.NewRecordTypeBuilder(recordType).
		WithField(logstore.SW_COLUMN_NAME_TS, schema.TypeInt64).
		WithField(logstore.SW_COLUMN_NAME_KEY, schema.TypeBytes).
		RecordTypeEnd()

	// compact the topic partition versions
	if err = logstore.CompactTopicPartitions(commandEnv, t, *timeAgo, recordType, storagePreference); err != nil {
		return err
	}

	return nil

}
