package shell

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"io"
	"strings"
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
	timeAgo := mqCommand.Duration("timeAgo", 0, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")
	if err := mqCommand.Parse(args); err != nil {
		return err
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
	t := topic.Topic{
		Namespace: *namespace,
		Name:      *topicName,
	}
	topicConf, err := fca.ReadTopicConfFromFiler(t)
	if err != nil {
		return err
	}

	// get record type
	recordType := topicConf.GetRecordType()
	for _, field := range recordType.GetFields() {
		fmt.Printf("field: %s, type: %s\n", field.GetName(), schema.TypeToString(field.GetType()))
	}

	var partitions []*mq_pb.Partition
	for _, assignment := range topicConf.BrokerPartitionAssignments {
		partitions = append(partitions, assignment.Partition)
	}

	// list the topic partition versions
	partitionVersions, err := collectTopicPartitionVersions(commandEnv, *namespace, *topicName, *timeAgo)
	if err != nil {
		return fmt.Errorf("list topic files: %v", err)
	}

	// compact the topic partition versions
	for _, partitionVersion := range partitionVersions {
		err = compactTopicPartitions(commandEnv, *namespace, *topicName, partitionVersion, partitions, recordType)
		if err != nil {
			return fmt.Errorf("compact topic partition %s: %v", partitionVersion, err)
		}
	}

	return nil

}

func collectTopicPartitionVersions(commandEnv *CommandEnv, namespace string, topicName string, timeAgo time.Duration) (partitionVersions []string, err error) {
	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(filer.TopicsDir+"/"+namespace+"/"+topicName), "", func(entry *filer_pb.Entry, isLast bool) error {
		t, err := time.Parse(topic.TIME_FORMAT, entry.Name)
		if err != nil {
			// skip non-partition directories
			return nil
		}
		println("t", t.Unix())
		if t.Unix() < time.Now().Unix()-int64(timeAgo/time.Second) {
			partitionVersions = append(partitionVersions, entry.Name)
		}
		return nil
	})
	return
}

func compactTopicPartitions(commandEnv *CommandEnv, namespace string, topicName string, partitionVersion string, partitions []*mq_pb.Partition, recordType *schema_pb.RecordType) error {
	for _, partition := range partitions {
		err := compactTopicPartition(commandEnv, namespace, topicName, partitionVersion, recordType, partition)
		if err != nil {
			return err
		}
	}
	return nil
}

func compactTopicPartition(commandEnv *CommandEnv, namespace string, topicName string, partitionVersion string, recordType *schema_pb.RecordType, partition *mq_pb.Partition) error {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, namespace, topicName)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionVersion, partition.RangeStart, partition.RangeStop)

	// compact the partition directory
	return compactTopicPartitionDir(commandEnv, partitionDir, recordType)
}

func compactTopicPartitionDir(commandEnv *CommandEnv, partitionDir string, recordType *schema_pb.RecordType) error {
	// read all log files
	logFiles, err := readAllLogFiles(commandEnv, partitionDir)
	if err != nil {
		return err
	}

	// divide log files into groups of 128MB
	logFileGroups := groupFilesBySize(logFiles, 128*1024*1024)

	// write to parquet file
	// TODO parallelize the writing
	for _, logFileGroup := range logFileGroups {
		err = writeLogFilesToParquet(commandEnv, partitionDir, recordType, logFileGroup)
		if err != nil {
			return err
		}
	}
	return nil
}

func groupFilesBySize(logFiles []*filer_pb.Entry, maxGroupSize int64) (logFileGroups [][]*filer_pb.Entry) {
	var logFileGroup []*filer_pb.Entry
	var groupSize int64
	for _, logFile := range logFiles {
		if groupSize+int64(logFile.Attributes.FileSize) > maxGroupSize {
			logFileGroups = append(logFileGroups, logFileGroup)
			logFileGroup = nil
			groupSize = 0
		}
		logFileGroup = append(logFileGroup, logFile)
		groupSize += int64(logFile.Attributes.FileSize)
	}
	if len(logFileGroup) > 0 {
		logFileGroups = append(logFileGroups, logFileGroup)
	}
	return
}

func readAllLogFiles(commandEnv *CommandEnv, partitionDir string) (logFiles []*filer_pb.Entry, err error) {
	err = filer_pb.ReadDirAllEntries(commandEnv, util.FullPath(partitionDir), "", func(entry *filer_pb.Entry, isLast bool) error {
		if strings.HasSuffix(entry.Name, ".parquet") {
			return nil
		}
		logFiles = append(logFiles, entry)
		return nil
	})
	return
}

func writeLogFilesToParquet(commandEnv *CommandEnv, partitionDir string, recordType *schema_pb.RecordType, logFileGroups []*filer_pb.Entry) error {
	for _, logFile := range logFileGroups {
		fmt.Printf("compact log file %s\n", logFile.Name)
	}

	// write to parquet file to partitionDir
	return nil
}
