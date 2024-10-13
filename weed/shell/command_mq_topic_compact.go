package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/schema"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
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
	replication := mqCommand.String("replication", "", "replication type")
	collection := mqCommand.String("collection", "", "optional collection name")
	dataCenter := mqCommand.String("dataCenter", "", "optional data center name")
	diskType := mqCommand.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	ttl := mqCommand.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	maxMB := mqCommand.Int("maxMB", 4, "split files larger than the limit")

	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	storagePreference := &operation.StoragePreference{
		Replication: *replication,
		Collection:  *collection,
		DataCenter:  *dataCenter,
		DiskType:    *diskType,
		Ttl:         *ttl,
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
		err = compactTopicPartitions(commandEnv, *namespace, *topicName, partitionVersion, partitions, recordType, storagePreference)
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

func compactTopicPartitions(commandEnv *CommandEnv, namespace string, topicName string, partitionVersion string, partitions []*mq_pb.Partition, recordType *schema_pb.RecordType, preference *operation.StoragePreference) error {
	for _, partition := range partitions {
		err := compactTopicPartition(commandEnv, namespace, topicName, partitionVersion, recordType, partition, preference)
		if err != nil {
			return err
		}
	}
	return nil
}

func compactTopicPartition(commandEnv *CommandEnv, namespace string, topicName string, partitionVersion string, recordType *schema_pb.RecordType, partition *mq_pb.Partition, preference *operation.StoragePreference) error {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, namespace, topicName)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionVersion, partition.RangeStart, partition.RangeStop)

	// compact the partition directory
	return compactTopicPartitionDir(commandEnv, topicName, partitionDir, recordType, preference)
}

func compactTopicPartitionDir(commandEnv *CommandEnv, topicName, partitionDir string, recordType *schema_pb.RecordType, preference *operation.StoragePreference) error {
	// read all log files
	logFiles, err := readAllLogFiles(commandEnv, partitionDir)
	if err != nil {
		return err
	}

	// divide log files into groups of 128MB
	logFileGroups := groupFilesBySize(logFiles, 128*1024*1024)

	// write to parquet file
	parquetLevels, err := schema.ToParquetLevels(recordType)
	if err != nil {
		return fmt.Errorf("ToParquetLevels failed %+v: %v", recordType, err)
	}

	// create a parquet schema
	parquetSchema, err := schema.ToParquetSchema(topicName, recordType)
	if err != nil {
		return fmt.Errorf("ToParquetSchema failed: %v", err)
	}

	// TODO parallelize the writing
	var parquetFileNames []string
	for _, logFileGroup := range logFileGroups {
		parquetFileName, err := writeLogFilesToParquet(commandEnv, partitionDir, recordType, logFileGroup, parquetSchema, parquetLevels)
		if err != nil {
			return err
		}
		fmt.Printf("write to parquet file %s\n", parquetFileName)
		parquetFileNames = append(parquetFileNames, parquetFileName)
	}

	// upload parquet files
	parts, err := operation.NewFileParts(parquetFileNames)
	if err != nil {
		return err
	}
	results, err := operation.SubmitFiles(func(_ context.Context) pb.ServerAddress { return pb.ServerAddress(commandEnv.option.FilerAddress) }, commandEnv.option.GrpcDialOption, parts, *preference, false)
	if err != nil {
		return err
	}
	for _, result := range results {
		if result.Error != "" {
			return fmt.Errorf("upload parquet file %s: %v", result.FileName, result.Error)
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

func writeLogFilesToParquet(commandEnv *CommandEnv, partitionDir string, recordType *schema_pb.RecordType, logFileGroups []*filer_pb.Entry, parquetSchema *parquet.Schema, parquetLevels *schema.ParquetLevels) (parquetFileName string, err error) {

	tempFile, err := os.CreateTemp(".", "t*.parquet")
	if err != nil {
		return "", fmt.Errorf("create temp file: %v", err)
	}
	defer tempFile.Close()

	writer := parquet.NewWriter(tempFile, parquetSchema, parquet.Compression(&zstd.Codec{Level: zstd.DefaultLevel}))
	rowBuilder := parquet.NewRowBuilder(parquetSchema)

	for _, logFile := range logFileGroups {
		fmt.Printf("compact log file %s/%s\n", partitionDir, logFile.Name)
		var rows []parquet.Row
		if err := iterateLogEntries(commandEnv, logFile, func(entry *filer_pb.LogEntry) error {

			println("adding row", string(entry.Key))

			// write to parquet file
			rowBuilder.Reset()

			record := &schema_pb.RecordValue{}
			if err := proto.Unmarshal(entry.Data, record); err != nil {
				return fmt.Errorf("unmarshal record value: %v", err)
			}

			if err := schema.AddRecordValue(rowBuilder, recordType, parquetLevels, record); err != nil {
				return fmt.Errorf("add record value: %v", err)
			}

			rows = append(rows, rowBuilder.Row())

			return nil

		}); err != nil {
			return "", fmt.Errorf("iterate log entry %v/%v: %v", partitionDir, logFile.Name, err)
		}

		println("writing rows", len(rows))

		if _, err := writer.WriteRows(rows); err != nil {
			return "", fmt.Errorf("write rows: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close writer: %v", err)
	}

	// write to parquet file to partitionDir
	parquetFileName = fmt.Sprintf("%s/%s.parquet", partitionDir, time.Now().Format("2006-01-02T15:04:05"))

	return tempFile.Name(), nil
}

func iterateLogEntries(commandEnv *CommandEnv, logFile *filer_pb.Entry, eachLogEntryFn func(entry *filer_pb.LogEntry) error) error {

	_, err := eachFile(logFile, func(fileId string) (targetUrls []string, err error) {
		println("lookup file id", fileId)
		return commandEnv.MasterClient.LookupFileId(fileId)
	}, func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
		if err := eachLogEntryFn(logEntry); err != nil {
			return true, err
		}
		return false, nil
	})
	return err
}

func eachFile(entry *filer_pb.Entry, lookupFileIdFn func(fileId string) (targetUrls []string, err error), eachLogEntryFn log_buffer.EachLogEntryFuncType) (processedTsNs int64, err error) {
	if len(entry.Content) > 0 {
		// skip .offset files
		return
	}
	var urlStrings []string
	for _, chunk := range entry.Chunks {
		if chunk.Size == 0 {
			continue
		}
		if chunk.IsChunkManifest {
			fmt.Printf("this should not happen. unexpected chunk manifest in %s", entry.Name)
			return
		}
		urlStrings, err = lookupFileIdFn(chunk.FileId)
		if err != nil {
			err = fmt.Errorf("lookup %s: %v", chunk.FileId, err)
			return
		}
		if len(urlStrings) == 0 {
			err = fmt.Errorf("no url found for %s", chunk.FileId)
			return
		}

		// try one of the urlString until util.Get(urlString) succeeds
		var processed bool
		for _, urlString := range urlStrings {
			println("reading url", urlString)
			var data []byte
			if data, _, err = util_http.Get(urlString); err == nil {
				processed = true
				if processedTsNs, err = eachChunk(data, eachLogEntryFn); err != nil {
					return
				}
				break
			}
			println("processed log data", len(data))
		}
		if !processed {
			err = fmt.Errorf("no data processed for %s %s", entry.Name, chunk.FileId)
			return
		}

	}
	return
}

func eachChunk(buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType) (processedTsNs int64, err error) {
	for pos := 0; pos+4 < len(buf); {

		size := util.BytesToUint32(buf[pos : pos+4])
		if pos+4+int(size) > len(buf) {
			err = fmt.Errorf("LogOnDiskReadFunc: read [%d,%d) from [0,%d)", pos, pos+int(size)+4, len(buf))
			return
		}
		entryData := buf[pos+4 : pos+4+int(size)]

		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			pos += 4 + int(size)
			err = fmt.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
			return
		}

		if _, err = eachLogEntryFn(logEntry); err != nil {
			err = fmt.Errorf("process log entry %v: %v", logEntry, err)
			return
		}

		processedTsNs = logEntry.TsNs

		pos += 4 + int(size)

	}

	return
}
