package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"io"
	"math"
)

func init() {
	Commands = append(Commands, &commandS3BucketQuotaEnforce{})
}

type commandS3BucketQuotaEnforce struct {
}

func (c *commandS3BucketQuotaEnforce) Name() string {
	return "s3.bucket.quota.enforce"
}

func (c *commandS3BucketQuotaEnforce) Help() string {
	return `check quota for all buckets, make the bucket read only if over the limit

	Example:
		s3.bucket.quota.enforce -apply
`
}

func (c *commandS3BucketQuotaEnforce) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketQuotaEnforce) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	applyQuotaLimit := bucketCommand.Bool("apply", false, "actually change the buckets readonly attribute")
	if err = bucketCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyQuotaLimit, "-apply")

	// collect collection information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}
	collectionInfos := make(map[string]*CollectionInfo)
	collectCollectionInfo(topologyInfo, collectionInfos)

	// read buckets path
	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %v", err)
	}

	// read existing filer configuration
	fc, err := filer.ReadFilerConf(commandEnv.option.FilerAddress, commandEnv.option.GrpcDialOption, commandEnv.MasterClient)
	if err != nil {
		return err
	}

	// process each bucket
	hasConfChanges := false
	err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			return nil
		}
		collection := getCollectionName(commandEnv, entry.Name)
		var collectionSize float64
		if collectionInfo, found := collectionInfos[collection]; found {
			collectionSize = collectionInfo.Size
		}
		if c.processEachBucket(fc, filerBucketsPath, entry, writer, collectionSize) {
			hasConfChanges = true
		}
		return nil
	}, "", false, math.MaxUint32)
	if err != nil {
		return fmt.Errorf("list buckets under %v: %v", filerBucketsPath, err)
	}

	// apply the configuration changes
	if hasConfChanges && *applyQuotaLimit {

		var buf2 bytes.Buffer
		fc.ToText(&buf2)

		if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf2.Bytes())
		}); err != nil && err != filer_pb.ErrNotFound {
			return err
		}
	}

	return err

}

func (c *commandS3BucketQuotaEnforce) processEachBucket(fc *filer.FilerConf, filerBucketsPath string, entry *filer_pb.Entry, writer io.Writer, collectionSize float64) (hasConfChanges bool) {

	locPrefix := filerBucketsPath + "/" + entry.Name + "/"
	locConf := fc.MatchStorageRule(locPrefix)
	locConf.LocationPrefix = locPrefix

	if entry.Quota > 0 {
		if locConf.ReadOnly {
			if collectionSize < float64(entry.Quota) {
				locConf.ReadOnly = false
				hasConfChanges = true
			}
		} else {
			if collectionSize > float64(entry.Quota) {
				locConf.ReadOnly = true
				hasConfChanges = true
			}
		}
	} else {
		if locConf.ReadOnly {
			locConf.ReadOnly = false
			hasConfChanges = true
		}
	}

	if hasConfChanges {
		fmt.Fprintf(writer, "  %s\tsize:%.0f", entry.Name, collectionSize)
		fmt.Fprintf(writer, "\tquota:%d\tusage:%.2f%%", entry.Quota, collectionSize*100/float64(entry.Quota))
		fmt.Fprintln(writer)
		if locConf.ReadOnly {
			fmt.Fprintf(writer, "    changing bucket %s to read only!\n", entry.Name)
		} else {
			fmt.Fprintf(writer, "    changing bucket %s to writable.\n", entry.Name)
		}
		fc.SetLocationConf(locConf)
	}

	return
}
