package shell

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/bucketsize"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	-sizeFrom=auto (default) sizes a remote-mounted bucket from a recursive
	filer listing, counting every byte it stores (noncurrent versions and
	in-progress multipart parts included), because its local volumes are only
	a cache; every other bucket
	from its collection's logical size in the master's topology.
	-sizeFrom=filer walks every bucket with a quota; -sizeFrom=collection
	walks none (it undercounts a remote-mounted bucket). Buckets without a
	quota are never walked. A bucket whose listing fails keeps its read-only
	state as it is.
`
}

func (c *commandS3BucketQuotaEnforce) HasTag(CommandTag) bool {
	return false
}

func (c *commandS3BucketQuotaEnforce) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	bucketCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	applyQuotaLimit := bucketCommand.Bool("apply", false, "actually change the buckets readonly attribute")
	sizeFrom := bucketCommand.String("sizeFrom", quotaSizeFromAuto, "where a bucket's size is read: 'auto' (filer listing for remote-mounted buckets, collection otherwise), 'filer' (filer listing for every bucket) or 'collection' (local volumes only)")
	if err = bucketCommand.Parse(args); err != nil {
		return err
	}
	if *sizeFrom != quotaSizeFromAuto && *sizeFrom != quotaSizeFromFiler && *sizeFrom != quotaSizeFromCollection {
		return fmt.Errorf("-sizeFrom must be %q, %q or %q, not %q", quotaSizeFromAuto, quotaSizeFromFiler, quotaSizeFromCollection, *sizeFrom)
	}
	infoAboutSimulationMode(writer, *applyQuotaLimit, "-apply")

	// read buckets path
	var filerBucketsPath string
	filerBucketsPath, err = readFilerBucketsPath(commandEnv)
	if err != nil {
		return fmt.Errorf("read buckets: %w", err)
	}

	var collectionSize, filerSize bucketSizeFunc
	if *sizeFrom != quotaSizeFromFiler {
		// collect collection information
		topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
		if err != nil {
			return err
		}
		collectionInfos := make(map[string]*CollectionInfo)
		collectCollectionInfo(topologyInfo, collectionInfos)
		collectionSize = func(entry *filer_pb.Entry) (float64, error) {
			var size float64
			if collectionInfo, found := collectionInfos[getCollectionName(commandEnv, entry.Name)]; found {
				size = collectionInfo.LogicalSize()
			}
			return size, nil
		}
	}
	if *sizeFrom != quotaSizeFromCollection {
		filerSize = func(entry *filer_pb.Entry) (float64, error) {
			size, err := bucketsize.Size(context.Background(), commandEnv, util.FullPath(filerBucketsPath).Child(entry.Name))
			return float64(size), err
		}
	}
	bucketSize := collectionSize
	switch *sizeFrom {
	case quotaSizeFromFiler:
		bucketSize = filerSize
	case quotaSizeFromAuto:
		mounts, err := filer.ReadMountMappings(commandEnv.option.GrpcDialOption, commandEnv.FilerAddress())
		if err != nil {
			return err
		}
		bucketSize = autoBucketSize(mounts, filerBucketsPath, filerSize, collectionSize)
	}

	// process each bucket: collect them first, so no bucket's walk runs
	// inside the listing stream of /buckets
	listBuckets := func() (buckets []*filer_pb.Entry, err error) {
		err = filer_pb.List(context.Background(), commandEnv, filerBucketsPath, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				buckets = append(buckets, entry)
			}
			return nil
		}, "", false, math.MaxUint32)
		if err != nil {
			return nil, fmt.Errorf("list buckets under %v: %w", filerBucketsPath, err)
		}
		return buckets, nil
	}
	buckets, err := listBuckets()
	if err != nil {
		return err
	}
	readConf := func() (*filer.FilerConf, error) {
		return filer.ReadFilerConf(commandEnv.FilerAddress(), commandEnv.option.GrpcDialOption, commandEnv.MasterClient)
	}
	fc, hasConfChanges, unsized, err := c.enforceQuotas(buckets, filerBucketsPath, bucketSize, readConf, listBuckets, writer)
	if err != nil {
		return err
	}

	// apply the configuration changes
	if hasConfChanges && *applyQuotaLimit {

		var buf2 bytes.Buffer
		fc.ToText(&buf2)

		if err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer.SaveInsideFiler(context.Background(), client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName, buf2.Bytes())
		}); err != nil && err != filer_pb.ErrNotFound {
			return err
		}
	}

	// the other buckets' changes are saved above; still fail the run so a
	// scheduler sees that some buckets were not checked
	if len(unsized) > 0 {
		return fmt.Errorf("could not size %d bucket(s), their read-only state is unchanged: %v", len(unsized), unsized)
	}

	return err

}

const (
	quotaSizeFromAuto       = "auto"
	quotaSizeFromFiler      = "filer"
	quotaSizeFromCollection = "collection"
)

// bucketSizeFunc returns the bytes a bucket holds, as compared with its quota.
type bucketSizeFunc func(entry *filer_pb.Entry) (float64, error)

// autoBucketSize sizes a remote-mounted bucket from the filer (its local
// volumes are only a cache) and every other bucket from its collection.
func autoBucketSize(mounts *remote_pb.RemoteStorageMapping, filerBucketsPath string, filerSize, collectionSize bucketSizeFunc) bucketSizeFunc {
	return func(entry *filer_pb.Entry) (float64, error) {
		if bucketsize.IsMounted(mounts, string(util.FullPath(filerBucketsPath).Child(entry.Name))) {
			return filerSize(entry)
		}
		return collectionSize(entry)
	}
}

// enforceQuotas sizes every bucket that has a quota, and only then reads
// filer.conf and lists the buckets again to apply the quotas, so a change
// made to filer.conf while the buckets were walked (an operator clearing a
// flag) is not overwritten by a stale copy, and a bucket whose quota changed
// meanwhile is skipped rather than judged on its old quota. A bucket that
// cannot be sized is reported and left as it is.
func (c *commandS3BucketQuotaEnforce) enforceQuotas(buckets []*filer_pb.Entry, filerBucketsPath string, bucketSize bucketSizeFunc, readConf func() (*filer.FilerConf, error), relist func() ([]*filer_pb.Entry, error), writer io.Writer) (fc *filer.FilerConf, hasConfChanges bool, unsized []string, err error) {
	sizes := make(map[string]float64)
	for _, entry := range buckets {
		// a bucket without a quota has nothing to enforce: do not size it
		if entry.Quota <= 0 {
			continue
		}
		size, sizeErr := bucketSize(entry)
		if sizeErr != nil {
			// neither strand a bucket read-only nor clear one on a size we do not have
			fmt.Fprintf(writer, "  %s\tsize error: %v (read-only state left unchanged)\n", entry.Name, sizeErr)
			unsized = append(unsized, entry.Name)
			continue
		}
		sizes[entry.Name] = size
	}

	if fc, err = readConf(); err != nil {
		return nil, false, unsized, err
	}
	current, err := relist()
	if err != nil {
		return nil, false, unsized, err
	}
	currentQuota := make(map[string]int64, len(current))
	for _, entry := range current {
		currentQuota[entry.Name] = entry.Quota
	}
	for _, entry := range buckets {
		size, sized := sizes[entry.Name]
		if !sized {
			continue
		}
		if quota, found := currentQuota[entry.Name]; !found || quota != entry.Quota {
			fmt.Fprintf(writer, "  %s\tquota changed while sizing, skipped\n", entry.Name)
			continue
		}
		if c.processEachBucket(fc, filerBucketsPath, entry, writer, size) {
			hasConfChanges = true
		}
	}
	return fc, hasConfChanges, unsized, nil
}

func (c *commandS3BucketQuotaEnforce) processEachBucket(fc *filer.FilerConf, filerBucketsPath string, entry *filer_pb.Entry, writer io.Writer, collectionSize float64) (hasConfChanges bool) {

	locPrefix := filerBucketsPath + "/" + entry.Name + "/"

	readOnly, hasConfChanges := fc.ApplyBucketQuotaReadOnly(locPrefix, collectionSize, float64(entry.Quota))
	if hasConfChanges {
		fmt.Fprintf(writer, "  %s\tsize:%.0f", entry.Name, collectionSize)
		fmt.Fprintf(writer, "\tquota:%d\tusage:%.2f%%", entry.Quota, collectionSize*100/float64(entry.Quota))
		fmt.Fprintln(writer)
		if readOnly {
			fmt.Fprintf(writer, "    changing bucket %s to read only!\n", entry.Name)
		} else {
			fmt.Fprintf(writer, "    changing bucket %s to writable.\n", entry.Name)
		}
	}

	return
}
