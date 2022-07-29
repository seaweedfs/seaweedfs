package command

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	defaultFnFormat = `{{.Id}}_{{.Name}}{{.Ext}}`
	timeFormat      = "2006-01-02T15:04:05"
)

var (
	export ExportOptions
)

type ExportOptions struct {
	dir        *string
	collection *string
	volumeId   *int
}

var cmdExport = &Command{
	UsageLine: "export -dir=/tmp -volumeId=234 -o=/dir/name.tar -fileNameFormat={{.Name}} -newer='" + timeFormat + "'",
	Short:     "list or export files from one volume data file",
	Long: `List all files in a volume, or Export all files in a volume to a tar file if the output is specified.

	The format of file name in the tar file can be customized. Default is {{.Mime}}/{{.Id}}:{{.Name}}. Also available is {{.Key}}.

  `,
}

func init() {
	cmdExport.Run = runExport // break init cycle
	export.dir = cmdExport.Flag.String("dir", ".", "input data directory to store volume data files")
	export.collection = cmdExport.Flag.String("collection", "", "the volume collection name")
	export.volumeId = cmdExport.Flag.Int("volumeId", -1, "a volume id. The volume .dat and .idx files should already exist in the dir.")
}

var (
	output      = cmdExport.Flag.String("o", "", "output tar file name, must ends with .tar, or just a \"-\" for stdout")
	format      = cmdExport.Flag.String("fileNameFormat", defaultFnFormat, "filename formatted with {{.Id}} {{.Name}} {{.Ext}}")
	newer       = cmdExport.Flag.String("newer", "", "export only files newer than this time, default is all files. Must be specified in RFC3339 without timezone, e.g. 2006-01-02T15:04:05")
	showDeleted = cmdExport.Flag.Bool("deleted", false, "export deleted files. only applies if -o is not specified")
	limit       = cmdExport.Flag.Int("limit", 0, "only show first n entries if specified")

	tarOutputFile          *tar.Writer
	tarHeader              tar.Header
	fileNameTemplate       *template.Template
	fileNameTemplateBuffer = bytes.NewBuffer(nil)
	newerThan              time.Time
	newerThanUnix          int64 = -1
	localLocation, _             = time.LoadLocation("Local")
)

func printNeedle(vid needle.VolumeId, n *needle.Needle, version needle.Version, deleted bool, offset int64, onDiskSize int64) {
	key := needle.NewFileIdFromNeedle(vid, n).String()
	size := int32(n.DataSize)
	if version == needle.Version1 {
		size = int32(n.Size)
	}
	fmt.Printf("%s\t%s\t%d\t%t\t%s\t%s\t%s\t%t\t%d\t%d\n",
		key,
		n.Name,
		size,
		n.IsCompressed(),
		n.Mime,
		n.LastModifiedString(),
		n.Ttl.String(),
		deleted,
		offset,
		offset+onDiskSize,
	)
}

type VolumeFileScanner4Export struct {
	version   needle.Version
	counter   int
	needleMap *needle_map.MemDb
	vid       needle.VolumeId
}

func (scanner *VolumeFileScanner4Export) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	scanner.version = superBlock.Version
	return nil

}
func (scanner *VolumeFileScanner4Export) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Export) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	needleMap := scanner.needleMap
	vid := scanner.vid

	nv, ok := needleMap.Get(n.Id)
	glog.V(3).Infof("key %d offset %d size %d disk_size %d compressed %v ok %v nv %+v",
		n.Id, offset, n.Size, n.DiskSize(scanner.version), n.IsCompressed(), ok, nv)
	if *showDeleted && n.Size > 0 || ok && nv.Size.IsValid() && nv.Offset.ToActualOffset() == offset {
		if newerThanUnix >= 0 && n.HasLastModifiedDate() && n.LastModified < uint64(newerThanUnix) {
			glog.V(3).Infof("Skipping this file, as it's old enough: LastModified %d vs %d",
				n.LastModified, newerThanUnix)
			return nil
		}
		scanner.counter++
		if *limit > 0 && scanner.counter > *limit {
			return io.EOF
		}
		if tarOutputFile != nil {
			return writeFile(vid, n)
		} else {
			printNeedle(vid, n, scanner.version, false, offset, n.DiskSize(scanner.version))
			return nil
		}
	}
	if !ok {
		if *showDeleted && tarOutputFile == nil {
			if n.DataSize > 0 {
				printNeedle(vid, n, scanner.version, true, offset, n.DiskSize(scanner.version))
			} else {
				n.Name = []byte("*tombstone")
				printNeedle(vid, n, scanner.version, true, offset, n.DiskSize(scanner.version))
			}
		}
		glog.V(2).Infof("This seems deleted %d size %d", n.Id, n.Size)
	} else {
		glog.V(2).Infof("Skipping later-updated Id %d size %d", n.Id, n.Size)
	}
	return nil
}

func runExport(cmd *Command, args []string) bool {

	var err error

	if *newer != "" {
		if newerThan, err = time.ParseInLocation(timeFormat, *newer, localLocation); err != nil {
			fmt.Println("cannot parse 'newer' argument: " + err.Error())
			return false
		}
		newerThanUnix = newerThan.Unix()
	}

	if *export.volumeId == -1 {
		return false
	}

	if *output != "" {
		if *output != "-" && !strings.HasSuffix(*output, ".tar") {
			fmt.Println("the output file", *output, "should be '-' or end with .tar")
			return false
		}

		if fileNameTemplate, err = template.New("name").Parse(*format); err != nil {
			fmt.Println("cannot parse format " + *format + ": " + err.Error())
			return false
		}

		var outputFile *os.File
		if *output == "-" {
			outputFile = os.Stdout
		} else {
			if outputFile, err = os.Create(*output); err != nil {
				glog.Fatalf("cannot open output tar %s: %s", *output, err)
			}
		}
		defer outputFile.Close()
		tarOutputFile = tar.NewWriter(outputFile)
		defer tarOutputFile.Close()
		t := time.Now()
		tarHeader = tar.Header{Mode: 0644,
			ModTime: t, Uid: os.Getuid(), Gid: os.Getgid(),
			Typeflag:   tar.TypeReg,
			AccessTime: t, ChangeTime: t}
	}

	fileName := strconv.Itoa(*export.volumeId)
	if *export.collection != "" {
		fileName = *export.collection + "_" + fileName
	}
	vid := needle.VolumeId(*export.volumeId)

	needleMap := needle_map.NewMemDb()
	defer needleMap.Close()

	if err := needleMap.LoadFromIdx(path.Join(util.ResolvePath(*export.dir), fileName+".idx")); err != nil {
		glog.Fatalf("cannot load needle map from %s.idx: %s", fileName, err)
	}

	volumeFileScanner := &VolumeFileScanner4Export{
		needleMap: needleMap,
		vid:       vid,
	}

	if tarOutputFile == nil {
		fmt.Printf("key\tname\tsize\tgzip\tmime\tmodified\tttl\tdeleted\tstart\tstop\n")
	}

	err = storage.ScanVolumeFile(util.ResolvePath(*export.dir), *export.collection, vid, storage.NeedleMapInMemory, volumeFileScanner)
	if err != nil && err != io.EOF {
		glog.Errorf("Export Volume File [ERROR] %s\n", err)
	}
	return true
}

type nameParams struct {
	Name string
	Id   types.NeedleId
	Mime string
	Key  string
	Ext  string
}

func writeFile(vid needle.VolumeId, n *needle.Needle) (err error) {
	key := needle.NewFileIdFromNeedle(vid, n).String()
	fileNameTemplateBuffer.Reset()
	if err = fileNameTemplate.Execute(fileNameTemplateBuffer,
		nameParams{
			Name: string(n.Name),
			Id:   n.Id,
			Mime: string(n.Mime),
			Key:  key,
			Ext:  filepath.Ext(string(n.Name)),
		},
	); err != nil {
		return err
	}

	fileName := fileNameTemplateBuffer.String()

	if n.IsCompressed() {
		if util.IsGzippedContent(n.Data) && path.Ext(fileName) != ".gz" {
			fileName = fileName + ".gz"
		}
		// TODO other compression method
	}

	tarHeader.Name, tarHeader.Size = fileName, int64(len(n.Data))
	if n.HasLastModifiedDate() {
		tarHeader.ModTime = time.Unix(int64(n.LastModified), 0)
	} else {
		tarHeader.ModTime = time.Unix(0, 0)
	}
	tarHeader.ChangeTime = tarHeader.ModTime
	if err = tarOutputFile.WriteHeader(&tarHeader); err != nil {
		return err
	}
	_, err = tarOutputFile.Write(n.Data)
	return
}
