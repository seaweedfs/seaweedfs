package command

import (
	"archive/tar"
	"bytes"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

const (
	defaultFnFormat = `{{.Mime}}/{{.Id}}:{{.Name}}`
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
	format      = cmdExport.Flag.String("fileNameFormat", defaultFnFormat, "filename formatted with {{.Mime}} {{.Id}} {{.Name}} {{.Ext}}")
	newer       = cmdExport.Flag.String("newer", "", "export only files newer than this time, default is all files. Must be specified in RFC3339 without timezone, e.g. 2006-01-02T15:04:05")
	showDeleted = cmdExport.Flag.Bool("deleted", false, "export deleted files. only applies if -o is not specified")
	volumeInfo  = cmdExport.Flag.Bool("volumeInfo", false, "show volume info")

	tarOutputFile          *tar.Writer
	tarHeader              tar.Header
	fileNameTemplate       *template.Template
	fileNameTemplateBuffer = bytes.NewBuffer(nil)
	newerThan              time.Time
	newerThanUnix          int64 = -1
	localLocation, _             = time.LoadLocation("Local")
)

func printNeedle(vid storage.VolumeId, n *storage.Needle, version storage.Version, deleted bool) {
	key := storage.NewFileIdFromNeedle(vid, n).String()
	size := n.DataSize
	if version == storage.Version1 {
		size = n.Size
	}
	fmt.Printf("\"%s\",\"%s\",%d,%t,%s,%s,%s,%t\n",
		key,
		strings.Replace(string(n.Name), "\"", "\"\"", -1),
		size,
		n.IsGzipped(),
		n.Mime,
		n.LastModifiedString(),
		n.Ttl.String(),
		deleted,
	)
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

	if (*volumeInfo == true) {
		showVolumeInfo(*export.dir, *export.collection, *export.volumeId)
		return true
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
			Typeflag: tar.TypeReg,
			AccessTime: t, ChangeTime: t}
	}

	fileName := strconv.Itoa(*export.volumeId)
	if *export.collection != "" {
		fileName = *export.collection + "_" + fileName
	}
	vid := storage.VolumeId(*export.volumeId)
	indexFile, err := os.OpenFile(path.Join(*export.dir, fileName+".idx"), os.O_RDONLY, 0644)
	if err != nil {
		glog.Fatalf("Create Volume Index [ERROR] %s\n", err)
	}
	defer indexFile.Close()

	needleMap, err := storage.LoadBtreeNeedleMap(indexFile)
	if err != nil {
		glog.Fatalf("cannot load needle map from %s: %s", indexFile.Name(), err)
	}

	var version storage.Version

	if tarOutputFile == nil {
		fmt.Printf("key,name,size,gzip,mime,modified,ttl,deleted\n")
	}

	err = storage.ScanVolumeFile(*export.dir, *export.collection, vid,
		storage.NeedleMapInMemory,
		func(superBlock storage.SuperBlock) error {
			version = superBlock.Version()
			return nil
		}, true, func(n *storage.Needle, offset int64) error {
			nv, ok := needleMap.Get(n.Id)
			glog.V(3).Infof("key %d offset %d size %d disk_size %d gzip %v ok %v nv %+v",
				n.Id, offset, n.Size, n.DiskSize(), n.IsGzipped(), ok, nv)
			if ok && nv.Size > 0 && int64(nv.Offset)*8 == offset {
				if newerThanUnix >= 0 && n.HasLastModifiedDate() && n.LastModified < uint64(newerThanUnix) {
					glog.V(3).Infof("Skipping this file, as it's old enough: LastModified %d vs %d",
						n.LastModified, newerThanUnix)
					return nil
				}

				if tarOutputFile != nil {
					return writeFile(vid, n, version)
				} else {
					printNeedle(vid, n, version, false)
					return nil
				}
			}
			if !ok {
				if *showDeleted && tarOutputFile == nil {
					if (n.DataSize > 0) {
						printNeedle(vid, n, version, true)
					} else {
						n.Name = []byte("*tombstone")
						printNeedle(vid, n, version, true)
					}
				}

				glog.V(2).Infof("This seems deleted %d size %d", n.Id, n.Size)
			} else {
				glog.V(2).Infof("Skipping later-updated Id %d size %d", n.Id, n.Size)
			}
			return nil
		})
	if err != nil {
		glog.Fatalf("Export Volume File [ERROR] %s\n", err)
	}
	return true
}

type nameParams struct {
	Name string
	Id   uint64
	Mime string
	Key  string
	Ext  string
}

func showVolumeInfo(dirname string, collection string, volumeId int) {
	vid := storage.VolumeId(volumeId)
	var volume *storage.Volume
	var err error
	if volume, err = storage.LoadVolume(dirname, collection, vid, storage.NeedleMapInMemory); err != nil {
		fmt.Println("Failed to load volume: " + err.Error())
		return
	}

	var ttl string
	if (volume.Ttl.Minutes() > 0) {
		ttl = volume.Ttl.String()
	} else {
		ttl = "none"
	}
	fmt.Printf("Collection: %s\n"+
		"VolumeId: %v\n"+
		"Content-Size: %v\n"+
		"Content-Count: %v\n"+
		"Deleted-Size: %v\n"+
		"Deleted-Count: %v\n"+
		"Modification-Time: %s\n"+
		"TTL: %s\n",
		volume.Collection,
		volume.Id,
		volume.ContentSize(),
		volume.FileCount(),
		volume.DeletedSize(),
		volume.DeletedCount(),
		volume.LastModifiedString(),
		ttl,
	)
}

func writeFile(vid storage.VolumeId, n *storage.Needle, version storage.Version) (err error) {
	key := storage.NewFileIdFromNeedle(vid, n).String()
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

	if n.IsGzipped() && path.Ext(fileName) != ".gz" {
		fileName = fileName + ".gz"
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
