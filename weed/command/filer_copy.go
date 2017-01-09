package command

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/operation"
	filer_operation "github.com/chrislusf/seaweedfs/weed/operation/filer"
	"github.com/chrislusf/seaweedfs/weed/security"
)

var (
	copy CopyOptions
)

type CopyOptions struct {
	master      *string
	include     *string
	replication *string
	collection  *string
	ttl         *string
	maxMB       *int
	secretKey   *string

	secret security.Secret
}

func init() {
	cmdCopy.Run = runCopy // break init cycle
	cmdCopy.IsDebug = cmdCopy.Flag.Bool("debug", false, "verbose debug information")
	copy.master = cmdCopy.Flag.String("master", "localhost:9333", "SeaweedFS master location")
	copy.include = cmdCopy.Flag.String("include", "", "pattens of files to copy, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	copy.replication = cmdCopy.Flag.String("replication", "", "replication type")
	copy.collection = cmdCopy.Flag.String("collection", "", "optional collection name")
	copy.ttl = cmdCopy.Flag.String("ttl", "", "time to live, e.g.: 1m, 1h, 1d, 1M, 1y")
	copy.maxMB = cmdCopy.Flag.Int("maxMB", 0, "split files larger than the limit")
	copy.secretKey = cmdCopy.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
}

var cmdCopy = &Command{
	UsageLine: "filer.copy file_or_dir1 [file_or_dir2 file_or_dir3] http://localhost:8888/path/to/a/folder/",
	Short:     "copy one or a list of files to a filer folder",
	Long: `copy one or a list of files, or batch copy one whole folder recursively, to a filer folder

  It can copy one or a list of files or folders.

  If copying a whole folder recursively:
  All files under the folder and subfolders will be copyed.
  Optional parameter "-include" allows you to specify the file name patterns.

  If any file has a ".gz" extension, the content are considered gzipped already, and will be stored as is.
  This can save volume server's gzipped processing and allow customizable gzip compression level.
  The file name will strip out ".gz" and stored. For example, "jquery.js.gz" will be stored as "jquery.js".

  If "maxMB" is set to a positive number, files larger than it would be split into chunks and copyed separatedly.
  The list of file ids of those chunks would be stored in an additional chunk, and this additional chunk's file id would be returned.

  `,
}

func runCopy(cmd *Command, args []string) bool {
	copy.secret = security.Secret(*copy.secretKey)
	if len(args) <= 1 {
		return false
	}
	filerDestination := args[len(args)-1]
	fileOrDirs := args[0 : len(args)-1]

	filerUrl, err := url.Parse(filerDestination)
	if err != nil {
		fmt.Printf("The last argument should be a URL on filer: %v\n", err)
		return false
	}
	path := filerUrl.Path
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	for _, fileOrDir := range fileOrDirs {
		if !doEachCopy(fileOrDir, filerUrl.Host, path) {
			return false
		}
	}
	return true
}

func doEachCopy(fileOrDir string, host string, path string) bool {
	f, err := os.Open(fileOrDir)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v", fileOrDir, err)
		return false
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		fmt.Printf("Failed to get stat for file %s: %v", fileOrDir, err)
		return false
	}

	mode := fi.Mode()
	if mode.IsDir() {
		files, _ := ioutil.ReadDir(fileOrDir)
		for _, subFileOrDir := range files {
			if !doEachCopy(fileOrDir+"/"+subFileOrDir.Name(), host, path+fi.Name()+"/") {
				return false
			}
		}
		return true
	}

	// this is a regular file
	if *copy.include != "" {
		if ok, _ := filepath.Match(*copy.include, filepath.Base(fileOrDir)); !ok {
			return true
		}
	}

	parts, err := operation.NewFileParts([]string{fileOrDir})
	if err != nil {
		fmt.Printf("Failed to read file %s: %v", fileOrDir, err)
	}

	results, err := operation.SubmitFiles(*copy.master, parts,
		*copy.replication, *copy.collection, "",
		*copy.ttl, *copy.maxMB, copy.secret)
	if err != nil {
		fmt.Printf("Failed to submit file %s: %v", fileOrDir, err)
	}

	if strings.HasSuffix(path, "/") {
		path = path + fi.Name()
	}

	if err = filer_operation.RegisterFile(host, path, results[0].Fid, copy.secret); err != nil {
		fmt.Printf("Failed to register file %s on %s: %v", fileOrDir, host, err)
		return false
	}

	fmt.Printf("Copy %s => http://%s%s\n", fileOrDir, host, path)

	return true
}
