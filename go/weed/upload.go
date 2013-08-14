package main

import (
	"code.google.com/p/weed-fs/go/operation"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

var (
	uploadReplication *string
	uploadDir         *string
	include           *string
)

func init() {
	cmdUpload.Run = runUpload // break init cycle
	cmdUpload.IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
	server = cmdUpload.Flag.String("server", "localhost:9333", "weedfs master location")
	uploadDir = cmdUpload.Flag.String("dir", "", "Upload the whole folder recursively if specified.")
	include = cmdUpload.Flag.String("include", "", "pattens of files to upload, e.g., *.pdf, *.html, ab?d.txt, works together with -dir")
	uploadReplication = cmdUpload.Flag.String("replication", "", "replication type(000,001,010,100,110,200)")
}

var cmdUpload = &Command{
	UsageLine: "upload -server=localhost:9333 file1 [file2 file3]\n upload -server=localhost:9333 -dir=one_directory -include=*.pdf",
	Short:     "upload one or a list of files",
	Long: `upload one or a list of files, or batch upload one whole folder recursively.
	
	If uploading a list of files:
  It uses consecutive file keys for the list of files.
  e.g. If the file1 uses key k, file2 can be read via k_1

  If uploading a whole folder recursively:
  All files under the folder and subfolders will be uploaded, each with its own file key.
  Optional parameter "-include" allows you to specify the file name patterns.
  
  If any file has a ".gz" extension, the content are considered gzipped already, and will be stored as is.
  This can save volume server's gzipped processing and allow customizable gzip compression level.
  The file name will strip out ".gz" and stored. For example, "jquery.js.gz" will be stored as "jquery.js".

  `,
}

func runUpload(cmd *Command, args []string) bool {
	if len(cmdUpload.Flag.Args()) == 0 {
		if *uploadDir == "" {
			return false
		}
		filepath.Walk(*uploadDir, func(path string, info os.FileInfo, err error) error {
			if err == nil {
				if !info.IsDir() {
					if *include != "" {
						if ok, _ := filepath.Match(*include, filepath.Base(path)); !ok {
							return nil
						}
					}
					parts, e := operation.NewFileParts([]string{path})
					if e != nil {
						return e
					}
					results, e := operation.SubmitFiles(*server, parts, *uploadReplication)
					bytes, _ := json.Marshal(results)
					fmt.Println(string(bytes))
					if e != nil {
						return e
					}
				}
			} else {
				fmt.Println(err)
			}
			return err
		})
	} else {
		parts, e := operation.NewFileParts(args)
		if e != nil {
			fmt.Println(e.Error())
		}
		results, _ := operation.SubmitFiles(*server, parts, *uploadReplication)
		bytes, _ := json.Marshal(results)
		fmt.Println(string(bytes))
	}
	return true
}
