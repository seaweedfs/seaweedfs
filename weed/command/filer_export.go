package command

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"
)

func init() {
	cmdFilerExport.Run = runFilerExport // break init cycle
}

var cmdFilerExport = &Command{
	UsageLine: "filer.export -sourceStore=mysql -targetStore=cassandra",
	Short:     "export meta data in filer store",
	Long: `Iterate the file tree and export all metadata out

	Both source and target store:
        * should be a store name already specified in filer.toml
        * do not need to be enabled state

	If target store is empty, only the directory tree will be listed.

	If target store is "notification", the list of entries will be sent to notification.
	This is usually used to bootstrap filer replication to a remote system.

  `,
}

var (
	// filerExportOutputFile  = cmdFilerExport.Flag.String("output", "", "the output file. If empty, only list out the directory tree")
	filerExportSourceStore = cmdFilerExport.Flag.String("sourceStore", "", "the source store name in filer.toml, default to currently enabled store")
	filerExportTargetStore = cmdFilerExport.Flag.String("targetStore", "", "the target store name in filer.toml, or \"notification\" to export all files to message queue")
	dir                    = cmdFilerExport.Flag.String("dir", "/", "only process files under this directory")
	dirListLimit           = cmdFilerExport.Flag.Int("dirListLimit", 100000, "limit directory list size")
	dryRun                 = cmdFilerExport.Flag.Bool("dryRun", false, "not actually moving data")
	verboseFilerExport     = cmdFilerExport.Flag.Bool("v", false, "verbose entry details")
)

type statistics struct {
	directoryCount int
	fileCount      int
}

func runFilerExport(cmd *Command, args []string) bool {

	weed_server.LoadConfiguration("filer", true)
	config := viper.GetViper()

	var sourceStore, targetStore filer2.FilerStore

	for _, store := range filer2.Stores {
		if store.GetName() == *filerExportSourceStore || *filerExportSourceStore == "" && config.GetBool(store.GetName()+".enabled") {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize source store for %s: %+v",
					store.GetName(), err)
			} else {
				sourceStore = store
			}
			break
		}
	}

	for _, store := range filer2.Stores {
		if store.GetName() == *filerExportTargetStore {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize target store for %s: %+v",
					store.GetName(), err)
			} else {
				targetStore = store
			}
			break
		}
	}

	if sourceStore == nil {
		glog.Errorf("Failed to find source store %s", *filerExportSourceStore)
		println("existing data sources are:")
		for _, store := range filer2.Stores {
			println("    " + store.GetName())
		}
		return false
	}

	if targetStore == nil && *filerExportTargetStore != "" && *filerExportTargetStore != "notification" {
		glog.Errorf("Failed to find target store %s", *filerExportTargetStore)
		println("existing data sources are:")
		for _, store := range filer2.Stores {
			println("    " + store.GetName())
		}
		return false
	}

	stat := statistics{}

	var fn func(level int, entry *filer2.Entry) error

	if *filerExportTargetStore == "notification" {
		weed_server.LoadConfiguration("notification", false)
		v := viper.GetViper()
		notification.LoadConfiguration(v.Sub("notification"))

		fn = func(level int, entry *filer2.Entry) error {
			printout(level, entry)
			if *dryRun {
				return nil
			}
			return notification.Queue.SendMessage(
				string(entry.FullPath),
				&filer_pb.EventNotification{
					NewEntry: entry.ToProtoEntry(),
				},
			)
		}
	} else if targetStore == nil {
		fn = printout
	} else {
		fn = func(level int, entry *filer2.Entry) error {
			printout(level, entry)
			if *dryRun {
				return nil
			}
			return targetStore.InsertEntry(entry)
		}
	}

	doTraverse(&stat, sourceStore, filer2.FullPath(*dir), 0, fn)

	glog.Infof("processed %d directories, %d files", stat.directoryCount, stat.fileCount)

	return true
}

func doTraverse(stat *statistics, filerStore filer2.FilerStore, parentPath filer2.FullPath, level int, fn func(level int, entry *filer2.Entry) error) {

	limit := *dirListLimit
	lastEntryName := ""
	for {
		entries, err := filerStore.ListDirectoryEntries(parentPath, lastEntryName, false, limit)
		if err != nil {
			break
		}
		for _, entry := range entries {
			if fnErr := fn(level, entry); fnErr != nil {
				glog.Errorf("failed to process entry: %s", entry.FullPath)
			}
			if entry.IsDirectory() {
				stat.directoryCount++
				doTraverse(stat, filerStore, entry.FullPath, level+1, fn)
			} else {
				stat.fileCount++
			}
		}
		if len(entries) < limit {
			break
		}
	}
}

func printout(level int, entry *filer2.Entry) error {
	for i := 0; i < level; i++ {
		if i == level-1 {
			print("+-")
		} else {
			print("| ")
		}
	}
	print(entry.FullPath.Name())
	if *verboseFilerExport {
		for _, chunk := range entry.Chunks {
			print("[")
			print(chunk.FileId)
			print(",")
			print(chunk.Offset)
			print(",")
			print(chunk.Size)
			print(")")
		}
	}
	println()
	return nil
}
