package command

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"
)

func init() {
	cmdFilerExport.Run = runFilerExport // break init cycle
}

var cmdFilerExport = &Command{
	UsageLine: "filer.export -sourceStore=mysql -targetStroe=cassandra",
	Short:     "export meta data in filer store",
	Long: `Iterate the file tree and export all metadata out

	Both source and target store:
        * should be a store name already specified in filer.toml
        * do not need to be enabled state

	If target store is empty, only the directory tree will be listed.

  `,
}

var (
	// filerExportOutputFile  = cmdFilerExport.Flag.String("output", "", "the output file. If empty, only list out the directory tree")
	filerExportSourceStore = cmdFilerExport.Flag.String("sourceStore", "", "the source store name in filer.toml")
	filerExportTargetStore = cmdFilerExport.Flag.String("targetStore", "", "the target store name in filer.toml")
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
		if store.GetName() == *filerExportSourceStore {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
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
				glog.Fatalf("Failed to initialize store for %s: %+v",
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

	stat := statistics{}

	var fn func(level int, entry *filer2.Entry) error

	if targetStore == nil {
		fn = printout
	} else {
		fn = func(level int, entry *filer2.Entry) error {
			return targetStore.InsertEntry(entry)
		}
	}

	doTraverse(&stat, sourceStore, filer2.FullPath("/"), 0, fn)

	glog.Infof("processed %d directories, %d files", stat.directoryCount, stat.fileCount)

	return true
}

func doTraverse(stat *statistics, filerStore filer2.FilerStore, parentPath filer2.FullPath, level int, fn func(level int, entry *filer2.Entry) error) {

	limit := 1000
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
		print("  ")
	}
	println(entry.FullPath.Name())
	return nil
}
