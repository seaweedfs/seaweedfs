package command

import (
	"context"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/util/log"
	"github.com/chrislusf/seaweedfs/weed/replication"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/azuresink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/b2sink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/filersink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/gcssink"
	_ "github.com/chrislusf/seaweedfs/weed/replication/sink/s3sink"
	"github.com/chrislusf/seaweedfs/weed/replication/sub"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/spf13/viper"
)

func init() {
	cmdFilerReplicate.Run = runFilerReplicate // break init cycle
}

var cmdFilerReplicate = &Command{
	UsageLine: "filer.replicate",
	Short:     "replicate file changes to another destination",
	Long: `replicate file changes to another destination

	filer.replicate listens on filer notifications. If any file is updated, it will fetch the updated content,
	and write to the other destination.

	Run "weed scaffold -config=replication" to generate a replication.toml file and customize the parameters.

  `,
}

func runFilerReplicate(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	util.LoadConfiguration("replication", true)
	util.LoadConfiguration("notification", true)
	config := util.GetViper()

	var notificationInput sub.NotificationInput

	validateOneEnabledInput(config)

	for _, input := range sub.NotificationInputs {
		if config.GetBool("notification." + input.GetName() + ".enabled") {
			if err := input.Initialize(config, "notification."+input.GetName()+"."); err != nil {
				log.Fatalf("Failed to initialize notification input for %s: %+v",
					input.GetName(), err)
			}
			log.Infof("Configure notification input to %s", input.GetName())
			notificationInput = input
			break
		}
	}

	if notificationInput == nil {
		println("No notification is defined in notification.toml file.")
		println("Please follow 'weed scaffold -config=notification' to see example notification configurations.")
		return true
	}

	// avoid recursive replication
	if config.GetBool("notification.source.filer.enabled") && config.GetBool("notification.sink.filer.enabled") {
		if config.GetString("source.filer.grpcAddress") == config.GetString("sink.filer.grpcAddress") {
			fromDir := config.GetString("source.filer.directory")
			toDir := config.GetString("sink.filer.directory")
			if strings.HasPrefix(toDir, fromDir) {
				log.Fatalf("recursive replication! source directory %s includes the sink directory %s", fromDir, toDir)
			}
		}
	}

	var dataSink sink.ReplicationSink
	for _, sk := range sink.Sinks {
		if config.GetBool("sink." + sk.GetName() + ".enabled") {
			if err := sk.Initialize(config, "sink."+sk.GetName()+"."); err != nil {
				log.Fatalf("Failed to initialize sink for %s: %+v",
					sk.GetName(), err)
			}
			log.Infof("Configure sink to %s", sk.GetName())
			dataSink = sk
			break
		}
	}

	if dataSink == nil {
		println("no data sink configured in replication.toml:")
		for _, sk := range sink.Sinks {
			println("    " + sk.GetName())
		}
		return true
	}

	replicator := replication.NewReplicator(config, "source.filer.", dataSink)

	for {
		key, m, err := notificationInput.ReceiveMessage()
		if err != nil {
			log.Errorf("receive %s: %+v", key, err)
			continue
		}
		if key == "" {
			// long poll received no messages
			continue
		}
		if m.OldEntry != nil && m.NewEntry == nil {
			log.Debugf("delete: %s", key)
		} else if m.OldEntry == nil && m.NewEntry != nil {
			log.Debugf("   add: %s", key)
		} else {
			log.Debugf("modify: %s", key)
		}
		if err = replicator.Replicate(context.Background(), key, m); err != nil {
			log.Errorf("replicate %s: %+v", key, err)
		} else {
			log.Debugf("replicated %s", key)
		}
	}

}

func validateOneEnabledInput(config *viper.Viper) {
	enabledInput := ""
	for _, input := range sub.NotificationInputs {
		if config.GetBool("notification." + input.GetName() + ".enabled") {
			if enabledInput == "" {
				enabledInput = input.GetName()
			} else {
				log.Fatalf("Notification input is enabled for both %s and %s", enabledInput, input.GetName())
			}
		}
	}
}
