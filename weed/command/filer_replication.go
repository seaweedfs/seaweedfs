package command

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/spf13/viper"
	"github.com/chrislusf/seaweedfs/weed/replication"
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

	Run "weed scaffold -config replication" to generate a replication.toml file and customize the parameters.

  `,
}

func runFilerReplicate(cmd *Command, args []string) bool {

	weed_server.LoadConfiguration("replication", true)
	config := viper.GetViper()

	var notificationInput replication.NotificationInput

	for _, input := range replication.NotificationInputs {
		if config.GetBool("notification." + input.GetName() + ".enabled") {
			viperSub := config.Sub("notification." + input.GetName())
			if err := input.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize notification input for %s: %+v",
					input.GetName(), err)
			}
			glog.V(0).Infof("Configure notification input to %s", input.GetName())
			notificationInput = input
			break
		}
	}

	replicator := replication.NewReplicator(config.Sub("source.filer"), config.Sub("sink.filer"))

	for {
		key, m, err := notificationInput.ReceiveMessage()
		if err != nil {
			glog.Errorf("receive %s: +v", key, err)
			continue
		}
		if err = replicator.Replicate(key, m); err != nil {
			glog.Errorf("replicate %s: +v", key, err)
		}
	}

	return true
}
