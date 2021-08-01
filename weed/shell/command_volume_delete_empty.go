package shell

import (
	"flag"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"io"
	"log"
	"time"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeDeleteEmpty{})
}

type commandVolumeDeleteEmpty struct {
}

func (c *commandVolumeDeleteEmpty) Name() string {
	return "volume.deleteEmpty"
}

func (c *commandVolumeDeleteEmpty) Help() string {
	return `delete empty volumes from all volume servers

	volume.deleteEmpty -quietFor=24h

	This command deletes all empty volumes from one volume server.

`
}

func (c *commandVolumeDeleteEmpty) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	volDeleteCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	quietPeriod := volDeleteCommand.Duration("quietFor", 24*time.Hour, "select empty volumes with no recent writes, avoid newly created ones")
	if err = volDeleteCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv)
	if err != nil {
		return err
	}

	quietSeconds := int64(*quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Size <= 8 && v.ModifiedAtSecond + quietSeconds < nowUnixSeconds {
					log.Printf("deleting empty volume %d from %s", v.Id, dn.Id)
					if deleteErr := deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(v.Id), dn.Id); deleteErr != nil {
						err = deleteErr
					}
				}
			}
		}
	})

	return
}
