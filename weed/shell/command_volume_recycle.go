package shell

import (
	"context"
	"errors"
	"flag"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"io"
	"log"
	"sort"
)

func init() {
	Commands = append(Commands, &commandVolumeRecycle{})
}

type commandVolumeRecycle struct {
}

func (c *commandVolumeRecycle) Name() string {
	return "volume.recycle"
}

func (c *commandVolumeRecycle) Help() string {
	return `volume.recycle -percentageUsed=1  -recycleVolumeCounter=1
	This command commandVolumeRecycle,When the cluster uses storage more than ${percentageUsed}, 
     it will trigger the deletion of the oldest ${recycleVolumeCounter} file
`
}



func (c *commandVolumeRecycle) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	recycleCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	percentageUsed := recycleCommand.Uint64("percentageUsed", 80, "the source volume server <host>:<port>")
	recycleVolumeCounter := recycleCommand.Int("recycleVolumeCounter", 80, "the source volume server <host>:<port>")
	if err = recycleCommand.Parse(args); err != nil {
		return nil
	}
	log.Printf("Param percentageUsed %d ,recycleVolumeCounter: %d \n", *percentageUsed, *recycleVolumeCounter)
	topologyInfo, _, err := collectTopologyInfo(commandEnv)
	if err != nil {
		log.Printf("Error %s",err.Error())
		return err
	}
	dataCenterInfo:=topologyInfo.DataCenterInfos
	volumeIdToVolumeMap := make(map[uint32]string)
	var volumeIds[] uint32
	var volumeServers[] string

	for _, dataCenter := range dataCenterInfo {
		if dataCenter.RackInfos == nil || len(dataCenter.RackInfos) == 0 {
			log.Println("Error dataCenter rack is empty")
			continue
		}
		for _, rack := range  dataCenter.RackInfos {
			if rack.DataNodeInfos == nil || len(rack.DataNodeInfos) == 0 {
				log.Println(" Error BuildClusterVo DataNodeInfos == nil || len(vr.DataNodeInfos) == 0")
				continue
			}
			for _, dataNode := range rack.DataNodeInfos {
				volumeServers=append(volumeServers, dataNode.Id)
				for _, disk := range dataNode.DiskInfos {
					if disk.VolumeInfos == nil || len(disk.VolumeInfos) == 0 {
						log.Println(" Error disk.VolumeInfos == nil || len(disk.VolumeInfos) == 0")
						continue
					}
					for _, volume := range  disk.VolumeInfos{
						volumeIdToVolumeMap[volume.Id] = dataNode.Id
						volumeIds = append(volumeIds, volume.Id)
					}
				}
			}
		}
	}

	sort.Slice(volumeIds, func(i, j int) bool {
		if volumeIds[i] < volumeIds[j] {
			return true
		}
		return false
	})
	diskStatus,errorDiskStatus:=volumeDisk(volumeServers,commandEnv)
	if errorDiskStatus!=nil {
		log.Println("Error",errorDiskStatus.Error())
		return errorDiskStatus
	}
	usedPer := diskStatus.Used*100/diskStatus.All
	log.Printf("Used:%d, all:%d,usedPer:%d\n",diskStatus.Used,diskStatus.All,usedPer)
	if usedPer>= *percentageUsed {
		deleteVolumeCounter:=*recycleVolumeCounter
		if len(volumeIds) < *recycleVolumeCounter{
			deleteVolumeCounter=len(volumeIds)
		}
		for i,volumeId := range volumeIds{
			if i >= deleteVolumeCounter{
				return
			}
			volumeServer :=volumeIdToVolumeMap[volumeId]
			err:=deleteVolume(commandEnv.option.GrpcDialOption, needle.VolumeId(volumeId), volumeServer)
			if err!=nil{
				log.Printf("Error deleteVolume %s volumeId is %d  %s\n",volumeServer,volumeId,err.Error())
				return  err
			}
			log.Printf("deleteVolume %s  volumeId is %d success\n",volumeServer,volumeId)

		}
	}
	log.Printf("VolumeRecycle do success\n")
	return nil
}


/*
 Get cluster storage information
*/
func volumeDisk(volumeServers []string,commandEnv *CommandEnv)  (volume_server_pb.DiskStatus,error){

	var diskAll  uint64
	var diskFree  uint64
	var diskUsed  uint64
	var diskStatus volume_server_pb.DiskStatus
	for _,volumeServer := range  volumeServers{
		err := operation.WithVolumeServerClient(volumeServer, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, statusErr := volumeServerClient.VolumeServerStatus(context.Background(), &volume_server_pb.VolumeServerStatusRequest{
			})
			if statusErr != nil {
				return statusErr
			}
			if(resp.DiskStatuses==nil || len(resp.DiskStatuses) == 0){
				return  errors.New(volumeServer+" Disk is empty")
			}
			for _,disk := range resp.DiskStatuses {
				diskFree+=disk.Free
				diskAll+=disk.All
				diskUsed+=disk.Used
			}
			return nil
		})
		if err != nil {
			return diskStatus,err
		}
	}
	diskStatus.All  = diskAll
	diskStatus.Used = diskUsed
	diskStatus.Free = diskFree
	return diskStatus,nil
}



