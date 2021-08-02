package shell

import (
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func TestParsing(t *testing.T) {
	topo := parseOutput(topoData)

	assert.Equal(t, 5, len(topo.DataCenterInfos))

}

func parseOutput(output string) *master_pb.TopologyInfo {
	lines := strings.Split(output, "\n")
	var topo *master_pb.TopologyInfo
	var dc *master_pb.DataCenterInfo
	var rack *master_pb.RackInfo
	var dn *master_pb.DataNodeInfo
	var disk *master_pb.DiskInfo
	for _, line := range lines {
		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		switch parts[0] {
		case "Topology":
			if topo == nil {
				topo = &master_pb.TopologyInfo{}
			}
		case "DataCenter":
			if dc == nil {
				dc = &master_pb.DataCenterInfo{
					Id: parts[1],
				}
				topo.DataCenterInfos = append(topo.DataCenterInfos, dc)
			} else {
				dc = nil
			}
		case "Rack":
			if rack == nil {
				rack = &master_pb.RackInfo{
					Id: parts[1],
				}
				dc.RackInfos = append(dc.RackInfos, rack)
			} else {
				rack = nil
			}
		case "DataNode":
			if dn == nil {
				dn = &master_pb.DataNodeInfo{
					Id:        parts[1],
					DiskInfos: make(map[string]*master_pb.DiskInfo),
				}
				rack.DataNodeInfos = append(rack.DataNodeInfos, dn)
			} else {
				dn = nil
			}
		case "Disk":
			if disk == nil {
				diskType := parts[1][:strings.Index(parts[1], "(")]
				maxVolumeCountStr := parts[1][strings.Index(parts[1], "/")+1:]
				maxVolumeCount, _ := strconv.Atoi(maxVolumeCountStr)
				disk = &master_pb.DiskInfo{
					Type:           diskType,
					MaxVolumeCount: uint64(maxVolumeCount),
				}
				dn.DiskInfos[types.ToDiskType(diskType).String()] = disk
			} else {
				disk = nil
			}
		case "volume":
			volumeLine := line[len("volume "):]
			volume := &master_pb.VolumeInformationMessage{}
			proto.UnmarshalText(volumeLine, volume)
			disk.VolumeInfos = append(disk.VolumeInfos, volume)
		}
	}

	return topo
}

const topoData = `
Topology volumeSizeLimit:20000 MB nvme(volume:1600/1601 active:1419 free:1 remote:0) hdd(volume:426/2759 active:18446744073709551344 free:2333 remote:0)
  DataCenter DefaultDataCenter hdd(volume:426/2759 active:18446744073709551344 free:2333 remote:0) nvme(volume:1600/1601 active:1419 free:1 remote:0)
    Rack DefaultRack nvme(volume:1600/1601 active:1419 free:1 remote:0) hdd(volume:426/2759 active:18446744073709551344 free:2333 remote:0)
      DataNode 10.244.107.16:8080 nvme(volume:32/33 active:33 free:1 remote:0)
        Disk nvme(volume:32/33 active:33 free:1 remote:0)
          volume id:36  size:3804920  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:10810  delete_count:9907  deleted_byte_count:2557278  version:3  compact_revision:6  modified_at_second:1627500575  disk_type:"nvme" 
          volume id:52  size:3834960  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:11124  delete_count:10252  deleted_byte_count:2599071  version:3  compact_revision:8  modified_at_second:1627504771  disk_type:"nvme" 
          volume id:515  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:3  modified_at_second:1627676213  disk_type:"nvme" 
          volume id:582  size:1087813800  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3719  delete_count:3716  deleted_byte_count:1074978006  version:3  compact_revision:1  modified_at_second:1627504948  disk_type:"nvme" 
          volume id:608  size:1083681304  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7883  delete_count:7882  deleted_byte_count:1078951183  version:3  modified_at_second:1627500577  disk_type:"nvme" 
          volume id:622  size:12583208  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627672952  disk_type:"nvme" 
          volume id:633  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627672953  disk_type:"nvme" 
          volume id:664  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627517790  disk_type:"nvme" 
          volume id:727  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627414988  disk_type:"nvme" 
          volume id:737  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1627415149  disk_type:"nvme" 
          volume id:739  size:117441120  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1627415464  disk_type:"nvme" 
          volume id:753  size:142606888  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1627415280  disk_type:"nvme" 
          volume id:754  size:109052352  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1627415433  disk_type:"nvme" 
          volume id:1079  size:20981451056  collection:"braingeneers-backups-swfs"  file_count:12262  delete_count:225  deleted_byte_count:349314158  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1106  size:20974117232  collection:"braingeneers-backups-swfs"  file_count:12258  delete_count:229  deleted_byte_count:383576860  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246103  size:21017335272  collection:"hengenlab"  file_count:8062  version:3  compact_revision:1  modified_at_second:1627416401  disk_type:"nvme" 
          volume id:246106  size:21091278592  collection:"hengenlab"  file_count:8043  version:3  compact_revision:1  modified_at_second:1627416401  disk_type:"nvme" 
          volume id:246112  size:21141587752  collection:"hengenlab"  file_count:8109  version:3  compact_revision:1  modified_at_second:1627416409  disk_type:"nvme" 
          volume id:246118  size:21027658872  collection:"hengenlab"  file_count:8003  version:3  compact_revision:1  modified_at_second:1627416369  disk_type:"nvme" 
          volume id:246119  size:21090047592  collection:"hengenlab"  file_count:8027  version:3  compact_revision:1  modified_at_second:1627416401  disk_type:"nvme" 
          volume id:246147  size:20993918552  collection:"hengenlab"  file_count:8643  delete_count:496  deleted_byte_count:1229459895  version:3  modified_at_second:1627414743  disk_type:"nvme" 
          volume id:246148  size:20988892312  collection:"hengenlab"  file_count:8641  delete_count:578  deleted_byte_count:1420198603  version:3  modified_at_second:1627414690  disk_type:"nvme" 
          volume id:246151  size:21067515568  collection:"hengenlab"  file_count:8011  version:3  modified_at_second:1627416930  disk_type:"nvme" 
          volume id:246156  size:21374966256  collection:"hengenlab"  file_count:8202  version:3  modified_at_second:1627416944  disk_type:"nvme" 
          volume id:246163  size:21293954224  collection:"hengenlab"  file_count:8134  version:3  modified_at_second:1627417333  disk_type:"nvme" 
          volume id:246169  size:21649654648  collection:"hengenlab"  file_count:8228  version:3  modified_at_second:1627417511  disk_type:"nvme" 
          volume id:246174  size:21118992920  collection:"hengenlab"  file_count:8106  version:3  modified_at_second:1627418092  disk_type:"nvme" 
          volume id:246202  size:21384772480  collection:"hengenlab"  file_count:8145  version:3  modified_at_second:1627420091  disk_type:"nvme" 
          volume id:246224  size:21800603816  collection:"hengenlab"  file_count:8349  version:3  modified_at_second:1627421747  disk_type:"nvme" 
          volume id:246231  size:21151072736  collection:"hengenlab"  file_count:8207  version:3  modified_at_second:1627422299  disk_type:"nvme" 
          volume id:246266  size:21310016776  collection:"hengenlab"  file_count:8019  version:3  modified_at_second:1627423869  disk_type:"nvme" 
          volume id:246291  size:21563812856  collection:"hengenlab"  file_count:8243  version:3  modified_at_second:1627425133  disk_type:"nvme" 
          volume id:246311  size:6461898408  collection:"hengenlab"  file_count:2437  version:3  modified_at_second:1627425695  disk_type:"nvme" 
        Disk nvme total size:412228916664 file_count:199695 deleted_file:33285 deleted_bytes:5541635054 
      DataNode 10.244.107.16:8080 total size:412228916664 file_count:199695 deleted_file:33285 deleted_bytes:5541635054 
      DataNode 10.244.107.37:8080 nvme(volume:32/32 active:32 free:0 remote:0)
        Disk nvme(volume:32/32 active:32 free:0 remote:0)
          volume id:57  size:3954520  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:11025  delete_count:10133  deleted_byte_count:2654098  version:3  compact_revision:7  modified_at_second:1627504757  disk_type:"nvme" 
          volume id:60  size:4002800  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:11132  delete_count:10231  deleted_byte_count:2675670  version:3  compact_revision:7  modified_at_second:1627504774  disk_type:"nvme" 
          volume id:578  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627516919  disk_type:"nvme" 
          volume id:599  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627517793  disk_type:"nvme" 
          volume id:610  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627516918  disk_type:"nvme" 
          volume id:760  size:41943328  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:3  modified_at_second:1627415104  disk_type:"nvme" 
          volume id:771  size:71303424  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1627415229  disk_type:"nvme" 
          volume id:779  size:46137712  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504940  disk_type:"nvme" 
          volume id:782  size:100663560  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627415422  disk_type:"nvme" 
          volume id:804  size:41943312  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627500575  disk_type:"nvme" 
          volume id:1124  size:20981558144  collection:"braingeneers-backups-swfs"  file_count:12294  delete_count:238  deleted_byte_count:353310263  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1132  size:20972966960  collection:"braingeneers-backups-swfs"  file_count:12252  delete_count:230  deleted_byte_count:365652225  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133787  size:21301768216  collection:"hengenlab"  file_count:8526  delete_count:8264  deleted_byte_count:20658424014  read_only:true  version:3  modified_at_second:1627419537  disk_type:"nvme" 
          volume id:133819  size:21226668664  collection:"hengenlab"  file_count:8129  delete_count:8129  deleted_byte_count:21226124008  read_only:true  version:3  modified_at_second:1627420866  disk_type:"nvme" 
          volume id:133869  size:21571728944  collection:"hengenlab"  file_count:8249  read_only:true  version:3  compact_revision:1  modified_at_second:1627423103  disk_type:"nvme" 
          volume id:246099  size:93611912  file_count:110  version:3  modified_at_second:1627423623  disk_type:"nvme" 
          volume id:246100  size:102506304  file_count:118  version:3  modified_at_second:1627423803  disk_type:"nvme" 
          volume id:246101  size:105559656  file_count:128  version:3  modified_at_second:1627424043  disk_type:"nvme" 
          volume id:246110  size:21077107912  collection:"hengenlab"  file_count:8028  read_only:true  version:3  compact_revision:1  modified_at_second:1627416386  disk_type:"nvme" 
          volume id:246111  size:21238297720  collection:"hengenlab"  file_count:8218  read_only:true  version:3  compact_revision:1  modified_at_second:1627416416  disk_type:"nvme" 
          volume id:246116  size:21116990672  collection:"hengenlab"  file_count:8000  read_only:true  version:3  compact_revision:1  modified_at_second:1627416386  disk_type:"nvme" 
          volume id:246121  size:21025910752  collection:"hengenlab"  file_count:7929  read_only:true  version:3  compact_revision:1  modified_at_second:1627416401  disk_type:"nvme" 
          volume id:246126  size:21135866264  collection:"hengenlab"  file_count:8120  read_only:true  version:3  compact_revision:1  modified_at_second:1627416416  disk_type:"nvme" 
          volume id:246145  size:20996024600  collection:"hengenlab"  file_count:8650  delete_count:569  deleted_byte_count:1406783580  read_only:true  version:3  modified_at_second:1627414655  disk_type:"nvme" 
          volume id:246159  size:21483403792  collection:"hengenlab"  file_count:8224  delete_count:3  deleted_byte_count:12582975  read_only:true  version:3  modified_at_second:1627417144  disk_type:"nvme" 
          volume id:246170  size:21584773216  collection:"hengenlab"  file_count:8255  read_only:true  version:3  modified_at_second:1627417520  disk_type:"nvme" 
          volume id:246177  size:21240770832  collection:"hengenlab"  file_count:8210  read_only:true  version:3  modified_at_second:1627418752  disk_type:"nvme" 
          volume id:246239  size:21089256352  collection:"hengenlab"  file_count:8018  read_only:true  version:3  modified_at_second:1627423046  disk_type:"nvme" 
          volume id:246262  size:21394156680  collection:"hengenlab"  file_count:8164  read_only:true  version:3  modified_at_second:1627423585  disk_type:"nvme" 
          volume id:246265  size:21327729128  collection:"hengenlab"  file_count:8186  read_only:true  version:3  modified_at_second:1627423877  disk_type:"nvme" 
          volume id:246275  size:1338420296  collection:"hengenlab"  file_count:515  version:3  modified_at_second:1627424194  disk_type:"nvme" 
          volume id:246277  size:1399878536  collection:"hengenlab"  file_count:528  version:3  modified_at_second:1627424194  disk_type:"nvme" 
        Disk nvme total size:364135876200 file_count:171029 deleted_file:37799 deleted_bytes:44036595503 
      DataNode 10.244.107.37:8080 total size:364135876200 file_count:171029 deleted_file:37799 deleted_bytes:44036595503 
      DataNode 10.244.11.0:8080 nvme(volume:69/69 active:68 free:0 remote:0)
        Disk nvme(volume:69/69 active:68 free:0 remote:0)
          volume id:74  size:624088  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:910  read_only:true  version:3  compact_revision:8  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:76  size:575032  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:856  read_only:true  version:3  compact_revision:6  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:204  size:1214000688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:580  read_only:true  version:3  modified_at_second:1623182699  disk_type:"nvme" 
          volume id:227  size:1101811464  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:528  read_only:true  version:3  modified_at_second:1623182338  disk_type:"nvme" 
          volume id:232  size:1164914112  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  read_only:true  version:3  modified_at_second:1623181649  disk_type:"nvme" 
          volume id:286  size:1163548288  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:566  read_only:true  version:3  modified_at_second:1623181548  disk_type:"nvme" 
          volume id:319  size:1189298656  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:593  read_only:true  version:3  modified_at_second:1623182442  disk_type:"nvme" 
          volume id:369  size:1093083336  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:523  read_only:true  version:3  modified_at_second:1623181034  disk_type:"nvme" 
          volume id:377  size:1103962224  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  read_only:true  version:3  modified_at_second:1623180941  disk_type:"nvme" 
          volume id:378  size:1085340552  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:552  read_only:true  version:3  modified_at_second:1623180833  disk_type:"nvme" 
          volume id:383  size:1169239608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:591  read_only:true  version:3  modified_at_second:1623181752  disk_type:"nvme" 
          volume id:404  size:1141464880  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:546  read_only:true  version:3  modified_at_second:1623181966  disk_type:"nvme" 
          volume id:415  size:1134207256  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:552  read_only:true  version:3  modified_at_second:1623181145  disk_type:"nvme" 
          volume id:440  size:1139081320  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:550  read_only:true  version:3  modified_at_second:1623181245  disk_type:"nvme" 
          volume id:468  size:1178437616  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:637  read_only:true  version:3  modified_at_second:1623182594  disk_type:"nvme" 
          volume id:475  size:1156692960  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:578  read_only:true  version:3  modified_at_second:1623181446  disk_type:"nvme" 
          volume id:480  size:1091564648  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:538  read_only:true  version:3  modified_at_second:1623180844  disk_type:"nvme" 
          volume id:521  size:1158613792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:565  read_only:true  version:3  modified_at_second:1623182103  disk_type:"nvme" 
          volume id:553  size:1089047024  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:608  read_only:true  version:3  modified_at_second:1623181345  disk_type:"nvme" 
          volume id:561  size:1092313176  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:689  read_only:true  version:3  modified_at_second:1623182209  disk_type:"nvme" 
          volume id:796  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:2  modified_at_second:1623182856  disk_type:"nvme" 
          volume id:811  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1623184702  disk_type:"nvme" 
          volume id:818  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  read_only:true  version:3  compact_revision:2  modified_at_second:1623183494  disk_type:"nvme" 
          volume id:835  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  read_only:true  version:3  compact_revision:2  modified_at_second:1623183822  disk_type:"nvme" 
          volume id:887  size:83886632  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  read_only:true  version:3  compact_revision:1  modified_at_second:1627504764  disk_type:"nvme" 
          volume id:924  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  read_only:true  version:3  compact_revision:1  modified_at_second:1623183679  disk_type:"nvme" 
          volume id:950  size:104857976  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194342  read_only:true  version:3  compact_revision:1  modified_at_second:1627504751  disk_type:"nvme" 
          volume id:955  size:201327056  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  read_only:true  version:3  compact_revision:2  modified_at_second:1623184026  disk_type:"nvme" 
          volume id:958  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  read_only:true  version:3  compact_revision:1  modified_at_second:1623184142  disk_type:"nvme" 
          volume id:970  size:171966984  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  read_only:true  version:3  compact_revision:2  modified_at_second:1623184419  disk_type:"nvme" 
          volume id:988  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1623182867  disk_type:"nvme" 
          volume id:989  size:8388776  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1623183080  disk_type:"nvme" 
          volume id:996  size:37748888  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:2  modified_at_second:1623182972  disk_type:"nvme" 
          volume id:1023  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1623183253  disk_type:"nvme" 
          volume id:1049  size:20975717256  collection:"braingeneers-backups-swfs"  file_count:12279  delete_count:221  deleted_byte_count:347033818  read_only:true  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1080  size:20972503688  collection:"braingeneers-backups-swfs"  file_count:12296  delete_count:244  deleted_byte_count:399119927  read_only:true  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1095  size:20974245384  collection:"braingeneers-backups-swfs"  file_count:12208  delete_count:244  deleted_byte_count:389921617  read_only:true  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1112  size:20990798696  collection:"braingeneers-backups-swfs"  file_count:12260  delete_count:238  deleted_byte_count:380046331  read_only:true  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133789  size:735486336  collection:"hengenlab"  file_count:282  read_only:true  version:3  compact_revision:1  modified_at_second:1627507744  disk_type:"nvme" 
          volume id:133792  size:881869784  collection:"hengenlab"  file_count:310  read_only:true  version:3  compact_revision:1  modified_at_second:1627507554  disk_type:"nvme" 
          volume id:133802  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507782  disk_type:"nvme" 
          volume id:133811  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507782  disk_type:"nvme" 
          volume id:133812  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507744  disk_type:"nvme" 
          volume id:133815  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:133816  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133832  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133834  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133837  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507634  disk_type:"nvme" 
          volume id:133845  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133855  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133867  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507096  disk_type:"nvme" 
          volume id:133876  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:133880  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507428  disk_type:"nvme" 
          volume id:133881  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133882  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507171  disk_type:"nvme" 
          volume id:133905  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507634  disk_type:"nvme" 
          volume id:133907  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133908  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:133913  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133928  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133932  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:133952  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507152  disk_type:"nvme" 
          volume id:133956  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133959  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:133960  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507153  disk_type:"nvme" 
          volume id:133971  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133972  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507046  disk_type:"nvme" 
          volume id:133986  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133992  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507786  disk_type:"nvme" 
        Disk nvme total size:107126713872 file_count:61758 deleted_file:949 deleted_bytes:1524510370 
      DataNode 10.244.11.0:8080 total size:107126713872 file_count:61758 deleted_file:949 deleted_bytes:1524510370 
      DataNode 10.244.11.19:8080 nvme(volume:66/66 active:62 free:0 remote:0)
        Disk nvme(volume:66/66 active:62 free:0 remote:0)
          volume id:70  size:529288  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:884  version:3  compact_revision:8  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:157  size:1122476512  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:548  version:3  modified_at_second:1623181523  disk_type:"nvme" 
          volume id:158  size:1196081064  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1623182569  disk_type:"nvme" 
          volume id:177  size:1133337560  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:702  version:3  modified_at_second:1623181220  disk_type:"nvme" 
          volume id:277  size:1161156640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:560  version:3  modified_at_second:1623181939  disk_type:"nvme" 
          volume id:315  size:1092148208  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:547  version:3  modified_at_second:1623181359  disk_type:"nvme" 
          volume id:323  size:1163779192  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:559  version:3  modified_at_second:1623182312  disk_type:"nvme" 
          volume id:342  size:1149035344  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1623181954  disk_type:"nvme" 
          volume id:353  size:1252673472  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:608  version:3  modified_at_second:1610497794  disk_type:"nvme" 
          volume id:418  size:1132380048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:549  version:3  modified_at_second:1623181663  disk_type:"nvme" 
          volume id:456  size:1107957400  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:555  version:3  modified_at_second:1623180914  disk_type:"nvme" 
          volume id:457  size:1192491288  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:595  version:3  modified_at_second:1623182726  disk_type:"nvme" 
          volume id:476  size:1147841016  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:573  version:3  modified_at_second:1623182117  disk_type:"nvme" 
          volume id:488  size:1133874736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  version:3  modified_at_second:1623181234  disk_type:"nvme" 
          volume id:489  size:1111273568  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:537  version:3  modified_at_second:1623181048  disk_type:"nvme" 
          volume id:490  size:1129110824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:544  version:3  modified_at_second:1623181535  disk_type:"nvme" 
          volume id:507  size:1108618968  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:632  version:3  modified_at_second:1623180928  disk_type:"nvme" 
          volume id:540  size:1189084392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:584  version:3  modified_at_second:1623182300  disk_type:"nvme" 
          volume id:558  size:1161785320  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:747  version:3  modified_at_second:1623182467  disk_type:"nvme" 
          volume id:894  size:67109000  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623183878  disk_type:"nvme" 
          volume id:902  size:268436096  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1623184409  disk_type:"nvme" 
          volume id:909  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623184037  disk_type:"nvme" 
          volume id:915  size:100663512  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623183176  disk_type:"nvme" 
          volume id:918  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623183739  disk_type:"nvme" 
          volume id:922  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1617901708  disk_type:"nvme" 
          volume id:923  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1617901544  disk_type:"nvme" 
          volume id:966  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183298  disk_type:"nvme" 
          volume id:968  size:79692256  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:1  modified_at_second:1627504913  disk_type:"nvme" 
          volume id:997  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183186  disk_type:"nvme" 
          volume id:998  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183727  disk_type:"nvme" 
          volume id:1001  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  replica_placement:1  version:3  compact_revision:2  modified_at_second:1623183459  disk_type:"nvme" 
          volume id:1002  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183449  disk_type:"nvme" 
          volume id:1014  size:205521504  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1623184132  disk_type:"nvme" 
          volume id:1042  size:376  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  file_count:1  version:3  modified_at_second:1615234634  disk_type:"nvme" 
          volume id:1064  size:20974995560  collection:"braingeneers-backups-swfs"  file_count:12287  delete_count:236  deleted_byte_count:359450306  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1086  size:20974060120  collection:"braingeneers-backups-swfs"  file_count:12171  delete_count:223  deleted_byte_count:381767404  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1102  size:20974457480  collection:"braingeneers-backups-swfs"  file_count:12357  delete_count:233  deleted_byte_count:380291056  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1105  size:20975999912  collection:"braingeneers-backups-swfs"  file_count:12219  delete_count:205  deleted_byte_count:360816088  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1712  size:161457328  file_count:334  version:3  modified_at_second:1627079927  disk_type:"nvme" 
          volume id:133798  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:133818  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506992  disk_type:"nvme" 
          volume id:133824  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133835  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507329  disk_type:"nvme" 
          volume id:133844  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507153  disk_type:"nvme" 
          volume id:133846  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133847  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133849  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507782  disk_type:"nvme" 
          volume id:133854  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506993  disk_type:"nvme" 
          volume id:133858  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507009  disk_type:"nvme" 
          volume id:133859  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507365  disk_type:"nvme" 
          volume id:133860  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133865  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133870  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506993  disk_type:"nvme" 
          volume id:133872  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133877  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:133897  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507046  disk_type:"nvme" 
          volume id:133904  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133942  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:133967  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507329  disk_type:"nvme" 
          volume id:133984  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507553  disk_type:"nvme" 
          volume id:134003  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507096  disk_type:"nvme" 
          volume id:134013  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:134022  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:134023  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:134031  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:134038  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
        Disk nvme total size:106118147352 file_count:60853 deleted_file:899 deleted_bytes:1490713531 
      DataNode 10.244.11.19:8080 total size:106118147352 file_count:60853 deleted_file:899 deleted_bytes:1490713531 
      DataNode 10.244.11.26:8080 nvme(volume:66/66 active:18446744073709551615 free:0 remote:0)
        Disk nvme(volume:66/66 active:18446744073709551615 free:0 remote:0)
          volume id:7  size:288797416  file_count:5629  delete_count:1  deleted_byte_count:266  version:3  modified_at_second:1627417083  disk_type:"nvme" 
          volume id:127  size:8  collection:"._.DS_Store"  version:3  modified_at_second:1610069605  disk_type:"nvme" 
          volume id:144  size:1656454968  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:856  version:3  modified_at_second:1610491353  disk_type:"nvme" 
          volume id:145  size:1662083456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:863  version:3  modified_at_second:1610491353  disk_type:"nvme" 
          volume id:183  size:1264059216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:766  version:3  modified_at_second:1610494389  disk_type:"nvme" 
          volume id:201  size:1220315640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:621  version:3  modified_at_second:1610494836  disk_type:"nvme" 
          volume id:222  size:1464201984  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:703  version:3  modified_at_second:1610496178  disk_type:"nvme" 
          volume id:230  size:1212205592  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:585  version:3  modified_at_second:1610496236  disk_type:"nvme" 
          volume id:234  size:1336320944  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:643  version:3  modified_at_second:1610496298  disk_type:"nvme" 
          volume id:235  size:1356021448  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:648  version:3  modified_at_second:1610496298  disk_type:"nvme" 
          volume id:247  size:1186730544  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:603  version:3  modified_at_second:1610496421  disk_type:"nvme" 
          volume id:298  size:1271544400  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:615  version:3  modified_at_second:1610497013  disk_type:"nvme" 
          volume id:303  size:1261139456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:607  version:3  modified_at_second:1610497082  disk_type:"nvme" 
          volume id:310  size:1183449136  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1610497162  disk_type:"nvme" 
          volume id:328  size:1269368376  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:611  version:3  modified_at_second:1610497407  disk_type:"nvme" 
          volume id:330  size:1211938144  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:596  version:3  modified_at_second:1610497483  disk_type:"nvme" 
          volume id:355  size:1288125688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:623  version:3  modified_at_second:1610497794  disk_type:"nvme" 
          volume id:409  size:1226643888  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610498523  disk_type:"nvme" 
          volume id:420  size:1259995816  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:603  version:3  modified_at_second:1610498676  disk_type:"nvme" 
          volume id:432  size:1270209832  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:625  version:3  modified_at_second:1610498827  disk_type:"nvme" 
          volume id:470  size:1251179768  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:674  version:3  modified_at_second:1610499335  disk_type:"nvme" 
          volume id:550  size:1268929112  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:705  version:3  modified_at_second:1610644201  disk_type:"nvme" 
          volume id:725  size:157216216  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1618444181  disk_type:"nvme" 
          volume id:729  size:142606824  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:3  modified_at_second:1627500611  disk_type:"nvme" 
          volume id:775  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1619387453  disk_type:"nvme" 
          volume id:781  size:134218040  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1617901623  disk_type:"nvme" 
          volume id:824  size:142606856  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1619387455  disk_type:"nvme" 
          volume id:827  size:75497816  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1619387460  disk_type:"nvme" 
          volume id:889  size:71303408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1619387460  disk_type:"nvme" 
          volume id:938  size:171966944  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1619387452  disk_type:"nvme" 
          volume id:948  size:171967008  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627500593  disk_type:"nvme" 
          volume id:977  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1617901619  disk_type:"nvme" 
          volume id:1026  size:104857944  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1618444164  disk_type:"nvme" 
          volume id:1043  size:8  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1045  size:8  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1081  size:20986367400  collection:"braingeneers-backups-swfs"  file_count:12221  delete_count:259  deleted_byte_count:448102839  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1093  size:20975891280  collection:"braingeneers-backups-swfs"  file_count:12292  delete_count:259  deleted_byte_count:412232025  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1096  size:20979632232  collection:"braingeneers-backups-swfs"  file_count:12181  delete_count:212  deleted_byte_count:340140861  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1130  size:20973820192  collection:"braingeneers-backups-swfs"  file_count:12337  delete_count:242  deleted_byte_count:369569018  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133795  size:21385503576  collection:"hengenlab"  file_count:8200  delete_count:8187  deleted_byte_count:21358739950  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133808  size:21202577760  collection:"hengenlab"  file_count:8071  delete_count:8062  deleted_byte_count:21180017048  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133809  size:21230369728  collection:"hengenlab"  file_count:8092  delete_count:8078  deleted_byte_count:21202564777  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133810  size:21235314952  collection:"hengenlab"  file_count:8121  delete_count:8103  deleted_byte_count:21190730900  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133813  size:21313247416  collection:"hengenlab"  file_count:8254  delete_count:8237  deleted_byte_count:21279140176  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133814  size:21289223464  collection:"hengenlab"  file_count:8186  delete_count:8173  deleted_byte_count:21249877828  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133820  size:21328570240  collection:"hengenlab"  file_count:8104  delete_count:8092  deleted_byte_count:21296570114  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133839  size:21083537992  collection:"hengenlab"  file_count:8122  delete_count:8113  deleted_byte_count:21064119544  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133853  size:21604021952  collection:"hengenlab"  file_count:8277  delete_count:8259  deleted_byte_count:21549990235  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133861  size:21144475920  collection:"hengenlab"  file_count:8141  delete_count:8130  deleted_byte_count:21119813441  read_only:true  version:3  modified_at_second:1627414998  disk_type:"nvme" 
          volume id:133868  size:21265175232  collection:"hengenlab"  file_count:8180  delete_count:8173  deleted_byte_count:21247850068  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133875  size:21037253832  collection:"hengenlab"  file_count:7999  delete_count:7987  deleted_byte_count:21008406537  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133885  size:21496349080  collection:"hengenlab"  file_count:8263  delete_count:8248  deleted_byte_count:21464338437  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133887  size:21082600992  collection:"hengenlab"  file_count:8108  delete_count:8101  deleted_byte_count:21062134970  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133895  size:21227764056  collection:"hengenlab"  file_count:8043  delete_count:8028  deleted_byte_count:21195768100  read_only:true  version:3  modified_at_second:1627414998  disk_type:"nvme" 
          volume id:133912  size:21136149656  collection:"hengenlab"  file_count:8097  delete_count:8083  deleted_byte_count:21102052930  read_only:true  version:3  modified_at_second:1627415000  disk_type:"nvme" 
          volume id:133919  size:21105633736  collection:"hengenlab"  file_count:8018  delete_count:8004  deleted_byte_count:21062105129  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133921  size:21380640952  collection:"hengenlab"  file_count:8224  delete_count:8203  deleted_byte_count:21323467123  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133924  size:21496832784  collection:"hengenlab"  file_count:8167  delete_count:8155  deleted_byte_count:21464828475  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133925  size:21108697560  collection:"hengenlab"  file_count:8097  delete_count:8080  deleted_byte_count:21058872203  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133929  size:21172350416  collection:"hengenlab"  file_count:8030  delete_count:8021  deleted_byte_count:21149792478  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133934  size:21249851816  collection:"hengenlab"  file_count:8113  delete_count:8094  deleted_byte_count:21201073969  read_only:true  version:3  modified_at_second:1627414999  disk_type:"nvme" 
          volume id:133941  size:21048173224  collection:"hengenlab"  file_count:8022  delete_count:7869  deleted_byte_count:20648130020  read_only:true  version:3  modified_at_second:1627415036  disk_type:"nvme" 
          volume id:133946  size:21038839736  collection:"hengenlab"  file_count:8043  delete_count:7919  deleted_byte_count:20735263790  read_only:true  version:3  modified_at_second:1627415027  disk_type:"nvme" 
          volume id:133947  size:21113908544  collection:"hengenlab"  file_count:8042  delete_count:256  deleted_byte_count:664802560  read_only:true  version:3  modified_at_second:1627416494  disk_type:"nvme" 
          volume id:133953  size:21149111216  collection:"hengenlab"  file_count:7995  delete_count:277  deleted_byte_count:746591929  read_only:true  version:3  modified_at_second:1627416480  disk_type:"nvme" 
          volume id:246168  size:19535090304  collection:"hengenlab"  file_count:7417  delete_count:1  deleted_byte_count:4194325  version:3  modified_at_second:1627504928  disk_type:"nvme" 
        Disk nvme total size:683067985504 file_count:286274 deleted_file:195908 deleted_bytes:511209670735 
      DataNode 10.244.11.26:8080 total size:683067985504 file_count:286274 deleted_file:195908 deleted_bytes:511209670735 
      DataNode 10.244.11.35:8080 nvme(volume:67/67 active:64 free:0 remote:0)
        Disk nvme(volume:67/67 active:64 free:0 remote:0)
          volume id:45  size:545648  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:812  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:111  size:662374536  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1034  version:3  compact_revision:2  modified_at_second:1627618182  disk_type:"nvme" 
          volume id:196  size:1159925296  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3382  version:3  modified_at_second:1623181741  disk_type:"nvme" 
          volume id:282  size:1173833248  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:568  version:3  modified_at_second:1623182545  disk_type:"nvme" 
          volume id:284  size:1106394184  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:535  version:3  modified_at_second:1623182351  disk_type:"nvme" 
          volume id:287  size:1166213152  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1623182558  disk_type:"nvme" 
          volume id:320  size:1105052992  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  version:3  modified_at_second:1623181433  disk_type:"nvme" 
          volume id:336  size:1148056416  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1623181562  disk_type:"nvme" 
          volume id:347  size:1587167392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:842  version:3  modified_at_second:1610497707  disk_type:"nvme" 
          volume id:350  size:1117751376  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:535  version:3  modified_at_second:1623181421  disk_type:"nvme" 
          volume id:356  size:1196620360  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:583  version:3  modified_at_second:1623182739  disk_type:"nvme" 
          volume id:382  size:1118311696  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1623181132  disk_type:"nvme" 
          volume id:449  size:1154088216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  version:3  modified_at_second:1623182198  disk_type:"nvme" 
          volume id:453  size:1164991128  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1623182184  disk_type:"nvme" 
          volume id:469  size:1140820728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:612  version:3  modified_at_second:1610499328  disk_type:"nvme" 
          volume id:487  size:1111840200  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:563  version:3  modified_at_second:1623180952  disk_type:"nvme" 
          volume id:524  size:1121395600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:543  version:3  modified_at_second:1623181119  disk_type:"nvme" 
          volume id:551  size:1085123224  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:614  version:3  modified_at_second:1623181256  disk_type:"nvme" 
          volume id:562  size:1156026888  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:717  version:3  modified_at_second:1623181993  disk_type:"nvme" 
          volume id:795  size:71303424  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623184096  disk_type:"nvme" 
          volume id:815  size:176161368  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:1  modified_at_second:1627500580  disk_type:"nvme" 
          volume id:850  size:79692200  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623183658  disk_type:"nvme" 
          volume id:863  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183379  disk_type:"nvme" 
          volume id:879  size:109052352  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623184652  disk_type:"nvme" 
          volume id:896  size:138412464  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623184233  disk_type:"nvme" 
          volume id:897  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183286  disk_type:"nvme" 
          volume id:925  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1623183197  disk_type:"nvme" 
          volume id:932  size:79692248  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183959  disk_type:"nvme" 
          volume id:952  size:100663544  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623184374  disk_type:"nvme" 
          volume id:960  size:209715864  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1619387459  disk_type:"nvme" 
          volume id:973  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1623183469  disk_type:"nvme" 
          volume id:1015  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  replica_placement:1  version:3  compact_revision:1  modified_at_second:1623183110  disk_type:"nvme" 
          volume id:1017  size:155190024  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  delete_count:2  deleted_byte_count:8388670  version:3  compact_revision:1  modified_at_second:1627504926  disk_type:"nvme" 
          volume id:1052  size:20979135608  collection:"braingeneers-backups-swfs"  file_count:12284  delete_count:262  deleted_byte_count:434825085  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1054  size:20975860960  collection:"braingeneers-backups-swfs"  file_count:12206  delete_count:252  deleted_byte_count:416631769  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1069  size:20973437296  collection:"braingeneers-backups-swfs"  file_count:12173  delete_count:192  deleted_byte_count:307674992  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1090  size:20976149576  collection:"braingeneers-backups-swfs"  file_count:12258  delete_count:271  deleted_byte_count:438418834  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1690  size:52115880  file_count:18  version:3  modified_at_second:1627078487  disk_type:"nvme" 
          volume id:1691  size:127169592  file_count:41  version:3  modified_at_second:1627078607  disk_type:"nvme" 
          volume id:133793  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:133800  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133801  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:133806  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507096  disk_type:"nvme" 
          volume id:133807  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506993  disk_type:"nvme" 
          volume id:133829  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507744  disk_type:"nvme" 
          volume id:133830  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133850  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507787  disk_type:"nvme" 
          volume id:133856  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133866  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507009  disk_type:"nvme" 
          volume id:133888  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:133893  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:133899  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133900  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507046  disk_type:"nvme" 
          volume id:133903  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133922  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507552  disk_type:"nvme" 
          volume id:133923  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507787  disk_type:"nvme" 
          volume id:133933  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133936  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507153  disk_type:"nvme" 
          volume id:133937  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507782  disk_type:"nvme" 
          volume id:133957  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507634  disk_type:"nvme" 
          volume id:133962  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507648  disk_type:"nvme" 
          volume id:133970  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507787  disk_type:"nvme" 
          volume id:133977  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:133996  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133997  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:134005  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507648  disk_type:"nvme" 
          volume id:134006  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507648  disk_type:"nvme" 
        Disk nvme total size:105852251944 file_count:63762 deleted_file:981 deleted_bytes:1614328027 
      DataNode 10.244.11.35:8080 total size:105852251944 file_count:63762 deleted_file:981 deleted_bytes:1614328027 
      DataNode 10.244.11.50:8080 nvme(volume:69/69 active:67 free:0 remote:0)
        Disk nvme(volume:69/69 active:67 free:0 remote:0)
          volume id:56  size:571720  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:835  version:3  compact_revision:8  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:179  size:1109217312  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:640  version:3  modified_at_second:1623181587  disk_type:"nvme" 
          volume id:214  size:1167120904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:565  version:3  modified_at_second:1623182362  disk_type:"nvme" 
          volume id:251  size:1123351664  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:552  version:3  modified_at_second:1623182621  disk_type:"nvme" 
          volume id:265  size:1110039368  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:539  version:3  modified_at_second:1623180975  disk_type:"nvme" 
          volume id:285  size:1128073600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:546  version:3  modified_at_second:1623182798  disk_type:"nvme" 
          volume id:312  size:1150263504  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1623182048  disk_type:"nvme" 
          volume id:352  size:1102353264  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:529  version:3  modified_at_second:1623180892  disk_type:"nvme" 
          volume id:359  size:1139335592  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:549  version:3  modified_at_second:1623181386  disk_type:"nvme" 
          volume id:361  size:1165494896  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:563  version:3  modified_at_second:1623182289  disk_type:"nvme" 
          volume id:391  size:1137412944  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:543  version:3  modified_at_second:1623181294  disk_type:"nvme" 
          volume id:400  size:1135825296  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:548  version:3  modified_at_second:1623181701  disk_type:"nvme" 
          volume id:437  size:1134849896  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:548  version:3  modified_at_second:1623181886  disk_type:"nvme" 
          volume id:438  size:1103700792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:532  version:3  modified_at_second:1623181497  disk_type:"nvme" 
          volume id:473  size:1113543640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:553  version:3  modified_at_second:1623181081  disk_type:"nvme" 
          volume id:478  size:1126705440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:565  version:3  modified_at_second:1623181181  disk_type:"nvme" 
          volume id:559  size:1078866520  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:678  version:3  modified_at_second:1623180797  disk_type:"nvme" 
          volume id:560  size:1154208824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:726  version:3  modified_at_second:1623182532  disk_type:"nvme" 
          volume id:565  size:907021680  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1411  version:3  modified_at_second:1623182131  disk_type:"nvme" 
          volume id:800  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  replica_placement:1  version:3  compact_revision:1  modified_at_second:1623182899  disk_type:"nvme" 
          volume id:810  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183787  disk_type:"nvme" 
          volume id:831  size:71303424  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627500577  disk_type:"nvme" 
          volume id:832  size:109137960  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:3  modified_at_second:1627504759  disk_type:"nvme" 
          volume id:843  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183346  disk_type:"nvme" 
          volume id:855  size:134218040  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623184455  disk_type:"nvme" 
          volume id:858  size:113246688  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1623183587  disk_type:"nvme" 
          volume id:861  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623184270  disk_type:"nvme" 
          volume id:885  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:3  modified_at_second:1623183222  disk_type:"nvme" 
          volume id:936  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627500605  disk_type:"nvme" 
          volume id:964  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:2  deleted_byte_count:8388670  version:3  compact_revision:1  modified_at_second:1627500575  disk_type:"nvme" 
          volume id:978  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627504768  disk_type:"nvme" 
          volume id:990  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623183527  disk_type:"nvme" 
          volume id:999  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1623182824  disk_type:"nvme" 
          volume id:1058  size:20974301928  collection:"braingeneers-backups-swfs"  file_count:12271  delete_count:226  deleted_byte_count:357273426  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1066  size:20983198856  collection:"braingeneers-backups-swfs"  file_count:12295  delete_count:250  deleted_byte_count:377071913  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1070  size:20990443648  collection:"braingeneers-backups-swfs"  file_count:12314  delete_count:243  deleted_byte_count:359409281  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1074  size:20973205968  collection:"braingeneers-backups-swfs"  file_count:12296  delete_count:259  deleted_byte_count:436010847  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1689  size:86017768  file_count:28  version:3  modified_at_second:1627417983  disk_type:"nvme" 
          volume id:133794  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507153  disk_type:"nvme" 
          volume id:133796  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507046  disk_type:"nvme" 
          volume id:133803  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:133804  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133821  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:133822  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507787  disk_type:"nvme" 
          volume id:133823  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133827  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133833  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:133842  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133848  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507742  disk_type:"nvme" 
          volume id:133851  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133863  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133871  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133889  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507648  disk_type:"nvme" 
          volume id:133909  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133918  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133939  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133955  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133973  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133978  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506993  disk_type:"nvme" 
          volume id:133981  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133983  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133987  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507744  disk_type:"nvme" 
          volume id:133988  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506992  disk_type:"nvme" 
          volume id:133990  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:134000  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:134004  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507428  disk_type:"nvme" 
          volume id:134008  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:134009  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:134019  size:11910676920  collection:"hengenlab"  file_count:4551  delete_count:759  deleted_byte_count:1984107610  version:3  modified_at_second:1627418220  disk_type:"nvme" 
        Disk nvme total size:117041884784 file_count:65804 deleted_file:1743 deleted_bytes:3539039087 
      DataNode 10.244.11.50:8080 total size:117041884784 file_count:65804 deleted_file:1743 deleted_bytes:3539039087 
      DataNode 10.244.11.52:8080 nvme(volume:69/69 active:18446744073709551613 free:0 remote:0)
        Disk nvme(volume:69/69 active:18446744073709551613 free:0 remote:0)
          volume id:5  size:867222824  file_count:5786  delete_count:1  deleted_byte_count:466  version:3  modified_at_second:1627416063  disk_type:"nvme" 
          volume id:51  size:3819480  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:10823  delete_count:9979  deleted_byte_count:2611708  version:3  compact_revision:6  modified_at_second:1627504769  disk_type:"nvme" 
          volume id:53  size:3907512  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:11182  delete_count:10273  deleted_byte_count:2626115  version:3  compact_revision:7  modified_at_second:1627504756  disk_type:"nvme" 
          volume id:156  size:1173203784  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:571  version:3  modified_at_second:1610491395  disk_type:"nvme" 
          volume id:165  size:1596585608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:879  version:3  modified_at_second:1610491424  disk_type:"nvme" 
          volume id:169  size:1860837016  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:898  version:3  modified_at_second:1610491453  disk_type:"nvme" 
          volume id:211  size:1168359520  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1623182750  disk_type:"nvme" 
          volume id:243  size:1938336304  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:937  version:3  modified_at_second:1610496363  disk_type:"nvme" 
          volume id:290  size:1185325216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610496936  disk_type:"nvme" 
          volume id:294  size:1264804928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:611  version:3  modified_at_second:1610497013  disk_type:"nvme" 
          volume id:313  size:1134907416  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:572  version:3  modified_at_second:1610497245  disk_type:"nvme" 
          volume id:314  size:1117357896  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:557  version:3  modified_at_second:1610497245  disk_type:"nvme" 
          volume id:331  size:1790162800  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:876  version:3  modified_at_second:1610497489  disk_type:"nvme" 
          volume id:387  size:1243238032  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:623  version:3  modified_at_second:1610498218  disk_type:"nvme" 
          volume id:402  size:1265153576  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:604  version:3  modified_at_second:1610498457  disk_type:"nvme" 
          volume id:405  size:1286493360  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:615  version:3  modified_at_second:1610498457  disk_type:"nvme" 
          volume id:411  size:1217380888  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:592  version:3  modified_at_second:1610498523  disk_type:"nvme" 
          volume id:443  size:1197826328  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:599  version:3  modified_at_second:1610498998  disk_type:"nvme" 
          volume id:495  size:1117441440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1161  version:3  modified_at_second:1610643719  disk_type:"nvme" 
          volume id:505  size:1214893912  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:682  version:3  modified_at_second:1610643815  disk_type:"nvme" 
          volume id:537  size:1286418000  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:653  version:3  modified_at_second:1610644085  disk_type:"nvme" 
          volume id:772  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1618444174  disk_type:"nvme" 
          volume id:774  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1618444167  disk_type:"nvme" 
          volume id:799  size:71303456  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504952  disk_type:"nvme" 
          volume id:807  size:109052376  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:1  modified_at_second:1627500582  disk_type:"nvme" 
          volume id:816  size:142606848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:3  modified_at_second:1619387452  disk_type:"nvme" 
          volume id:845  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1618444179  disk_type:"nvme" 
          volume id:869  size:205521512  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1619387455  disk_type:"nvme" 
          volume id:886  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1617901709  disk_type:"nvme" 
          volume id:900  size:146801288  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1618444176  disk_type:"nvme" 
          volume id:901  size:71303408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1619387458  disk_type:"nvme" 
          volume id:953  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1618444190  disk_type:"nvme" 
          volume id:983  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1617901557  disk_type:"nvme" 
          volume id:993  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1617901738  disk_type:"nvme" 
          volume id:995  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627500583  disk_type:"nvme" 
          volume id:1032  size:8  collection:"swfstest"  version:3  modified_at_second:1613592642  disk_type:"nvme" 
          volume id:1034  size:314720  collection:"swfstest"  file_count:1  version:3  modified_at_second:1615234671  disk_type:"nvme" 
          volume id:1036  size:8  collection:"swfstest"  version:3  modified_at_second:1613592642  disk_type:"nvme" 
          volume id:1040  size:8  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  version:3  modified_at_second:1615234634  disk_type:"nvme" 
          volume id:1108  size:20973179648  collection:"braingeneers-backups-swfs"  file_count:12158  delete_count:244  deleted_byte_count:385586736  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1113  size:20973829552  collection:"braingeneers-backups-swfs"  file_count:12295  delete_count:244  deleted_byte_count:373814340  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1118  size:20975423768  collection:"braingeneers-backups-swfs"  file_count:12224  delete_count:241  deleted_byte_count:390765751  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1128  size:20973005760  collection:"braingeneers-backups-swfs"  file_count:12190  delete_count:202  deleted_byte_count:344596437  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133799  size:21750903688  collection:"hengenlab"  file_count:8212  delete_count:8193  deleted_byte_count:21712289804  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133805  size:21273889584  collection:"hengenlab"  file_count:8145  delete_count:8112  deleted_byte_count:21207557857  read_only:true  version:3  modified_at_second:1627415108  disk_type:"nvme" 
          volume id:133838  size:21263362952  collection:"hengenlab"  file_count:8081  delete_count:8049  deleted_byte_count:21206621302  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133840  size:21105726928  collection:"hengenlab"  file_count:7979  delete_count:7955  deleted_byte_count:21053067891  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133843  size:21232229808  collection:"hengenlab"  file_count:8165  delete_count:8136  deleted_byte_count:21165213558  read_only:true  version:3  modified_at_second:1627415108  disk_type:"nvme" 
          volume id:133874  size:21388518216  collection:"hengenlab"  file_count:8258  delete_count:8228  deleted_byte_count:21317668425  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133879  size:21308860696  collection:"hengenlab"  file_count:8134  delete_count:8105  deleted_byte_count:21239616052  read_only:true  version:3  modified_at_second:1627415108  disk_type:"nvme" 
          volume id:133891  size:21362969712  collection:"hengenlab"  file_count:8224  delete_count:8191  deleted_byte_count:21288679522  read_only:true  version:3  modified_at_second:1627415108  disk_type:"nvme" 
          volume id:133896  size:21320296680  collection:"hengenlab"  file_count:8088  delete_count:8056  deleted_byte_count:21268045834  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133901  size:21452285712  collection:"hengenlab"  file_count:8275  delete_count:8236  deleted_byte_count:21367797021  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133926  size:21496921192  collection:"hengenlab"  file_count:8262  delete_count:8222  deleted_byte_count:21409836098  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133935  size:21569024976  collection:"hengenlab"  file_count:8244  delete_count:8217  deleted_byte_count:21511029987  read_only:true  version:3  modified_at_second:1627415106  disk_type:"nvme" 
          volume id:133943  size:21075121824  collection:"hengenlab"  file_count:8036  delete_count:8003  deleted_byte_count:21024539839  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133950  size:21358468792  collection:"hengenlab"  file_count:8146  delete_count:8124  deleted_byte_count:21307547425  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133954  size:21214186200  collection:"hengenlab"  file_count:8146  delete_count:8117  deleted_byte_count:21148574345  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133963  size:21211632160  collection:"hengenlab"  file_count:8117  delete_count:8080  deleted_byte_count:21136356572  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133974  size:21672456128  collection:"hengenlab"  file_count:8256  delete_count:8229  deleted_byte_count:21621369690  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133976  size:21244430736  collection:"hengenlab"  file_count:8169  delete_count:8151  deleted_byte_count:21202380779  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:133982  size:21537083600  collection:"hengenlab"  file_count:8294  delete_count:8263  deleted_byte_count:21478826246  read_only:true  version:3  modified_at_second:1627415106  disk_type:"nvme" 
          volume id:133993  size:21564559944  collection:"hengenlab"  file_count:8317  delete_count:8285  deleted_byte_count:21496628385  version:3  modified_at_second:1627504914  disk_type:"nvme" 
          volume id:133995  size:21423980504  collection:"hengenlab"  file_count:8144  delete_count:8114  deleted_byte_count:21370150782  version:3  modified_at_second:1627504954  disk_type:"nvme" 
          volume id:134001  size:21561824664  collection:"hengenlab"  file_count:8226  delete_count:8202  deleted_byte_count:21506874515  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:134012  size:21315415112  collection:"hengenlab"  file_count:8205  delete_count:8178  deleted_byte_count:21273728833  read_only:true  version:3  modified_at_second:1627415108  disk_type:"nvme" 
          volume id:134017  size:21038970704  collection:"hengenlab"  file_count:8041  delete_count:8019  deleted_byte_count:21006048043  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:134021  size:21812890272  collection:"hengenlab"  file_count:8169  delete_count:8146  deleted_byte_count:21758730994  read_only:true  version:3  modified_at_second:1627415107  disk_type:"nvme" 
          volume id:246138  size:17534561016  collection:"hengenlab"  file_count:6813  delete_count:4263  deleted_byte_count:11139431579  version:3  compact_revision:1  modified_at_second:1627607477  disk_type:"nvme" 
        Disk nvme total size:662392206984 file_count:300461 deleted_file:229062 deleted_bytes:545735390285 
      DataNode 10.244.11.52:8080 total size:662392206984 file_count:300461 deleted_file:229062 deleted_bytes:545735390285 
      DataNode 10.244.11.6:8080 nvme(volume:67/67 active:62 free:0 remote:0)
        Disk nvme(volume:67/67 active:62 free:0 remote:0)
          volume id:54  size:582088  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:866  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:114  size:1177912456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  compact_revision:2  modified_at_second:1623182645  disk_type:"nvme" 
          volume id:188  size:1121756368  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3593  version:3  modified_at_second:1623180997  disk_type:"nvme" 
          volume id:212  size:1085089760  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:525  version:3  modified_at_second:1623180819  disk_type:"nvme" 
          volume id:213  size:1093854120  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:533  version:3  modified_at_second:1623180866  disk_type:"nvme" 
          volume id:246  size:1131416216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1623182775  disk_type:"nvme" 
          volume id:275  size:1110571296  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:535  version:3  modified_at_second:1623181614  disk_type:"nvme" 
          volume id:301  size:1146491504  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:550  version:3  modified_at_second:1623181914  disk_type:"nvme" 
          volume id:318  size:1161820360  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:580  version:3  modified_at_second:1623182159  disk_type:"nvme" 
          volume id:372  size:1109617464  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:534  version:3  modified_at_second:1623182389  disk_type:"nvme" 
          volume id:379  size:1130289520  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:563  version:3  modified_at_second:1623181676  disk_type:"nvme" 
          volume id:393  size:1093146672  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:521  version:3  modified_at_second:1623181269  disk_type:"nvme" 
          volume id:423  size:1161451208  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:556  version:3  modified_at_second:1623182020  disk_type:"nvme" 
          volume id:426  size:1105000584  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:534  version:3  modified_at_second:1623181408  disk_type:"nvme" 
          volume id:464  size:1159754728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:570  version:3  modified_at_second:1623182262  disk_type:"nvme" 
          volume id:496  size:1156809856  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1127  version:3  modified_at_second:1623181470  disk_type:"nvme" 
          volume id:541  size:1125292336  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:557  version:3  modified_at_second:1623181059  disk_type:"nvme" 
          volume id:563  size:888898664  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1454  version:3  modified_at_second:1623182506  disk_type:"nvme" 
          volume id:566  size:848439816  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1364  version:3  modified_at_second:1623181207  disk_type:"nvme" 
          volume id:847  size:142606872  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623184293  disk_type:"nvme" 
          volume id:870  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183901  disk_type:"nvme" 
          volume id:878  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1623182929  disk_type:"nvme" 
          volume id:913  size:176161392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1623184715  disk_type:"nvme" 
          volume id:914  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623183321  disk_type:"nvme" 
          volume id:939  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627504756  disk_type:"nvme" 
          volume id:942  size:83886648  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623184016  disk_type:"nvme" 
          volume id:943  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623183611  disk_type:"nvme" 
          volume id:951  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623182985  disk_type:"nvme" 
          volume id:1005  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623182879  disk_type:"nvme" 
          volume id:1016  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1623183069  disk_type:"nvme" 
          volume id:1020  size:41943328  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183243  disk_type:"nvme" 
          volume id:1021  size:150995672  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1623184430  disk_type:"nvme" 
          volume id:1022  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183690  disk_type:"nvme" 
          volume id:1051  size:20975020608  collection:"braingeneers-backups-swfs"  file_count:12343  delete_count:252  deleted_byte_count:417786404  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1067  size:20975862216  collection:"braingeneers-backups-swfs"  file_count:12195  delete_count:239  deleted_byte_count:365540086  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1076  size:20973686664  collection:"braingeneers-backups-swfs"  file_count:12181  delete_count:239  deleted_byte_count:386434480  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1101  size:20977481056  collection:"braingeneers-backups-swfs"  file_count:12056  delete_count:235  deleted_byte_count:391033535  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1688  size:99418104  file_count:36  version:3  modified_at_second:1627079989  disk_type:"nvme" 
          volume id:133788  size:721435976  collection:"hengenlab"  file_count:280  read_only:true  version:3  compact_revision:1  modified_at_second:1627507172  disk_type:"nvme" 
          volume id:133790  size:760234520  collection:"hengenlab"  file_count:302  read_only:true  version:3  compact_revision:1  modified_at_second:1627507154  disk_type:"nvme" 
          volume id:133817  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507171  disk_type:"nvme" 
          volume id:133852  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507634  disk_type:"nvme" 
          volume id:133857  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507171  disk_type:"nvme" 
          volume id:133862  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:133884  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133886  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507202  disk_type:"nvme" 
          volume id:133892  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507364  disk_type:"nvme" 
          volume id:133894  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507574  disk_type:"nvme" 
          volume id:133898  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133910  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133914  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507649  disk_type:"nvme" 
          volume id:133915  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:133917  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507782  disk_type:"nvme" 
          volume id:133920  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507152  disk_type:"nvme" 
          volume id:133927  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507786  disk_type:"nvme" 
          volume id:133938  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:133958  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507084  disk_type:"nvme" 
          volume id:133964  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507170  disk_type:"nvme" 
          volume id:133965  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507096  disk_type:"nvme" 
          volume id:133966  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627507788  disk_type:"nvme" 
          volume id:133968  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507783  disk_type:"nvme" 
          volume id:134007  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507635  disk_type:"nvme" 
          volume id:134010  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:134014  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507009  disk_type:"nvme" 
          volume id:134015  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507046  disk_type:"nvme" 
          volume id:134020  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507555  disk_type:"nvme" 
          volume id:134032  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627507552  disk_type:"nvme" 
        Disk nvme total size:106360886704 file_count:65576 deleted_file:966 deleted_bytes:1564988840 
      DataNode 10.244.11.6:8080 total size:106360886704 file_count:65576 deleted_file:966 deleted_bytes:1564988840 
      DataNode 10.244.14.13:8080 nvme(volume:55/55 active:55 free:0 remote:0)
        Disk nvme(volume:55/55 active:55 free:0 remote:0)
          volume id:50  size:606568  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:875  version:3  compact_revision:8  modified_at_second:1627507801  disk_type:"nvme" 
          volume id:62  size:539952  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:845  version:3  compact_revision:8  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:107  size:314720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507804  disk_type:"nvme" 
          volume id:187  size:1096273400  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3550  version:3  modified_at_second:1623181484  disk_type:"nvme" 
          volume id:225  size:1132809152  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:541  version:3  modified_at_second:1623181070  disk_type:"nvme" 
          volume id:240  size:1087756960  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:530  version:3  modified_at_second:1623180808  disk_type:"nvme" 
          volume id:241  size:1115629952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:537  version:3  modified_at_second:1623182518  disk_type:"nvme" 
          volume id:256  size:1180952408  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1623182034  disk_type:"nvme" 
          volume id:260  size:1140938104  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:565  version:3  modified_at_second:1623181689  disk_type:"nvme" 
          volume id:280  size:1161495456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:560  version:3  modified_at_second:1623182276  disk_type:"nvme" 
          volume id:332  size:1143200456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:561  version:3  modified_at_second:1623181281  disk_type:"nvme" 
          volume id:375  size:1126027536  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:541  version:3  modified_at_second:1623182634  disk_type:"nvme" 
          volume id:417  size:1113452152  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:542  version:3  modified_at_second:1623180986  disk_type:"nvme" 
          volume id:421  size:1155229640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:553  version:3  modified_at_second:1623181397  disk_type:"nvme" 
          volume id:442  size:1133829768  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:549  version:3  modified_at_second:1623181600  disk_type:"nvme" 
          volume id:462  size:1151065072  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1623182376  disk_type:"nvme" 
          volume id:465  size:1188575600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1623182145  disk_type:"nvme" 
          volume id:482  size:1174497216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:578  version:3  modified_at_second:1623181900  disk_type:"nvme" 
          volume id:484  size:1103270248  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:542  version:3  modified_at_second:1623182786  disk_type:"nvme" 
          volume id:535  size:1094099840  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:535  version:3  modified_at_second:1623180878  disk_type:"nvme" 
          volume id:738  size:117441120  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504861  disk_type:"nvme" 
          volume id:750  size:71303424  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:768  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183232  disk_type:"nvme" 
          volume id:787  size:104857976  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504767  disk_type:"nvme" 
          volume id:791  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183799  disk_type:"nvme" 
          volume id:794  size:37748888  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623183153  disk_type:"nvme" 
          volume id:828  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1623182834  disk_type:"nvme" 
          volume id:848  size:205521512  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1623184727  disk_type:"nvme" 
          volume id:865  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623183913  disk_type:"nvme" 
          volume id:899  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623184558  disk_type:"nvme" 
          volume id:903  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623182997  disk_type:"nvme" 
          volume id:906  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623183416  disk_type:"nvme" 
          volume id:907  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627504765  disk_type:"nvme" 
          volume id:910  size:71303400  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504866  disk_type:"nvme" 
          volume id:945  size:41943328  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623182919  disk_type:"nvme" 
          volume id:979  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623184004  disk_type:"nvme" 
          volume id:991  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  replica_placement:1  version:3  compact_revision:1  modified_at_second:1623182889  disk_type:"nvme" 
          volume id:1007  size:109052368  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623183517  disk_type:"nvme" 
          volume id:1010  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:3  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:1013  size:75497816  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623183059  disk_type:"nvme" 
          volume id:1057  size:20978064192  collection:"braingeneers-backups-swfs"  file_count:12314  delete_count:266  deleted_byte_count:429920466  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1071  size:20983645528  collection:"braingeneers-backups-swfs"  file_count:12232  delete_count:226  deleted_byte_count:337465633  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1085  size:20980098720  collection:"braingeneers-backups-swfs"  file_count:12199  delete_count:219  deleted_byte_count:351274345  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1107  size:20976787256  collection:"braingeneers-backups-swfs"  file_count:12172  delete_count:234  deleted_byte_count:390347084  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246219  size:21568158600  collection:"hengenlab"  file_count:8212  read_only:true  version:3  modified_at_second:1627421476  disk_type:"nvme" 
          volume id:246280  size:21407063640  collection:"hengenlab"  file_count:8202  read_only:true  version:3  modified_at_second:1627424647  disk_type:"nvme" 
          volume id:246329  size:21804618416  collection:"hengenlab"  file_count:7699  delete_count:45  deleted_byte_count:188745804  version:3  modified_at_second:1627427159  disk_type:"nvme" 
          volume id:246345  size:21686381280  collection:"hengenlab"  file_count:7709  delete_count:114  deleted_byte_count:412094707  version:3  modified_at_second:1627427893  disk_type:"nvme" 
          volume id:246435  size:21127287088  collection:"hengenlab"  file_count:8059  version:3  modified_at_second:1627432819  disk_type:"nvme" 
          volume id:246439  size:21187939544  collection:"hengenlab"  file_count:8060  version:3  modified_at_second:1627433637  disk_type:"nvme" 
          volume id:246465  size:21302108360  collection:"hengenlab"  file_count:8110  version:3  modified_at_second:1627435459  disk_type:"nvme" 
          volume id:246467  size:21106211024  collection:"hengenlab"  file_count:7989  version:3  modified_at_second:1627435443  disk_type:"nvme" 
          volume id:246500  size:21299912208  collection:"hengenlab"  file_count:8139  version:3  modified_at_second:1627437468  disk_type:"nvme" 
          volume id:246529  size:21387182640  collection:"hengenlab"  file_count:7596  delete_count:1362  deleted_byte_count:5033550990  version:3  modified_at_second:1627521905  disk_type:"nvme" 
          volume id:246537  size:2061659304  collection:"hengenlab"  file_count:789  version:3  compact_revision:1  modified_at_second:1627528630  disk_type:"nvme" 
        Disk nvme total size:320705385640 file_count:143670 deleted_file:2470 deleted_bytes:7160176376 
      DataNode 10.244.14.13:8080 total size:320705385640 file_count:143670 deleted_file:2470 deleted_bytes:7160176376 
      DataNode 10.244.14.16:8080 nvme(volume:51/51 active:51 free:0 remote:0)
        Disk nvme(volume:51/51 active:51 free:0 remote:0)
          volume id:30  size:648000  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:902  version:3  compact_revision:7  modified_at_second:1627505190  disk_type:"nvme" 
          volume id:32  size:564624  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:842  version:3  compact_revision:7  modified_at_second:1627500691  disk_type:"nvme" 
          volume id:161  size:1513609624  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:842  version:3  modified_at_second:1610491424  disk_type:"nvme" 
          volume id:185  size:1115169696  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3651  version:3  modified_at_second:1623182480  disk_type:"nvme" 
          volume id:186  size:1117871912  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3492  version:3  modified_at_second:1623182672  disk_type:"nvme" 
          volume id:208  size:1122742344  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:537  version:3  modified_at_second:1610494885  disk_type:"nvme" 
          volume id:210  size:1144331496  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:557  version:3  modified_at_second:1610495937  disk_type:"nvme" 
          volume id:239  size:1088423064  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:528  version:3  modified_at_second:1623182686  disk_type:"nvme" 
          volume id:242  size:1157461608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:563  version:3  modified_at_second:1610496354  disk_type:"nvme" 
          volume id:261  size:1368092280  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:678  version:3  modified_at_second:1610496544  disk_type:"nvme" 
          volume id:267  size:1222228680  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:587  version:3  modified_at_second:1610496631  disk_type:"nvme" 
          volume id:295  size:1310628488  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:634  version:3  modified_at_second:1610497013  disk_type:"nvme" 
          volume id:335  size:1133122320  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:556  version:3  modified_at_second:1610497554  disk_type:"nvme" 
          volume id:370  size:1108723656  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:531  version:3  modified_at_second:1610497955  disk_type:"nvme" 
          volume id:414  size:1503127160  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:728  version:3  modified_at_second:1610498598  disk_type:"nvme" 
          volume id:451  size:1369016200  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:656  version:3  modified_at_second:1610499083  disk_type:"nvme" 
          volume id:479  size:1109948384  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:551  version:3  modified_at_second:1610499500  disk_type:"nvme" 
          volume id:486  size:1155571464  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1610499565  disk_type:"nvme" 
          volume id:508  size:1269203968  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:714  version:3  modified_at_second:1610643815  disk_type:"nvme" 
          volume id:532  size:1229401224  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:649  version:3  modified_at_second:1610644025  disk_type:"nvme" 
          volume id:603  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627500690  disk_type:"nvme" 
          volume id:640  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627500690  disk_type:"nvme" 
          volume id:723  size:201327112  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1617901713  disk_type:"nvme" 
          volume id:724  size:138412472  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1618444183  disk_type:"nvme" 
          volume id:735  size:146801240  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1619387461  disk_type:"nvme" 
          volume id:756  size:306185064  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  version:3  compact_revision:1  modified_at_second:1618444172  disk_type:"nvme" 
          volume id:767  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1617901606  disk_type:"nvme" 
          volume id:778  size:109052368  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1618444189  disk_type:"nvme" 
          volume id:809  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1619387464  disk_type:"nvme" 
          volume id:812  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1617901679  disk_type:"nvme" 
          volume id:817  size:205521504  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1623184210  disk_type:"nvme" 
          volume id:873  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1618444187  disk_type:"nvme" 
          volume id:883  size:109052376  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504925  disk_type:"nvme" 
          volume id:884  size:171966992  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1619387463  disk_type:"nvme" 
          volume id:893  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  version:3  compact_revision:2  modified_at_second:1612586373  disk_type:"nvme" 
          volume id:898  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184398  disk_type:"nvme" 
          volume id:971  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1618444168  disk_type:"nvme" 
          volume id:972  size:138582776  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1623184122  disk_type:"nvme" 
          volume id:981  size:142606800  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1619387449  disk_type:"nvme" 
          volume id:1000  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184677  disk_type:"nvme" 
          volume id:1073  size:20974354856  collection:"braingeneers-backups-swfs"  file_count:12217  delete_count:265  deleted_byte_count:434691544  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1078  size:20971811752  collection:"braingeneers-backups-swfs"  file_count:12332  delete_count:237  deleted_byte_count:394927193  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1109  size:20972641960  collection:"braingeneers-backups-swfs"  file_count:12269  delete_count:242  deleted_byte_count:399545377  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1115  size:20982669168  collection:"braingeneers-backups-swfs"  file_count:12209  delete_count:233  deleted_byte_count:385827098  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246181  size:21492162832  collection:"hengenlab"  file_count:8199  delete_count:1  deleted_byte_count:4194325  version:3  modified_at_second:1627504749  disk_type:"nvme" 
          volume id:246196  size:21810328512  collection:"hengenlab"  file_count:8345  read_only:true  version:3  modified_at_second:1627419331  disk_type:"nvme" 
          volume id:246308  size:22074879448  collection:"hengenlab"  file_count:8483  version:3  modified_at_second:1627426119  disk_type:"nvme" 
          volume id:246314  size:22754854488  collection:"hengenlab"  file_count:8677  version:3  modified_at_second:1627426211  disk_type:"nvme" 
          volume id:246442  size:21077837888  collection:"hengenlab"  file_count:8039  version:3  modified_at_second:1627433651  disk_type:"nvme" 
          volume id:246473  size:587394360  collection:"hengenlab"  file_count:227  version:3  modified_at_second:1627435784  disk_type:"nvme" 
          volume id:246475  size:713161392  collection:"hengenlab"  file_count:268  version:3  modified_at_second:1627435784  disk_type:"nvme" 
        Disk nvme total size:218759028160 file_count:110110 deleted_file:979 deleted_bytes:1623379879 
      DataNode 10.244.14.16:8080 total size:218759028160 file_count:110110 deleted_file:979 deleted_bytes:1623379879 
      DataNode 10.244.14.38:8080 nvme(volume:48/48 active:48 free:0 remote:0)
        Disk nvme(volume:48/48 active:48 free:0 remote:0)
          volume id:46  size:588904  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:914  version:3  compact_revision:7  modified_at_second:1627505190  disk_type:"nvme" 
          volume id:67  size:607952  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:865  version:3  compact_revision:8  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:82  size:624632  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:882  version:3  compact_revision:6  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:151  size:1557314160  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:823  version:3  modified_at_second:1610491378  disk_type:"nvme" 
          volume id:173  size:1262093040  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:788  version:3  modified_at_second:1610491476  disk_type:"nvme" 
          volume id:181  size:1208448816  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:717  version:3  modified_at_second:1623182415  disk_type:"nvme" 
          volume id:262  size:1179347032  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:588  version:3  modified_at_second:1623182067  disk_type:"nvme" 
          volume id:266  size:1160008976  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:556  version:3  modified_at_second:1623182811  disk_type:"nvme" 
          volume id:311  size:1115993712  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1610497245  disk_type:"nvme" 
          volume id:363  size:1209730440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1610497862  disk_type:"nvme" 
          volume id:431  size:1126797960  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:555  version:3  modified_at_second:1623181873  disk_type:"nvme" 
          volume id:433  size:1209883088  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:591  version:3  modified_at_second:1623182608  disk_type:"nvme" 
          volume id:434  size:1167964704  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1610498827  disk_type:"nvme" 
          volume id:466  size:1170821024  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:575  version:3  modified_at_second:1623182429  disk_type:"nvme" 
          volume id:477  size:1112893904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:555  version:3  modified_at_second:1610499421  disk_type:"nvme" 
          volume id:494  size:1154467824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1137  version:3  modified_at_second:1610643725  disk_type:"nvme" 
          volume id:499  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:3  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:534  size:1326592720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:671  version:3  modified_at_second:1610644085  disk_type:"nvme" 
          volume id:547  size:1189140320  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:660  version:3  modified_at_second:1610644201  disk_type:"nvme" 
          volume id:549  size:1182767848  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:646  version:3  modified_at_second:1610644201  disk_type:"nvme" 
          volume id:567  size:913832888  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1438  version:3  modified_at_second:1623182236  disk_type:"nvme" 
          volume id:576  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:616  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:733  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1618444177  disk_type:"nvme" 
          volume id:792  size:167772560  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183949  disk_type:"nvme" 
          volume id:803  size:79692208  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:3  modified_at_second:1623183775  disk_type:"nvme" 
          volume id:814  size:150996760  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  version:3  compact_revision:2  modified_at_second:1623184502  disk_type:"nvme" 
          volume id:849  size:201327080  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1617901727  disk_type:"nvme" 
          volume id:851  size:109052368  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:2  deleted_byte_count:8388670  version:3  compact_revision:1  modified_at_second:1627500577  disk_type:"nvme" 
          volume id:904  size:8  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  version:3  compact_revision:1  modified_at_second:1612382093  disk_type:"nvme" 
          volume id:926  size:138412400  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623184109  disk_type:"nvme" 
          volume id:947  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504950  disk_type:"nvme" 
          volume id:980  size:138412472  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183938  disk_type:"nvme" 
          volume id:984  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623184386  disk_type:"nvme" 
          volume id:1004  size:134218040  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623184665  disk_type:"nvme" 
          volume id:1028  size:142606840  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1623184223  disk_type:"nvme" 
          volume id:1046  size:8  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1050  size:20983327888  collection:"braingeneers-backups-swfs"  file_count:12264  delete_count:230  deleted_byte_count:370961817  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1059  size:20995125632  collection:"braingeneers-backups-swfs"  file_count:12209  delete_count:228  deleted_byte_count:354199353  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1063  size:20975272200  collection:"braingeneers-backups-swfs"  file_count:12222  delete_count:239  deleted_byte_count:372617511  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1104  size:20971935200  collection:"braingeneers-backups-swfs"  file_count:12292  delete_count:231  deleted_byte_count:350682314  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246179  size:21257143840  collection:"hengenlab"  file_count:8030  delete_count:1  deleted_byte_count:1048597  version:3  modified_at_second:1627500588  disk_type:"nvme" 
          volume id:246180  size:21162493216  collection:"hengenlab"  file_count:8052  read_only:true  version:3  modified_at_second:1627418743  disk_type:"nvme" 
          volume id:246188  size:21060079488  collection:"hengenlab"  file_count:8045  read_only:true  version:3  modified_at_second:1627418950  disk_type:"nvme" 
          volume id:246214  size:21471574352  collection:"hengenlab"  file_count:8202  read_only:true  version:3  modified_at_second:1627421176  disk_type:"nvme" 
          volume id:246286  size:21733675264  collection:"hengenlab"  file_count:8331  read_only:true  version:3  modified_at_second:1627424954  disk_type:"nvme" 
          volume id:246342  size:21201042600  collection:"hengenlab"  file_count:7543  delete_count:192  deleted_byte_count:478154688  version:3  modified_at_second:1627427872  disk_type:"nvme" 
          volume id:246355  size:3325961784  collection:"hengenlab"  file_count:1300  delete_count:24  deleted_byte_count:100664436  version:3  modified_at_second:1627500603  disk_type:"nvme" 
        Disk nvme total size:236842981136 file_count:113225 deleted_file:1148 deleted_bytes:2040911721 
      DataNode 10.244.14.38:8080 total size:236842981136 file_count:113225 deleted_file:1148 deleted_bytes:2040911721 
      DataNode 10.244.14.40:8080 nvme(volume:48/48 active:48 free:0 remote:0)
        Disk nvme(volume:48/48 active:48 free:0 remote:0)
          volume id:44  size:532120  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:847  version:3  compact_revision:8  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:69  size:595808  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:852  version:3  compact_revision:7  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:118  size:1179638096  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:601  version:3  compact_revision:2  modified_at_second:1610643741  disk_type:"nvme" 
          volume id:149  size:1504362152  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:800  version:3  modified_at_second:1610491378  disk_type:"nvme" 
          volume id:180  size:1214354616  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:738  version:3  modified_at_second:1610494389  disk_type:"nvme" 
          volume id:194  size:1251697440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3397  version:3  modified_at_second:1610494777  disk_type:"nvme" 
          volume id:198  size:1170992464  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:588  version:3  modified_at_second:1610494836  disk_type:"nvme" 
          volume id:199  size:1186136184  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:602  version:3  modified_at_second:1610494836  disk_type:"nvme" 
          volume id:218  size:1257822856  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:604  version:3  modified_at_second:1610496071  disk_type:"nvme" 
          volume id:221  size:1220325024  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:586  version:3  modified_at_second:1610496168  disk_type:"nvme" 
          volume id:248  size:1141899112  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:578  version:3  modified_at_second:1610496421  disk_type:"nvme" 
          volume id:334  size:1167170624  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:578  version:3  modified_at_second:1610497477  disk_type:"nvme" 
          volume id:338  size:1203090488  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:591  version:3  modified_at_second:1610497560  disk_type:"nvme" 
          volume id:343  size:1252389264  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:626  version:3  modified_at_second:1610497637  disk_type:"nvme" 
          volume id:366  size:1131643384  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:541  version:3  modified_at_second:1610497954  disk_type:"nvme" 
          volume id:491  size:1132250216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1124  version:3  modified_at_second:1610643725  disk_type:"nvme" 
          volume id:510  size:1150974464  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:593  version:3  modified_at_second:1610643888  disk_type:"nvme" 
          volume id:512  size:1198198728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:619  version:3  modified_at_second:1610643888  disk_type:"nvme" 
          volume id:513  size:1156946392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:596  version:3  modified_at_second:1610643888  disk_type:"nvme" 
          volume id:548  size:1158916552  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:635  version:3  modified_at_second:1610644201  disk_type:"nvme" 
          volume id:556  size:1159536984  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:669  version:3  modified_at_second:1610644260  disk_type:"nvme" 
          volume id:602  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:629  size:12583192  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:652  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:684  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:716  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:717  size:113246768  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:726  size:138412448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1619387469  disk_type:"nvme" 
          volume id:742  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1617901677  disk_type:"nvme" 
          volume id:743  size:134218064  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1617901660  disk_type:"nvme" 
          volume id:777  size:138412472  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1619387462  disk_type:"nvme" 
          volume id:806  size:142606896  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504767  disk_type:"nvme" 
          volume id:819  size:109052360  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504744  disk_type:"nvme" 
          volume id:839  size:117276912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1613770290  disk_type:"nvme" 
          volume id:841  size:176161392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1619387466  disk_type:"nvme" 
          volume id:929  size:239076048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1618444184  disk_type:"nvme" 
          volume id:1009  size:109052352  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1619387455  disk_type:"nvme" 
          volume id:1019  size:113246768  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1618444182  disk_type:"nvme" 
          volume id:1024  size:159384496  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627500595  disk_type:"nvme" 
          volume id:1053  size:20978356456  collection:"braingeneers-backups-swfs"  file_count:12212  delete_count:225  deleted_byte_count:358144375  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1055  size:20979735904  collection:"braingeneers-backups-swfs"  file_count:12318  delete_count:218  deleted_byte_count:354130118  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1062  size:20986883784  collection:"braingeneers-backups-swfs"  file_count:12136  delete_count:246  deleted_byte_count:397472684  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1123  size:20972215416  collection:"braingeneers-backups-swfs"  file_count:12211  delete_count:227  deleted_byte_count:378646248  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246213  size:21782268984  collection:"hengenlab"  file_count:8350  read_only:true  version:3  modified_at_second:1627421176  disk_type:"nvme" 
          volume id:246287  size:21163284272  collection:"hengenlab"  file_count:8046  read_only:true  version:3  modified_at_second:1627424941  disk_type:"nvme" 
          volume id:246357  size:21156503440  collection:"hengenlab"  file_count:7702  delete_count:43  deleted_byte_count:176380261  version:3  modified_at_second:1627504939  disk_type:"nvme" 
          volume id:246488  size:21362137136  collection:"hengenlab"  file_count:8153  version:3  modified_at_second:1627436797  disk_type:"nvme" 
          volume id:246578  size:4211530048  collection:"hengenlab"  file_count:1619  version:3  modified_at_second:1627439154  disk_type:"nvme" 
        Disk nvme total size:198319668800 file_count:99595 deleted_file:962 deleted_bytes:1677356712 
      DataNode 10.244.14.40:8080 total size:198319668800 file_count:99595 deleted_file:962 deleted_bytes:1677356712 
      DataNode 10.244.14.42:8080 nvme(volume:53/53 active:53 free:0 remote:0)
        Disk nvme(volume:53/53 active:53 free:0 remote:0)
          volume id:34  size:559192  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:868  version:3  compact_revision:7  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:72  size:535304  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:858  version:3  compact_revision:6  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:113  size:1232309792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:629  version:3  compact_revision:2  modified_at_second:1610643741  disk_type:"nvme" 
          volume id:122  size:12583192  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:3  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:139  size:1565810440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2438  delete_count:1883  deleted_byte_count:408258526  version:3  modified_at_second:1610491323  disk_type:"nvme" 
          volume id:148  size:1689304736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:874  version:3  modified_at_second:1610491353  disk_type:"nvme" 
          volume id:152  size:1572132664  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:851  version:3  modified_at_second:1610491378  disk_type:"nvme" 
          volume id:154  size:1423748440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:750  version:3  modified_at_second:1610491378  disk_type:"nvme" 
          volume id:162  size:1466592168  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:826  version:3  modified_at_second:1610491424  disk_type:"nvme" 
          volume id:202  size:1216812728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:616  version:3  modified_at_second:1610494836  disk_type:"nvme" 
          volume id:209  size:1392406504  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:673  version:3  modified_at_second:1610495945  disk_type:"nvme" 
          volume id:216  size:1187489448  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:571  version:3  modified_at_second:1610496064  disk_type:"nvme" 
          volume id:258  size:1285514928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:636  version:3  modified_at_second:1610496544  disk_type:"nvme" 
          volume id:259  size:1418767640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:708  version:3  modified_at_second:1610496544  disk_type:"nvme" 
          volume id:291  size:1205796160  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610496936  disk_type:"nvme" 
          volume id:317  size:1197451080  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:602  version:3  modified_at_second:1610497331  disk_type:"nvme" 
          volume id:339  size:1198784320  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:588  version:3  modified_at_second:1610497560  disk_type:"nvme" 
          volume id:386  size:1255099232  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:631  version:3  modified_at_second:1610498218  disk_type:"nvme" 
          volume id:447  size:1200800168  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:599  version:3  modified_at_second:1610498999  disk_type:"nvme" 
          volume id:452  size:1312230328  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:629  version:3  modified_at_second:1610499083  disk_type:"nvme" 
          volume id:460  size:1303078744  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:645  version:3  modified_at_second:1610499157  disk_type:"nvme" 
          volume id:474  size:1887242272  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:925  version:3  modified_at_second:1610499431  disk_type:"nvme" 
          volume id:501  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:3  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:530  size:1253806728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:669  version:3  modified_at_second:1610644025  disk_type:"nvme" 
          volume id:581  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:592  size:12583192  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:593  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:594  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:597  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:600  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:606  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:620  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:663  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:668  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:719  size:159484792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:21072287  version:3  compact_revision:1  modified_at_second:1619387463  disk_type:"nvme" 
          volume id:763  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1618444180  disk_type:"nvme" 
          volume id:867  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627500602  disk_type:"nvme" 
          volume id:891  size:244264328  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  version:3  compact_revision:2  modified_at_second:1619387474  disk_type:"nvme" 
          volume id:962  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1618444185  disk_type:"nvme" 
          volume id:975  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1619387462  disk_type:"nvme" 
          volume id:1035  size:2256  collection:"swfstest"  file_count:1  version:3  modified_at_second:1613592642  disk_type:"nvme" 
          volume id:1039  size:314720  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  file_count:1  version:3  modified_at_second:1615234636  disk_type:"nvme" 
          volume id:1098  size:20973674480  collection:"braingeneers-backups-swfs"  file_count:12287  delete_count:227  deleted_byte_count:363217189  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1117  size:20976367656  collection:"braingeneers-backups-swfs"  file_count:12224  delete_count:229  deleted_byte_count:381874187  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1137  size:20972013512  collection:"braingeneers-backups-swfs"  file_count:12286  delete_count:223  deleted_byte_count:353515699  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1144  size:20980307664  collection:"braingeneers-backups-swfs"  file_count:12301  delete_count:235  deleted_byte_count:370673355  version:3  compact_revision:2  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1645  size:8913611152  collection:"hengenlab"  file_count:3433  version:3  compact_revision:4  modified_at_second:1627507573  disk_type:"nvme" 
          volume id:246364  size:21112377664  collection:"hengenlab"  file_count:7936  delete_count:40  deleted_byte_count:167773973  version:3  modified_at_second:1627428720  disk_type:"nvme" 
          volume id:246370  size:21082313784  collection:"hengenlab"  file_count:8692  delete_count:3  deleted_byte_count:12583023  version:3  modified_at_second:1627429075  disk_type:"nvme" 
          volume id:246422  size:21207507408  collection:"hengenlab"  file_count:8115  version:3  modified_at_second:1627432018  disk_type:"nvme" 
          volume id:246457  size:21273517664  collection:"hengenlab"  file_count:8120  version:3  modified_at_second:1627434466  disk_type:"nvme" 
          volume id:246458  size:21393938840  collection:"hengenlab"  file_count:8159  version:3  modified_at_second:1627434473  disk_type:"nvme" 
          volume id:246587  size:6317115064  collection:"hengenlab"  file_count:2412  version:3  modified_at_second:1627439213  disk_type:"nvme" 
        Disk nvme total size:233296711568 file_count:113200 deleted_file:2842 deleted_bytes:2083162574 
      DataNode 10.244.14.42:8080 total size:233296711568 file_count:113200 deleted_file:2842 deleted_bytes:2083162574 
      DataNode 10.244.14.54:8080 nvme(volume:49/49 active:49 free:0 remote:0)
        Disk nvme(volume:49/49 active:49 free:0 remote:0)
          volume id:65  size:592296  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:874  version:3  compact_revision:8  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:68  size:550704  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:855  version:3  compact_revision:8  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:115  size:1296619736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:698  version:3  compact_revision:2  modified_at_second:1610643650  disk_type:"nvme" 
          volume id:120  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:4  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:123  size:144  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:3  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:133  size:8  collection:".DS_Store"  version:3  compact_revision:1  modified_at_second:1610394116  disk_type:"nvme" 
          volume id:135  size:8  collection:".DS_Store"  version:3  modified_at_second:1610069605  disk_type:"nvme" 
          volume id:175  size:1122297720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:700  version:3  compact_revision:1  modified_at_second:1627507803  disk_type:"nvme" 
          volume id:191  size:1245408440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3312  version:3  modified_at_second:1610494777  disk_type:"nvme" 
          volume id:206  size:1254471552  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:602  version:3  modified_at_second:1610494893  disk_type:"nvme" 
          volume id:236  size:1214762280  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1610496298  disk_type:"nvme" 
          volume id:238  size:1283041256  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:615  version:3  modified_at_second:1610496298  disk_type:"nvme" 
          volume id:245  size:1265549600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:645  version:3  modified_at_second:1610496421  disk_type:"nvme" 
          volume id:254  size:1250499056  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:607  version:3  modified_at_second:1610496480  disk_type:"nvme" 
          volume id:297  size:1283466864  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:622  version:3  modified_at_second:1610497013  disk_type:"nvme" 
          volume id:337  size:1167874736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:574  version:3  modified_at_second:1610497560  disk_type:"nvme" 
          volume id:348  size:1184975696  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:568  version:3  modified_at_second:1610497700  disk_type:"nvme" 
          volume id:406  size:1185077976  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:566  version:3  modified_at_second:1610498457  disk_type:"nvme" 
          volume id:413  size:1445852896  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:702  version:3  modified_at_second:1610498598  disk_type:"nvme" 
          volume id:448  size:1198099808  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:598  version:3  modified_at_second:1610498999  disk_type:"nvme" 
          volume id:471  size:1251064952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:674  version:3  modified_at_second:1610499335  disk_type:"nvme" 
          volume id:472  size:1250977184  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:660  version:3  modified_at_second:1610499335  disk_type:"nvme" 
          volume id:514  size:1262797752  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:661  version:3  modified_at_second:1610643888  disk_type:"nvme" 
          volume id:544  size:1193792688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:585  version:3  modified_at_second:1610644140  disk_type:"nvme" 
          volume id:545  size:1240097296  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:681  version:3  modified_at_second:1610644201  disk_type:"nvme" 
          volume id:635  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:643  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:644  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:658  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:685  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:690  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:701  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:710  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:721  size:176181264  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1618444188  disk_type:"nvme" 
          volume id:744  size:180355792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1619387456  disk_type:"nvme" 
          volume id:805  size:234881624  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1617901683  disk_type:"nvme" 
          volume id:820  size:205521528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627500582  disk_type:"nvme" 
          volume id:821  size:215866544  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1619387465  disk_type:"nvme" 
          volume id:856  size:188744560  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:2  modified_at_second:1627500585  disk_type:"nvme" 
          volume id:1089  size:20983270464  collection:"braingeneers-backups-swfs"  file_count:12230  delete_count:236  deleted_byte_count:367424623  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1114  size:20974105816  collection:"braingeneers-backups-swfs"  file_count:12247  delete_count:242  deleted_byte_count:379897789  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1121  size:20974478768  collection:"braingeneers-backups-swfs"  file_count:12198  delete_count:223  deleted_byte_count:357455804  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1142  size:20981836200  collection:"braingeneers-backups-swfs"  file_count:12177  delete_count:236  deleted_byte_count:361487567  version:3  compact_revision:2  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1643  size:8931501032  collection:"hengenlab"  file_count:3424  version:3  compact_revision:4  modified_at_second:1627507114  disk_type:"nvme" 
          volume id:246257  size:21208314368  collection:"hengenlab"  file_count:8148  read_only:true  version:3  modified_at_second:1627423488  disk_type:"nvme" 
          volume id:246361  size:21246477600  collection:"hengenlab"  file_count:7577  delete_count:74  deleted_byte_count:310381990  version:3  modified_at_second:1627428479  disk_type:"nvme" 
          volume id:246437  size:19655019848  collection:"hengenlab"  file_count:7518  version:3  modified_at_second:1627432971  disk_type:"nvme" 
          volume id:246443  size:217060280  collection:"hengenlab"  file_count:90  version:3  modified_at_second:1627432971  disk_type:"nvme" 
          volume id:246444  size:210768488  collection:"hengenlab"  file_count:84  version:3  modified_at_second:1627432971  disk_type:"nvme" 
        Disk nvme total size:180228393200 file_count:92134 deleted_file:1014 deleted_bytes:1789230792 
      DataNode 10.244.14.54:8080 total size:180228393200 file_count:92134 deleted_file:1014 deleted_bytes:1789230792 
      DataNode 10.244.14.7:8080 nvme(volume:52/52 active:52 free:0 remote:0)
        Disk nvme(volume:52/52 active:52 free:0 remote:0)
          volume id:41  size:583672  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:866  version:3  compact_revision:7  modified_at_second:1627500691  disk_type:"nvme" 
          volume id:63  size:559088  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:847  version:3  compact_revision:8  modified_at_second:1627500691  disk_type:"nvme" 
          volume id:109  size:376  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1  version:3  compact_revision:2  modified_at_second:1627500691  disk_type:"nvme" 
          volume id:190  size:1109040768  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3551  version:3  modified_at_second:1623180963  disk_type:"nvme" 
          volume id:192  size:1182930304  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3450  version:3  modified_at_second:1623182172  disk_type:"nvme" 
          volume id:228  size:1155850784  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1623182659  disk_type:"nvme" 
          volume id:252  size:1136211808  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  version:3  modified_at_second:1623181714  disk_type:"nvme" 
          volume id:253  size:1152299312  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1623182006  disk_type:"nvme" 
          volume id:268  size:1110550136  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:534  version:3  modified_at_second:1623181510  disk_type:"nvme" 
          volume id:305  size:1130166824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:561  version:3  modified_at_second:1623182762  disk_type:"nvme" 
          volume id:380  size:1117994288  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:567  version:3  modified_at_second:1623182493  disk_type:"nvme" 
          volume id:416  size:1143219176  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:556  version:3  modified_at_second:1623181372  disk_type:"nvme" 
          volume id:425  size:1129231688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:546  version:3  modified_at_second:1623181170  disk_type:"nvme" 
          volume id:427  size:1102357232  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:529  version:3  modified_at_second:1623180903  disk_type:"nvme" 
          volume id:429  size:1161577792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1623181575  disk_type:"nvme" 
          volume id:435  size:1110503240  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:544  version:3  modified_at_second:1623181106  disk_type:"nvme" 
          volume id:439  size:1150908624  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:554  version:3  modified_at_second:1623182249  disk_type:"nvme" 
          volume id:467  size:1172172720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:625  version:3  modified_at_second:1623181927  disk_type:"nvme" 
          volume id:481  size:1139901224  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:565  version:3  modified_at_second:1623181308  disk_type:"nvme" 
          volume id:720  size:176161336  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1623184640  disk_type:"nvme" 
          volume id:732  size:138412448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623183855  disk_type:"nvme" 
          volume id:746  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504769  disk_type:"nvme" 
          volume id:758  size:88081016  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1623184084  disk_type:"nvme" 
          volume id:785  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623184363  disk_type:"nvme" 
          volume id:801  size:75497816  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623183090  disk_type:"nvme" 
          volume id:802  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183634  disk_type:"nvme" 
          volume id:822  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623183646  disk_type:"nvme" 
          volume id:823  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623183762  disk_type:"nvme" 
          volume id:829  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623184246  disk_type:"nvme" 
          volume id:833  size:335545152  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  delete_count:2  deleted_byte_count:67108968  version:3  compact_revision:1  modified_at_second:1623184525  disk_type:"nvme" 
          volume id:857  size:79692200  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183100  disk_type:"nvme" 
          volume id:905  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183209  disk_type:"nvme" 
          volume id:933  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627500690  disk_type:"nvme" 
          volume id:934  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1617901485  disk_type:"nvme" 
          volume id:949  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623182950  disk_type:"nvme" 
          volume id:985  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623182962  disk_type:"nvme" 
          volume id:1011  size:104857984  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:3  modified_at_second:1623183368  disk_type:"nvme" 
          volume id:1018  size:109052336  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183356  disk_type:"nvme" 
          volume id:1030  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1623183482  disk_type:"nvme" 
          volume id:1056  size:20979047696  collection:"braingeneers-backups-swfs"  file_count:12172  delete_count:222  deleted_byte_count:354862328  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1065  size:20982781936  collection:"braingeneers-backups-swfs"  file_count:12329  delete_count:263  deleted_byte_count:405184167  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1082  size:20973680512  collection:"braingeneers-backups-swfs"  file_count:12203  delete_count:240  deleted_byte_count:375809328  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1083  size:20973117760  collection:"braingeneers-backups-swfs"  file_count:12201  delete_count:253  deleted_byte_count:408689779  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246189  size:21300912680  collection:"hengenlab"  file_count:8155  read_only:true  version:3  modified_at_second:1627418950  disk_type:"nvme" 
          volume id:246216  size:21826498352  collection:"hengenlab"  file_count:8319  read_only:true  version:3  modified_at_second:1627421278  disk_type:"nvme" 
          volume id:246300  size:21224168192  collection:"hengenlab"  file_count:8084  read_only:true  version:3  modified_at_second:1627425325  disk_type:"nvme" 
          volume id:246330  size:21362892376  collection:"hengenlab"  file_count:7442  delete_count:52  deleted_byte_count:218106224  version:3  modified_at_second:1627427138  disk_type:"nvme" 
          volume id:246358  size:21661297032  collection:"hengenlab"  file_count:7879  delete_count:48  deleted_byte_count:192554346  version:3  modified_at_second:1627500593  disk_type:"nvme" 
          volume id:246380  size:21133425560  collection:"hengenlab"  file_count:8075  delete_count:6  deleted_byte_count:25166106  version:3  modified_at_second:1627429671  disk_type:"nvme" 
          volume id:246384  size:21720995776  collection:"hengenlab"  file_count:8262  version:3  modified_at_second:1627429845  disk_type:"nvme" 
          volume id:246434  size:20981185168  collection:"hengenlab"  file_count:7956  version:3  modified_at_second:1627432809  disk_type:"nvme" 
          volume id:246451  size:257955584  collection:"hengenlab"  file_count:105  version:3  modified_at_second:1627433574  disk_type:"nvme" 
        Disk nvme total size:275421129336 file_count:129784 deleted_file:1087 deleted_bytes:2051675588 
      DataNode 10.244.14.7:8080 total size:275421129336 file_count:129784 deleted_file:1087 deleted_bytes:2051675588 
      DataNode 10.244.16.105:8080 hdd(volume:11/91 active:18446744073709551607 free:80 remote:0)
        Disk hdd(volume:11/91 active:18446744073709551607 free:80 remote:0)
          volume id:411672  size:20977426496  collection:"hengenlab"  file_count:8034  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627703997 
          volume id:411766  size:20991227544  collection:"hengenlab"  file_count:8102  delete_count:2113  deleted_byte_count:5206960233  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411768  size:16217247664  collection:"hengenlab"  file_count:6290  delete_count:663  deleted_byte_count:1560694285  version:3  compact_revision:2  modified_at_second:1627843742 
          volume id:411771  size:17830653080  collection:"hengenlab"  file_count:6980  delete_count:557  deleted_byte_count:1299266257  version:3  compact_revision:2  modified_at_second:1627843714 
          volume id:411786  size:58721720  collection:"hengenlab"  file_count:26  version:3  compact_revision:1  modified_at_second:1627526065 
          volume id:411879  size:21019024920  collection:"hengenlab"  file_count:8021  delete_count:1428  deleted_byte_count:3713951935  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411880  size:11365571592  collection:"hengenlab"  file_count:4493  delete_count:38  deleted_byte_count:87032606  version:3  compact_revision:4  modified_at_second:1627843726 
          volume id:411898  size:21028460248  collection:"hengenlab"  file_count:8071  delete_count:1536  deleted_byte_count:3924994777  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411944  size:19968845784  collection:"hengenlab"  file_count:7711  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:1  modified_at_second:1627843719 
          volume id:411978  size:9744231728  collection:"hengenlab"  file_count:3819  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627843736 
          volume id:412029  size:9916864984  collection:"hengenlab"  file_count:3805  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:1  modified_at_second:1627843726 
        Disk hdd total size:169118275760 file_count:65352 deleted_file:6347 deleted_bytes:15827503353 
      DataNode 10.244.16.105:8080 total size:169118275760 file_count:65352 deleted_file:6347 deleted_bytes:15827503353 
      DataNode 10.244.16.109:8080 hdd(volume:28/91 active:18446744073709551606 free:63 remote:0)
        Disk hdd(volume:28/91 active:18446744073709551606 free:63 remote:0)
          volume id:246132  size:21058344480  collection:"hengenlab"  file_count:8073  version:3  compact_revision:1  modified_at_second:1627670913 
          volume id:246143  size:21022971736  collection:"hengenlab"  file_count:8725  delete_count:531  deleted_byte_count:1341449919  version:3  modified_at_second:1627670233 
          volume id:246155  size:21165681304  collection:"hengenlab"  file_count:8155  version:3  modified_at_second:1627670725 
          volume id:246162  size:21326008384  collection:"hengenlab"  file_count:8116  version:3  modified_at_second:1627672622 
          volume id:246207  size:21287198904  collection:"hengenlab"  file_count:8110  version:3  modified_at_second:1627671903 
          volume id:246232  size:21071010368  collection:"hengenlab"  file_count:8060  version:3  modified_at_second:1627675625 
          volume id:246316  size:22111171216  collection:"hengenlab"  file_count:8799  version:3  modified_at_second:1627672922 
          volume id:246325  size:21683026640  collection:"hengenlab"  file_count:7991  version:3  modified_at_second:1627669902 
          volume id:246349  size:21491426128  collection:"hengenlab"  file_count:7636  delete_count:32  deleted_byte_count:134219209  version:3  modified_at_second:1627671706 
          volume id:246379  size:21234733048  collection:"hengenlab"  file_count:8120  delete_count:5  deleted_byte_count:20971735  version:3  modified_at_second:1627674784 
          volume id:246386  size:21134592392  collection:"hengenlab"  file_count:8086  version:3  modified_at_second:1627672234 
          volume id:246396  size:21297928576  collection:"hengenlab"  file_count:8118  version:3  modified_at_second:1627674285 
          volume id:246409  size:21669134560  collection:"hengenlab"  file_count:8298  version:3  modified_at_second:1627670400 
          volume id:246441  size:21123695696  collection:"hengenlab"  file_count:8042  version:3  modified_at_second:1627671141 
          volume id:246480  size:21194588824  collection:"hengenlab"  file_count:8054  version:3  modified_at_second:1627671344 
          volume id:246511  size:21111213688  collection:"hengenlab"  file_count:8047  version:3  modified_at_second:1627670557 
          volume id:246572  size:21475386200  collection:"hengenlab"  file_count:8204  version:3  modified_at_second:1627673715 
          volume id:246655  size:21200635416  collection:"hengenlab"  file_count:8029  version:3  modified_at_second:1627671522 
          volume id:246788  size:20972680848  collection:"hengenlab"  file_count:7999  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627700090 
          volume id:247162  size:11805700264  collection:"hengenlab"  file_count:4496  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:1  modified_at_second:1627843719 
          volume id:411734  size:13092644032  collection:"hengenlab"  file_count:5032  delete_count:139  deleted_byte_count:279103045  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411746  size:20974925368  collection:"hengenlab"  file_count:8084  delete_count:57  deleted_byte_count:100810404  version:3  compact_revision:2  modified_at_second:1627701127 
          volume id:411886  size:19405906328  collection:"hengenlab"  file_count:7519  delete_count:1146  deleted_byte_count:2736462084  version:3  compact_revision:3  modified_at_second:1627843702 
          volume id:411906  size:21000741928  collection:"hengenlab"  file_count:7933  delete_count:1408  deleted_byte_count:3731223029  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411910  size:9184924704  collection:"hengenlab"  file_count:3548  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843718 
          volume id:411915  size:9321638088  collection:"hengenlab"  file_count:3655  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:2  modified_at_second:1627843702 
          volume id:411921  size:14239631728  collection:"hengenlab"  file_count:5509  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:3  modified_at_second:1627843735 
          volume id:411997  size:10613401472  collection:"hengenlab"  file_count:4082  delete_count:8  deleted_byte_count:17825960  version:3  compact_revision:2  modified_at_second:1627843724 
        Disk hdd total size:534270942320 file_count:204520 deleted_file:3335 deleted_bytes:8387231398 
      DataNode 10.244.16.109:8080 total size:534270942320 file_count:204520 deleted_file:3335 deleted_bytes:8387231398 
      DataNode 10.244.16.111:8080 hdd(volume:10/91 active:18446744073709551608 free:81 remote:0)
        Disk hdd(volume:10/91 active:18446744073709551608 free:81 remote:0)
          volume id:411731  size:11041845712  collection:"hengenlab"  file_count:4328  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:4  modified_at_second:1627843736 
          volume id:411749  size:15839442992  collection:"hengenlab"  file_count:6152  delete_count:691  deleted_byte_count:1661617930  version:3  compact_revision:2  modified_at_second:1627843732 
          volume id:411800  size:21002193408  collection:"hengenlab"  file_count:8047  delete_count:1553  deleted_byte_count:3945690961  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411801  size:5243000  collection:"hengenlab"  file_count:2  version:3  compact_revision:1  modified_at_second:1627524409 
          volume id:411825  size:20983314512  collection:"hengenlab"  file_count:8152  delete_count:1565  deleted_byte_count:3892032612  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411830  size:20979666360  collection:"hengenlab"  file_count:8090  delete_count:1619  deleted_byte_count:4121810760  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411903  size:21028663960  collection:"hengenlab"  file_count:8085  delete_count:2311  deleted_byte_count:5888622291  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411935  size:7822161536  collection:"hengenlab"  file_count:3129  version:3  compact_revision:1  modified_at_second:1627618125 
          volume id:411989  size:10108862568  collection:"hengenlab"  file_count:3947  delete_count:88  deleted_byte_count:210180878  version:3  compact_revision:2  modified_at_second:1627843720 
          volume id:411996  size:11119897928  collection:"hengenlab"  file_count:4242  delete_count:10  deleted_byte_count:29360338  version:3  compact_revision:2  modified_at_second:1627843744 
        Disk hdd total size:139931291976 file_count:54174 deleted_file:7839 deleted_bytes:19757704420 
      DataNode 10.244.16.111:8080 total size:139931291976 file_count:54174 deleted_file:7839 deleted_bytes:19757704420 
      DataNode 10.244.16.113:8080 hdd(volume:16/87 active:18446744073709551599 free:71 remote:0)
        Disk hdd(volume:16/87 active:18446744073709551599 free:71 remote:0)
          volume id:411666  size:9016547472  collection:"hengenlab"  file_count:3452  delete_count:2  deleted_byte_count:2097194  version:3  compact_revision:1  modified_at_second:1627843715 
          volume id:411668  size:9202893840  collection:"hengenlab"  file_count:3543  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627843684 
          volume id:411683  size:11181630688  collection:"hengenlab"  file_count:4245  version:3  compact_revision:2  modified_at_second:1627608542 
          volume id:411718  size:11338532520  collection:"hengenlab"  file_count:4397  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:4  modified_at_second:1627843730 
          volume id:411719  size:20980243888  collection:"hengenlab"  file_count:8154  delete_count:150  deleted_byte_count:339264245  version:3  compact_revision:2  modified_at_second:1627704643 
          volume id:411742  size:11220642024  collection:"hengenlab"  file_count:4406  delete_count:5  deleted_byte_count:17825897  version:3  compact_revision:4  modified_at_second:1627843723 
          volume id:411774  size:413147104  collection:"hengenlab"  file_count:145  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411776  size:20986017496  collection:"hengenlab"  file_count:8124  delete_count:51  deleted_byte_count:85309527  version:3  compact_revision:2  modified_at_second:1627701780 
          volume id:411835  size:21006400216  collection:"hengenlab"  file_count:8087  delete_count:1548  deleted_byte_count:3986368184  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411846  size:11200737944  collection:"hengenlab"  file_count:4348  version:3  compact_revision:4  modified_at_second:1627843734 
          volume id:411893  size:12886872008  collection:"hengenlab"  file_count:4990  delete_count:273  deleted_byte_count:608551124  version:3  compact_revision:2  modified_at_second:1627609261 
          volume id:411908  size:10337525600  collection:"hengenlab"  file_count:4062  delete_count:1165  deleted_byte_count:2920129545  version:3  compact_revision:2  modified_at_second:1627607477 
          volume id:411969  size:10956589432  collection:"hengenlab"  file_count:4158  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627843737 
          volume id:411973  size:12219002136  collection:"hengenlab"  file_count:4488  delete_count:5  deleted_byte_count:8388713  version:3  compact_revision:2  modified_at_second:1627843722 
          volume id:411998  size:10714716832  collection:"hengenlab"  file_count:4196  delete_count:7  deleted_byte_count:16777363  version:3  compact_revision:2  modified_at_second:1627843742 
          volume id:412024  size:9351795144  collection:"hengenlab"  file_count:3622  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843689 
        Disk hdd total size:193013294344 file_count:74417 deleted_file:3215 deleted_bytes:8003586349 
      DataNode 10.244.16.113:8080 total size:193013294344 file_count:74417 deleted_file:3215 deleted_bytes:8003586349 
      DataNode 10.244.16.115:8080 hdd(volume:14/88 active:18446744073709551606 free:74 remote:0)
        Disk hdd(volume:14/88 active:18446744073709551606 free:74 remote:0)
          volume id:411680  size:15080342928  collection:"hengenlab"  file_count:5850  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:3  modified_at_second:1627843721 
          volume id:411699  size:20999744272  collection:"hengenlab"  file_count:8116  delete_count:1445  deleted_byte_count:3651895668  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411702  size:13631720  collection:"hengenlab"  file_count:4  version:3  compact_revision:1  modified_at_second:1627527081 
          volume id:411721  size:12804978144  collection:"hengenlab"  file_count:4933  delete_count:225  deleted_byte_count:495367244  version:3  compact_revision:4  modified_at_second:1627843718 
          volume id:411754  size:20997906008  collection:"hengenlab"  file_count:8186  delete_count:2465  deleted_byte_count:5961803335  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411807  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627524245 
          volume id:411828  size:21010056688  collection:"hengenlab"  file_count:8163  delete_count:1717  deleted_byte_count:4249377968  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411857  size:13134025448  collection:"hengenlab"  file_count:5091  delete_count:117  deleted_byte_count:189209269  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411864  size:423554448  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9339  delete_count:250  deleted_byte_count:39111669  version:3  compact_revision:1  modified_at_second:1627763831 
          volume id:411890  size:10544130960  collection:"hengenlab"  file_count:4119  delete_count:12  deleted_byte_count:31457532  version:3  compact_revision:4  modified_at_second:1627843710 
          volume id:411896  size:9116052272  collection:"hengenlab"  file_count:3576  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:2  modified_at_second:1627843728 
          volume id:411930  size:17108060688  collection:"hengenlab"  file_count:6799  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:1  modified_at_second:1627843724 
          volume id:412019  size:11051418808  collection:"hengenlab"  file_count:4147  delete_count:142  deleted_byte_count:391121830  version:3  compact_revision:2  modified_at_second:1627843719 
          volume id:412033  size:9757944808  collection:"hengenlab"  file_count:3709  delete_count:4  deleted_byte_count:16777300  version:3  compact_revision:1  modified_at_second:1627843742 
        Disk hdd total size:162041847200 file_count:72032 deleted_file:6382 deleted_bytes:15040801984 
      DataNode 10.244.16.115:8080 total size:162041847200 file_count:72032 deleted_file:6382 deleted_bytes:15040801984 
      DataNode 10.244.16.119:8080 hdd(volume:11/91 active:18446744073709551606 free:80 remote:0)
        Disk hdd(volume:11/91 active:18446744073709551606 free:80 remote:0)
          volume id:411700  size:1048640  collection:"hengenlab"  file_count:1  version:3  compact_revision:1  modified_at_second:1627526056 
          volume id:411709  size:20972820528  collection:"hengenlab"  file_count:8109  delete_count:115  deleted_byte_count:250133345  version:3  compact_revision:2  modified_at_second:1627703138 
          volume id:411795  size:20979805704  collection:"hengenlab"  file_count:8145  delete_count:145  deleted_byte_count:307755521  version:3  compact_revision:2  modified_at_second:1627703294 
          volume id:411827  size:21034784864  collection:"hengenlab"  file_count:8260  delete_count:2542  deleted_byte_count:6147081247  version:3  compact_revision:1  modified_at_second:1627571069 
          volume id:411841  size:16717792160  collection:"hengenlab"  file_count:6492  delete_count:721  deleted_byte_count:1744708457  version:3  compact_revision:2  modified_at_second:1627843729 
          volume id:411918  size:10276162736  collection:"hengenlab"  file_count:3946  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:3  modified_at_second:1627843737 
          volume id:411929  size:7918028088  collection:"hengenlab"  file_count:3176  version:3  compact_revision:1  modified_at_second:1627611409 
          volume id:411943  size:19730459888  collection:"hengenlab"  file_count:7574  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:1  modified_at_second:1627843740 
          volume id:411949  size:20296779920  collection:"hengenlab"  file_count:7730  delete_count:5  deleted_byte_count:5242985  version:3  compact_revision:1  modified_at_second:1627843733 
          volume id:411959  size:20972517216  collection:"hengenlab"  file_count:8066  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:1  modified_at_second:1627703107 
          volume id:412017  size:10396379712  collection:"hengenlab"  file_count:4002  delete_count:163  deleted_byte_count:425725279  version:3  compact_revision:2  modified_at_second:1627843742 
        Disk hdd total size:169296579456 file_count:65501 deleted_file:3699 deleted_bytes:8904764250 
      DataNode 10.244.16.119:8080 total size:169296579456 file_count:65501 deleted_file:3699 deleted_bytes:8904764250 
      DataNode 10.244.16.120:8080 hdd(volume:12/88 active:18446744073709551606 free:76 remote:0)
        Disk hdd(volume:12/88 active:18446744073709551606 free:76 remote:0)
          volume id:411691  size:17329839840  collection:"hengenlab"  file_count:6725  delete_count:568  deleted_byte_count:1368138726  version:3  compact_revision:2  modified_at_second:1627843733 
          volume id:411725  size:12490053632  collection:"hengenlab"  file_count:4880  version:3  compact_revision:4  modified_at_second:1627843686 
          volume id:411735  size:13217507528  collection:"hengenlab"  file_count:5150  delete_count:186  deleted_byte_count:337558326  version:3  compact_revision:3  modified_at_second:1627607477 
          volume id:411772  size:15612258448  collection:"hengenlab"  file_count:5910  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:3  modified_at_second:1627607477 
          volume id:411842  size:16614826248  collection:"hengenlab"  file_count:6483  delete_count:597  deleted_byte_count:1458708031  version:3  compact_revision:2  modified_at_second:1627843701 
          volume id:411870  size:13632056  collection:"hengenlab"  file_count:10  version:3  compact_revision:1  modified_at_second:1627523623 
          volume id:411974  size:13700151256  collection:"hengenlab"  file_count:5015  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:2  modified_at_second:1627843680 
          volume id:411975  size:9862483768  collection:"hengenlab"  file_count:3854  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:3  modified_at_second:1627843733 
          volume id:411983  size:10304232208  collection:"hengenlab"  file_count:3967  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843743 
          volume id:411987  size:10362849136  collection:"hengenlab"  file_count:4021  delete_count:14  deleted_byte_count:33554726  version:3  compact_revision:2  modified_at_second:1627843696 
          volume id:412025  size:9611253592  collection:"hengenlab"  file_count:3683  delete_count:3  deleted_byte_count:12582975  version:3  compact_revision:2  modified_at_second:1627843741 
          volume id:412028  size:9540573984  collection:"hengenlab"  file_count:3715  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627843737 
        Disk hdd total size:138659661696 file_count:53413 deleted_file:1380 deleted_bytes:3242000316 
      DataNode 10.244.16.120:8080 total size:138659661696 file_count:53413 deleted_file:1380 deleted_bytes:3242000316 
      DataNode 10.244.16.65:8080 hdd(volume:9/90 active:18446744073709551610 free:81 remote:0)
        Disk hdd(volume:9/90 active:18446744073709551610 free:81 remote:0)
          volume id:411667  size:9502047744  collection:"hengenlab"  file_count:3591  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:1  modified_at_second:1627843728 
          volume id:411697  size:5243168  collection:"hengenlab"  file_count:5  version:3  compact_revision:1  modified_at_second:1627527081 
          volume id:411714  size:17491395904  collection:"hengenlab"  file_count:6885  delete_count:651  deleted_byte_count:1495790614  version:3  compact_revision:2  modified_at_second:1627843723 
          volume id:411733  size:10485992  collection:"hengenlab"  file_count:4  version:3  compact_revision:1  modified_at_second:1627526516 
          volume id:411813  size:20755617216  collection:"hengenlab"  file_count:8108  delete_count:240  deleted_byte_count:541743222  version:3  compact_revision:2  modified_at_second:1627843732 
          volume id:411818  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627522290 
          volume id:411866  size:20988360592  collection:"hengenlab"  file_count:8016  delete_count:1508  deleted_byte_count:3928332868  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411946  size:19825586120  collection:"hengenlab"  file_count:7513  version:3  compact_revision:1  modified_at_second:1627843732 
          volume id:412000  size:10448263664  collection:"hengenlab"  file_count:4006  delete_count:36  deleted_byte_count:88081140  version:3  compact_revision:2  modified_at_second:1627843733 
        Disk hdd total size:99027000408 file_count:38128 deleted_file:2438 deleted_bytes:6060239363 
      DataNode 10.244.16.65:8080 total size:99027000408 file_count:38128 deleted_file:2438 deleted_bytes:6060239363 
      DataNode 10.244.16.69:8080 hdd(volume:15/88 active:18446744073709551606 free:73 remote:0)
        Disk hdd(volume:15/88 active:18446744073709551606 free:73 remote:0)
          volume id:411713  size:13818469408  collection:"hengenlab"  file_count:5424  delete_count:112  deleted_byte_count:208359274  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411793  size:21005328904  collection:"hengenlab"  file_count:8295  delete_count:1819  deleted_byte_count:4450705254  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411837  size:20988574632  collection:"hengenlab"  file_count:8160  delete_count:1567  deleted_byte_count:3925691116  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411838  size:15008508456  collection:"hengenlab"  file_count:5849  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:4  modified_at_second:1627843719 
          volume id:411850  size:1048640  collection:"hengenlab"  file_count:1  version:3  compact_revision:1  modified_at_second:1627523809 
          volume id:411860  size:420112720  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9442  delete_count:271  deleted_byte_count:29546350  version:3  compact_revision:1  modified_at_second:1627762123 
          volume id:411865  size:442392768  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9478  delete_count:244  deleted_byte_count:32475539  version:3  compact_revision:1  modified_at_second:1627764191 
          volume id:411868  size:20972398400  collection:"hengenlab"  file_count:8101  delete_count:985  deleted_byte_count:2180106940  version:3  compact_revision:3  modified_at_second:1627696649 
          volume id:411871  size:12329805552  collection:"hengenlab"  file_count:4808  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:4  modified_at_second:1627843725 
          volume id:411874  size:16113797624  collection:"hengenlab"  file_count:6334  delete_count:729  deleted_byte_count:1622141539  version:3  compact_revision:3  modified_at_second:1627843734 
          volume id:411914  size:20984608288  collection:"hengenlab"  file_count:8157  delete_count:2343  deleted_byte_count:6026008543  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411942  size:19899798504  collection:"hengenlab"  file_count:7735  delete_count:2  deleted_byte_count:2097194  version:3  compact_revision:1  modified_at_second:1627843722 
          volume id:411971  size:11924701448  collection:"hengenlab"  file_count:4300  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:2  modified_at_second:1627843729 
          volume id:412005  size:10520767216  collection:"hengenlab"  file_count:4093  delete_count:49  deleted_byte_count:133170181  version:3  compact_revision:2  modified_at_second:1627843727 
          volume id:412027  size:9725406568  collection:"hengenlab"  file_count:3734  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:1  modified_at_second:1627843739 
        Disk hdd total size:194155719128 file_count:93911 deleted_file:8130 deleted_bytes:18635467943 
      DataNode 10.244.16.69:8080 total size:194155719128 file_count:93911 deleted_file:8130 deleted_bytes:18635467943 
      DataNode 10.244.16.89:8080 hdd(volume:11/91 active:18446744073709551608 free:80 remote:0)
        Disk hdd(volume:11/91 active:18446744073709551608 free:80 remote:0)
          volume id:411689  size:17721480736  collection:"hengenlab"  file_count:6838  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:3  modified_at_second:1627843728 
          volume id:411736  size:6291632  collection:"hengenlab"  file_count:3  version:3  compact_revision:1  modified_at_second:1627526065 
          volume id:411744  size:20992042128  collection:"hengenlab"  file_count:8139  delete_count:1649  deleted_byte_count:3997475113  version:3  compact_revision:1  modified_at_second:1627571069 
          volume id:411826  size:21055317568  collection:"hengenlab"  file_count:8254  delete_count:2279  deleted_byte_count:5438881964  version:3  compact_revision:1  modified_at_second:1627571069 
          volume id:411892  size:21019062712  collection:"hengenlab"  file_count:8082  delete_count:1574  deleted_byte_count:3995713981  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411917  size:16156430128  collection:"hengenlab"  file_count:6270  delete_count:68  deleted_byte_count:159054334  version:3  compact_revision:3  modified_at_second:1627843741 
          volume id:411932  size:17476189904  collection:"hengenlab"  file_count:6824  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843737 
          volume id:411934  size:17662154256  collection:"hengenlab"  file_count:6889  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:1  modified_at_second:1627843706 
          volume id:411951  size:18886558064  collection:"hengenlab"  file_count:7225  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843724 
          volume id:411988  size:10240976400  collection:"hengenlab"  file_count:3905  delete_count:91  deleted_byte_count:205024517  version:3  compact_revision:2  modified_at_second:1627843695 
          volume id:412009  size:11811033752  collection:"hengenlab"  file_count:4406  delete_count:61  deleted_byte_count:155190529  version:3  compact_revision:2  modified_at_second:1627843739 
        Disk hdd total size:173027537280 file_count:66835 deleted_file:5728 deleted_bytes:13973360660 
      DataNode 10.244.16.89:8080 total size:173027537280 file_count:66835 deleted_file:5728 deleted_bytes:13973360660 
      DataNode 10.244.16.97:8080 hdd(volume:10/88 active:18446744073709551610 free:78 remote:0)
        Disk hdd(volume:10/88 active:18446744073709551610 free:78 remote:0)
          volume id:411694  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627522738 
          volume id:411745  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523320 
          volume id:411812  size:21025219208  collection:"hengenlab"  file_count:8162  delete_count:1641  deleted_byte_count:4120037316  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411831  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523132 
          volume id:411851  size:20707778976  collection:"hengenlab"  file_count:8133  delete_count:246  deleted_byte_count:560626694  version:3  compact_revision:2  modified_at_second:1627843736 
          volume id:411862  size:431544360  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9319  delete_count:259  deleted_byte_count:31888543  version:3  compact_revision:1  modified_at_second:1627576156 
          volume id:411957  size:13489303224  collection:"hengenlab"  file_count:5173  version:3  compact_revision:1  modified_at_second:1627614963 
          volume id:411966  size:10782973592  collection:"hengenlab"  file_count:4115  delete_count:3  deleted_byte_count:3145791  version:3  compact_revision:2  modified_at_second:1627843707 
          volume id:411972  size:12313393160  collection:"hengenlab"  file_count:4472  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843745 
          volume id:412004  size:10772632176  collection:"hengenlab"  file_count:4140  delete_count:28  deleted_byte_count:79692364  version:3  compact_revision:2  modified_at_second:1627843733 
        Disk hdd total size:89522844720 file_count:43514 deleted_file:2180 deleted_bytes:4801682227 
      DataNode 10.244.16.97:8080 total size:89522844720 file_count:43514 deleted_file:2180 deleted_bytes:4801682227 
      DataNode 10.244.16.98:8080 hdd(volume:16/90 active:18446744073709551592 free:74 remote:0)
        Disk hdd(volume:16/90 active:18446744073709551592 free:74 remote:0)
          volume id:411673  size:20976550232  collection:"hengenlab"  file_count:8071  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627703449 
          volume id:411707  size:17874044264  collection:"hengenlab"  file_count:6894  delete_count:36  deleted_byte_count:100664052  version:3  compact_revision:4  modified_at_second:1627843733 
          volume id:411720  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627524407 
          volume id:411748  size:21000373448  collection:"hengenlab"  file_count:7970  delete_count:1380  deleted_byte_count:3565195673  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411760  size:14333970528  collection:"hengenlab"  file_count:5564  version:3  compact_revision:3  modified_at_second:1627611016 
          volume id:411809  size:11310452088  collection:"hengenlab"  file_count:4415  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:4  modified_at_second:1627843729 
          volume id:411810  size:20976280960  collection:"hengenlab"  file_count:8157  delete_count:141  deleted_byte_count:296091258  version:3  compact_revision:2  modified_at_second:1627703861 
          volume id:411811  size:20993007800  collection:"hengenlab"  file_count:8261  delete_count:1346  deleted_byte_count:3385831758  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411815  size:20984547408  collection:"hengenlab"  file_count:8148  delete_count:133  deleted_byte_count:239143575  version:3  compact_revision:2  modified_at_second:1627702576 
          volume id:411821  size:21006604824  collection:"hengenlab"  file_count:8070  delete_count:1458  deleted_byte_count:3653227255  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411853  size:20985752376  collection:"hengenlab"  file_count:8066  delete_count:1465  deleted_byte_count:3668882240  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411881  size:18276124664  collection:"hengenlab"  file_count:7091  delete_count:468  deleted_byte_count:1040392637  version:3  compact_revision:2  modified_at_second:1627843735 
          volume id:411895  size:21080309584  collection:"hengenlab"  file_count:8146  delete_count:2420  deleted_byte_count:5989979359  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411913  size:20984013304  collection:"hengenlab"  file_count:8035  version:3  compact_revision:2  modified_at_second:1627703453 
          volume id:411920  size:9411225816  collection:"hengenlab"  file_count:3640  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843721 
          volume id:411925  size:8018464800  collection:"hengenlab"  file_count:3147  version:3  compact_revision:2  modified_at_second:1627616331 
        Disk hdd total size:268211722104 file_count:103675 deleted_file:8854 deleted_bytes:21953039442 
      DataNode 10.244.16.98:8080 total size:268211722104 file_count:103675 deleted_file:8854 deleted_bytes:21953039442 
      DataNode 10.244.216.104:8080 nvme(volume:92/92 active:92 free:0 remote:0)
        Disk nvme(volume:92/92 active:92 free:0 remote:0)
          volume id:6  size:154572064  file_count:5480  version:3  modified_at_second:1626566497  disk_type:"nvme" 
          volume id:31  size:632704  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:843  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:58  size:601456  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:900  version:3  compact_revision:8  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:79  size:606224  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:854  version:3  compact_revision:6  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:81  size:622232  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:901  version:3  compact_revision:7  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:116  size:2202353384  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1126  version:3  compact_revision:2  modified_at_second:1623132644  disk_type:"nvme" 
          volume id:126  size:8  collection:"._.DS_Store"  replica_placement:1  version:3  modified_at_second:1623132657  disk_type:"nvme" 
          volume id:129  size:8  collection:"._.DS_Store"  replica_placement:1  version:3  modified_at_second:1623132667  disk_type:"nvme" 
          volume id:130  size:8  collection:"._.DS_Store"  version:3  modified_at_second:1610069605  disk_type:"nvme" 
          volume id:137  size:1630156472  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2460  delete_count:1887  deleted_byte_count:432763215  version:3  modified_at_second:1610491324  disk_type:"nvme" 
          volume id:138  size:1603047968  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2449  delete_count:1881  deleted_byte_count:418068489  version:3  modified_at_second:1610491324  disk_type:"nvme" 
          volume id:150  size:1575240048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:839  version:3  modified_at_second:1610491378  disk_type:"nvme" 
          volume id:153  size:1494952944  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:787  version:3  modified_at_second:1623132680  disk_type:"nvme" 
          volume id:159  size:3110118664  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1495  version:3  modified_at_second:1610491401  disk_type:"nvme" 
          volume id:164  size:1458776248  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:813  version:3  modified_at_second:1623132694  disk_type:"nvme" 
          volume id:167  size:1764458648  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:850  version:3  modified_at_second:1610491453  disk_type:"nvme" 
          volume id:168  size:1695114936  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:816  version:3  modified_at_second:1610491453  disk_type:"nvme" 
          volume id:200  size:1152898088  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:588  version:3  modified_at_second:1610494836  disk_type:"nvme" 
          volume id:205  size:1154243552  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:553  version:3  modified_at_second:1610494893  disk_type:"nvme" 
          volume id:231  size:1152295576  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1610496236  disk_type:"nvme" 
          volume id:244  size:1174086520  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:568  version:3  modified_at_second:1610496354  disk_type:"nvme" 
          volume id:264  size:1234236192  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:593  version:3  modified_at_second:1610496631  disk_type:"nvme" 
          volume id:269  size:1187392304  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1623132737  disk_type:"nvme" 
          volume id:276  size:1246990632  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:601  version:3  modified_at_second:1623132751  disk_type:"nvme" 
          volume id:281  size:1753868488  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:919  version:3  modified_at_second:1610496873  disk_type:"nvme" 
          volume id:288  size:1181077768  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:592  version:3  modified_at_second:1623132779  disk_type:"nvme" 
          volume id:293  size:1302889432  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:629  version:3  modified_at_second:1610497013  disk_type:"nvme" 
          volume id:300  size:1246727888  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:598  version:3  modified_at_second:1623132794  disk_type:"nvme" 
          volume id:302  size:1342897048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:644  version:3  modified_at_second:1610497082  disk_type:"nvme" 
          volume id:321  size:1138789880  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1623132808  disk_type:"nvme" 
          volume id:340  size:1161117576  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:568  version:3  modified_at_second:1623132822  disk_type:"nvme" 
          volume id:346  size:1288754128  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:645  version:3  modified_at_second:1610497637  disk_type:"nvme" 
          volume id:358  size:1151521568  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:559  version:3  modified_at_second:1610497787  disk_type:"nvme" 
          volume id:371  size:1312268072  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:628  version:3  modified_at_second:1610498031  disk_type:"nvme" 
          volume id:374  size:1323581208  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:634  version:3  modified_at_second:1623132836  disk_type:"nvme" 
          volume id:376  size:1302828304  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:626  version:3  modified_at_second:1623132865  disk_type:"nvme" 
          volume id:388  size:1324169520  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:664  version:3  modified_at_second:1610498218  disk_type:"nvme" 
          volume id:392  size:1134250720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:545  version:3  modified_at_second:1610498311  disk_type:"nvme" 
          volume id:394  size:1341215576  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:641  version:3  modified_at_second:1623132879  disk_type:"nvme" 
          volume id:401  size:1150668440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:552  version:3  modified_at_second:1610498457  disk_type:"nvme" 
          volume id:408  size:1367251616  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:668  version:3  modified_at_second:1610498529  disk_type:"nvme" 
          volume id:412  size:1156052976  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:564  version:3  modified_at_second:1623132893  disk_type:"nvme" 
          volume id:419  size:1133426832  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:545  version:3  modified_at_second:1610498668  disk_type:"nvme" 
          volume id:430  size:1145801792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:552  version:3  modified_at_second:1610498760  disk_type:"nvme" 
          volume id:450  size:1278326168  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:616  version:3  modified_at_second:1610499083  disk_type:"nvme" 
          volume id:459  size:1212072184  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:601  version:3  modified_at_second:1623132908  disk_type:"nvme" 
          volume id:503  size:1795992728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:969  version:3  modified_at_second:1610643823  disk_type:"nvme" 
          volume id:590  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507789  disk_type:"nvme" 
          volume id:618  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:649  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:707  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:734  size:100663544  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1617901669  disk_type:"nvme" 
          volume id:745  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1618444184  disk_type:"nvme" 
          volume id:755  size:109052328  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1619387454  disk_type:"nvme" 
          volume id:759  size:82165760  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1619387464  disk_type:"nvme" 
          volume id:773  size:142606856  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1619387462  disk_type:"nvme" 
          volume id:780  size:142606832  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1619387455  disk_type:"nvme" 
          volume id:784  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184338  disk_type:"nvme" 
          volume id:786  size:239076032  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1619387472  disk_type:"nvme" 
          volume id:825  size:100663544  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1617901627  disk_type:"nvme" 
          volume id:840  size:109052368  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623184606  disk_type:"nvme" 
          volume id:852  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1618444171  disk_type:"nvme" 
          volume id:864  size:113246768  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504954  disk_type:"nvme" 
          volume id:866  size:109052312  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1619387472  disk_type:"nvme" 
          volume id:871  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623184200  disk_type:"nvme" 
          volume id:875  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184328  disk_type:"nvme" 
          volume id:880  size:104857920  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1618444170  disk_type:"nvme" 
          volume id:881  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184479  disk_type:"nvme" 
          volume id:895  size:142606888  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1618444171  disk_type:"nvme" 
          volume id:911  size:146801232  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1619387449  disk_type:"nvme" 
          volume id:916  size:111997040  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:33557045  version:3  compact_revision:2  modified_at_second:1627500611  disk_type:"nvme" 
          volume id:921  size:50332080  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504749  disk_type:"nvme" 
          volume id:927  size:113438912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504916  disk_type:"nvme" 
          volume id:930  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504956  disk_type:"nvme" 
          volume id:940  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1617901476  disk_type:"nvme" 
          volume id:944  size:104857928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623184059  disk_type:"nvme" 
          volume id:946  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184187  disk_type:"nvme" 
          volume id:969  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623184467  disk_type:"nvme" 
          volume id:986  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627504742  disk_type:"nvme" 
          volume id:992  size:46137696  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1619387463  disk_type:"nvme" 
          volume id:994  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1619387462  disk_type:"nvme" 
          volume id:1003  size:142606848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1619387458  disk_type:"nvme" 
          volume id:1006  size:168896560  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1617901572  disk_type:"nvme" 
          volume id:1025  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1618444180  disk_type:"nvme" 
          volume id:1072  size:20977959168  collection:"braingeneers-backups-swfs"  file_count:12243  delete_count:214  deleted_byte_count:368569899  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1084  size:20974675784  collection:"braingeneers-backups-swfs"  file_count:12149  delete_count:216  deleted_byte_count:352173501  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1087  size:20975802264  collection:"braingeneers-backups-swfs"  file_count:12268  delete_count:249  deleted_byte_count:384729337  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1092  size:20973656776  collection:"braingeneers-backups-swfs"  file_count:12162  delete_count:244  deleted_byte_count:385207912  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1094  size:20972954464  collection:"braingeneers-backups-swfs"  file_count:12254  delete_count:230  deleted_byte_count:352275616  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1097  size:20972823488  collection:"braingeneers-backups-swfs"  file_count:12260  delete_count:225  deleted_byte_count:369502703  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1119  size:20973407464  collection:"braingeneers-backups-swfs"  file_count:12191  delete_count:251  deleted_byte_count:429317624  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1133  size:20979013528  collection:"braingeneers-backups-swfs"  file_count:12195  delete_count:228  deleted_byte_count:369277930  version:3  modified_at_second:1627607153  disk_type:"nvme" 
        Disk nvme total size:225927923296 file_count:137074 deleted_file:5631 deleted_bytes:3916414967 
      DataNode 10.244.216.104:8080 total size:225927923296 file_count:137074 deleted_file:5631 deleted_bytes:3916414967 
      DataNode 10.244.216.115:8080 nvme(volume:74/74 active:69 free:0 remote:0)
        Disk nvme(volume:74/74 active:69 free:0 remote:0)
          volume id:3  size:297552792  file_count:290  version:3  modified_at_second:1626640527  disk_type:"nvme" 
          volume id:55  size:661136  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:892  version:3  compact_revision:7  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:73  size:628400  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:948  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:117  size:8  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  replica_placement:1  version:3  compact_revision:2  modified_at_second:1623174832  disk_type:"nvme" 
          volume id:131  size:8  collection:".DS_Store"  replica_placement:1  version:3  modified_at_second:1623174313  disk_type:"nvme" 
          volume id:134  size:8  collection:".DS_Store"  replica_placement:1  version:3  modified_at_second:1623174842  disk_type:"nvme" 
          volume id:142  size:1559633608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2416  delete_count:1853  deleted_byte_count:384450144  version:3  modified_at_second:1623174854  disk_type:"nvme" 
          volume id:155  size:1270832232  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:620  version:3  modified_at_second:1623174868  disk_type:"nvme" 
          volume id:163  size:1474682472  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:831  version:3  modified_at_second:1623174882  disk_type:"nvme" 
          volume id:170  size:1847542352  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:887  version:3  modified_at_second:1623174896  disk_type:"nvme" 
          volume id:174  size:1212956456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:755  version:3  modified_at_second:1623174910  disk_type:"nvme" 
          volume id:178  size:1225699864  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:774  version:3  modified_at_second:1623174339  disk_type:"nvme" 
          volume id:184  size:1303411928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:785  version:3  modified_at_second:1623174924  disk_type:"nvme" 
          volume id:263  size:1204740008  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:578  version:3  modified_at_second:1610496631  disk_type:"nvme" 
          volume id:304  size:1347904096  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:646  version:3  modified_at_second:1610497082  disk_type:"nvme" 
          volume id:309  size:1703950792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:837  version:3  modified_at_second:1610497170  disk_type:"nvme" 
          volume id:360  size:1169493040  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1623174407  disk_type:"nvme" 
          volume id:396  size:1191652408  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:575  version:3  modified_at_second:1610498385  disk_type:"nvme" 
          volume id:398  size:1245932376  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610498385  disk_type:"nvme" 
          volume id:407  size:1387053048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:673  version:3  modified_at_second:1610498529  disk_type:"nvme" 
          volume id:410  size:1175456952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:572  version:3  modified_at_second:1610498523  disk_type:"nvme" 
          volume id:424  size:1195974440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:575  version:3  modified_at_second:1623174435  disk_type:"nvme" 
          volume id:522  size:1342299832  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:668  version:3  modified_at_second:1610643966  disk_type:"nvme" 
          volume id:538  size:1210268872  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:613  version:3  modified_at_second:1623174463  disk_type:"nvme" 
          volume id:554  size:1174587720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:688  version:3  modified_at_second:1623174476  disk_type:"nvme" 
          volume id:580  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:639  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:642  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:679  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:765  size:239076024  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1618444183  disk_type:"nvme" 
          volume id:834  size:213910272  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  version:3  compact_revision:1  modified_at_second:1619387469  disk_type:"nvme" 
          volume id:836  size:171967008  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1618444183  disk_type:"nvme" 
          volume id:872  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1617901725  disk_type:"nvme" 
          volume id:874  size:134218064  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1623172843  disk_type:"nvme" 
          volume id:876  size:209715896  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1623172869  disk_type:"nvme" 
          volume id:882  size:113246752  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:1  modified_at_second:1627504745  disk_type:"nvme" 
          volume id:908  size:138412464  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1618444165  disk_type:"nvme" 
          volume id:919  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623172907  disk_type:"nvme" 
          volume id:920  size:113246768  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623174569  disk_type:"nvme" 
          volume id:928  size:138412440  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623172920  disk_type:"nvme" 
          volume id:937  size:113246760  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:1  modified_at_second:1627504745  disk_type:"nvme" 
          volume id:963  size:201327128  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623172983  disk_type:"nvme" 
          volume id:965  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623172996  disk_type:"nvme" 
          volume id:1008  size:138412448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:3  modified_at_second:1623174608  disk_type:"nvme" 
          volume id:1012  size:272679104  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  version:3  compact_revision:3  modified_at_second:1623173022  disk_type:"nvme" 
          volume id:1110  size:20986677760  collection:"braingeneers-backups-swfs"  file_count:12129  delete_count:225  deleted_byte_count:373699582  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1125  size:20979792272  collection:"braingeneers-backups-swfs"  file_count:12312  delete_count:253  deleted_byte_count:400756796  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1126  size:20976056280  collection:"braingeneers-backups-swfs"  file_count:12266  delete_count:248  deleted_byte_count:398799062  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1138  size:20989427240  collection:"braingeneers-backups-swfs"  file_count:12263  delete_count:204  deleted_byte_count:311650638  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246201  size:21384261328  collection:"hengenlab"  file_count:8215  read_only:true  version:3  modified_at_second:1627420098  disk_type:"nvme" 
          volume id:246240  size:21135039848  collection:"hengenlab"  file_count:8095  read_only:true  version:3  modified_at_second:1627423054  disk_type:"nvme" 
          volume id:246276  size:21144142368  collection:"hengenlab"  file_count:8052  read_only:true  version:3  modified_at_second:1627424647  disk_type:"nvme" 
          volume id:246372  size:21212400664  collection:"hengenlab"  file_count:8684  delete_count:13  deleted_byte_count:54526463  version:3  modified_at_second:1627429092  disk_type:"nvme" 
          volume id:246378  size:20998753624  collection:"hengenlab"  file_count:8038  delete_count:3  deleted_byte_count:12583053  version:3  modified_at_second:1627429662  disk_type:"nvme" 
          volume id:246433  size:21298865592  collection:"hengenlab"  file_count:8174  version:3  modified_at_second:1627432819  disk_type:"nvme" 
          volume id:246450  size:21345526048  collection:"hengenlab"  file_count:8173  version:3  modified_at_second:1627433965  disk_type:"nvme" 
          volume id:246462  size:21191151728  collection:"hengenlab"  file_count:8097  version:3  modified_at_second:1627434848  disk_type:"nvme" 
          volume id:246471  size:21128006912  collection:"hengenlab"  file_count:8081  delete_count:1  deleted_byte_count:4194325  version:3  modified_at_second:1627500576  disk_type:"nvme" 
          volume id:246483  size:21077677080  collection:"hengenlab"  file_count:8114  version:3  modified_at_second:1627436472  disk_type:"nvme" 
          volume id:246491  size:21520002056  collection:"hengenlab"  file_count:8203  version:3  modified_at_second:1627436960  disk_type:"nvme" 
          volume id:246602  size:21426843352  collection:"hengenlab"  file_count:8195  version:3  modified_at_second:1627439899  disk_type:"nvme" 
          volume id:246642  size:21299151352  collection:"hengenlab"  file_count:8150  version:3  modified_at_second:1627441211  disk_type:"nvme" 
          volume id:246671  size:13369876656  collection:"hengenlab"  file_count:5164  version:3  compact_revision:1  modified_at_second:1627528747  disk_type:"nvme" 
          volume id:246744  size:14194623656  collection:"hengenlab"  file_count:5431  version:3  compact_revision:1  modified_at_second:1627528659  disk_type:"nvme" 
          volume id:246760  size:13673325384  collection:"hengenlab"  file_count:5196  version:3  compact_revision:1  modified_at_second:1627528688  disk_type:"nvme" 
          volume id:246786  size:14171590384  collection:"hengenlab"  file_count:5416  version:3  compact_revision:1  modified_at_second:1627528615  disk_type:"nvme" 
          volume id:246809  size:13970260096  collection:"hengenlab"  file_count:5350  version:3  compact_revision:1  modified_at_second:1627618163  disk_type:"nvme" 
          volume id:246828  size:8240065248  collection:"hengenlab"  file_count:3142  version:3  compact_revision:1  modified_at_second:1627618178  disk_type:"nvme" 
          volume id:246896  size:2700383904  collection:"hengenlab"  file_count:1022  version:3  compact_revision:1  modified_at_second:1627528627  disk_type:"nvme" 
          volume id:246901  size:1741901064  collection:"hengenlab"  file_count:665  version:3  compact_revision:1  modified_at_second:1627528633  disk_type:"nvme" 
          volume id:246920  size:2612123096  collection:"hengenlab"  file_count:998  version:3  compact_revision:1  modified_at_second:1627528664  disk_type:"nvme" 
          volume id:246950  size:2879566056  collection:"hengenlab"  file_count:1101  version:3  compact_revision:1  modified_at_second:1627528777  disk_type:"nvme" 
          volume id:247055  size:4170454488  collection:"hengenlab"  file_count:1583  version:3  compact_revision:1  modified_at_second:1627528697  disk_type:"nvme" 
          volume id:247057  size:3690312136  collection:"hengenlab"  file_count:1418  version:3  compact_revision:1  modified_at_second:1627528724  disk_type:"nvme" 
        Disk nvme total size:483676864296 file_count:208614 deleted_file:2802 deleted_bytes:1949048740 
      DataNode 10.244.216.115:8080 total size:483676864296 file_count:208614 deleted_file:2802 deleted_bytes:1949048740 
      DataNode 10.244.216.119:8080 nvme(volume:47/47 active:47 free:0 remote:0)
        Disk nvme(volume:47/47 active:47 free:0 remote:0)
          volume id:59  size:562688  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:907  version:3  compact_revision:8  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:80  size:587552  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:839  version:3  compact_revision:8  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:124  size:8388776  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:132  size:8  collection:".DS_Store"  version:3  modified_at_second:1610069605  disk_type:"nvme" 
          volume id:143  size:1402413064  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:692  version:3  modified_at_second:1610491343  disk_type:"nvme" 
          volume id:146  size:1604797608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:834  version:3  modified_at_second:1610491353  disk_type:"nvme" 
          volume id:171  size:1814029024  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:874  version:3  modified_at_second:1610491453  disk_type:"nvme" 
          volume id:176  size:1218650056  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:761  version:3  modified_at_second:1610491476  disk_type:"nvme" 
          volume id:182  size:1299684992  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:786  version:3  modified_at_second:1610494389  disk_type:"nvme" 
          volume id:203  size:1269298488  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:606  version:3  modified_at_second:1610494892  disk_type:"nvme" 
          volume id:207  size:1259353968  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:603  version:3  modified_at_second:1610494893  disk_type:"nvme" 
          volume id:223  size:1430634688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:689  version:3  modified_at_second:1610496178  disk_type:"nvme" 
          volume id:308  size:1218204048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610497162  disk_type:"nvme" 
          volume id:316  size:1481558640  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:730  version:3  modified_at_second:1610497252  disk_type:"nvme" 
          volume id:354  size:1228968456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:595  version:3  modified_at_second:1610497794  disk_type:"nvme" 
          volume id:368  size:1356782040  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:649  version:3  modified_at_second:1610497960  disk_type:"nvme" 
          volume id:389  size:1298978368  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:623  version:3  modified_at_second:1610498317  disk_type:"nvme" 
          volume id:395  size:1221106984  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:590  version:3  modified_at_second:1610498385  disk_type:"nvme" 
          volume id:397  size:1321427632  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:635  version:3  modified_at_second:1610498385  disk_type:"nvme" 
          volume id:399  size:1217579976  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:588  version:3  modified_at_second:1610498385  disk_type:"nvme" 
          volume id:455  size:1272114416  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:626  version:3  modified_at_second:1610499157  disk_type:"nvme" 
          volume id:539  size:1217814168  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:598  version:3  modified_at_second:1610644140  disk_type:"nvme" 
          volume id:552  size:2693106344  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1558  version:3  modified_at_second:1610644276  disk_type:"nvme" 
          volume id:570  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:571  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:591  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:612  size:471736  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:636  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:655  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:656  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:722  size:256445448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  version:3  compact_revision:1  modified_at_second:1619387453  disk_type:"nvme" 
          volume id:751  size:247464832  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  version:3  compact_revision:1  modified_at_second:1619387464  disk_type:"nvme" 
          volume id:808  size:171966944  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1619387469  disk_type:"nvme" 
          volume id:813  size:276824936  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504924  disk_type:"nvme" 
          volume id:844  size:171966992  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627500598  disk_type:"nvme" 
          volume id:854  size:142606888  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1618444185  disk_type:"nvme" 
          volume id:935  size:268436096  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1617901650  disk_type:"nvme" 
          volume id:982  size:218104648  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:10  version:3  compact_revision:3  modified_at_second:1619387472  disk_type:"nvme" 
          volume id:1038  size:8  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  version:3  modified_at_second:1615234634  disk_type:"nvme" 
          volume id:1111  size:20992311808  collection:"braingeneers-backups-swfs"  file_count:12332  delete_count:227  deleted_byte_count:340575187  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1131  size:20975001016  collection:"braingeneers-backups-swfs"  file_count:12212  delete_count:254  deleted_byte_count:409474937  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1136  size:20971824008  collection:"braingeneers-backups-swfs"  file_count:12174  delete_count:218  deleted_byte_count:362416322  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1141  size:21083986224  collection:"braingeneers-backups-swfs"  file_count:12362  delete_count:224  deleted_byte_count:343942797  version:3  compact_revision:4  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1644  size:9426453224  collection:"hengenlab"  file_count:3614  delete_count:27  deleted_byte_count:62915127  version:3  compact_revision:4  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246178  size:21711767592  collection:"hengenlab"  file_count:8273  delete_count:31  deleted_byte_count:70255243  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246193  size:21502111304  collection:"hengenlab"  file_count:8179  delete_count:24  deleted_byte_count:56623608  read_only:true  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246227  size:9440774688  collection:"hengenlab"  file_count:3598  version:3  compact_revision:1  modified_at_second:1627528715  disk_type:"nvme" 
        Disk nvme total size:174728115592 file_count:88203 deleted_file:1007 deleted_bytes:1654591905 
      DataNode 10.244.216.119:8080 total size:174728115592 file_count:88203 deleted_file:1007 deleted_bytes:1654591905 
      DataNode 10.244.216.121:8080 nvme(volume:68/68 active:67 free:0 remote:0)
        Disk nvme(volume:68/68 active:67 free:0 remote:0)
          volume id:1  size:467947296  file_count:5823  delete_count:1  deleted_byte_count:294  version:3  modified_at_second:1627443123  disk_type:"nvme" 
          volume id:4  size:441258608  file_count:5832  version:3  modified_at_second:1627443063  disk_type:"nvme" 
          volume id:71  size:610784  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:859  version:3  compact_revision:7  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:75  size:580576  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:881  version:3  compact_revision:8  modified_at_second:1627507801  disk_type:"nvme" 
          volume id:140  size:1506928952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2396  delete_count:1864  deleted_byte_count:399350512  version:3  modified_at_second:1623171889  disk_type:"nvme" 
          volume id:147  size:1667938056  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:854  version:3  modified_at_second:1623171904  disk_type:"nvme" 
          volume id:160  size:1194083240  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:581  version:3  modified_at_second:1610491395  disk_type:"nvme" 
          volume id:166  size:1467965456  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:817  version:3  modified_at_second:1623171919  disk_type:"nvme" 
          volume id:172  size:1691025904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:812  version:3  modified_at_second:1623171934  disk_type:"nvme" 
          volume id:189  size:1220039200  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:4642  version:3  modified_at_second:1610494518  disk_type:"nvme" 
          volume id:195  size:1242480352  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3468  version:3  modified_at_second:1623171976  disk_type:"nvme" 
          volume id:233  size:1338611528  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:641  version:3  modified_at_second:1623172005  disk_type:"nvme" 
          volume id:273  size:1200692744  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:615  version:3  modified_at_second:1623172033  disk_type:"nvme" 
          volume id:274  size:1590254712  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:791  version:3  modified_at_second:1610496726  disk_type:"nvme" 
          volume id:278  size:1229501720  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:595  version:3  modified_at_second:1623172048  disk_type:"nvme" 
          volume id:279  size:1259710904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:610  version:3  modified_at_second:1623172062  disk_type:"nvme" 
          volume id:324  size:1228986784  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:592  version:3  modified_at_second:1623172076  disk_type:"nvme" 
          volume id:326  size:1222977192  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:587  version:3  modified_at_second:1610497407  disk_type:"nvme" 
          volume id:403  size:1223593064  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:586  version:3  modified_at_second:1623172133  disk_type:"nvme" 
          volume id:422  size:1235862880  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:593  version:3  modified_at_second:1623172190  disk_type:"nvme" 
          volume id:461  size:1196079944  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1623172247  disk_type:"nvme" 
          volume id:493  size:1214228208  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1166  version:3  modified_at_second:1623172318  disk_type:"nvme" 
          volume id:506  size:1262940608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:710  version:3  modified_at_second:1623172332  disk_type:"nvme" 
          volume id:529  size:1293102176  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:681  version:3  modified_at_second:1623172359  disk_type:"nvme" 
          volume id:572  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:584  size:8388776  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:625  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:660  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:677  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:681  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:689  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:699  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:706  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:711  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:730  size:109052320  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623172569  disk_type:"nvme" 
          volume id:736  size:184550104  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627500600  disk_type:"nvme" 
          volume id:740  size:138412464  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1618444166  disk_type:"nvme" 
          volume id:752  size:209715912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1619387458  disk_type:"nvme" 
          volume id:797  size:167772584  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1623171556  disk_type:"nvme" 
          volume id:941  size:268436144  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:2  modified_at_second:1617901734  disk_type:"nvme" 
          volume id:1044  size:86712  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  file_count:1  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1047  size:8  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1088  size:20974667904  collection:"braingeneers-backups-swfs"  file_count:12173  delete_count:242  deleted_byte_count:374645876  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1120  size:20996395720  collection:"braingeneers-backups-swfs"  file_count:12233  delete_count:212  deleted_byte_count:348970606  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1122  size:20992144360  collection:"braingeneers-backups-swfs"  file_count:12207  delete_count:230  deleted_byte_count:363012031  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1127  size:20976711152  collection:"braingeneers-backups-swfs"  file_count:12297  delete_count:229  deleted_byte_count:364484950  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133787  size:21301768216  collection:"hengenlab"  file_count:8526  delete_count:8264  deleted_byte_count:20658424014  read_only:true  version:3  modified_at_second:1627419563  disk_type:"nvme" 
          volume id:133789  size:735486336  collection:"hengenlab"  file_count:282  read_only:true  version:3  compact_revision:1  modified_at_second:1627507743  disk_type:"nvme" 
          volume id:133792  size:881869784  collection:"hengenlab"  file_count:310  read_only:true  version:3  compact_revision:1  modified_at_second:1627507554  disk_type:"nvme" 
          volume id:133819  size:21226668664  collection:"hengenlab"  file_count:8129  delete_count:8129  deleted_byte_count:21226124008  read_only:true  version:3  modified_at_second:1627419753  disk_type:"nvme" 
          volume id:133873  size:21645954720  collection:"hengenlab"  file_count:8361  read_only:true  version:3  compact_revision:1  modified_at_second:1627423103  disk_type:"nvme" 
          volume id:246278  size:21216024872  collection:"hengenlab"  file_count:8116  delete_count:1  deleted_byte_count:4194325  version:3  modified_at_second:1627504758  disk_type:"nvme" 
          volume id:246304  size:21180999232  collection:"hengenlab"  file_count:8115  read_only:true  version:3  modified_at_second:1627425536  disk_type:"nvme" 
          volume id:246305  size:21253580256  collection:"hengenlab"  file_count:8113  read_only:true  version:3  modified_at_second:1627425536  disk_type:"nvme" 
          volume id:246331  size:21139438032  collection:"hengenlab"  file_count:7420  delete_count:50  deleted_byte_count:209717518  version:3  modified_at_second:1627427144  disk_type:"nvme" 
          volume id:246392  size:21483531064  collection:"hengenlab"  file_count:8199  version:3  modified_at_second:1627430347  disk_type:"nvme" 
          volume id:246418  size:21437637296  collection:"hengenlab"  file_count:8201  version:3  modified_at_second:1627431671  disk_type:"nvme" 
          volume id:246509  size:21324437576  collection:"hengenlab"  file_count:8131  delete_count:1  deleted_byte_count:85  version:3  modified_at_second:1627521514  disk_type:"nvme" 
          volume id:246523  size:21022332720  collection:"hengenlab"  file_count:7739  delete_count:827  deleted_byte_count:2901782526  version:3  modified_at_second:1627521905  disk_type:"nvme" 
          volume id:246589  size:21164642208  collection:"hengenlab"  file_count:8073  version:3  modified_at_second:1627439400  disk_type:"nvme" 
          volume id:246595  size:21191150048  collection:"hengenlab"  file_count:8067  version:3  modified_at_second:1627439729  disk_type:"nvme" 
          volume id:246620  size:21362763672  collection:"hengenlab"  file_count:8162  version:3  modified_at_second:1627440399  disk_type:"nvme" 
          volume id:246648  size:21280598032  collection:"hengenlab"  file_count:8102  version:3  modified_at_second:1627441550  disk_type:"nvme" 
          volume id:246670  size:13572658056  collection:"hengenlab"  file_count:5124  version:3  compact_revision:1  modified_at_second:1627671372  disk_type:"nvme" 
          volume id:246700  size:12345506560  collection:"hengenlab"  file_count:4699  version:3  compact_revision:1  modified_at_second:1627672427  disk_type:"nvme" 
          volume id:246721  size:12449856352  collection:"hengenlab"  file_count:4762  version:3  compact_revision:1  modified_at_second:1627671671  disk_type:"nvme" 
          volume id:246742  size:20670639592  collection:"hengenlab"  file_count:7916  delete_count:2237  deleted_byte_count:5824138895  version:3  modified_at_second:1627521784  disk_type:"nvme" 
          volume id:246746  size:1430471024  collection:"hengenlab"  file_count:536  delete_count:133  deleted_byte_count:362810089  version:3  modified_at_second:1627521784  disk_type:"nvme" 
        Disk nvme total size:493787887720 file_count:229768 deleted_file:22421 deleted_bytes:53041850064 
      DataNode 10.244.216.121:8080 total size:493787887720 file_count:229768 deleted_file:22421 deleted_bytes:53041850064 
      DataNode 10.244.216.124:8080 nvme(volume:69/69 active:67 free:0 remote:0)
        Disk nvme(volume:69/69 active:67 free:0 remote:0)
          volume id:61  size:615912  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:879  version:3  compact_revision:8  modified_at_second:1627513382  disk_type:"nvme" 
          volume id:237  size:1364951800  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:655  version:3  modified_at_second:1610496298  disk_type:"nvme" 
          volume id:249  size:1156345656  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1610496421  disk_type:"nvme" 
          volume id:292  size:1238778960  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:620  version:3  modified_at_second:1610496936  disk_type:"nvme" 
          volume id:296  size:1162877928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:562  version:3  modified_at_second:1610497004  disk_type:"nvme" 
          volume id:307  size:1167087136  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1610497162  disk_type:"nvme" 
          volume id:327  size:1206497736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:579  version:3  modified_at_second:1610497407  disk_type:"nvme" 
          volume id:357  size:1129977984  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:549  version:3  modified_at_second:1610497787  disk_type:"nvme" 
          volume id:364  size:1183530952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:569  version:3  modified_at_second:1610497862  disk_type:"nvme" 
          volume id:373  size:1278907048  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:611  version:3  modified_at_second:1610498031  disk_type:"nvme" 
          volume id:436  size:1117460680  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:547  version:3  modified_at_second:1610498821  disk_type:"nvme" 
          volume id:463  size:1106554216  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:553  version:3  modified_at_second:1623133765  disk_type:"nvme" 
          volume id:485  size:1235448568  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:598  version:3  modified_at_second:1623133793  disk_type:"nvme" 
          volume id:492  size:1147160824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1139  version:3  modified_at_second:1623133807  disk_type:"nvme" 
          volume id:509  size:1207966936  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:619  version:3  modified_at_second:1610643888  disk_type:"nvme" 
          volume id:523  size:1335123824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:670  version:3  modified_at_second:1623133850  disk_type:"nvme" 
          volume id:527  size:1253834800  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:663  version:3  modified_at_second:1623133864  disk_type:"nvme" 
          volume id:531  size:1287189864  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:678  version:3  modified_at_second:1623133878  disk_type:"nvme" 
          volume id:536  size:1290519256  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:654  version:3  modified_at_second:1623133893  disk_type:"nvme" 
          volume id:557  size:1123381424  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:701  version:3  modified_at_second:1623133907  disk_type:"nvme" 
          volume id:647  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627672953  disk_type:"nvme" 
          volume id:653  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627513390  disk_type:"nvme" 
          volume id:662  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627513387  disk_type:"nvme" 
          volume id:669  size:1082993928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9738  delete_count:9737  deleted_byte_count:1078157927  version:3  modified_at_second:1627500596  disk_type:"nvme" 
          volume id:674  size:1083867360  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8810  delete_count:8808  deleted_byte_count:1074892513  version:3  modified_at_second:1627504775  disk_type:"nvme" 
          volume id:697  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627515992  disk_type:"nvme" 
          volume id:704  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627513391  disk_type:"nvme" 
          volume id:1033  size:376  collection:"swfstest"  file_count:1  version:3  modified_at_second:1623134314  disk_type:"nvme" 
          volume id:1037  size:8  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  replica_placement:1  version:3  modified_at_second:1623134327  disk_type:"nvme" 
          volume id:1124  size:20981558144  collection:"braingeneers-backups-swfs"  file_count:12294  delete_count:238  deleted_byte_count:353310263  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1132  size:20972966960  collection:"braingeneers-backups-swfs"  file_count:12252  delete_count:230  deleted_byte_count:365652225  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1134  size:20972489728  collection:"braingeneers-backups-swfs"  file_count:12218  delete_count:217  deleted_byte_count:359714514  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1135  size:20972086640  collection:"braingeneers-backups-swfs"  file_count:12232  delete_count:233  deleted_byte_count:370529147  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1140  size:21014815008  collection:"braingeneers-backups-swfs"  file_count:12261  delete_count:220  deleted_byte_count:364939661  version:3  compact_revision:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1767  size:245280920  file_count:112  version:3  modified_at_second:1627431723  disk_type:"nvme" 
          volume id:133787  size:21301768216  collection:"hengenlab"  file_count:8526  delete_count:8264  deleted_byte_count:20658424014  read_only:true  version:3  modified_at_second:1627413970  disk_type:"nvme" 
          volume id:133819  size:21226668664  collection:"hengenlab"  file_count:8129  delete_count:8129  deleted_byte_count:21226124008  read_only:true  version:3  modified_at_second:1627413963  disk_type:"nvme" 
          volume id:133878  size:13631888  collection:"hengenlab"  file_count:7  read_only:true  version:3  compact_revision:1  modified_at_second:1627513374  disk_type:"nvme" 
          volume id:133883  size:18874712  collection:"hengenlab"  file_count:6  read_only:true  version:3  compact_revision:1  modified_at_second:1627513357  disk_type:"nvme" 
          volume id:133902  size:15728984  collection:"hengenlab"  file_count:6  read_only:true  version:3  compact_revision:1  modified_at_second:1627513353  disk_type:"nvme" 
          volume id:133906  size:9437360  collection:"hengenlab"  file_count:3  read_only:true  version:3  compact_revision:1  modified_at_second:1627513371  disk_type:"nvme" 
          volume id:133916  size:10485992  collection:"hengenlab"  file_count:4  read_only:true  version:3  compact_revision:1  modified_at_second:1627513365  disk_type:"nvme" 
          volume id:133930  size:9437360  collection:"hengenlab"  file_count:3  read_only:true  version:3  compact_revision:1  modified_at_second:1627513375  disk_type:"nvme" 
          volume id:133931  size:30409160  collection:"hengenlab"  file_count:8  read_only:true  version:3  compact_revision:1  modified_at_second:1627513362  disk_type:"nvme" 
          volume id:133940  size:10485992  collection:"hengenlab"  file_count:4  read_only:true  version:3  compact_revision:1  modified_at_second:1627513381  disk_type:"nvme" 
          volume id:133944  size:27263432  collection:"hengenlab"  file_count:8  read_only:true  version:3  compact_revision:1  modified_at_second:1627513361  disk_type:"nvme" 
          volume id:133998  size:20971976  collection:"hengenlab"  file_count:8  read_only:true  version:3  compact_revision:1  modified_at_second:1627513353  disk_type:"nvme" 
          volume id:134016  size:27263432  collection:"hengenlab"  file_count:8  read_only:true  version:3  compact_revision:1  modified_at_second:1627513354  disk_type:"nvme" 
          volume id:134026  size:11534624  collection:"hengenlab"  file_count:5  read_only:true  version:3  compact_revision:1  modified_at_second:1627513367  disk_type:"nvme" 
          volume id:134027  size:18874712  collection:"hengenlab"  file_count:6  read_only:true  version:3  compact_revision:1  modified_at_second:1627513378  disk_type:"nvme" 
          volume id:134035  size:7340432  collection:"hengenlab"  file_count:7  read_only:true  version:3  compact_revision:1  modified_at_second:1627513356  disk_type:"nvme" 
          volume id:134044  size:13631720  collection:"hengenlab"  file_count:4  read_only:true  version:3  compact_revision:1  modified_at_second:1627513373  disk_type:"nvme" 
          volume id:134080  size:20971976  collection:"hengenlab"  file_count:8  read_only:true  version:3  compact_revision:1  modified_at_second:1627513354  disk_type:"nvme" 
          volume id:134097  size:9437528  collection:"hengenlab"  file_count:6  read_only:true  version:3  compact_revision:1  modified_at_second:1627513365  disk_type:"nvme" 
          volume id:134098  size:8388728  collection:"hengenlab"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1627513356  disk_type:"nvme" 
          volume id:134124  size:23069240  collection:"hengenlab"  file_count:10  read_only:true  version:3  compact_revision:1  modified_at_second:1627513352  disk_type:"nvme" 
          volume id:134126  size:5243000  collection:"hengenlab"  file_count:2  read_only:true  version:3  compact_revision:1  modified_at_second:1627513372  disk_type:"nvme" 
          volume id:134130  size:9437360  collection:"hengenlab"  file_count:3  read_only:true  version:3  compact_revision:1  modified_at_second:1627513373  disk_type:"nvme" 
          volume id:134152  size:15728984  collection:"hengenlab"  file_count:6  read_only:true  version:3  compact_revision:1  modified_at_second:1627513371  disk_type:"nvme" 
          volume id:134170  size:18874712  collection:"hengenlab"  file_count:6  version:3  compact_revision:1  modified_at_second:1627513372  disk_type:"nvme" 
          volume id:134194  size:11534624  collection:"hengenlab"  file_count:5  version:3  compact_revision:1  modified_at_second:1627513370  disk_type:"nvme" 
          volume id:134245  size:21333464920  collection:"hengenlab"  file_count:8233  delete_count:1356  deleted_byte_count:3363590657  version:3  modified_at_second:1627504865  disk_type:"nvme" 
          volume id:246154  size:21297033344  collection:"hengenlab"  file_count:8121  read_only:true  version:3  modified_at_second:1627419656  disk_type:"nvme" 
          volume id:246255  size:21292259792  collection:"hengenlab"  file_count:8058  read_only:true  version:3  modified_at_second:1627423488  disk_type:"nvme" 
          volume id:246282  size:21406049120  collection:"hengenlab"  file_count:8163  read_only:true  version:3  modified_at_second:1627424758  disk_type:"nvme" 
          volume id:246322  size:21614436384  collection:"hengenlab"  file_count:9033  version:3  modified_at_second:1627426441  disk_type:"nvme" 
          volume id:246400  size:21121703536  collection:"hengenlab"  file_count:8120  version:3  modified_at_second:1627431006  disk_type:"nvme" 
          volume id:246414  size:21486611344  collection:"hengenlab"  file_count:8180  version:3  modified_at_second:1627431506  disk_type:"nvme" 
          volume id:246425  size:1747029080  collection:"hengenlab"  file_count:662  version:3  modified_at_second:1627432019  disk_type:"nvme" 
        Disk nvme total size:324540519312 file_count:168301 deleted_file:37432 deleted_bytes:49215334929 
      DataNode 10.244.216.124:8080 total size:324540519312 file_count:168301 deleted_file:37432 deleted_bytes:49215334929 
      DataNode 10.244.216.77:8080 nvme(volume:58/58 active:57 free:0 remote:0)
        Disk nvme(volume:58/58 active:57 free:0 remote:0)
          volume id:2  size:237058544  file_count:5594  delete_count:1  deleted_byte_count:197  version:3  modified_at_second:1626640345  disk_type:"nvme" 
          volume id:38  size:604200  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:869  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:39  size:578216  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:848  version:3  compact_revision:7  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:121  size:12583160  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:3  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:128  size:8  collection:"._.DS_Store"  version:3  modified_at_second:1610069605  disk_type:"nvme" 
          volume id:136  size:8  collection:".DS_Store"  version:3  compact_revision:1  modified_at_second:1610070116  disk_type:"nvme" 
          volume id:217  size:1209636576  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:581  version:3  modified_at_second:1623175319  disk_type:"nvme" 
          volume id:219  size:1197692984  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1623175333  disk_type:"nvme" 
          volume id:226  size:1461113792  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:702  version:3  modified_at_second:1623175360  disk_type:"nvme" 
          volume id:271  size:1536308152  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:765  version:3  modified_at_second:1623175388  disk_type:"nvme" 
          volume id:299  size:1258933600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:603  version:3  modified_at_second:1623175402  disk_type:"nvme" 
          volume id:325  size:1246875352  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:598  version:3  modified_at_second:1623175416  disk_type:"nvme" 
          volume id:329  size:1218014304  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:601  version:3  modified_at_second:1623175430  disk_type:"nvme" 
          volume id:333  size:1196940808  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1623175443  disk_type:"nvme" 
          volume id:345  size:1205177112  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:609  version:3  modified_at_second:1623175471  disk_type:"nvme" 
          volume id:367  size:1257405952  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:602  version:3  modified_at_second:1610497960  disk_type:"nvme" 
          volume id:384  size:1245513472  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:627  version:3  modified_at_second:1623175540  disk_type:"nvme" 
          volume id:385  size:1265778408  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:635  version:3  modified_at_second:1610498218  disk_type:"nvme" 
          volume id:441  size:1220881832  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:589  version:3  modified_at_second:1623175568  disk_type:"nvme" 
          volume id:445  size:1568529440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:779  version:3  modified_at_second:1610499009  disk_type:"nvme" 
          volume id:483  size:1215104824  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:597  version:3  modified_at_second:1623175595  disk_type:"nvme" 
          volume id:498  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:504  size:1226829928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:697  version:3  modified_at_second:1623175637  disk_type:"nvme" 
          volume id:525  size:1377305584  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:687  version:3  modified_at_second:1610643966  disk_type:"nvme" 
          volume id:526  size:1282701392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:641  version:3  modified_at_second:1610643966  disk_type:"nvme" 
          volume id:528  size:1327751736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:695  version:3  modified_at_second:1610644025  disk_type:"nvme" 
          volume id:596  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:648  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507798  disk_type:"nvme" 
          volume id:666  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:671  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:687  size:16777576  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:713  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:747  size:138412464  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1618444173  disk_type:"nvme" 
          volume id:749  size:138412488  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1618444179  disk_type:"nvme" 
          volume id:770  size:171967016  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504767  disk_type:"nvme" 
          volume id:776  size:205521416  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:2  modified_at_second:1619387461  disk_type:"nvme" 
          volume id:846  size:180355792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1619387463  disk_type:"nvme" 
          volume id:931  size:134218088  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1617901742  disk_type:"nvme" 
          volume id:961  size:209715936  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627500580  disk_type:"nvme" 
          volume id:974  size:207600392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:8  version:3  compact_revision:1  modified_at_second:1619387472  disk_type:"nvme" 
          volume id:976  size:142606864  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:2  modified_at_second:1627504752  disk_type:"nvme" 
          volume id:1029  size:167772560  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1613770360  disk_type:"nvme" 
          volume id:1041  size:8  collection:"pvc-0544e3a6-da8f-42fe-a149-3a103bcc8552"  version:3  modified_at_second:1615234634  disk_type:"nvme" 
          volume id:1048  size:8  collection:"pvc-73532100-54aa-469c-b8a7-1774be8c3b9e"  version:3  compact_revision:3  modified_at_second:1625624761  disk_type:"nvme" 
          volume id:1075  size:20980265944  collection:"braingeneers-backups-swfs"  file_count:12204  delete_count:220  deleted_byte_count:341737893  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1091  size:20979698232  collection:"braingeneers-backups-swfs"  file_count:12215  delete_count:233  deleted_byte_count:385517642  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1099  size:20972466088  collection:"braingeneers-backups-swfs"  file_count:12236  delete_count:240  deleted_byte_count:404330634  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1129  size:20973262376  collection:"braingeneers-backups-swfs"  file_count:12222  delete_count:221  deleted_byte_count:345307871  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246272  size:21262264696  collection:"hengenlab"  file_count:8123  read_only:true  version:3  modified_at_second:1627424197  disk_type:"nvme" 
          volume id:246389  size:21190435064  collection:"hengenlab"  file_count:8096  delete_count:6  deleted_byte_count:12583038  version:3  modified_at_second:1627504944  disk_type:"nvme" 
          volume id:246464  size:21333635120  collection:"hengenlab"  file_count:8201  version:3  modified_at_second:1627435459  disk_type:"nvme" 
          volume id:246484  size:21157516152  collection:"hengenlab"  file_count:8049  version:3  modified_at_second:1627436478  disk_type:"nvme" 
          volume id:246539  size:12094202280  collection:"hengenlab"  file_count:4624  version:3  compact_revision:1  modified_at_second:1627528771  disk_type:"nvme" 
          volume id:246591  size:21264489512  collection:"hengenlab"  file_count:8130  read_only:true  version:3  modified_at_second:1627439571  disk_type:"nvme" 
          volume id:246617  size:21144028736  collection:"hengenlab"  file_count:8071  version:3  modified_at_second:1627440224  disk_type:"nvme" 
          volume id:246635  size:21297929752  collection:"hengenlab"  file_count:8139  version:3  modified_at_second:1627440889  disk_type:"nvme" 
          volume id:246652  size:21099579176  collection:"hengenlab"  file_count:8055  version:3  modified_at_second:1627442183  disk_type:"nvme" 
          volume id:246690  size:2794514912  collection:"hengenlab"  file_count:1069  version:3  compact_revision:1  modified_at_second:1627528591  disk_type:"nvme" 
        Disk nvme total size:295115049088 file_count:138995 deleted_file:925 deleted_bytes:1506254636 
      DataNode 10.244.216.77:8080 total size:295115049088 file_count:138995 deleted_file:925 deleted_bytes:1506254636 
      DataNode 10.244.216.96:8080 nvme(volume:54/54 active:54 free:0 remote:0)
        Disk nvme(volume:54/54 active:54 free:0 remote:0)
          volume id:49  size:620856  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:891  version:3  compact_revision:8  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:77  size:619536  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:892  version:3  compact_revision:7  modified_at_second:1627507800  disk_type:"nvme" 
          volume id:125  size:8  collection:"._.DS_Store"  version:3  compact_revision:1  modified_at_second:1610394116  disk_type:"nvme" 
          volume id:193  size:1251540904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:3472  version:3  modified_at_second:1610494777  disk_type:"nvme" 
          volume id:224  size:1473093376  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:708  version:3  modified_at_second:1610496178  disk_type:"nvme" 
          volume id:257  size:1145971536  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1610496537  disk_type:"nvme" 
          volume id:272  size:1208778992  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:608  version:3  modified_at_second:1610496719  disk_type:"nvme" 
          volume id:283  size:1241199976  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:600  version:3  modified_at_second:1610496866  disk_type:"nvme" 
          volume id:289  size:1243481280  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:623  version:3  modified_at_second:1610496936  disk_type:"nvme" 
          volume id:306  size:1195553616  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:591  version:3  modified_at_second:1610497162  disk_type:"nvme" 
          volume id:322  size:1195975408  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:596  version:3  modified_at_second:1610497331  disk_type:"nvme" 
          volume id:341  size:1239397416  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:629  version:3  modified_at_second:1610497637  disk_type:"nvme" 
          volume id:349  size:1142160784  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:549  version:3  modified_at_second:1610497700  disk_type:"nvme" 
          volume id:351  size:1137331600  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:547  version:3  modified_at_second:1610497700  disk_type:"nvme" 
          volume id:362  size:1154734680  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:558  version:3  modified_at_second:1610497862  disk_type:"nvme" 
          volume id:365  size:1089066584  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:521  version:3  modified_at_second:1610497955  disk_type:"nvme" 
          volume id:444  size:1487526264  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:738  version:3  modified_at_second:1610499008  disk_type:"nvme" 
          volume id:454  size:1228957688  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:592  version:3  modified_at_second:1610499083  disk_type:"nvme" 
          volume id:511  size:2214084672  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1106  version:3  modified_at_second:1623175727  disk_type:"nvme" 
          volume id:518  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1627507789  disk_type:"nvme" 
          volume id:533  size:1103653736  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:540  version:3  modified_at_second:1610644079  disk_type:"nvme" 
          volume id:542  size:1175342904  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:582  version:3  modified_at_second:1623175755  disk_type:"nvme" 
          volume id:564  size:884884392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1452  version:3  modified_at_second:1623175768  disk_type:"nvme" 
          volume id:585  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:587  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:605  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507791  disk_type:"nvme" 
          volume id:607  size:8388808  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:611  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:645  size:12583192  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627507789  disk_type:"nvme" 
          volume id:650  size:8388776  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627507793  disk_type:"nvme" 
          volume id:659  size:12583176  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:672  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:680  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:693  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507796  disk_type:"nvme" 
          volume id:694  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507797  disk_type:"nvme" 
          volume id:696  size:12583176  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627507792  disk_type:"nvme" 
          volume id:705  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:709  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627507795  disk_type:"nvme" 
          volume id:748  size:234881648  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:7  version:3  compact_revision:1  modified_at_second:1617901734  disk_type:"nvme" 
          volume id:1027  size:222299096  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:11  version:3  compact_revision:2  modified_at_second:1619387473  disk_type:"nvme" 
          volume id:1031  size:8  collection:"swfstest"  replica_placement:1  version:3  modified_at_second:1623176448  disk_type:"nvme" 
          volume id:1103  size:20977418120  collection:"braingeneers-backups-swfs"  file_count:12141  delete_count:222  deleted_byte_count:349264316  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1139  size:20986196552  collection:"braingeneers-backups-swfs"  file_count:12267  delete_count:223  deleted_byte_count:356436042  version:3  compact_revision:2  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1143  size:20972913568  collection:"braingeneers-backups-swfs"  file_count:12178  delete_count:224  deleted_byte_count:363552822  version:3  compact_revision:2  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1145  size:21048148008  collection:"braingeneers-backups-swfs"  file_count:12399  delete_count:230  deleted_byte_count:379166350  version:3  compact_revision:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1642  size:9100146128  collection:"hengenlab"  file_count:3480  delete_count:16  deleted_byte_count:41943376  version:3  compact_revision:4  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246199  size:21403378272  collection:"hengenlab"  file_count:8154  delete_count:9  deleted_byte_count:34603197  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246205  size:21375756440  collection:"hengenlab"  file_count:8181  delete_count:10  deleted_byte_count:35651794  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246252  size:21526280816  collection:"hengenlab"  file_count:8194  delete_count:12  deleted_byte_count:34603260  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246269  size:21725222792  collection:"hengenlab"  file_count:8334  delete_count:11  deleted_byte_count:27263207  version:3  modified_at_second:1627521881  disk_type:"nvme" 
          volume id:246290  size:21724377808  collection:"hengenlab"  file_count:8308  delete_count:13  deleted_byte_count:38797585  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246307  size:21390829120  collection:"hengenlab"  file_count:8104  delete_count:19  deleted_byte_count:41943439  version:3  modified_at_second:1627521881  disk_type:"nvme" 
          volume id:246340  size:21378038880  collection:"hengenlab"  file_count:7477  delete_count:65  deleted_byte_count:253758075  version:3  modified_at_second:1627521882  disk_type:"nvme" 
          volume id:246360  size:3123609192  collection:"hengenlab"  file_count:1192  version:3  compact_revision:1  modified_at_second:1627528621  disk_type:"nvme" 
        Disk nvme total size:271112526976 file_count:127825 deleted_file:1054 deleted_bytes:1956983463 
      DataNode 10.244.216.96:8080 total size:271112526976 file_count:127825 deleted_file:1054 deleted_bytes:1956983463 
      DataNode 10.244.216.99:8080 nvme(volume:49/49 active:49 free:0 remote:0)
        Disk nvme(volume:49/49 active:49 free:0 remote:0)
          volume id:29  size:529376  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:857  version:3  compact_revision:6  modified_at_second:1627500690  disk_type:"nvme" 
          volume id:47  size:598712  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:920  version:3  compact_revision:8  modified_at_second:1627505190  disk_type:"nvme" 
          volume id:197  size:1091620784  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:548  version:3  modified_at_second:1623182091  disk_type:"nvme" 
          volume id:220  size:1152092072  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:555  version:3  modified_at_second:1623181638  disk_type:"nvme" 
          volume id:229  size:1134909392  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:543  version:3  modified_at_second:1623181157  disk_type:"nvme" 
          volume id:250  size:1142798728  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:580  version:3  modified_at_second:1623181979  disk_type:"nvme" 
          volume id:255  size:1095269672  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:535  version:3  modified_at_second:1623182222  disk_type:"nvme" 
          volume id:270  size:1199706512  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:609  version:3  modified_at_second:1623182325  disk_type:"nvme" 
          volume id:344  size:1187094264  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:591  version:3  modified_at_second:1623182455  disk_type:"nvme" 
          volume id:381  size:1077258400  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:542  version:3  modified_at_second:1623181321  disk_type:"nvme" 
          volume id:390  size:1110493800  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:529  version:3  modified_at_second:1623181008  disk_type:"nvme" 
          volume id:428  size:1178569608  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:568  version:3  modified_at_second:1623182712  disk_type:"nvme" 
          volume id:431  size:1126797960  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:555  version:3  modified_at_second:1623181763  disk_type:"nvme" 
          volume id:446  size:1404180928  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:696  version:3  modified_at_second:1610499009  disk_type:"nvme" 
          volume id:458  size:1141606496  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:572  version:3  modified_at_second:1623181457  disk_type:"nvme" 
          volume id:543  size:1190882840  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:586  version:3  modified_at_second:1623182582  disk_type:"nvme" 
          volume id:546  size:1168631080  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:644  version:3  modified_at_second:1623181627  disk_type:"nvme" 
          volume id:555  size:1099880440  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:613  version:3  modified_at_second:1623180855  disk_type:"nvme" 
          volume id:568  size:853097112  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:1356  version:3  modified_at_second:1623181021  disk_type:"nvme" 
          volume id:637  size:4194392  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627505191  disk_type:"nvme" 
          volume id:798  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623183273  disk_type:"nvme" 
          volume id:826  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1623183121  disk_type:"nvme" 
          volume id:837  size:104857952  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1623184072  disk_type:"nvme" 
          volume id:838  size:75497856  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:2  deleted_byte_count:8388677  version:3  compact_revision:1  modified_at_second:1627500577  disk_type:"nvme" 
          volume id:853  size:79692216  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1623183834  disk_type:"nvme" 
          volume id:859  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1618444183  disk_type:"nvme" 
          volume id:860  size:100663568  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623183263  disk_type:"nvme" 
          volume id:862  size:109052376  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627500582  disk_type:"nvme" 
          volume id:877  size:37748912  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:2  modified_at_second:1623183981  disk_type:"nvme" 
          volume id:888  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183552  disk_type:"nvme" 
          volume id:892  size:104857944  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1618444168  disk_type:"nvme" 
          volume id:912  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1623183539  disk_type:"nvme" 
          volume id:917  size:41943304  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1623183391  disk_type:"nvme" 
          volume id:954  size:201327128  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623184630  disk_type:"nvme" 
          volume id:956  size:142606872  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:1  modified_at_second:1623183845  disk_type:"nvme" 
          volume id:957  size:75497848  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:3  modified_at_second:1627505191  disk_type:"nvme" 
          volume id:959  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1623184258  disk_type:"nvme" 
          volume id:967  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  delete_count:1  deleted_byte_count:4194335  version:3  compact_revision:2  modified_at_second:1627504756  disk_type:"nvme" 
          volume id:987  size:109052360  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  delete_count:1  deleted_byte_count:4194342  version:3  compact_revision:2  modified_at_second:1627504764  disk_type:"nvme" 
          volume id:1060  size:20977909896  collection:"braingeneers-backups-swfs"  file_count:12264  delete_count:231  deleted_byte_count:360228000  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1061  size:20972527616  collection:"braingeneers-backups-swfs"  file_count:12244  delete_count:226  deleted_byte_count:351170696  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1068  size:20983266928  collection:"braingeneers-backups-swfs"  file_count:12188  delete_count:222  deleted_byte_count:372826219  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1116  size:20977766728  collection:"braingeneers-backups-swfs"  file_count:12176  delete_count:211  deleted_byte_count:317023642  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:246192  size:21569860624  collection:"hengenlab"  file_count:8239  read_only:true  version:3  modified_at_second:1627419229  disk_type:"nvme" 
          volume id:246198  size:21638300624  collection:"hengenlab"  file_count:8250  read_only:true  version:3  modified_at_second:1627419609  disk_type:"nvme" 
          volume id:246256  size:21605810464  collection:"hengenlab"  file_count:8279  read_only:true  version:3  modified_at_second:1627423494  disk_type:"nvme" 
          volume id:246309  size:21136409392  collection:"hengenlab"  file_count:8118  version:3  modified_at_second:1627426109  disk_type:"nvme" 
          volume id:246313  size:21143966648  collection:"hengenlab"  file_count:8112  version:3  modified_at_second:1627426110  disk_type:"nvme" 
          volume id:246390  size:2639501832  collection:"hengenlab"  file_count:1017  version:3  modified_at_second:1627430033  disk_type:"nvme" 
        Disk nvme total size:214645512456 file_count:103356 deleted_file:895 deleted_bytes:1422220253 
      DataNode 10.244.216.99:8080 total size:214645512456 file_count:103356 deleted_file:895 deleted_bytes:1422220253 
      DataNode 10.244.80.134:8080 hdd(volume:12/90 active:18446744073709551605 free:78 remote:0)
        Disk hdd(volume:12/90 active:18446744073709551605 free:78 remote:0)
          volume id:411675  size:10925121480  collection:"hengenlab"  file_count:4168  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:4  modified_at_second:1627843728 
          volume id:411687  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523170 
          volume id:411737  size:21003049800  collection:"hengenlab"  file_count:8134  delete_count:1736  deleted_byte_count:4244054587  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411758  size:21013439504  collection:"hengenlab"  file_count:8127  delete_count:1471  deleted_byte_count:3755111049  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411791  size:20974119312  collection:"hengenlab"  file_count:8078  delete_count:156  deleted_byte_count:335792052  version:3  compact_revision:2  modified_at_second:1627704288 
          volume id:411814  size:99191032  collection:"hengenlab"  file_count:50  version:3  compact_revision:1  modified_at_second:1627525396 
          volume id:411889  size:10659886216  collection:"hengenlab"  file_count:4215  delete_count:29  deleted_byte_count:71303777  version:3  compact_revision:4  modified_at_second:1627843740 
          volume id:411960  size:20973696416  collection:"hengenlab"  file_count:7972  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:1  modified_at_second:1627702876 
          volume id:411963  size:20980601448  collection:"hengenlab"  file_count:7939  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627702925 
          volume id:411964  size:10968757944  collection:"hengenlab"  file_count:4217  delete_count:3  deleted_byte_count:3145791  version:3  compact_revision:2  modified_at_second:1627843733 
          volume id:411976  size:12354509912  collection:"hengenlab"  file_count:4562  delete_count:9  deleted_byte_count:15728829  version:3  compact_revision:2  modified_at_second:1627843737 
          volume id:412010  size:11502343208  collection:"hengenlab"  file_count:4471  delete_count:63  deleted_byte_count:176162091  version:3  compact_revision:2  modified_at_second:1627843736 
        Disk hdd total size:161454716280 file_count:61933 deleted_file:3476 deleted_bytes:8623318461 
      DataNode 10.244.80.134:8080 total size:161454716280 file_count:61933 deleted_file:3476 deleted_bytes:8623318461 
      DataNode 10.244.80.140:8080 hdd(volume:13/90 active:9 free:77 remote:0)
        Disk hdd(volume:13/90 active:9 free:77 remote:0)
          volume id:411682  size:10224440944  collection:"hengenlab"  file_count:4003  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:3  modified_at_second:1627843736 
          volume id:411695  size:20976107224  collection:"hengenlab"  file_count:8196  delete_count:107  deleted_byte_count:213840753  version:3  compact_revision:2  modified_at_second:1627703622 
          volume id:411708  size:13297386192  collection:"hengenlab"  file_count:5126  delete_count:118  deleted_byte_count:219681828  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411730  size:9409806072  collection:"hengenlab"  file_count:3635  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:2  modified_at_second:1627843741 
          volume id:411803  size:21002314288  collection:"hengenlab"  file_count:8072  delete_count:1602  deleted_byte_count:3980876253  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411817  size:20995447416  collection:"hengenlab"  file_count:8091  delete_count:1581  deleted_byte_count:4004982858  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411829  size:21001195528  collection:"hengenlab"  file_count:8166  delete_count:1438  deleted_byte_count:3647096970  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411852  size:12087574080  collection:"hengenlab"  file_count:4685  delete_count:177  deleted_byte_count:464608069  version:3  compact_revision:3  modified_at_second:1627843741 
          volume id:411905  size:20996535136  collection:"hengenlab"  file_count:8084  delete_count:1680  deleted_byte_count:4385158464  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411962  size:20973083080  collection:"hengenlab"  file_count:8056  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627702680 
          volume id:412013  size:9106611576  collection:"hengenlab"  file_count:3574  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:3  modified_at_second:1627843705 
          volume id:412016  size:11357864984  collection:"hengenlab"  file_count:4329  delete_count:169  deleted_byte_count:438308317  version:3  compact_revision:2  modified_at_second:1627843734 
          volume id:412031  size:9574720120  collection:"hengenlab"  file_count:3767  delete_count:3  deleted_byte_count:3145791  version:3  compact_revision:1  modified_at_second:1627843736 
        Disk hdd total size:201003086640 file_count:77784 deleted_file:6883 deleted_bytes:17381816719 
      DataNode 10.244.80.140:8080 total size:201003086640 file_count:77784 deleted_file:6883 deleted_bytes:17381816719 
      DataNode 10.244.80.141:8080 hdd(volume:20/90 active:18446744073709551611 free:70 remote:0)
        Disk hdd(volume:20/90 active:18446744073709551611 free:70 remote:0)
          volume id:133842  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627503390 
          volume id:133871  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627502492 
          volume id:133872  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627502512 
          volume id:133928  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627501592 
          volume id:134023  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627501592 
          volume id:134031  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627503392 
          volume id:411670  size:20991493528  collection:"hengenlab"  file_count:8089  delete_count:1595  deleted_byte_count:4184556443  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411705  size:21016882360  collection:"hengenlab"  file_count:8153  delete_count:1679  deleted_byte_count:4279403246  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411782  size:72353264  collection:"hengenlab"  file_count:27  version:3  compact_revision:1  modified_at_second:1627526155 
          volume id:411798  size:20985823160  collection:"hengenlab"  file_count:8133  delete_count:1706  deleted_byte_count:4257830234  version:3  compact_revision:1  modified_at_second:1627609261 
          volume id:411822  size:20339366800  collection:"hengenlab"  file_count:8024  delete_count:1131  deleted_byte_count:2661575468  version:3  compact_revision:3  modified_at_second:1627843706 
          volume id:411833  size:21010453856  collection:"hengenlab"  file_count:8013  delete_count:1428  deleted_byte_count:3577934150  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411847  size:8961732960  collection:"hengenlab"  file_count:3492  delete_count:402  deleted_byte_count:899558882  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411863  size:450799120  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9286  delete_count:226  deleted_byte_count:52260686  version:3  compact_revision:1  modified_at_second:1627576155 
          volume id:411924  size:19830101072  collection:"hengenlab"  file_count:7535  delete_count:4  deleted_byte_count:7340116  version:3  compact_revision:2  modified_at_second:1627843739 
          volume id:411945  size:19297621912  collection:"hengenlab"  file_count:7484  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627843728 
          volume id:411954  size:18566585328  collection:"hengenlab"  file_count:7083  delete_count:3  deleted_byte_count:12582975  version:3  compact_revision:1  modified_at_second:1627843706 
          volume id:411977  size:12048972160  collection:"hengenlab"  file_count:4472  delete_count:3  deleted_byte_count:12582975  version:3  compact_revision:2  modified_at_second:1627843731 
          volume id:412011  size:13056698352  collection:"hengenlab"  file_count:4880  delete_count:67  deleted_byte_count:183502207  version:3  compact_revision:2  modified_at_second:1627843731 
          volume id:412012  size:12884104024  collection:"hengenlab"  file_count:4865  delete_count:65  deleted_byte_count:153093461  version:3  compact_revision:2  modified_at_second:1627843740 
        Disk hdd total size:209512987944 file_count:89536 deleted_file:8310 deleted_bytes:20283269440 
      DataNode 10.244.80.141:8080 total size:209512987944 file_count:89536 deleted_file:8310 deleted_bytes:20283269440 
      DataNode 10.244.80.143:8080 nvme(volume:101/101 active:95 free:0 remote:0)
        Disk nvme(volume:101/101 active:95 free:0 remote:0)
          volume id:35  size:552064  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:868  version:3  compact_revision:7  modified_at_second:1627672955  disk_type:"nvme" 
          volume id:37  size:591040  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:887  version:3  compact_revision:6  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:42  size:955456  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:915  version:3  compact_revision:8  modified_at_second:1627507799  disk_type:"nvme" 
          volume id:48  size:648448  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:871  version:3  compact_revision:7  modified_at_second:1627672955  disk_type:"nvme" 
          volume id:64  size:517592  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:842  version:3  compact_revision:4  modified_at_second:1627515234  disk_type:"nvme" 
          volume id:119  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:4  modified_at_second:1627507794  disk_type:"nvme" 
          volume id:215  size:1201648024  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:577  version:3  modified_at_second:1627446041  disk_type:"nvme" 
          volume id:519  size:104857968  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1627415028  disk_type:"nvme" 
          volume id:586  size:83886632  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:6  version:3  compact_revision:2  modified_at_second:1627415114  disk_type:"nvme" 
          volume id:728  size:109052352  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1627415218  disk_type:"nvme" 
          volume id:731  size:71303424  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1627415300  disk_type:"nvme" 
          volume id:757  size:134220680  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  delete_count:1  deleted_byte_count:33557046  version:3  compact_revision:2  modified_at_second:1627504753  disk_type:"nvme" 
          volume id:761  size:37748928  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627415208  disk_type:"nvme" 
          volume id:762  size:104857944  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:2  modified_at_second:1627415311  disk_type:"nvme" 
          volume id:766  size:109052352  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:1  modified_at_second:1627415411  disk_type:"nvme" 
          volume id:769  size:67109048  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627415401  disk_type:"nvme" 
          volume id:788  size:46137712  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:4  version:3  compact_revision:1  modified_at_second:1627415497  disk_type:"nvme" 
          volume id:790  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1627415486  disk_type:"nvme" 
          volume id:1124  size:20981558144  collection:"braingeneers-backups-swfs"  file_count:12294  delete_count:238  deleted_byte_count:353310263  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133787  size:21301768216  collection:"hengenlab"  file_count:8526  delete_count:8264  deleted_byte_count:20658424014  read_only:true  version:3  modified_at_second:1627418836  disk_type:"nvme" 
          volume id:133819  size:21226668664  collection:"hengenlab"  file_count:8129  delete_count:8129  deleted_byte_count:21226124008  read_only:true  version:3  modified_at_second:1627419166  disk_type:"nvme" 
          volume id:133826  size:20996386664  collection:"hengenlab"  file_count:7925  read_only:true  version:3  compact_revision:1  modified_at_second:1627420219  disk_type:"nvme" 
          volume id:133828  size:3386232504  collection:"hengenlab"  file_count:1299  version:3  compact_revision:2  modified_at_second:1627672815  disk_type:"nvme" 
          volume id:133831  size:21149835000  collection:"hengenlab"  file_count:8102  read_only:true  version:3  compact_revision:1  modified_at_second:1627420246  disk_type:"nvme" 
          volume id:133864  size:21147725272  collection:"hengenlab"  file_count:8062  read_only:true  version:3  compact_revision:1  modified_at_second:1627420790  disk_type:"nvme" 
          volume id:246129  size:20996663480  collection:"hengenlab"  file_count:7985  read_only:true  version:3  compact_revision:1  modified_at_second:1627416369  disk_type:"nvme" 
          volume id:246134  size:21102062712  collection:"hengenlab"  file_count:8107  read_only:true  version:3  compact_revision:1  modified_at_second:1627416409  disk_type:"nvme" 
          volume id:246137  size:21070660448  collection:"hengenlab"  file_count:8067  read_only:true  version:3  compact_revision:1  modified_at_second:1627416015  disk_type:"nvme" 
          volume id:246139  size:21087378424  collection:"hengenlab"  file_count:8143  read_only:true  version:3  compact_revision:1  modified_at_second:1627416103  disk_type:"nvme" 
          volume id:246160  size:21173604912  collection:"hengenlab"  file_count:8044  read_only:true  version:3  modified_at_second:1627417136  disk_type:"nvme" 
          volume id:246191  size:21109310752  collection:"hengenlab"  file_count:8066  read_only:true  version:3  modified_at_second:1627419229  disk_type:"nvme" 
          volume id:246206  size:21228541464  collection:"hengenlab"  file_count:8082  read_only:true  version:3  modified_at_second:1627420699  disk_type:"nvme" 
          volume id:246208  size:21065433552  collection:"hengenlab"  file_count:8030  read_only:true  version:3  modified_at_second:1627420700  disk_type:"nvme" 
          volume id:246209  size:21226447896  collection:"hengenlab"  file_count:8146  read_only:true  version:3  modified_at_second:1627420707  disk_type:"nvme" 
          volume id:246211  size:21042204688  collection:"hengenlab"  file_count:8116  read_only:true  version:3  modified_at_second:1627421160  disk_type:"nvme" 
          volume id:246223  size:21155858416  collection:"hengenlab"  file_count:8099  read_only:true  version:3  modified_at_second:1627421741  disk_type:"nvme" 
          volume id:246225  size:21707382056  collection:"hengenlab"  file_count:8258  read_only:true  version:3  modified_at_second:1627421741  disk_type:"nvme" 
          volume id:246241  size:21374844896  collection:"hengenlab"  file_count:8083  read_only:true  version:3  modified_at_second:1627423054  disk_type:"nvme" 
          volume id:246279  size:21178668712  collection:"hengenlab"  file_count:8084  read_only:true  version:3  modified_at_second:1627424647  disk_type:"nvme" 
          volume id:246310  size:21256214592  collection:"hengenlab"  file_count:8112  read_only:true  version:3  modified_at_second:1627426110  disk_type:"nvme" 
          volume id:246312  size:21139384064  collection:"hengenlab"  file_count:8010  read_only:true  version:3  modified_at_second:1627426100  disk_type:"nvme" 
          volume id:246324  size:21423012704  collection:"hengenlab"  file_count:7876  version:3  modified_at_second:1627426646  disk_type:"nvme" 
          volume id:246327  size:21333965256  collection:"hengenlab"  file_count:7432  version:3  modified_at_second:1627426798  disk_type:"nvme" 
          volume id:246353  size:21539528792  collection:"hengenlab"  file_count:7623  version:3  modified_at_second:1627428047  disk_type:"nvme" 
          volume id:246363  size:21104327016  collection:"hengenlab"  file_count:7931  delete_count:36  deleted_byte_count:150996633  version:3  modified_at_second:1627428714  disk_type:"nvme" 
          volume id:246401  size:21233335320  collection:"hengenlab"  file_count:8045  version:3  modified_at_second:1627431006  disk_type:"nvme" 
          volume id:246412  size:21402967832  collection:"hengenlab"  file_count:8164  version:3  modified_at_second:1627431341  disk_type:"nvme" 
          volume id:246432  size:21349863192  collection:"hengenlab"  file_count:8173  version:3  modified_at_second:1627432826  disk_type:"nvme" 
          volume id:246440  size:21214912272  collection:"hengenlab"  file_count:8123  version:3  modified_at_second:1627433651  disk_type:"nvme" 
          volume id:246454  size:21182583592  collection:"hengenlab"  file_count:8089  version:3  modified_at_second:1627434137  disk_type:"nvme" 
          volume id:246460  size:21313723000  collection:"hengenlab"  file_count:8143  version:3  modified_at_second:1627434637  disk_type:"nvme" 
          volume id:246474  size:21485514192  collection:"hengenlab"  file_count:8211  version:3  modified_at_second:1627435962  disk_type:"nvme" 
          volume id:246508  size:21254246712  collection:"hengenlab"  file_count:8121  read_only:true  version:3  modified_at_second:1627437774  disk_type:"nvme" 
          volume id:246515  size:21255182720  collection:"hengenlab"  file_count:8159  version:3  modified_at_second:1627438116  disk_type:"nvme" 
          volume id:246534  size:11952411712  collection:"hengenlab"  file_count:4563  version:3  compact_revision:1  modified_at_second:1627672900  disk_type:"nvme" 
          volume id:246568  size:21482416816  collection:"hengenlab"  file_count:8176  version:3  modified_at_second:1627438973  disk_type:"nvme" 
          volume id:246643  size:21212728664  collection:"hengenlab"  file_count:8091  version:3  modified_at_second:1627441212  disk_type:"nvme" 
          volume id:246646  size:21230091592  collection:"hengenlab"  file_count:8110  version:3  modified_at_second:1627441365  disk_type:"nvme" 
          volume id:246654  size:21256629168  collection:"hengenlab"  file_count:8221  version:3  modified_at_second:1627442183  disk_type:"nvme" 
          volume id:246667  size:21470296632  collection:"hengenlab"  file_count:7833  delete_count:696  deleted_byte_count:2816158582  version:3  modified_at_second:1627521906  disk_type:"nvme" 
          volume id:246762  size:13993967752  collection:"hengenlab"  file_count:5330  version:3  compact_revision:1  modified_at_second:1627671971  disk_type:"nvme" 
          volume id:246804  size:14747219496  collection:"hengenlab"  file_count:5616  version:3  compact_revision:1  modified_at_second:1627672312  disk_type:"nvme" 
          volume id:246821  size:5053374912  collection:"hengenlab"  file_count:1930  version:3  compact_revision:1  modified_at_second:1627671741  disk_type:"nvme" 
          volume id:246846  size:15621328600  collection:"hengenlab"  file_count:5960  version:3  compact_revision:1  modified_at_second:1627672550  disk_type:"nvme" 
          volume id:246857  size:9179967608  collection:"hengenlab"  file_count:3501  version:3  compact_revision:1  modified_at_second:1627671462  disk_type:"nvme" 
          volume id:246860  size:3318059704  collection:"hengenlab"  file_count:1276  version:3  compact_revision:1  modified_at_second:1627672026  disk_type:"nvme" 
          volume id:246908  size:2606815832  collection:"hengenlab"  file_count:998  version:3  compact_revision:1  modified_at_second:1627671547  disk_type:"nvme" 
          volume id:246919  size:2756880144  collection:"hengenlab"  file_count:1056  version:3  compact_revision:1  modified_at_second:1627672233  disk_type:"nvme" 
          volume id:246923  size:3006331520  collection:"hengenlab"  file_count:1145  version:3  compact_revision:1  modified_at_second:1627672445  disk_type:"nvme" 
          volume id:246927  size:2843798936  collection:"hengenlab"  file_count:1086  version:3  compact_revision:1  modified_at_second:1627672947  disk_type:"nvme" 
          volume id:246928  size:2784207504  collection:"hengenlab"  file_count:1056  version:3  compact_revision:1  modified_at_second:1627671756  disk_type:"nvme" 
          volume id:246951  size:3024285968  collection:"hengenlab"  file_count:1143  version:3  compact_revision:1  modified_at_second:1627671257  disk_type:"nvme" 
          volume id:246984  size:2888888600  collection:"hengenlab"  file_count:1102  version:3  compact_revision:1  modified_at_second:1627672088  disk_type:"nvme" 
          volume id:247014  size:2794514912  collection:"hengenlab"  file_count:1069  version:3  compact_revision:1  modified_at_second:1627672123  disk_type:"nvme" 
          volume id:247016  size:3105078816  collection:"hengenlab"  file_count:1182  version:3  compact_revision:1  modified_at_second:1627672389  disk_type:"nvme" 
          volume id:247074  size:2857610056  collection:"hengenlab"  file_count:1096  version:3  compact_revision:1  modified_at_second:1627672645  disk_type:"nvme" 
          volume id:247093  size:2576585584  collection:"hengenlab"  file_count:987  version:3  compact_revision:1  modified_at_second:1627671649  disk_type:"nvme" 
          volume id:247098  size:2826037632  collection:"hengenlab"  file_count:1088  version:3  compact_revision:1  modified_at_second:1627671893  disk_type:"nvme" 
          volume id:247108  size:2750832040  collection:"hengenlab"  file_count:1054  version:3  compact_revision:1  modified_at_second:1627672601  disk_type:"nvme" 
          volume id:247111  size:2410727864  collection:"hengenlab"  file_count:922  version:3  compact_revision:1  modified_at_second:1627672153  disk_type:"nvme" 
          volume id:247142  size:2827020992  collection:"hengenlab"  file_count:1073  version:3  compact_revision:1  modified_at_second:1627671400  disk_type:"nvme" 
          volume id:247147  size:2616432096  collection:"hengenlab"  file_count:998  version:3  compact_revision:1  modified_at_second:1627671796  disk_type:"nvme" 
          volume id:247162  size:2420165216  collection:"hengenlab"  file_count:925  read_only:true  version:3  compact_revision:1  modified_at_second:1627672615  disk_type:"nvme" 
          volume id:247167  size:2300624864  collection:"hengenlab"  file_count:877  version:3  compact_revision:1  modified_at_second:1627671995  disk_type:"nvme" 
          volume id:247194  size:2024843552  collection:"hengenlab"  file_count:773  version:3  compact_revision:1  modified_at_second:1627672342  disk_type:"nvme" 
          volume id:247196  size:2161275312  collection:"hengenlab"  file_count:812  version:3  compact_revision:1  modified_at_second:1627672048  disk_type:"nvme" 
          volume id:247197  size:1916903432  collection:"hengenlab"  file_count:752  version:3  compact_revision:1  modified_at_second:1627672928  disk_type:"nvme" 
          volume id:247205  size:1991582056  collection:"hengenlab"  file_count:758  version:3  compact_revision:1  modified_at_second:1627671698  disk_type:"nvme" 
          volume id:247209  size:1645315304  collection:"hengenlab"  file_count:628  version:3  compact_revision:2  modified_at_second:1627671498  disk_type:"nvme" 
          volume id:247216  size:1438677080  collection:"hengenlab"  file_count:550  version:3  compact_revision:2  modified_at_second:1627671781  disk_type:"nvme" 
          volume id:247227  size:1227907928  collection:"hengenlab"  file_count:454  version:3  compact_revision:2  modified_at_second:1627671503  disk_type:"nvme" 
          volume id:247229  size:1125684104  collection:"hengenlab"  file_count:443  version:3  compact_revision:2  modified_at_second:1627671636  disk_type:"nvme" 
          volume id:247238  size:853559408  collection:"hengenlab"  file_count:331  version:3  compact_revision:2  modified_at_second:1627671676  disk_type:"nvme" 
          volume id:247242  size:28755752  file_count:11  version:3  modified_at_second:1627449005  disk_type:"nvme" 
          volume id:247261  size:831538304  collection:"hengenlab"  file_count:313  version:3  compact_revision:2  modified_at_second:1627672052  disk_type:"nvme" 
          volume id:247267  size:819070200  collection:"hengenlab"  file_count:315  version:3  compact_revision:2  modified_at_second:1627672798  disk_type:"nvme" 
          volume id:247285  size:693188024  collection:"hengenlab"  file_count:266  version:3  compact_revision:2  modified_at_second:1627671388  disk_type:"nvme" 
          volume id:247295  size:683865312  collection:"hengenlab"  file_count:262  version:3  compact_revision:2  modified_at_second:1627672648  disk_type:"nvme" 
          volume id:247313  size:527444936  collection:"hengenlab"  file_count:200  version:3  compact_revision:2  modified_at_second:1627672586  disk_type:"nvme" 
          volume id:247323  size:420846328  collection:"hengenlab"  file_count:164  version:3  compact_revision:1  modified_at_second:1627507786  disk_type:"nvme" 
          volume id:247328  size:54706272  collection:"hengenlab"  file_count:22  version:3  compact_revision:1  modified_at_second:1627507787  disk_type:"nvme" 
        Disk nvme total size:996707130184 file_count:387380 deleted_file:17364 deleted_bytes:45238570546 
      DataNode 10.244.80.143:8080 total size:996707130184 file_count:387380 deleted_file:17364 deleted_bytes:45238570546 
      DataNode 10.244.80.144:8080 hdd(volume:8/92 active:18446744073709551613 free:84 remote:0)
        Disk hdd(volume:8/92 active:18446744073709551613 free:84 remote:0)
          volume id:411663  size:9165165000  collection:"hengenlab"  file_count:3579  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843722 
          volume id:411899  size:20987709344  collection:"hengenlab"  file_count:8172  delete_count:1670  deleted_byte_count:4186581607  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411912  size:9780260536  collection:"hengenlab"  file_count:3675  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:2  modified_at_second:1627843741 
          volume id:411936  size:19749281296  collection:"hengenlab"  file_count:7667  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:1  modified_at_second:1627843740 
          volume id:411937  size:19947403744  collection:"hengenlab"  file_count:7664  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627843745 
          volume id:411939  size:19769183072  collection:"hengenlab"  file_count:7606  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:1  modified_at_second:1627843732 
          volume id:411991  size:9944244000  collection:"hengenlab"  file_count:3865  delete_count:89  deleted_byte_count:206005689  version:3  compact_revision:2  modified_at_second:1627843715 
          volume id:412001  size:10948213800  collection:"hengenlab"  file_count:4180  delete_count:52  deleted_byte_count:145753156  version:3  compact_revision:2  modified_at_second:1627843742 
        Disk hdd total size:120291460792 file_count:46408 deleted_file:1820 deleted_bytes:4566652193 
      DataNode 10.244.80.144:8080 total size:120291460792 file_count:46408 deleted_file:1820 deleted_bytes:4566652193 
      DataNode 10.244.80.145:8080 hdd(volume:10/90 active:18446744073709551613 free:80 remote:0)
        Disk hdd(volume:10/90 active:18446744073709551613 free:80 remote:0)
          volume id:411693  size:21005745488  collection:"hengenlab"  file_count:8183  delete_count:2267  deleted_byte_count:5584520032  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411726  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523126 
          volume id:411836  size:17758175912  collection:"hengenlab"  file_count:6899  delete_count:454  deleted_byte_count:1105798495  version:3  compact_revision:2  modified_at_second:1627843725 
          volume id:411873  size:16990152440  collection:"hengenlab"  file_count:6670  delete_count:543  deleted_byte_count:1298690618  version:3  compact_revision:2  modified_at_second:1627843738 
          volume id:411875  size:9036328584  collection:"hengenlab"  file_count:3519  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:2  modified_at_second:1627843742 
          volume id:411878  size:21015592712  collection:"hengenlab"  file_count:8082  delete_count:1402  deleted_byte_count:3597776530  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411887  size:18731439016  collection:"hengenlab"  file_count:7261  delete_count:471  deleted_byte_count:1103119223  version:3  compact_revision:2  modified_at_second:1627843715 
          volume id:411888  size:6917160672  collection:"hengenlab"  file_count:2753  delete_count:673  deleted_byte_count:1567438719  version:3  compact_revision:2  modified_at_second:1627607477 
          volume id:411970  size:11036248712  collection:"hengenlab"  file_count:4155  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:2  modified_at_second:1627843730 
          volume id:411994  size:11059388320  collection:"hengenlab"  file_count:4249  delete_count:5  deleted_byte_count:8388713  version:3  compact_revision:2  modified_at_second:1627843719 
        Disk hdd total size:133550231864 file_count:51771 deleted_file:5821 deleted_bytes:14284606824 
      DataNode 10.244.80.145:8080 total size:133550231864 file_count:51771 deleted_file:5821 deleted_bytes:14284606824 
      DataNode 10.244.80.146:8080 hdd(volume:13/88 active:18446744073709551608 free:75 remote:0)
        Disk hdd(volume:13/88 active:18446744073709551608 free:75 remote:0)
          volume id:133825  size:9225833640  collection:"hengenlab"  file_count:3559  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843739 
          volume id:411686  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627522290 
          volume id:411690  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627522290 
          volume id:411738  size:21025894536  collection:"hengenlab"  file_count:8089  delete_count:1639  deleted_byte_count:4127582642  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411750  size:13765460952  collection:"hengenlab"  file_count:5365  version:3  compact_revision:3  modified_at_second:1627843735 
          volume id:411777  size:21046474416  collection:"hengenlab"  file_count:8289  delete_count:2487  deleted_byte_count:5924696211  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411796  size:20993425872  collection:"hengenlab"  file_count:8135  delete_count:1502  deleted_byte_count:3870962435  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411858  size:21046206000  collection:"hengenlab"  file_count:8220  delete_count:2471  deleted_byte_count:6063418663  version:3  compact_revision:1  modified_at_second:1627571069 
          volume id:411928  size:9095454264  collection:"hengenlab"  file_count:3558  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627843703 
          volume id:411982  size:10421317104  collection:"hengenlab"  file_count:3989  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627843728 
          volume id:411984  size:10009814624  collection:"hengenlab"  file_count:3842  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:2  modified_at_second:1627843737 
          volume id:412008  size:11965793208  collection:"hengenlab"  file_count:4456  delete_count:52  deleted_byte_count:117441604  version:3  compact_revision:2  modified_at_second:1627843740 
          volume id:412030  size:9624310920  collection:"hengenlab"  file_count:3749  delete_count:6  deleted_byte_count:22020222  version:3  compact_revision:1  modified_at_second:1627843722 
        Disk hdd total size:158219985552 file_count:61251 deleted_file:8167 deleted_bytes:20155482115 
      DataNode 10.244.80.146:8080 total size:158219985552 file_count:61251 deleted_file:8167 deleted_bytes:20155482115 
      DataNode 10.244.80.147:8080 hdd(volume:16/90 active:18446744073709551601 free:74 remote:0)
        Disk hdd(volume:16/90 active:18446744073709551601 free:74 remote:0)
          volume id:411665  size:9234805288  collection:"hengenlab"  file_count:3527  delete_count:3  deleted_byte_count:3145791  version:3  compact_revision:1  modified_at_second:1627843728 
          volume id:411703  size:20979965400  collection:"hengenlab"  file_count:8104  delete_count:104  deleted_byte_count:195001608  version:3  compact_revision:2  modified_at_second:1627703324 
          volume id:411704  size:21002179704  collection:"hengenlab"  file_count:8067  delete_count:1578  deleted_byte_count:4010373136  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411710  size:9437360  collection:"hengenlab"  file_count:3  version:3  compact_revision:1  modified_at_second:1627523993 
          volume id:411759  size:20996758312  collection:"hengenlab"  file_count:8123  delete_count:1537  deleted_byte_count:4018732086  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411775  size:21001393496  collection:"hengenlab"  file_count:8112  delete_count:1564  deleted_byte_count:3918816453  version:3  compact_revision:1  modified_at_second:1627571069 
          volume id:411778  size:12775626152  collection:"hengenlab"  file_count:5044  delete_count:152  deleted_byte_count:303006707  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411843  size:20980953512  collection:"hengenlab"  file_count:8010  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:2  modified_at_second:1627700684 
          volume id:411869  size:20977725816  collection:"hengenlab"  file_count:7982  delete_count:1468  deleted_byte_count:3804294082  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411901  size:21004990728  collection:"hengenlab"  file_count:8077  delete_count:1658  deleted_byte_count:4312970222  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411902  size:20971945640  collection:"hengenlab"  file_count:8017  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627703277 
          volume id:411933  size:8098190912  collection:"hengenlab"  file_count:3255  version:3  compact_revision:1  modified_at_second:1627617095 
          volume id:411941  size:19573411416  collection:"hengenlab"  file_count:7597  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843731 
          volume id:411947  size:19830507872  collection:"hengenlab"  file_count:7612  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843738 
          volume id:411965  size:11001156144  collection:"hengenlab"  file_count:4207  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:2  modified_at_second:1627843736 
          volume id:411990  size:10037202472  collection:"hengenlab"  file_count:3877  delete_count:14  deleted_byte_count:36700454  version:3  compact_revision:2  modified_at_second:1627843737 
        Disk hdd total size:258476250224 file_count:99614 deleted_file:8088 deleted_bytes:20632400877 
      DataNode 10.244.80.147:8080 total size:258476250224 file_count:99614 deleted_file:8088 deleted_bytes:20632400877 
      DataNode 10.244.80.149:8080 hdd(volume:13/89 active:18446744073709551602 free:76 remote:0)
        Disk hdd(volume:13/89 active:18446744073709551602 free:76 remote:0)
          volume id:411679  size:12243575888  collection:"hengenlab"  file_count:4729  version:3  compact_revision:2  modified_at_second:1627609576 
          volume id:411698  size:21007978464  collection:"hengenlab"  file_count:8153  delete_count:1602  deleted_byte_count:3988154444  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411701  size:11149355920  collection:"hengenlab"  file_count:4403  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:4  modified_at_second:1627843731 
          volume id:411723  size:20998589440  collection:"hengenlab"  file_count:8033  delete_count:1442  deleted_byte_count:3716214068  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411747  size:17553131856  collection:"hengenlab"  file_count:6786  delete_count:568  deleted_byte_count:1366271174  version:3  compact_revision:2  modified_at_second:1627843702 
          volume id:411797  size:20983185336  collection:"hengenlab"  file_count:8029  delete_count:249  deleted_byte_count:572789818  version:3  compact_revision:2  modified_at_second:1627703704 
          volume id:411824  size:5243000  collection:"hengenlab"  file_count:2  version:3  compact_revision:1  modified_at_second:1627524780 
          volume id:411844  size:20972143216  collection:"hengenlab"  file_count:8159  delete_count:103  deleted_byte_count:178069635  version:3  compact_revision:2  modified_at_second:1627702135 
          volume id:411856  size:14680352  collection:"hengenlab"  file_count:5  version:3  compact_revision:1  modified_at_second:1627524554 
          volume id:411876  size:15729016  collection:"hengenlab"  file_count:6  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411938  size:19410846296  collection:"hengenlab"  file_count:7506  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:1  modified_at_second:1627843746 
          volume id:411955  size:18257393928  collection:"hengenlab"  file_count:7002  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:1  modified_at_second:1627843738 
          volume id:412021  size:9340989152  collection:"hengenlab"  file_count:3602  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627843737 
        Disk hdd total size:171952841864 file_count:66415 deleted_file:3971 deleted_bytes:9838276502 
      DataNode 10.244.80.149:8080 total size:171952841864 file_count:66415 deleted_file:3971 deleted_bytes:9838276502 
      DataNode 10.244.80.154:8080 hdd(volume:18/86 active:18446744073709551608 free:68 remote:0)
        Disk hdd(volume:18/86 active:18446744073709551608 free:68 remote:0)
          volume id:411685  size:11497499296  collection:"hengenlab"  file_count:4471  delete_count:232  deleted_byte_count:565885294  version:3  compact_revision:5  modified_at_second:1627843741 
          volume id:411727  size:21009339472  collection:"hengenlab"  file_count:8110  delete_count:1372  deleted_byte_count:3442446742  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411761  size:526395800  collection:"hengenlab"  file_count:190  version:3  compact_revision:1  modified_at_second:1627523320 
          volume id:411762  size:447751592  collection:"hengenlab"  file_count:172  version:3  compact_revision:1  modified_at_second:1627528127 
          volume id:411764  size:553660064  collection:"hengenlab"  file_count:213  version:3  compact_revision:1  modified_at_second:1627523303 
          volume id:411773  size:13331768832  collection:"hengenlab"  file_count:5154  delete_count:80  deleted_byte_count:135261209  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411790  size:20973104848  collection:"hengenlab"  file_count:8121  delete_count:61  deleted_byte_count:109090481  version:3  compact_revision:2  modified_at_second:1627701472 
          volume id:411845  size:10660662160  collection:"hengenlab"  file_count:4137  delete_count:2  deleted_byte_count:2097194  version:3  compact_revision:4  modified_at_second:1627843743 
          volume id:411859  size:387887048  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9443  delete_count:260  deleted_byte_count:41241326  version:3  compact_revision:1  modified_at_second:1627576152 
          volume id:411923  size:19872622400  collection:"hengenlab"  file_count:7565  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:2  modified_at_second:1627843737 
          volume id:411927  size:9718495064  collection:"hengenlab"  file_count:3691  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:2  modified_at_second:1627843739 
          volume id:411931  size:17603523536  collection:"hengenlab"  file_count:6851  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:1  modified_at_second:1627843724 
          volume id:411940  size:19854630176  collection:"hengenlab"  file_count:7645  delete_count:6  deleted_byte_count:15728766  version:3  compact_revision:1  modified_at_second:1627843724 
          volume id:411948  size:19574509752  collection:"hengenlab"  file_count:7522  delete_count:2  deleted_byte_count:2097194  version:3  compact_revision:1  modified_at_second:1627843690 
          volume id:411952  size:18109160600  collection:"hengenlab"  file_count:7019  delete_count:4  deleted_byte_count:4194388  version:3  compact_revision:1  modified_at_second:1627843737 
          volume id:411953  size:18466850312  collection:"hengenlab"  file_count:7138  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:1  modified_at_second:1627843717 
          volume id:412007  size:9615085776  collection:"hengenlab"  file_count:3780  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:3  modified_at_second:1627843727 
          volume id:412026  size:9292320696  collection:"hengenlab"  file_count:3635  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627843732 
        Disk hdd total size:221495267424 file_count:94857 deleted_file:2030 deleted_bytes:4354742985 
      DataNode 10.244.80.154:8080 total size:221495267424 file_count:94857 deleted_file:2030 deleted_bytes:4354742985 
      DataNode 10.244.80.156:8080 hdd(volume:10/92 active:18446744073709551604 free:82 remote:0)
        Disk hdd(volume:10/92 active:18446744073709551604 free:82 remote:0)
          volume id:411717  size:20997625840  collection:"hengenlab"  file_count:8202  delete_count:2565  deleted_byte_count:6264865093  version:3  compact_revision:1  modified_at_second:1627609261 
          volume id:411732  size:20991507016  collection:"hengenlab"  file_count:8199  delete_count:247  deleted_byte_count:545925375  version:3  compact_revision:2  modified_at_second:1627704861 
          volume id:411751  size:20212871392  collection:"hengenlab"  file_count:7862  delete_count:1412  deleted_byte_count:3283153607  version:3  compact_revision:3  modified_at_second:1627843654 
          volume id:411757  size:20990395952  collection:"hengenlab"  file_count:8057  delete_count:1422  deleted_byte_count:3698718144  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411765  size:9785726448  collection:"hengenlab"  file_count:3862  delete_count:663  deleted_byte_count:1629992501  version:3  compact_revision:2  modified_at_second:1627609260 
          volume id:411794  size:20976658824  collection:"hengenlab"  file_count:8166  delete_count:270  deleted_byte_count:585165817  version:3  compact_revision:2  modified_at_second:1627704628 
          volume id:411802  size:21042465576  collection:"hengenlab"  file_count:8226  delete_count:2425  deleted_byte_count:5780431329  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411883  size:9412521008  collection:"hengenlab"  file_count:3590  version:3  compact_revision:2  modified_at_second:1627843731 
          volume id:411956  size:18249829552  collection:"hengenlab"  file_count:7068  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:1  modified_at_second:1627843717 
          volume id:412002  size:10651729792  collection:"hengenlab"  file_count:4165  delete_count:47  deleted_byte_count:109052891  version:3  compact_revision:2  modified_at_second:1627843726 
        Disk hdd total size:173311331400 file_count:67397 deleted_file:9054 deleted_bytes:21903596276 
      DataNode 10.244.80.156:8080 total size:173311331400 file_count:67397 deleted_file:9054 deleted_bytes:21903596276 
      DataNode 10.244.80.159:8080 hdd(volume:11/90 active:18446744073709551602 free:79 remote:0)
        Disk hdd(volume:11/90 active:18446744073709551602 free:79 remote:0)
          volume id:411711  size:20998456432  collection:"hengenlab"  file_count:8257  delete_count:1816  deleted_byte_count:4499105022  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411722  size:20971941296  collection:"hengenlab"  file_count:8232  delete_count:520  deleted_byte_count:1055575931  version:3  compact_revision:3  modified_at_second:1627701826 
          volume id:411752  size:20988602232  collection:"hengenlab"  file_count:8206  delete_count:1696  deleted_byte_count:4160916433  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411753  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523703 
          volume id:411788  size:11274978584  collection:"hengenlab"  file_count:4428  version:3  compact_revision:4  modified_at_second:1627843737 
          volume id:411808  size:13631720  collection:"hengenlab"  file_count:4  version:3  compact_revision:1  modified_at_second:1627524409 
          volume id:411823  size:21031622392  collection:"hengenlab"  file_count:8205  delete_count:1724  deleted_byte_count:4309731628  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411832  size:16472150072  collection:"hengenlab"  file_count:6453  delete_count:661  deleted_byte_count:1619461643  version:3  compact_revision:2  modified_at_second:1627843737 
          volume id:411834  size:11871131760  collection:"hengenlab"  file_count:4592  delete_count:2  deleted_byte_count:2097194  version:3  compact_revision:5  modified_at_second:1627843724 
          volume id:411877  size:10679734288  collection:"hengenlab"  file_count:4199  delete_count:8  deleted_byte_count:24117416  version:3  compact_revision:4  modified_at_second:1627843739 
          volume id:411981  size:10156297120  collection:"hengenlab"  file_count:3939  delete_count:4  deleted_byte_count:7340116  version:3  compact_revision:2  modified_at_second:1627843742 
        Disk hdd total size:144458545904 file_count:56515 deleted_file:6431 deleted_bytes:15678345383 
      DataNode 10.244.80.159:8080 total size:144458545904 file_count:56515 deleted_file:6431 deleted_bytes:15678345383 
      DataNode 10.244.80.163:8080 hdd(volume:14/87 active:18446744073709551599 free:73 remote:0)
        Disk hdd(volume:14/87 active:18446744073709551599 free:73 remote:0)
          volume id:411669  size:9380712976  collection:"hengenlab"  file_count:3655  delete_count:4  deleted_byte_count:7340116  version:3  compact_revision:1  modified_at_second:1627843729 
          volume id:411671  size:20974522272  collection:"hengenlab"  file_count:8085  delete_count:4  deleted_byte_count:7340116  version:3  compact_revision:2  modified_at_second:1627705070 
          volume id:411674  size:8911531000  collection:"hengenlab"  file_count:3483  delete_count:5  deleted_byte_count:11534441  version:3  compact_revision:2  modified_at_second:1627843740 
          volume id:411692  size:20984704840  collection:"hengenlab"  file_count:8098  delete_count:129  deleted_byte_count:240002133  version:3  compact_revision:2  modified_at_second:1627703500 
          volume id:411728  size:4194368  collection:"hengenlab"  file_count:1  version:3  compact_revision:1  modified_at_second:1627527447 
          volume id:411779  size:20987091896  collection:"hengenlab"  file_count:8114  delete_count:1339  deleted_byte_count:3399261701  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411783  size:20988873016  collection:"hengenlab"  file_count:8215  delete_count:2331  deleted_byte_count:5603388342  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411785  size:22020608  collection:"hengenlab"  file_count:9  version:3  compact_revision:1  modified_at_second:1627528037 
          volume id:411789  size:8554822576  collection:"hengenlab"  file_count:3407  delete_count:494  deleted_byte_count:1140851375  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411799  size:2097272  collection:"hengenlab"  file_count:2  version:3  compact_revision:1  modified_at_second:1627526065 
          volume id:411854  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627523170 
          volume id:411909  size:12933339696  collection:"hengenlab"  file_count:5011  version:3  compact_revision:2  modified_at_second:1627610271 
          volume id:411926  size:9212697120  collection:"hengenlab"  file_count:3565  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:2  modified_at_second:1627843706 
          volume id:411985  size:9946696984  collection:"hengenlab"  file_count:3893  delete_count:7  deleted_byte_count:19923091  version:3  compact_revision:2  modified_at_second:1627843743 
        Disk hdd total size:142903304632 file_count:55538 deleted_file:4315 deleted_bytes:10438029965 
      DataNode 10.244.80.163:8080 total size:142903304632 file_count:55538 deleted_file:4315 deleted_bytes:10438029965 
      DataNode 10.244.80.167:8080 hdd(volume:9/88 active:18446744073709551609 free:79 remote:0)
        Disk hdd(volume:9/88 active:18446744073709551609 free:79 remote:0)
          volume id:411684  size:12251200536  collection:"hengenlab"  file_count:4821  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:4  modified_at_second:1627843737 
          volume id:411784  size:14100248744  collection:"hengenlab"  file_count:5363  delete_count:283  deleted_byte_count:612749267  version:3  compact_revision:2  modified_at_second:1627609259 
          volume id:411839  size:22020608  collection:"hengenlab"  file_count:9  version:3  compact_revision:1  modified_at_second:1627527081 
          volume id:411849  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627524849 
          volume id:411861  size:454823872  collection:"pvc-98352fb2-44e8-4c55-984c-b81bc55fc4bf"  file_count:9416  delete_count:261  deleted_byte_count:31921284  version:3  compact_revision:1  modified_at_second:1627576153 
          volume id:411904  size:9251995464  collection:"hengenlab"  file_count:3548  delete_count:5  deleted_byte_count:14680169  version:3  compact_revision:2  modified_at_second:1627843724 
          volume id:411979  size:10175624520  collection:"hengenlab"  file_count:3946  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:2  modified_at_second:1627843729 
          volume id:411993  size:10925752072  collection:"hengenlab"  file_count:4148  delete_count:10  deleted_byte_count:35651794  version:3  compact_revision:2  modified_at_second:1627843741 
          volume id:412014  size:10762886152  collection:"hengenlab"  file_count:4192  delete_count:153  deleted_byte_count:396364941  version:3  compact_revision:2  modified_at_second:1627843733 
        Disk hdd total size:67944551976 file_count:35443 deleted_file:717 deleted_bytes:1102901896 
      DataNode 10.244.80.167:8080 total size:67944551976 file_count:35443 deleted_file:717 deleted_bytes:1102901896 
      DataNode 10.244.80.169:8080 nvme(volume:95/95 active:82 free:0 remote:0)
        Disk nvme(volume:95/95 active:82 free:0 remote:0)
          volume id:33  size:561568  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:851  version:3  compact_revision:7  modified_at_second:1627672955  disk_type:"nvme" 
          volume id:40  size:592816  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:826  version:3  compact_revision:7  modified_at_second:1627672955  disk_type:"nvme" 
          volume id:43  size:617000  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:895  version:3  compact_revision:7  modified_at_second:1627672955  disk_type:"nvme" 
          volume id:66  size:610664  collection:"pvc-f7b4a7e3-32ed-4cbc-8fd4-0665a7c36118"  file_count:901  version:3  compact_revision:7  modified_at_second:1627672956  disk_type:"nvme" 
          volume id:141  size:1506022288  collection:"pvc-4d87da96-2523-4cca-9fea-25d115ee25ce"  file_count:2374  delete_count:1841  deleted_byte_count:388723784  version:3  modified_at_second:1627446024  disk_type:"nvme" 
          volume id:569  size:12583192  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:2  modified_at_second:1627507790  disk_type:"nvme" 
          volume id:595  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627672953  disk_type:"nvme" 
          volume id:631  size:4194408  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627515990  disk_type:"nvme" 
          volume id:692  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627516917  disk_type:"nvme" 
          volume id:741  size:8388792  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:2  version:3  compact_revision:1  modified_at_second:1627415160  disk_type:"nvme" 
          volume id:764  size:71303448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627414976  disk_type:"nvme" 
          volume id:783  size:243270432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:9  version:3  compact_revision:1  modified_at_second:1627415079  disk_type:"nvme" 
          volume id:789  size:138412448  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:5  version:3  compact_revision:2  modified_at_second:1627415170  disk_type:"nvme" 
          volume id:793  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:1  modified_at_second:1627415443  disk_type:"nvme" 
          volume id:842  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:1  modified_at_second:1627415269  disk_type:"nvme" 
          volume id:868  size:33554528  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:1  version:3  compact_revision:2  modified_at_second:1627415362  disk_type:"nvme" 
          volume id:890  size:71303432  collection:"pvc-d93e0b88-4dfb-4c5f-8b46-da97bba24e34"  file_count:3  version:3  compact_revision:3  modified_at_second:1627672952  disk_type:"nvme" 
          volume id:1077  size:20972998352  collection:"braingeneers-backups-swfs"  file_count:12147  delete_count:243  deleted_byte_count:396660056  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:1100  size:20974608920  collection:"braingeneers-backups-swfs"  file_count:12252  delete_count:198  deleted_byte_count:303236107  version:3  modified_at_second:1627607153  disk_type:"nvme" 
          volume id:133787  size:21301728024  collection:"hengenlab"  file_count:8526  delete_count:7008  deleted_byte_count:17425588517  version:3  modified_at_second:1627414007  disk_type:"nvme" 
          volume id:133819  size:21226668664  collection:"hengenlab"  file_count:8129  delete_count:8129  deleted_byte_count:21226124008  version:3  modified_at_second:1627414332  disk_type:"nvme" 
          volume id:133836  size:21244742400  collection:"hengenlab"  file_count:8072  version:3  compact_revision:1  modified_at_second:1627420253  disk_type:"nvme" 
          volume id:133841  size:21140030384  collection:"hengenlab"  file_count:8121  version:3  compact_revision:1  modified_at_second:1627420233  disk_type:"nvme" 
          volume id:246127  size:21101266560  collection:"hengenlab"  file_count:8038  version:3  compact_revision:1  modified_at_second:1627416392  disk_type:"nvme" 
          volume id:246135  size:21064817840  collection:"hengenlab"  file_count:8081  version:3  compact_revision:1  modified_at_second:1627416409  disk_type:"nvme" 
          volume id:246144  size:21013850264  collection:"hengenlab"  file_count:8635  delete_count:571  deleted_byte_count:1461299369  version:3  modified_at_second:1627414577  disk_type:"nvme" 
          volume id:246146  size:21054935360  collection:"hengenlab"  file_count:8719  delete_count:541  deleted_byte_count:1370414224  version:3  modified_at_second:1627414663  disk_type:"nvme" 
          volume id:246153  size:21183691960  collection:"hengenlab"  file_count:8007  version:3  modified_at_second:1627416939  disk_type:"nvme" 
          volume id:246172  size:21660385832  collection:"hengenlab"  file_count:8263  version:3  modified_at_second:1627417612  disk_type:"nvme" 
          volume id:246175  size:21159383464  collection:"hengenlab"  file_count:8041  version:3  modified_at_second:1627418084  disk_type:"nvme" 
          volume id:246176  size:21083567776  collection:"hengenlab"  file_count:8043  version:3  modified_at_second:1627418076  disk_type:"nvme" 
          volume id:246212  size:21414255232  collection:"hengenlab"  file_count:8102  version:3  modified_at_second:1627421166  disk_type:"nvme" 
          volume id:246233  size:21086043664  collection:"hengenlab"  file_count:7972  version:3  modified_at_second:1627422283  disk_type:"nvme" 
          volume id:246242  size:21198462832  collection:"hengenlab"  file_count:8083  version:3  modified_at_second:1627423046  disk_type:"nvme" 
          volume id:246243  size:21048018304  collection:"hengenlab"  file_count:8027  version:3  modified_at_second:1627423041  disk_type:"nvme" 
          volume id:246274  size:21422008664  collection:"hengenlab"  file_count:8150  version:3  modified_at_second:1627424647  disk_type:"nvme" 
          volume id:246296  size:21490999024  collection:"hengenlab"  file_count:8184  version:3  modified_at_second:1627425220  disk_type:"nvme" 
          volume id:246336  size:21351235904  collection:"hengenlab"  file_count:7410  delete_count:36  deleted_byte_count:150996609  version:3  modified_at_second:1627427287  disk_type:"nvme" 
          volume id:246343  size:21586547488  collection:"hengenlab"  file_count:7771  delete_count:194  deleted_byte_count:540020714  version:3  modified_at_second:1627427872  disk_type:"nvme" 
          volume id:246371  size:21197726272  collection:"hengenlab"  file_count:8666  delete_count:6  deleted_byte_count:25166061  version:3  modified_at_second:1627429075  disk_type:"nvme" 
          volume id:246381  size:21060855608  collection:"hengenlab"  file_count:8048  delete_count:4  deleted_byte_count:16777409  version:3  modified_at_second:1627429671  disk_type:"nvme" 
          volume id:246394  size:21418696808  collection:"hengenlab"  file_count:8170  version:3  modified_at_second:1627430517  disk_type:"nvme" 
          volume id:246420  size:21577098968  collection:"hengenlab"  file_count:8220  version:3  modified_at_second:1627431839  disk_type:"nvme" 
          volume id:246426  size:21286509368  collection:"hengenlab"  file_count:8124  delete_count:3  deleted_byte_count:6291519  version:3  modified_at_second:1627504948  disk_type:"nvme" 
          volume id:246452  size:21078835312  collection:"hengenlab"  file_count:8024  version:3  modified_at_second:1627433959  disk_type:"nvme" 
          volume id:246466  size:21127956848  collection:"hengenlab"  file_count:8086  version:3  modified_at_second:1627435449  disk_type:"nvme" 
          volume id:246469  size:21609542120  collection:"hengenlab"  file_count:8250  version:3  modified_at_second:1627435620  disk_type:"nvme" 
          volume id:246487  size:21287442424  collection:"hengenlab"  file_count:8111  version:3  modified_at_second:1627436797  disk_type:"nvme" 
          volume id:246496  size:21027864400  collection:"hengenlab"  file_count:8039  version:3  modified_at_second:1627437129  disk_type:"nvme" 
          volume id:246501  size:21129973720  collection:"hengenlab"  file_count:8052  delete_count:1  deleted_byte_count:70  version:3  modified_at_second:1627437461  disk_type:"nvme" 
          volume id:246535  size:11664276264  collection:"hengenlab"  file_count:4448  version:3  compact_revision:1  modified_at_second:1627672220  disk_type:"nvme" 
          volume id:246548  size:21269863608  collection:"hengenlab"  file_count:8644  delete_count:2931  deleted_byte_count:6309826926  version:3  modified_at_second:1627607477  disk_type:"nvme" 
          volume id:246605  size:21525654920  collection:"hengenlab"  file_count:8230  version:3  modified_at_second:1627440065  disk_type:"nvme" 
          volume id:246623  size:21661971032  collection:"hengenlab"  file_count:8252  version:3  modified_at_second:1627440566  disk_type:"nvme" 
          volume id:246626  size:21767879672  collection:"hengenlab"  file_count:8296  version:3  modified_at_second:1627440723  disk_type:"nvme" 
          volume id:246653  size:21162084032  collection:"hengenlab"  file_count:8033  version:3  modified_at_second:1627442174  disk_type:"nvme" 
          volume id:246664  size:21353108088  collection:"hengenlab"  file_count:8128  delete_count:41  deleted_byte_count:169115246  version:3  modified_at_second:1627521905  disk_type:"nvme" 
          volume id:246686  size:13168463584  collection:"hengenlab"  file_count:5027  version:3  compact_revision:1  modified_at_second:1627672794  disk_type:"nvme" 
          volume id:246745  size:14926452688  collection:"hengenlab"  file_count:5708  version:3  compact_revision:1  modified_at_second:1627671345  disk_type:"nvme" 
          volume id:246757  size:233525928  file_count:104  version:3  modified_at_second:1627448403  disk_type:"nvme" 
          volume id:246769  size:14123240184  collection:"hengenlab"  file_count:5398  version:3  compact_revision:1  modified_at_second:1627672734  disk_type:"nvme" 
          volume id:246796  size:21705124144  collection:"hengenlab"  file_count:8278  delete_count:2278  deleted_byte_count:5994511715  version:3  modified_at_second:1627521784  disk_type:"nvme" 
          volume id:246839  size:12223358896  collection:"hengenlab"  file_count:4672  version:3  compact_revision:1  modified_at_second:1627671863  disk_type:"nvme" 
          volume id:246942  size:2901945320  collection:"hengenlab"  file_count:1119  version:3  compact_revision:1  modified_at_second:1627672918  disk_type:"nvme" 
          volume id:246955  size:3110322144  collection:"hengenlab"  file_count:1190  version:3  compact_revision:1  modified_at_second:1627672330  disk_type:"nvme" 
          volume id:246989  size:2874322224  collection:"hengenlab"  file_count:1084  version:3  compact_revision:1  modified_at_second:1627671536  disk_type:"nvme" 
          volume id:246990  size:2839669520  collection:"hengenlab"  file_count:1095  version:3  compact_revision:1  modified_at_second:1627672139  disk_type:"nvme" 
          volume id:247029  size:2995960680  collection:"hengenlab"  file_count:1149  version:3  compact_revision:1  modified_at_second:1627672107  disk_type:"nvme" 
          volume id:247035  size:3044324616  collection:"hengenlab"  file_count:1161  version:3  compact_revision:1  modified_at_second:1627671489  disk_type:"nvme" 
          volume id:247060  size:4516542160  collection:"hengenlab"  file_count:1713  version:3  compact_revision:1  modified_at_second:1627671421  disk_type:"nvme" 
          volume id:247062  size:3386103288  collection:"hengenlab"  file_count:1291  version:3  compact_revision:1  modified_at_second:1627672464  disk_type:"nvme" 
          volume id:247065  size:2715064920  collection:"hengenlab"  file_count:1039  version:3  compact_revision:1  modified_at_second:1627671384  disk_type:"nvme" 
          volume id:247066  size:3114565864  collection:"hengenlab"  file_count:1174  version:3  compact_revision:1  modified_at_second:1627671272  disk_type:"nvme" 
          volume id:247068  size:2946626792  collection:"hengenlab"  file_count:1140  version:3  compact_revision:1  modified_at_second:1627671242  disk_type:"nvme" 
          volume id:247070  size:3063134704  collection:"hengenlab"  file_count:1163  version:3  compact_revision:1  modified_at_second:1627672630  disk_type:"nvme" 
          volume id:247076  size:2890986032  collection:"hengenlab"  file_count:1107  version:3  compact_revision:1  modified_at_second:1627671631  disk_type:"nvme" 
          volume id:247086  size:2627787296  collection:"hengenlab"  file_count:997  version:3  compact_revision:1  modified_at_second:1627672356  disk_type:"nvme" 
          volume id:247097  size:2613221424  collection:"hengenlab"  file_count:988  version:3  compact_revision:1  modified_at_second:1627672370  disk_type:"nvme" 
          volume id:247101  size:2876305016  collection:"hengenlab"  file_count:1090  version:3  compact_revision:1  modified_at_second:1627672039  disk_type:"nvme" 
          volume id:247114  size:2354168024  collection:"hengenlab"  file_count:902  version:3  compact_revision:1  modified_at_second:1627672062  disk_type:"nvme" 
          volume id:247115  size:2451737640  collection:"hengenlab"  file_count:933  version:3  compact_revision:1  modified_at_second:1627671474  disk_type:"nvme" 
          volume id:247117  size:2789630248  collection:"hengenlab"  file_count:1070  version:3  compact_revision:1  modified_at_second:1627672567  disk_type:"nvme" 
          volume id:247126  size:2820039224  collection:"hengenlab"  file_count:1075  version:3  compact_revision:1  modified_at_second:1627671773  disk_type:"nvme" 
          volume id:247133  size:2842750640  collection:"hengenlab"  file_count:1091  version:3  compact_revision:1  modified_at_second:1627671713  disk_type:"nvme" 
          volume id:247135  size:2751521840  collection:"hengenlab"  file_count:1043  version:3  compact_revision:1  modified_at_second:1627671688  disk_type:"nvme" 
          volume id:247138  size:2870193144  collection:"hengenlab"  file_count:1099  version:3  compact_revision:1  modified_at_second:1627672405  disk_type:"nvme" 
          volume id:247148  size:2617301984  collection:"hengenlab"  file_count:1005  version:3  compact_revision:1  modified_at_second:1627672663  disk_type:"nvme" 
          volume id:247150  size:2775998200  collection:"hengenlab"  file_count:1060  version:3  compact_revision:1  modified_at_second:1627672010  disk_type:"nvme" 
          volume id:247158  size:2585958672  collection:"hengenlab"  file_count:992  version:3  compact_revision:1  modified_at_second:1627672582  disk_type:"nvme" 
          volume id:247159  size:2615447568  collection:"hengenlab"  file_count:992  version:3  compact_revision:1  modified_at_second:1627672828  disk_type:"nvme" 
          volume id:247168  size:2231596584  collection:"hengenlab"  file_count:853  version:3  compact_revision:1  modified_at_second:1627672165  disk_type:"nvme" 
          volume id:247170  size:2424359408  collection:"hengenlab"  file_count:923  version:3  compact_revision:1  modified_at_second:1627671876  disk_type:"nvme" 
          volume id:247183  size:2262005904  collection:"hengenlab"  file_count:864  version:3  compact_revision:1  modified_at_second:1627672072  disk_type:"nvme" 
          volume id:247211  size:1684227816  collection:"hengenlab"  file_count:637  version:3  compact_revision:2  modified_at_second:1627671981  disk_type:"nvme" 
          volume id:247228  size:293786808  collection:"hengenlab"  file_count:115  version:3  compact_revision:2  modified_at_second:1627672090  disk_type:"nvme" 
        Disk nvme total size:1006464213680 file_count:398816 deleted_file:24025 deleted_bytes:55784752334 
      DataNode 10.244.80.169:8080 total size:1006464213680 file_count:398816 deleted_file:24025 deleted_bytes:55784752334 
      DataNode 10.244.80.171:8080 hdd(volume:16/87 active:18446744073709551600 free:71 remote:0)
        Disk hdd(volume:16/87 active:18446744073709551600 free:71 remote:0)
          volume id:411664  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627610818 
          volume id:411696  size:4194368  collection:"hengenlab"  file_count:1  version:3  compact_revision:1  modified_at_second:1627523809 
          volume id:411706  size:7340264  collection:"hengenlab"  file_count:4  version:3  compact_revision:1  modified_at_second:1627526515 
          volume id:411716  size:21000296976  collection:"hengenlab"  file_count:7985  delete_count:1370  deleted_byte_count:3528378174  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411724  size:21010819184  collection:"hengenlab"  file_count:8067  delete_count:1483  deleted_byte_count:3767887994  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411729  size:20979764560  collection:"hengenlab"  file_count:8138  delete_count:96  deleted_byte_count:198275837  version:3  compact_revision:2  modified_at_second:1627703346 
          volume id:411739  size:12749517664  collection:"hengenlab"  file_count:4940  delete_count:115  deleted_byte_count:299895151  version:3  compact_revision:5  modified_at_second:1627843697 
          volume id:411755  size:18084502720  collection:"hengenlab"  file_count:7125  delete_count:422  deleted_byte_count:938718302  version:3  compact_revision:2  modified_at_second:1627843726 
          volume id:411767  size:446702960  collection:"hengenlab"  file_count:171  version:3  compact_revision:1  modified_at_second:1627523183 
          volume id:411891  size:21026997376  collection:"hengenlab"  file_count:8220  delete_count:1701  deleted_byte_count:4238119698  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411894  size:18091922800  collection:"hengenlab"  file_count:7112  delete_count:637  deleted_byte_count:1508635198  version:3  compact_revision:2  modified_at_second:1627843713 
          volume id:411911  size:20978548352  collection:"hengenlab"  file_count:8159  delete_count:954  deleted_byte_count:2385829477  version:3  compact_revision:2  modified_at_second:1627702924 
          volume id:411950  size:18275154328  collection:"hengenlab"  file_count:7069  version:3  compact_revision:1  modified_at_second:1627843717 
          volume id:411967  size:10834082264  collection:"hengenlab"  file_count:4109  delete_count:3  deleted_byte_count:6291519  version:3  compact_revision:2  modified_at_second:1627843721 
          volume id:411980  size:10315093016  collection:"hengenlab"  file_count:3929  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627843689 
          volume id:412015  size:11693275568  collection:"hengenlab"  file_count:4421  delete_count:166  deleted_byte_count:416288158  version:3  compact_revision:2  modified_at_second:1627843708 
        Disk hdd total size:205498212408 file_count:79450 deleted_file:6950 deleted_bytes:17297756755 
      DataNode 10.244.80.171:8080 total size:205498212408 file_count:79450 deleted_file:6950 deleted_bytes:17297756755 
      DataNode 10.244.80.173:8080 hdd(volume:10/90 active:18446744073709551606 free:80 remote:0)
        Disk hdd(volume:10/90 active:18446744073709551606 free:80 remote:0)
          volume id:411677  size:9413657016  collection:"hengenlab"  file_count:3666  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627843735 
          volume id:411681  size:16491608416  collection:"hengenlab"  file_count:6383  delete_count:4  deleted_byte_count:4194388  version:3  compact_revision:2  modified_at_second:1627843744 
          volume id:411770  size:351280976  collection:"hengenlab"  file_count:143  version:3  compact_revision:1  modified_at_second:1627524409 
          volume id:411804  size:20983729728  collection:"hengenlab"  file_count:8192  delete_count:1837  deleted_byte_count:4536925211  version:3  compact_revision:1  modified_at_second:1627571068 
          volume id:411872  size:11827184528  collection:"hengenlab"  file_count:4564  delete_count:1  deleted_byte_count:4194325  version:3  compact_revision:3  modified_at_second:1627843711 
          volume id:411900  size:12062763200  collection:"hengenlab"  file_count:4738  delete_count:195  deleted_byte_count:493883391  version:3  compact_revision:4  modified_at_second:1627843709 
          volume id:411919  size:9444792736  collection:"hengenlab"  file_count:3652  delete_count:7  deleted_byte_count:23068819  version:3  compact_revision:2  modified_at_second:1627843730 
          volume id:411958  size:20976107664  collection:"hengenlab"  file_count:8056  delete_count:4  deleted_byte_count:13631572  version:3  compact_revision:1  modified_at_second:1627703962 
          volume id:412003  size:10447815176  collection:"hengenlab"  file_count:4044  delete_count:35  deleted_byte_count:87032543  version:3  compact_revision:2  modified_at_second:1627843720 
          volume id:412006  size:11528445368  collection:"hengenlab"  file_count:4317  delete_count:71  deleted_byte_count:178259411  version:3  compact_revision:2  modified_at_second:1627843723 
        Disk hdd total size:123527384808 file_count:47755 deleted_file:2157 deleted_bytes:5350626907 
      DataNode 10.244.80.173:8080 total size:123527384808 file_count:47755 deleted_file:2157 deleted_bytes:5350626907 
      DataNode 10.244.80.182:8080 hdd(volume:23/81 active:18446744073709551597 free:58 remote:0)
        Disk hdd(volume:23/81 active:18446744073709551597 free:58 remote:0)
          volume id:133822  size:9348388960  collection:"hengenlab"  file_count:3621  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:2  modified_at_second:1627843731 
          volume id:133830  size:8  collection:"hengenlab"  version:3  compact_revision:2  modified_at_second:1627618126 
          volume id:133850  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627506991 
          volume id:133966  size:9472294744  collection:"hengenlab"  file_count:3634  delete_count:6  deleted_byte_count:22020222  version:3  compact_revision:2  modified_at_second:1627843677 
          volume id:134022  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627506992 
          volume id:411676  size:20995380656  collection:"hengenlab"  file_count:8083  delete_count:1628  deleted_byte_count:4232086349  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411741  size:20978338976  collection:"hengenlab"  file_count:8026  delete_count:81  deleted_byte_count:161965983  version:3  compact_revision:2  modified_at_second:1627702345 
          volume id:411787  size:21004437928  collection:"hengenlab"  file_count:8063  delete_count:1635  deleted_byte_count:4174116078  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411805  size:21010830360  collection:"hengenlab"  file_count:8058  delete_count:1452  deleted_byte_count:3663707621  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411806  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627526064 
          volume id:411816  size:7972844136  collection:"hengenlab"  file_count:3173  delete_count:535  deleted_byte_count:1274718168  version:3  compact_revision:2  modified_at_second:1627571096 
          volume id:411820  size:11103604584  collection:"hengenlab"  file_count:4416  delete_count:2  deleted_byte_count:5242922  version:3  compact_revision:3  modified_at_second:1627843718 
          volume id:411840  size:8  collection:"hengenlab"  version:3  compact_revision:1  modified_at_second:1627522290 
          volume id:411882  size:9476786448  collection:"hengenlab"  file_count:3569  delete_count:1  deleted_byte_count:1048597  version:3  compact_revision:2  modified_at_second:1627843743 
          volume id:411884  size:20975768288  collection:"hengenlab"  file_count:8098  delete_count:189  deleted_byte_count:394743945  version:3  compact_revision:2  modified_at_second:1627703989 
          volume id:411885  size:10611745152  collection:"hengenlab"  file_count:4129  delete_count:34  deleted_byte_count:95421130  version:3  compact_revision:4  modified_at_second:1627843724 
          volume id:411907  size:20994844640  collection:"hengenlab"  file_count:8070  delete_count:1605  deleted_byte_count:4107716304  version:3  compact_revision:1  modified_at_second:1627609259 
          volume id:411916  size:11843699336  collection:"hengenlab"  file_count:4592  delete_count:2  deleted_byte_count:8388650  version:3  compact_revision:3  modified_at_second:1627843732 
          volume id:411922  size:19872656216  collection:"hengenlab"  file_count:7602  delete_count:6  deleted_byte_count:18874494  version:3  compact_revision:2  modified_at_second:1627843738 
          volume id:411986  size:10450097872  collection:"hengenlab"  file_count:3984  delete_count:12  deleted_byte_count:31457532  version:3  compact_revision:2  modified_at_second:1627843739 
          volume id:411995  size:10589403672  collection:"hengenlab"  file_count:4040  delete_count:7  deleted_byte_count:19923091  version:3  compact_revision:2  modified_at_second:1627843694 
          volume id:412020  size:9992180880  collection:"hengenlab"  file_count:3848  delete_count:7  deleted_byte_count:19923091  version:3  compact_revision:2  modified_at_second:1627843728 
          volume id:412023  size:11391321408  collection:"hengenlab"  file_count:4442  delete_count:191  deleted_byte_count:399495827  version:3  compact_revision:1  modified_at_second:1627843730 
        Disk hdd total size:258084624296 file_count:99448 deleted_file:7395 deleted_bytes:18639238654 
      DataNode 10.244.80.182:8080 total size:258084624296 file_count:99448 deleted_file:7395 deleted_bytes:18639238654 
      DataNode 10.244.80.187:8080 hdd(volume:9/91 active:18446744073709551608 free:82 remote:0)
        Disk hdd(volume:9/91 active:18446744073709551608 free:82 remote:0)
          volume id:133842  size:8  collection:"hengenlab"  read_only:true  version:3  compact_revision:1  modified_at_second:1627506990 
          volume id:411712  size:20998686088  collection:"hengenlab"  file_count:8257  delete_count:1802  deleted_byte_count:4297127481  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411740  size:20987167216  collection:"hengenlab"  file_count:8054  delete_count:1417  deleted_byte_count:3594015227  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411743  size:10176141608  collection:"hengenlab"  file_count:3972  delete_count:4  deleted_byte_count:10485844  version:3  compact_revision:4  modified_at_second:1627843724 
          volume id:411763  size:12703548240  collection:"hengenlab"  file_count:4968  delete_count:3  deleted_byte_count:12582975  version:3  compact_revision:3  modified_at_second:1627843743 
          volume id:411848  size:11875047160  collection:"hengenlab"  file_count:4644  delete_count:4  deleted_byte_count:7340116  version:3  compact_revision:4  modified_at_second:1627843716 
          volume id:411897  size:9159541464  collection:"hengenlab"  file_count:3605  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:2  modified_at_second:1627843743 
          volume id:411961  size:13624062208  collection:"hengenlab"  file_count:5228  version:3  compact_revision:1  modified_at_second:1627613659 
          volume id:411992  size:10680394344  collection:"hengenlab"  file_count:4112  delete_count:8  deleted_byte_count:14680232  version:3  compact_revision:2  modified_at_second:1627843740 
        Disk hdd total size:110204588336 file_count:42840 deleted_file:3241 deleted_bytes:7945669122 
      DataNode 10.244.80.187:8080 total size:110204588336 file_count:42840 deleted_file:3241 deleted_bytes:7945669122 
      DataNode 10.244.80.188:8080 hdd(volume:22/89 active:20 free:67 remote:0)
        Disk hdd(volume:22/89 active:20 free:67 remote:0)
          volume id:133787  size:20977333768  collection:"hengenlab"  file_count:8145  delete_count:373  deleted_byte_count:852883566  version:3  compact_revision:3  modified_at_second:1627704822 
          volume id:133791  size:12894894240  collection:"hengenlab"  file_count:4990  version:3  compact_revision:4  modified_at_second:1627843739 
          volume id:133797  size:20976269128  collection:"hengenlab"  file_count:8074  delete_count:1365  deleted_byte_count:3624138703  version:3  compact_revision:2  modified_at_second:1627571055 
          volume id:133890  size:20973749832  collection:"hengenlab"  file_count:8105  delete_count:87  deleted_byte_count:158198416  version:3  compact_revision:3  modified_at_second:1627704768 
          volume id:133911  size:7193833056  collection:"hengenlab"  file_count:2839  version:3  compact_revision:2  modified_at_second:1627843714 
          volume id:133949  size:7705370032  collection:"hengenlab"  file_count:2945  version:3  compact_revision:2  modified_at_second:1627843735 
          volume id:133980  size:12177944664  collection:"hengenlab"  file_count:4754  version:3  compact_revision:4  modified_at_second:1627843741 
          volume id:134024  size:20987628488  collection:"hengenlab"  file_count:8147  delete_count:1533  deleted_byte_count:3904661820  version:3  compact_revision:2  modified_at_second:1627571060 
          volume id:246130  size:21628018400  collection:"hengenlab"  file_count:8140  delete_count:282  deleted_byte_count:761272098  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:246152  size:21460301248  collection:"hengenlab"  file_count:8216  delete_count:91  deleted_byte_count:236980087  version:3  modified_at_second:1627521766 
          volume id:246218  size:22123069400  collection:"hengenlab"  file_count:8438  delete_count:259  deleted_byte_count:689149263  version:3  modified_at_second:1627521766 
          volume id:246234  size:21548861680  collection:"hengenlab"  file_count:8177  delete_count:184  deleted_byte_count:504368920  version:3  modified_at_second:1627521636 
          volume id:246235  size:21563474080  collection:"hengenlab"  file_count:8243  delete_count:166  deleted_byte_count:400559518  version:3  modified_at_second:1627521732 
          volume id:246238  size:21939256008  collection:"hengenlab"  file_count:8327  delete_count:305  deleted_byte_count:819992837  version:3  modified_at_second:1627521732 
          volume id:246244  size:21177236216  collection:"hengenlab"  file_count:8091  delete_count:73  deleted_byte_count:164635721  version:3  modified_at_second:1627521821 
          volume id:246264  size:21709311176  collection:"hengenlab"  file_count:8377  delete_count:137  deleted_byte_count:344984381  version:3  modified_at_second:1627521766 
          volume id:246270  size:22130167328  collection:"hengenlab"  file_count:8420  delete_count:333  deleted_byte_count:868227921  version:3  modified_at_second:1627521633 
          volume id:411756  size:20991502016  collection:"hengenlab"  file_count:8230  delete_count:2438  deleted_byte_count:5914063320  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411855  size:7178928632  collection:"hengenlab"  file_count:2749  version:3  compact_revision:1  modified_at_second:1627843726 
          volume id:411867  size:7349929896  collection:"hengenlab"  file_count:2873  version:3  compact_revision:1  modified_at_second:1627843701 
          volume id:411968  size:10013989648  collection:"hengenlab"  file_count:3769  version:3  compact_revision:2  modified_at_second:1627843711 
          volume id:412018  size:9021709232  collection:"hengenlab"  file_count:3500  version:3  compact_revision:3  modified_at_second:1627843738 
        Disk hdd total size:373722778168 file_count:143549 deleted_file:7626 deleted_bytes:19244116571 
      DataNode 10.244.80.188:8080 total size:373722778168 file_count:143549 deleted_file:7626 deleted_bytes:19244116571 
      DataNode 10.244.80.190:8080 hdd(volume:16/85 active:18446744073709551613 free:69 remote:0)
        Disk hdd(volume:16/85 active:18446744073709551613 free:69 remote:0)
          volume id:411656  size:312783808  file_count:853  delete_count:21  deleted_byte_count:1104358  version:3  modified_at_second:1627843685 
          volume id:411657  size:406532592  file_count:915  delete_count:33  deleted_byte_count:1879726  version:3  modified_at_second:1627843741 
          volume id:411658  size:391617112  file_count:935  delete_count:19  deleted_byte_count:1293103  version:3  modified_at_second:1627843686 
          volume id:411659  size:378765040  file_count:891  delete_count:17  deleted_byte_count:1501286  version:3  modified_at_second:1627843688 
          volume id:411660  size:381905776  file_count:919  delete_count:32  deleted_byte_count:6239636  version:3  modified_at_second:1627843740 
          volume id:411661  size:374743672  file_count:865  delete_count:19  deleted_byte_count:853319  version:3  modified_at_second:1627843741 
          volume id:411662  size:453987360  file_count:933  delete_count:14  deleted_byte_count:807287  version:3  modified_at_second:1627843687 
          volume id:411678  size:13310756544  collection:"hengenlab"  file_count:5141  delete_count:7  deleted_byte_count:16777363  version:3  compact_revision:3  modified_at_second:1627843723 
          volume id:411688  size:11320049832  collection:"hengenlab"  file_count:4445  delete_count:3  deleted_byte_count:9437247  version:3  compact_revision:4  modified_at_second:1627843734 
          volume id:411715  size:20979163616  collection:"hengenlab"  file_count:8105  delete_count:128  deleted_byte_count:229514021  version:3  compact_revision:2  modified_at_second:1627703154 
          volume id:411769  size:21020447216  collection:"hengenlab"  file_count:8257  delete_count:2450  deleted_byte_count:5980737503  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411780  size:21027400496  collection:"hengenlab"  file_count:8122  delete_count:2251  deleted_byte_count:5533972074  version:3  compact_revision:1  modified_at_second:1627571060 
          volume id:411781  size:21037408832  collection:"hengenlab"  file_count:8108  delete_count:1304  deleted_byte_count:3224044243  version:3  compact_revision:1  modified_at_second:1627607477 
          volume id:411792  size:21012031144  collection:"hengenlab"  file_count:8191  delete_count:1701  deleted_byte_count:4217474712  version:3  compact_revision:1  modified_at_second:1627571055 
          volume id:411819  size:13154203544  collection:"hengenlab"  file_count:5103  delete_count:211  deleted_byte_count:478778819  version:3  compact_revision:2  modified_at_second:1627609260 
          volume id:411999  size:10404127864  collection:"hengenlab"  file_count:4016  delete_count:79  deleted_byte_count:217723915  version:3  compact_revision:2  modified_at_second:1627843731 
        Disk hdd total size:155965924448 file_count:65799 deleted_file:8289 deleted_bytes:19922138612 
      DataNode 10.244.80.190:8080 total size:155965924448 file_count:65799 deleted_file:8289 deleted_bytes:19922138612 
    Rack DefaultRack total size:14536458701800 file_count:6344037 deleted_file:789918 deleted_bytes:1752599154873 
  DataCenter DefaultDataCenter total size:14536458701800 file_count:6344037 deleted_file:789918 deleted_bytes:1752599154873 
total size:14536458701800 file_count:6344037 deleted_file:789918 deleted_bytes:1752599154873
`
