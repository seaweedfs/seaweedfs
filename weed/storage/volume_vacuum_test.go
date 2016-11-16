package storage

import (
	"testing"
)

/*
makediff test steps
1. launch weed server at your local/dev environment, (option
"garbageThreshold" for master and option "max" for volume should be set with specific value which would let
preparing test prerequisite easier )
   a) ./weed master -garbageThreshold=0.99 -mdir=./m
   b) ./weed volume -dir=./data -max=1 -mserver=localhost:9333 -port=8080
2. upload 4 different files, you could call dir/assign to get 4 different fids
   a)  upload file A with fid a
   b)  upload file B with fid b
   c)  upload file C with fid c
   d)  upload file D with fid d
3. update file A and C
   a)  modify file A and upload file A with fid a
   b)  modify file C and upload file C with fid c
   c)  record the current 1.idx's file size(lastCompactIndexOffset value)
4. Compacting the data file
   a)  run curl http://localhost:8080/admin/vacuum/compact?volumeId=1
   b)  verify the 1.cpd and 1.cpx is created under volume directory
5. update file B and delete file D
   a)  modify file B and upload file B with fid b
   d)  delete file B with fid b
6. Now you could run the following UT case, the case should be run successfully
7. Compact commit manually
   a)  mv 1.cpd 1.dat
   b)  mv 1.cpx 1.idx
8. Restart Volume Server
9. Now you should get updated file A,B,C
*/

func TestMakeDiff(t *testing.T) {

	v := new(Volume)
	//lastCompactIndexOffset value is the index file size before step 4
	v.lastCompactIndexOffset = 96
	v.SuperBlock.version = 0x2
	/*
		err := v.makeupDiff(
			"/yourpath/1.cpd",
			"/yourpath/1.cpx",
			"/yourpath/1.dat",
			"/yourpath/1.idx")
		if err != nil {
			t.Errorf("makeupDiff err is %v", err)
		} else {
			t.Log("makeupDiff Succeeded")
		}
	*/
}
