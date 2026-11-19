package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newTailTestStore returns a store holding one empty volume vid.
func newTailTestStore(t *testing.T, vid needle.VolumeId) *storage.Store {
	t.Helper()
	store := newVolumeCopyTestStore(t, t.TempDir())
	if err := store.AddVolume(vid, "", storage.NeedleMapInMemory, "000", "", 0,
		needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume %d: %v", vid, err)
	}
	return store
}

// serveTailSource serves the store over in-process gRPC with the real
// VolumeTailSender and returns its address.
func serveTailSource(t *testing.T, store *storage.Store) pb.ServerAddress {
	t.Helper()
	port := serveGrpc(t, func(s *grpc.Server) {
		volume_server_pb.RegisterVolumeServerServer(s, &VolumeServer{store: store})
	})
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:%d.%d", port-10000, port))
}

// runTailReceiver has target follow source's volume vid from sinceNs through
// the real VolumeTailReceiver, until the source is idle for a second.
func runTailReceiver(t *testing.T, target *storage.Store, source pb.ServerAddress, vid needle.VolumeId, sinceNs uint64) error {
	t.Helper()
	vs := &VolumeServer{
		store:                         target,
		grpcDialOption:                grpc.WithTransportCredentials(insecure.NewCredentials()),
		AllowUntrustedRemoteEndpoints: true,
	}
	_, err := vs.VolumeTailReceiver(context.Background(), &volume_server_pb.VolumeTailReceiverRequest{
		VolumeId:           uint32(vid),
		SinceNs:            sinceNs,
		IdleTimeoutSeconds: 1,
		SourceVolumeServer: string(source),
	})
	return err
}

func writeTailTestNeedle(t *testing.T, store *storage.Store, vid needle.VolumeId, k uint64, cookie types.Cookie, data []byte) *needle.Needle {
	t.Helper()
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(k)
	n.Cookie = cookie
	n.Data = data
	n.Checksum = needle.NewCRC(data)
	if _, err := store.WriteVolumeNeedle(vid, n, true, false); err != nil {
		t.Fatalf("write %d: %v", k, err)
	}
	return n
}

func deleteTailTestNeedle(t *testing.T, store *storage.Store, vid needle.VolumeId, k uint64, cookie types.Cookie) {
	t.Helper()
	n := new(needle.Needle)
	n.Id = types.Uint64ToNeedleId(k)
	n.Cookie = cookie
	if _, err := store.DeleteVolumeNeedle(vid, n, false); err != nil {
		t.Fatalf("delete %d: %v", k, err)
	}
}

// damageRecordOfRow flips one byte in the data of the record index row `row`
// of v names; header and trailer stay intact. It returns the row's key.
func damageRecordOfRow(t *testing.T, v *storage.Volume, row int64) types.NeedleId {
	t.Helper()
	v.SyncToDisk()
	idxBytes, err := os.ReadFile(v.FileName(".idx"))
	if err != nil {
		t.Fatal(err)
	}
	key, offset, size := idx.IdxFileEntry(idxBytes[row*types.NeedleMapEntrySize : (row+1)*types.NeedleMapEntrySize])
	if offset.IsZero() || size <= 0 || size.IsDeleted() {
		t.Fatalf("index row %d (key %d, size %d) is not a write row", row, key, size)
	}
	f, err := os.OpenFile(v.FileName(".dat"), os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dataSize := make([]byte, 4)
	if _, err := f.ReadAt(dataSize, offset.ToActualOffset()+types.NeedleHeaderSize); err != nil {
		t.Fatal(err)
	}
	at := offset.ToActualOffset() + types.NeedleHeaderSize + 4 + int64(util.BytesToUint32(dataSize))/2
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, at); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, at); err != nil {
		t.Fatal(err)
	}
	return key
}

// buildDamagedTailSource writes keys 1..30, deletes deleteFrom..deleteFrom+9,
// writes keys 31..34, then damages the data of key 23, whose write row (22 of
// 44) is the first row every append-time search probes. It returns the store,
// the live data and the append time of key 30's write.
func buildDamagedTailSource(t *testing.T, vid needle.VolumeId, deleteFrom uint64) (*storage.Store, map[uint64][]byte, uint64) {
	t.Helper()
	store := newTailTestStore(t, vid)
	rnd := rand.New(rand.NewSource(5))
	live := map[uint64][]byte{}
	cookies := map[uint64]types.Cookie{}
	var key30AppendAtNs uint64
	write := func(k uint64) {
		cookies[k] = types.Cookie(rnd.Uint32())
		data := make([]byte, 256+rnd.Intn(512))
		rnd.Read(data)
		n := writeTailTestNeedle(t, store, vid, k, cookies[k], data)
		live[k] = data
		if k == 30 {
			key30AppendAtNs = n.AppendAtNs
		}
	}
	for k := uint64(1); k <= 30; k++ {
		write(k)
	}
	for k := deleteFrom; k < deleteFrom+10; k++ {
		deleteTailTestNeedle(t, store, vid, k, cookies[k])
		delete(live, k)
	}
	for k := uint64(31); k <= 34; k++ {
		write(k)
	}
	if key := damageRecordOfRow(t, store.GetVolume(vid), 22); key != types.Uint64ToNeedleId(23) {
		t.Fatalf("layout: row 22 is key %d, want 23", key)
	}
	return store, live, key30AppendAtNs
}

// A tail that starts past a deleted needle whose body is damaged still runs.
func TestTailPastDamagedDeletedBody(t *testing.T) {
	const vid = needle.VolumeId(61)
	source, live, since := buildDamagedTailSource(t, vid, 16)
	if _, ok := live[23]; ok {
		t.Fatal("layout: key 23 must be deleted")
	}
	target := newTailTestStore(t, vid)
	if err := runTailReceiver(t, target, serveTailSource(t, source), vid, since); err != nil {
		t.Fatalf("tail from after the damaged deleted record: %v", err)
	}
	for k := uint64(31); k <= 34; k++ {
		n := new(needle.Needle)
		n.Id = types.Uint64ToNeedleId(k)
		if _, err := target.ReadVolumeNeedle(vid, n, nil, nil); err != nil || !bytes.Equal(n.Data, live[k]) {
			t.Fatalf("key %d on the target: err %v, data equal %v", k, err, bytes.Equal(n.Data, live[k]))
		}
	}
}

// A tail over a live needle whose body is damaged fails on that needle's CRC,
// not on the search for where to start.
func TestTailReportsDamagedLiveBody(t *testing.T) {
	const vid = needle.VolumeId(62)
	source, live, _ := buildDamagedTailSource(t, vid, 1)
	if _, ok := live[23]; !ok {
		t.Fatal("layout: key 23 must be live")
	}
	target := newTailTestStore(t, vid)
	err := runTailReceiver(t, target, serveTailSource(t, source), vid, 0)
	if err == nil {
		t.Fatal("tail over a damaged live needle reported no error")
	}
	if strings.Contains(err.Error(), "fail to locate") || !strings.Contains(err.Error(), "invalid CRC for needle 17") {
		t.Fatalf("tail error %q, want the CRC error of needle 23 (0x17), not a search failure", err)
	}
}
