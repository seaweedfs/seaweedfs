package directory

import {
    "testing"
    "log"
}
func TestSerialDeserialization(t *testing.T) {
    f1 := &FileId{VolumeId: 345, Key:8698, Hashcode: 23849095}
    log.Println("vid", f1.VolumeId, "key", f1.Key, "hash", f1.Hashcode)

    f2 := ParseFileId(t.String())

    log.Println("vvid", f2.VolumeId, "vkey", f2.Key, "vhash", f2.Hashcode)

    t.
}
