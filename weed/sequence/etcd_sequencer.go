package sequence

/*
Note :
(1) store the sequence in the ETCD cluster, and local file(sequence.dat)
(2) batch get the sequences from ETCD cluster, and store the max sequence id in the local file
(3) the sequence range is : [currentSeqId, maxSeqId), when the currentSeqId >= maxSeqId, fetch the new maxSeqId.
*/

import (
	"context"
	"fmt"
	"sync"
	"time"

	"io"
	"os"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"go.etcd.io/etcd/client"
)

const (
	// EtcdKeyPrefix                   = "/seaweedfs"
	EtcdKeySequence                 = "/master/sequence"
	EtcdContextTimeoutSecond        = 100 * time.Second
	DefaultEtcdSteps         uint64 = 500 // internal counter
	SequencerFileName               = "sequencer.dat"
	FileMaxSequenceLength           = 128
)

type EtcdSequencer struct {
	sequenceLock sync.Mutex

	// available sequence range : [currentSeqId, maxSeqId)
	currentSeqId uint64
	maxSeqId     uint64

	keysAPI client.KeysAPI
	seqFile *os.File
}

func NewEtcdSequencer(etcdUrls string, metaFolder string) (*EtcdSequencer, error) {
	file, err := openSequenceFile(metaFolder + "/" + SequencerFileName)
	if nil != err {
		return nil, fmt.Errorf("open sequence file fialed, %v", err)
	}

	cli, err := client.New(client.Config{
		Endpoints: strings.Split(etcdUrls, ","),
		Username:  "",
		Password:  "",
	})
	if err != nil {
		return nil, err
	}
	keysApi := client.NewKeysAPI(cli)

	// TODO: the current sequence id in local file is not used
	maxValue, _, err := readSequenceFile(file)
	if err != nil {
		return nil, fmt.Errorf("read sequence from file failed, %v", err)
	}
	glog.V(4).Infof("read sequence from file : %d", maxValue)

	newSeq, err := setMaxSequenceToEtcd(keysApi, maxValue)
	if err != nil {
		return nil, err
	}

	sequencer := &EtcdSequencer{maxSeqId: newSeq,
		currentSeqId: newSeq,
		keysAPI:      keysApi,
		seqFile:      file,
	}
	return sequencer, nil
}

func (es *EtcdSequencer) NextFileId(count uint64) uint64 {
	es.sequenceLock.Lock()
	defer es.sequenceLock.Unlock()

	if (es.currentSeqId + count) >= es.maxSeqId {
		reqSteps := DefaultEtcdSteps
		if count > DefaultEtcdSteps {
			reqSteps += count
		}
		maxId, err := batchGetSequenceFromEtcd(es.keysAPI, reqSteps)
		glog.V(4).Infof("get max sequence id from etcd, %d", maxId)
		if err != nil {
			glog.Error(err)
			return 0
		}
		es.currentSeqId, es.maxSeqId = maxId-reqSteps, maxId
		glog.V(4).Infof("current id : %d, max id : %d", es.currentSeqId, es.maxSeqId)

		if err := writeSequenceFile(es.seqFile, es.maxSeqId, es.currentSeqId); err != nil {
			glog.Errorf("flush sequence to file failed, %v", err)
		}
	}

	ret := es.currentSeqId
	es.currentSeqId += count
	return ret
}

/**
instead of collecting the max value from volume server,
the max value should be saved in local config file and ETCD cluster
*/
func (es *EtcdSequencer) SetMax(seenValue uint64) {
	es.sequenceLock.Lock()
	defer es.sequenceLock.Unlock()
	if seenValue > es.maxSeqId {
		maxId, err := setMaxSequenceToEtcd(es.keysAPI, seenValue)
		if err != nil {
			glog.Errorf("set Etcd Max sequence failed : %v", err)
			return
		}
		es.currentSeqId, es.maxSeqId = maxId, maxId

		if err := writeSequenceFile(es.seqFile, maxId, maxId); err != nil {
			glog.Errorf("flush sequence to file failed, %v", err)
		}
	}
}

func (es *EtcdSequencer) GetMax() uint64 {
	return es.maxSeqId
}

func (es *EtcdSequencer) Peek() uint64 {
	return es.currentSeqId
}

func batchGetSequenceFromEtcd(kvApi client.KeysAPI, step uint64) (uint64, error) {
	if step <= 0 {
		return 0, fmt.Errorf("the step must be large than 1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), EtcdContextTimeoutSecond)
	var endSeqValue uint64 = 0
	defer cancel()
	for {
		getResp, err := kvApi.Get(ctx, EtcdKeySequence, &client.GetOptions{Recursive: false, Quorum: true})
		if err != nil {
			return 0, err
		}
		if getResp.Node == nil {
			continue
		}

		prevValue := getResp.Node.Value
		prevSeqValue, err := strconv.ParseUint(prevValue, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("get sequence from etcd failed, %v", err)
		}
		endSeqValue = prevSeqValue + step
		endSeqStr := strconv.FormatUint(endSeqValue, 10)

		_, err = kvApi.Set(ctx, EtcdKeySequence, endSeqStr, &client.SetOptions{PrevValue: prevValue})
		if err == nil {
			break
		}
		glog.Error(err)
	}

	return endSeqValue, nil
}

/**
update the value of the key EtcdKeySequence in ETCD cluster with the parameter of maxSeq,
when the value of the key EtcdKeySequence is equal to or large than the parameter maxSeq,
return the value of EtcdKeySequence in the ETCD cluster;
when the value of the EtcdKeySequence is less than the parameter maxSeq,
return the value of the parameter maxSeq
*/
func setMaxSequenceToEtcd(kvApi client.KeysAPI, maxSeq uint64) (uint64, error) {
	maxSeqStr := strconv.FormatUint(maxSeq, 10)
	ctx, cancel := context.WithTimeout(context.Background(), EtcdContextTimeoutSecond)
	defer cancel()

	for {
		getResp, err := kvApi.Get(ctx, EtcdKeySequence, &client.GetOptions{Recursive: false, Quorum: true})
		if err != nil {
			if ce, ok := err.(client.Error); ok && (ce.Code == client.ErrorCodeKeyNotFound) {
				_, err := kvApi.Create(ctx, EtcdKeySequence, maxSeqStr)
				if err == nil {
					continue
				}
				if ce, ok = err.(client.Error); ok && (ce.Code == client.ErrorCodeNodeExist) {
					continue
				}
				return 0, err
			} else {
				return 0, err
			}
		}

		if getResp.Node == nil {
			continue
		}
		prevSeqStr := getResp.Node.Value
		prevSeq, err := strconv.ParseUint(prevSeqStr, 10, 64)
		if err != nil {
			return 0, err
		}
		if prevSeq >= maxSeq {
			return prevSeq, nil
		}

		_, err = kvApi.Set(ctx, EtcdKeySequence, maxSeqStr, &client.SetOptions{PrevValue: prevSeqStr})
		if err != nil {
			return 0, err
		}
	}
}

func openSequenceFile(file string) (*os.File, error) {
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		fid, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		if err := writeSequenceFile(fid, 1, 0); err != nil {
			return nil, err
		}
		return fid, nil
	} else {
		return os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0644)
	}
}

/*
read sequence and step from sequence file
*/
func readSequenceFile(file *os.File) (uint64, uint64, error) {
	sequence := make([]byte, FileMaxSequenceLength)
	size, err := file.ReadAt(sequence, 0)
	if (err != nil) && (err != io.EOF) {
		err := fmt.Errorf("cannot read file %s, %v", file.Name(), err)
		return 0, 0, err
	}
	sequence = sequence[0:size]
	seqs := strings.Split(string(sequence), ":")
	maxId, err := strconv.ParseUint(seqs[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("parse sequence from file failed, %v", err)
	}

	if len(seqs) > 1 {
		step, err := strconv.ParseUint(seqs[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse sequence from file failed, %v", err)
		}
		return maxId, step, nil
	}

	return maxId, 0, nil
}

/**
write the sequence and step to sequence file
*/
func writeSequenceFile(file *os.File, sequence, step uint64) error {
	_ = step
	seqStr := fmt.Sprintf("%d:%d", sequence, sequence)
	if _, err := file.Seek(0, 0); err != nil {
		err = fmt.Errorf("cannot seek to the beginning of %s: %v", file.Name(), err)
		return err
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncate sequence file faield : %v", err)
	}
	if _, err := file.WriteString(seqStr); err != nil {
		return fmt.Errorf("write file %s failed, %v", file.Name(), err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("flush file %s failed, %v", file.Name(), err)
	}
	return nil
}

// the UT helper method
// func deleteEtcdKey(kvApi client.KeysAPI, key string) error {
// 	ctx, cancel := context.WithTimeout(context.Background(), EtcdContextTimeoutSecond)
// 	defer cancel()
// 	_, err := kvApi.Delete(ctx, key, &client.DeleteOptions{Dir: false})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
