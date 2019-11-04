package sequence

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"go.etcd.io/etcd/client"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	EtcdKeySequence                 = "/master/sequence"
	EtcdKeyPrefix                   = "/seaweedfs"
	EtcdContextTimeoutSecond        = 100 * time.Second
	DefaultEtcdSteps         uint64 = 500 // internal counter
	SequencerFileName               = "sequencer.dat"
	FileMaxSequenceLength           = 128
)

type EtcdSequencer struct {
	sequenceLock sync.Mutex

	// available sequence range : [steps, maxCounter)
	maxCounter uint64
	steps      uint64

	etcdClient client.Client
	keysAPI    client.KeysAPI
	seqFile    *os.File
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

	maxValue, _, err := readSequenceFile(file)
	if err != nil {
		return nil, fmt.Errorf("read sequence from file failed, %v", err)
	}
	glog.V(4).Infof("read sequence from file : %d", maxValue)

	newSeq, err := setMaxSequenceToEtcd(keysApi, maxValue)
	if err != nil {
		return nil, err
	}

	// make the step and max the same, and then they are fake,
	// after invoking the NextFileId(), they are different and real
	maxCounter, steps := newSeq, newSeq
	sequencer := &EtcdSequencer{maxCounter: maxCounter,
		steps:      steps,
		etcdClient: cli,
		keysAPI:    keysApi,
		seqFile:    file,
	}
	return sequencer, nil
}

func (es *EtcdSequencer) NextFileId(count uint64) (new uint64, cnt uint64) {
	es.sequenceLock.Lock()
	defer es.sequenceLock.Unlock()
	if (es.steps + count) >= es.maxCounter {
		reqSteps := DefaultEtcdSteps
		if count > DefaultEtcdSteps {
			reqSteps += count
		}
		maxId, err := batchGetSequenceFromEtcd(es.keysAPI, reqSteps)
		glog.V(4).Infof("get max sequence id from etcd, %d", maxId)
		if err != nil {
			glog.Error(err)
			return 0, 0
		}
		es.steps, es.maxCounter = maxId-reqSteps, maxId
		glog.V(4).Infof("current id : %d, max id : %d", es.steps, es.maxCounter)

		if err := writeSequenceFile(es.seqFile, es.maxCounter, es.steps); err != nil {
			glog.Errorf("flush sequence to file failed, %v", err)
		}
	}
	ret := es.steps
	es.steps += count
	return ret, count
}

/**
instead of collecting the max value from volume server,
the max value should be saved in local config file and ETCD cluster
*/
func (es *EtcdSequencer) SetMax(seenValue uint64) {
	es.sequenceLock.Lock()
	defer es.sequenceLock.Unlock()
	if seenValue > es.maxCounter {
		maxId, err := setMaxSequenceToEtcd(es.keysAPI, seenValue)
		if err != nil {
			glog.Errorf("set Etcd Max sequence failed : %v", err)
			return
		}
		es.steps, es.maxCounter = maxId, maxId

		if err := writeSequenceFile(es.seqFile, maxId, maxId); err != nil {
			glog.Errorf("flush sequence to file failed, %v", err)
		}
	}
}

func (es *EtcdSequencer) GetMax() uint64 {
	return es.maxCounter
}

func (es *EtcdSequencer) Peek() uint64 {
	return es.steps
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
  update the key of EtcdKeySequence in ETCD cluster with the parameter of maxSeq,
until the value of EtcdKeySequence is equal to or larger than the maxSeq
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
					continue // create ETCD key success, retry get ETCD value
				}
				if ce, ok = err.(client.Error); ok && (ce.Code == client.ErrorCodeNodeExist) {
					continue // ETCD key exist, retry get ETCD value
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

	return maxSeq, nil
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
	sequence : step 以冒号分割
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
先不存放step到文件中
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

func deleteEtcdKey(kvApi client.KeysAPI, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdContextTimeoutSecond)
	defer cancel()
	_, err := kvApi.Delete(ctx, key, &client.DeleteOptions{Dir: false})
	if err != nil {
		return err
	}
	return nil
}

//func (es *EtcdSequencer) Load() error {
//	es.sequenceLock.Lock()
//	defer es.sequenceLock.Unlock()
//	reqSteps := DefaultEtcdSteps
//	maxId, err := batchGetSequenceFromEtcd(es.keysAPI, reqSteps)
//	glog.V(4).Infof("get max sequence id from etcd, %d", maxId)
//	if err != nil {
//		glog.Error(err)
//		return err
//	}
//	es.steps, es.maxCounter = maxId-reqSteps, maxId
//	glog.V(4).Infof("current id : %d, max id : %d", es.steps, es.maxCounter)
//
//	if err := writeSequenceFile(es.seqFile, es.maxCounter, es.steps); err != nil {
//		glog.Errorf("flush sequence to file failed, %v", err)
//		return err
//	}
//	return nil
//}

//func getEtcdKey(kv client.KeysAPI, key string) (string, error) {
//	resp, err := kv.Get(context.Background(), key, &client.GetOptions{Recursive: false, Quorum: true})
//	if err != nil {
//		glog.Warningf("key:%s result:%v", EtcdKeySequence, err)
//		return "", err
//	}
//	if resp.Node == nil {
//		return "", fmt.Errorf("the key is not exist, %s", key)
//	}
//	return resp.Node.Value, nil
//}

//func (es *EtcdSequencer) setLocalSequence(maxValue uint64) {
//	es.sequenceLock.Lock()
//	defer es.sequenceLock.Unlock()
//	if maxValue > es.maxCounter {
//		es.maxCounter, es.steps = maxValue, maxValue-DefaultEtcdSteps
//
//		if err := writeSequenceFile(es.seqFile, es.maxCounter, es.steps); err != nil {
//			glog.Errorf("flush sequence to file failed, %v", err)
//		}
//	}
//}

//func getEtcdKeysApi(etcdUrls, user, passwd string) (client.KeysAPI, error) {
//	cli, err := client.New(client.Config{
//		Endpoints: strings.Split(etcdUrls, ","),
//		Username:  user,
//		Password:  passwd,
//	})
//	if err != nil {
//		return nil, err
//	}
//	keysApi := client.NewKeysAPI(cli)
//	return keysApi, nil
//}

//func (es *EtcdSequencer) asyncStartWatcher() {
//	es.startWatcher(es.keysAPI, EtcdKeySequence, func(value string, index uint64) {
//		newValue, err := strconv.ParseUint(value, 10, 64)
//		if err != nil {
//			glog.Warning(err)
//		}
//		es.setLocalSequence(newValue)
//	})
//}

//func (es *EtcdSequencer) startWatcher(kvApi client.KeysAPI, key string, callback func(value string, index uint64)) {
//	ctx, cancel := context.WithTimeout(context.Background(), EtcdContextTimeoutSecond)
//	defer cancel()
//	ctx.Done()
//
//	getResp, err := kvApi.Get(ctx, key, &client.GetOptions{Recursive: false, Quorum: true})
//	if err != nil {
//		return
//	}
//
//	watcher := kvApi.Watcher(key, &client.WatcherOptions{AfterIndex: getResp.Index, Recursive: false})
//	go func(w client.Watcher) {
//		for {
//			resp, err := w.Next(context.Background())
//			if err != nil {
//				glog.Error(err)
//				continue
//			}
//			callback(resp.Node.Value, resp.Index)
//		}
//	}(watcher)
//	return
//}
