package shardkv

import (
	"6.824ds/src/labgob"
	"6.824ds/src/raft"
	"6.824ds/src/shardmaster"
	"bytes"
	"log"
)

func (kv *ShardKV) NoLockSnapshot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.raftNowTerm)
	e.Encode(kv.db)
	e.Encode(kv.successRecords)
	e.Encode(kv.configNum)
	e.Encode(kv.isReConf)
	e.Encode(kv.globalConfigNum)
	e.Encode(kv.shardAccess)

	return raft.Compress(w.Bytes())
}

func (kv *ShardKV) InstallSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(raft.Decompress(data))
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	// supress warning of decoding into non zero value
	kv.raftNowTerm = 0
	kv.db = nil
	kv.successRecords = nil
	kv.configNum = 0
	kv.isReConf = false
	kv.globalConfigNum = make(map[int]int)
	kv.shardAccess = [shardmaster.NShards]bool{}
	if d.Decode(&kv.raftNowTerm) != nil || d.Decode(&kv.db) != nil ||
		d.Decode(&kv.successRecords) != nil || d.Decode(&kv.configNum) != nil ||
		d.Decode(&kv.isReConf) != nil ||
		d.Decode(&kv.globalConfigNum) != nil ||
		d.Decode(&kv.shardAccess) != nil {
		log.Fatalf("Decode persistent error!")
	} else {
		// kv.LogPrintf("Reading from snapshot, raftNowTerm %d, db %+v, successRecords %+v", kv.raftNowTerm, kv.db, kv.successRecords)
		kv.LogPrintf("Reading from snapshot")
	}
	kv.mu.Unlock()

}
