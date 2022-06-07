package shardkv

import (
	"log"
	"strconv"
	"strings"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type OperationArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append" or "Get"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id string
}

type OperationReply struct {
	Err   Err
	Value string
}

func splitId(opId string) (string, int) {
	ss := strings.Split(opId, ":")
	client := ss[0]
	seq, err := strconv.Atoi(ss[1])
	if err != nil {
		log.Fatalf("Parse int fail %s", client)
	}
	return client, seq
}

func CopyShardDB(dst *ShardDB, src ShardDB) {
	dst.ID = src.ID
	// override
	dst.DB = make(map[string]string)
	CopyDB(dst.DB, src.DB)
}

func CopyDB(dst map[string]string, src map[string]string) {
	var keys []string
	for k, _ := range dst {
		keys = append(keys, k)
	}
	for _, k := range keys { // empty dst
		delete(dst, k)
	}

	for k, v := range src {
		dst[k] = v
	}
}
