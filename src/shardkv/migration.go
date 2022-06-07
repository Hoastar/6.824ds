package shardkv

import (
	"6.824ds/src/shardmaster"
	"fmt"
	"time"
)

type MigrateArgs struct {
	FromGid       int
	ShardNeed     int
	ConfigOfShard shardmaster.Config
}

type MigrateReply struct {
	Shard         ShardDB
	ClientSuccess map[string]OpRecord
	Err           string
}

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	kv.LogPrintf("Receving migrate request %+v", args)
	kv.mu.Lock()
	if args.ConfigOfShard.Num > kv.configNum { // havent new enough to reply, return WrongLeader
		kv.LogPrintf("request %+v newer than self %+v. Not ready to migrate. reject with wrong leader", args.ConfigOfShard, kv.configNum)
		reply.Err = ErrWrongLeader
	} else if args.ConfigOfShard.Num == kv.configNum && !kv.isReConf {
		kv.LogPrintf("request %+v has same config num to self %+v, but no is not config updating, reject wrong leader", args.ConfigOfShard, kv.configNum)
		reply.Err = ErrWrongLeader
	} else if args.ConfigOfShard.Num < kv.globalConfigNum[args.FromGid] {
		kv.LogPrintf("Saw request which is older than cfg num the group %d has declared. Must be zombie. Reject", args.FromGid)
		reply.Err = ErrWrongLeader
	} else {
		if _, ok := kv.db[args.ShardNeed]; !ok {
			panic(fmt.Sprintf("shardId: %d Not found ", args.ShardNeed))
		}
		CopyShardDB(&reply.Shard, kv.db[args.ShardNeed])
		reply.ClientSuccess = make(map[string]OpRecord)
		successRecordUnion(reply.ClientSuccess, kv.successRecords)
		reply.Err = OK
		kv.LogPrintf("request migrate reply sucess")
	}

	kv.mu.Unlock()
}

func (kv *ShardKV) sendMigrateShard(config shardmaster.Config, gid int, shardId int) MigrateReply {
	if gid == 0 {
		var reply MigrateReply
		reply.Err = OK
		reply.Shard = ShardDB{shardId, make(map[string]string)}
		reply.ClientSuccess = nil
		return reply
	}
	servers := config.Groups[gid]
	args := MigrateArgs{kv.gid, shardId, config}
	retry := 0
	for i := 0; ; i = (i + 1) % len(servers) { // retry entil Err == OK
		var reply MigrateReply
		if retry > 12 { // limit the retry
			reply.Err = ErrWrongLeader
			return reply
		}
		kv.LogPrintf("Calling gid %d server %s migrateshard with config %+v", gid, servers[i], args)
		srv := kv.make_end(servers[i])
		ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
		if ok && reply.Err == OK {
			kv.LogPrintf("Sucessfully get ret from gid %d server %s reply %+v", gid, servers[i], reply)
			return reply
		}
		time.Sleep(20 * time.Millisecond)
		retry++
	}

}
