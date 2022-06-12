package shardkv

import (
	"6.824ds/src/shardmaster"
	"fmt"
	"sync"
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

// MigrationLoop 迁移线程
func (kv *ShardKV) MigrationLoop() {
	for !kv.killed() {

		msg := <-kv.cfgPendingCh

		kv.mu.Lock()
		currentCfg := kv.mck.Query(kv.configNum)
		if kv.configNum >= msg.Config { // avoid dup cfgdone
			kv.LogPrintf("Migration loop receive old cfg :%+v cfg now %+v. do nothing", msg.Config, kv.configNum)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		// 分片迁移一定会发生在将OldConfig --> NewConfig的过程
		// msg.Config大于kv.configNum，那么势必更新config
		nextCfg := kv.mck.Query(msg.Config)
		kv.LogPrintf("Start migration from config %+v to %+v", currentCfg, nextCfg)

		// for all updating shard, receive, if saw new  cfg, abort
		// 记录需要被迁移进来的shard
		migrateIn := make(map[int][]int)

		// 判断是否有数据要被迁入：
		// nextShards: shard -> gid
		for nextShard, nextGid := range nextCfg.Shards {
			targetGid := currentCfg.Shards[nextShard]
			if nextGid == kv.gid && targetGid != nextGid { // shard does not belong to old but belongs to new
				// only ask the server has the shards in currentCfg for data
				// 属于新配置但不属于旧配置中的 gid->shards 记录：迁入到新配置
				if _, ok := migrateIn[targetGid]; !ok {
					migrateIn[targetGid] = make([]int, 0)
				}
				migrateIn[targetGid] = append(migrateIn[targetGid], nextShard)
			}
		}

		wg := sync.WaitGroup{}
		// 数据迁移
		for gid, shardIds := range migrateIn {
			for _, shardId := range shardIds {
				wg.Add(1)
				go func(gid int, shardId int) { // do in parallel
					// 主动向等于gid的group要自己在新配置中需要，但是在旧配置中不存在的shard以及successRecord
					reply := kv.sendMigrateShard(currentCfg, gid, shardId)
					if reply.Err == OK {
						// 寻求成功之后，在raft层通过start，完成日志同步，LogApplier()异步接受raft的同步结果msg
						_, _, isLeader := kv.rf.Start(Op{OpTypeConfigDone, "", "", nextCfg.Num, reply.Shard, reply.ClientSuccess, randstring(10)})
						kv.LogPrintf("Received migration reply %+v from group %d, send cfgdone to raft, is leader %t", reply, gid, isLeader)
					}
					wg.Done()
				}(gid, shardId)
			}
		}
		wg.Wait()

		// if pending nolonger leader, it's ok just quit
		kv.mu.Lock()
		delete(kv.cfgPending, nextCfg.Num)
		kv.mu.Unlock()
	}

}

// MigrateShard rpc handler
func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	kv.LogPrintf("Receiving migrate request %+v", args)
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
		// 拷贝想要迁移的shard对应的ShardDB
		CopyShardDB(&reply.Shard, kv.db[args.ShardNeed])
		reply.ClientSuccess = make(map[string]OpRecord)
		// success opRecord
		successRecordUnion(reply.ClientSuccess, kv.successRecords)
		reply.Err = OK
		kv.LogPrintf("request migrate reply sucess")
	}

	kv.mu.Unlock()
}

// sendMigrateShard call MigrateShard 进行数据迁移
func (kv *ShardKV) sendMigrateShard(config shardmaster.Config, gid int, shardId int) MigrateReply {
	if gid == 0 {
		var reply MigrateReply
		reply.Err = OK
		reply.Shard = ShardDB{shardId, make(map[string]string)}
		reply.ClientSuccess = nil
		return reply
	}

	// 获取被迁移shard的config group server成员
	servers := config.Groups[gid]
	// 构建数据迁移参数
	args := MigrateArgs{kv.gid, shardId, config}
	retry := 0

	// 遍历group server成员寻找
	for i := 0; ; i = (i + 1) % len(servers) { // retry until Err == OK（）
		var reply MigrateReply
		if retry > 12 { // limit the retry
			reply.Err = ErrWrongLeader
			return reply
		}
		kv.LogPrintf("Calling sendMigrate %d server %s migrateshard with config %+v", gid, servers[i], args)
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
