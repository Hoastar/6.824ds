package shardkv

import (
	"6.824ds/src/labgob"
	"6.824ds/src/labrpc"
	"6.824ds/src/raft"
	"6.824ds/src/shardmaster"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpTypeGet        = "get"
	OpTypePut        = "put"
	OpTypeAppend     = "app"
	OpTypeConfig     = "cfg"
	OpTypeConfigDone = "cfgDone"
)

type ShardDB struct {
	ID int
	DB map[string]string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType        string
	Key           string
	Value         string
	Config        int
	Shard         ShardDB
	ClientSuccess map[string]OpRecord
	OpId          string
}

type NotifyMsg struct {
	Info  string
	Value string
}

const (
	NotifyMsgInfoNoKey      = "nokey"
	NotifyMsgInfoGetOK      = "getok"
	NotifyMsgInfoPutOK      = "putok"
	NotifyMsgInfoAppOK      = "appok"
	NotifyMsgInfoNotLeader  = "notleader"
	NotifyMsgInfoWrongGroup = "wg"
)

type Pending struct {
	OpId     string
	index    int
	term     int
	notifyCh chan NotifyMsg
}

const CfgNotifyMsgInfoRun = "run"

type CfgNotifyMsg struct {
	Info   string
	Config int
}

type CfgPending struct {
	ConfigNum int
	notifyCh  chan CfgNotifyMsg
}

type OpRecord struct {
	OpSeq  int
	NtfMsg NotifyMsg
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	mck       *shardmaster.Clerk
	configNum int

	// To determine is still leader
	// raftIndex   int
	// latest term server can observe from applyMsg
	raftNowTerm int64

	// notification when command applied
	pendingByOpId map[string]*Pending

	// kv database
	db map[int]ShardDB

	// op record clientid -> client op
	successRecords map[string]OpRecord

	// silly way to avoid blocking the chan
	bufferChan chan raft.ApplyMsg

	// is config updating
	isReConf bool
	// pending config routine
	cfgPending   map[int]bool
	cfgPendingCh chan CfgNotifyMsg

	// config which is updating
	shardAccess [shardmaster.NShards]bool

	// config history for shard GC
	globalConfigNum map[int]int

	dead int32

	lastMsgIndex int
}

func (kv *ShardKV) MakePending(opId string, index int, term int) *Pending {
	p := &Pending{}
	p.OpId = opId
	p.index = index
	p.term = term
	p.notifyCh = make(chan NotifyMsg)

	kv.pendingByOpId[opId] = p
	return p
}

const Debug = 0

func (kv *ShardKV) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Printf("[Gid %d Server %d]: %s", kv.gid, kv.me, s)
	}
	return
}

func (kv *ShardKV) FindRecordAndSet(opId string, reply *OperationReply, setreplyOK func(*OperationReply, NotifyMsg)) bool {

	found := false
	cl, seq := splitId(opId)
	kv.mu.Lock()
	clientRecord, ok1 := kv.successRecords[cl]
	if ok1 && clientRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d", cl, clientRecord.OpSeq, clientRecord.NtfMsg, seq)
	} else if ok1 && clientRecord.OpSeq == seq { // found existing record ,this is dup request
		found = true
		setreplyOK(reply, clientRecord.NtfMsg)
	} else {
		// nothing found
	}
	kv.mu.Unlock()
	return found
}

func (kv *ShardKV) SaveRecord(opId string, notifyMsg NotifyMsg) {
	// only save success op
	kv.LogPrintf("Saving the completed record id %s msg %+v", opId, notifyMsg)
	cl, seq := splitId(opId)
	clientRecord, ok1 := kv.successRecords[cl]
	if ok1 && clientRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d %+v", cl, clientRecord.OpSeq, clientRecord.NtfMsg, seq, notifyMsg)
	}
	kv.successRecords[cl] = OpRecord{seq, notifyMsg}
}

func (kv *ShardKV) DoOp(args *OperationArgs, reply *OperationReply, setreplyOK func(*OperationReply, NotifyMsg)) {
	// setreplyerr: err since not leader

	setReplyErr := func(reply *OperationReply) {
		reply.Err = ErrWrongLeader
	}

	var commandOp Op
	commandOp.Key = args.Key
	commandOp.OpType = args.Op
	commandOp.OpId = args.Id
	commandOp.Value = args.Value

	kv.mu.Lock()
	term, isLeader := kv.rf.GetState()
	rejectNotLeader := false
	nowTerm := atomic.LoadInt64(&kv.raftNowTerm)

	if !isLeader {
		kv.LogPrintf("Not leader, reject with ErrWronleader")
		rejectNotLeader = true
	} else if nowTerm < int64(term) { // leader haven't seen all the applys from prev term
		rejectNotLeader = true
		kv.LogPrintf("New leader haven't seen it's current term: %d, max term saw %d", term, nowTerm)
	}
	kv.mu.Unlock()
	if rejectNotLeader {
		setReplyErr(reply)
		return
	}

	// check if is dup record
	if kv.FindRecordAndSet(args.Id, reply, setreplyOK) {
		kv.LogPrintf("Found args %+v called previously, return %+v", args, reply)
		return
	}

	// lock to make sure server won't try to find the pending(fast receive from raft) before the pending is created
	kv.mu.Lock()

	shardId := key2shard(args.Key)
	if !kv.shardAccess[shardId] { // accessible
		kv.LogPrintf("Wrong group for shard: %d. Return ErrWrongGroup, current cfg %+v, updating %t", shardId, kv.configNum, kv.isReConf)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// CAUTION: Start will block! should avoid dead lock!
	index, submitTerm, isLeader := kv.rf.Start(commandOp)

	if !isLeader {
		kv.LogPrintf("Not leader when calling Start(), reject with ErrWronleader")
		setReplyErr(reply)
		kv.mu.Unlock()
		return
	}
	kv.LogPrintf("Receive op %+v from client", commandOp)

	pending := kv.MakePending(commandOp.OpId, index, submitTerm)
	kv.mu.Unlock()

	notifyMsg := <-pending.notifyCh // must wait for the notification, even lose the leader state
	if notifyMsg.Info == NotifyMsgInfoNotLeader {
		setReplyErr(reply)
		kv.LogPrintf("Op %+v failed since no longer leader: %+v", commandOp, notifyMsg)
	} else if notifyMsg.Info == NotifyMsgInfoWrongGroup {
		reply.Err = ErrWrongGroup
		kv.LogPrintf("Op %+v failed since it's updating: %+v", commandOp, notifyMsg)
	} else {
		setreplyOK(reply, notifyMsg)
		kv.LogPrintf("Got notification op %+v success: %+v", commandOp, notifyMsg)
	}
}

func (kv *ShardKV) Get(args *OperationArgs, reply *OperationReply) {
	// Your code here.
	replyOK := func(reply *OperationReply, notifyMsg NotifyMsg) {
		if notifyMsg.Info == NotifyMsgInfoGetOK {
			reply.Err = OK
			reply.Value = notifyMsg.Value
		} else {
			reply.Err = ErrNoKey
		}
	}
	kv.DoOp(args, reply, replyOK)
}

func (kv *ShardKV) PutAppend(args *OperationArgs, reply *OperationReply) {
	// Your code here.
	kv.DoOp(args, reply, func(reply *OperationReply, _ NotifyMsg) { reply.Err = OK })
}

func (kv *ShardKV) DeletePending(p *Pending) { // should be called when holding mu
	delete(kv.pendingByOpId, p.OpId)
}

func (kv *ShardKV) NotifyOpDone(opId string, index int, successmsg NotifyMsg, opterm int) {
	// find whether
	kv.mu.Lock()

	if len(kv.pendingByOpId) == 0 { // nobody is waiting for that command
		kv.mu.Unlock()
		kv.LogPrintf("Nothing pending, quit for op %s, index %d", opId, index)
		return
	}

	var p *Pending
	var ok bool
	var notifyMsg NotifyMsg
	issuedBySelf := false
	if p, ok = kv.pendingByOpId[opId]; ok {
		kv.LogPrintf("Notifying pending %+v since op success", p)
		notifyMsg = successmsg
		kv.DeletePending(p)
		issuedBySelf = true
	} else { // not found, not this server's pending
		kv.LogPrintf("Pending opId %s index %d not found in pending table", opId, index)
		p = nil
	}
	kv.mu.Unlock()
	if p != nil {
		go func() { p.notifyCh <- notifyMsg }() // non blocking for receive fromm apply
	}

	if !issuedBySelf {
		kv.PendingGC(opterm) //seems to be ok to run async. but i don't
	}

}

func (kv *ShardKV) BufferedLogReader() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.bufferChan <- applyMsg
	}
}

func (kv *ShardKV) LogApplier() {

	for { // this for should never block!
		select {
		case applyMsg := <-kv.bufferChan:
			kv.LogPrintf("Receiving ApplyMsg from raft %+v", applyMsg)

			if !applyMsg.CommandValid { // not real command, set the term and skip
				atomic.StoreInt64(&kv.raftNowTerm, int64(applyMsg.Term))
				if applyMsg.CommandType == raft.CommandTypeNewLeader { // new leader, need to gc the pending, otherwise test will block forever when only 1 client
					go kv.PendingGC(applyMsg.Term)
				}
				if applyMsg.CommandType == raft.CommandTypeUpdateStateMachine {
					data := applyMsg.Command.([]byte)
					// gc before snapshot is installed
					kv.PendingGC(applyMsg.Term)
					kv.InstallSnapshot(data)
				}
				continue
			}

			_commandOp := applyMsg.Command.(Op)
			commandOp := _commandOp // copy op to avoid race
			CopyShardDB(&commandOp.Shard, _commandOp.Shard)

			allUpdated := func() {
				migrationDone := true
				configUpdating := kv.mck.Query(kv.configNum + 1)
				for shardId, gid := range configUpdating.Shards {
					if gid == kv.gid && !kv.shardAccess[shardId] { // found something missing
						migrationDone = false
						break
					}
				}
				if migrationDone {
					kv.isReConf = false
					kv.configNum = commandOp.Config
					kv.LogPrintf("All needed done, update to the new cfg")
				}
			}

			// is config update
			if commandOp.OpType == OpTypeConfig {
				kv.LogPrintf("Receving config update isreconf %t, current cfg %+v, newcfg %+v, access: %+v", kv.isReConf, kv.configNum, commandOp.Config, kv.shardAccess)
				kv.mu.Lock()
				if commandOp.Config <= kv.configNum { // only update one config once, even multiple update
					kv.LogPrintf("applier update config: current %+v recevied %+v, num is old, skip", kv.configNum, commandOp.Config)
				} else if commandOp.Config == kv.configNum+1 { // not updating and saw new
					// no mater isreconf or not check if no pending, do migration
					if _, ok := kv.cfgPending[commandOp.Config]; ok { // something pending
						go func() { kv.cfgPendingCh <- CfgNotifyMsg{CfgNotifyMsgInfoRun, commandOp.Config} }()
					} else { // no pendining  do nothing
					}
					kv.isReConf = true

					// disable the access to the shard which will lose in next cfg
					config := kv.mck.Query(kv.configNum)
					configUpdating := kv.mck.Query(kv.configNum + 1)
					for i, _ := range kv.shardAccess {
						if config.Shards[i] == kv.gid && configUpdating.Shards[i] != kv.gid {
							kv.shardAccess[i] = false
						}
					}
					allUpdated()
				} else {
					panic(fmt.Sprintf("bad config update command %+v, current cft %+v", commandOp.Config, kv.configNum))
				}
				// else is updating just ignore
				kv.lastMsgIndex = applyMsg.LogIndex

				kv.mu.Unlock()
				continue // next command
			} else if commandOp.OpType == OpTypeConfigDone {
				// here i think config done won't be duplicated
				kv.LogPrintf("Receving config done isreconf %t, current cfg %+v, cfgdone %+v, access: %+v", kv.isReConf, kv.configNum, commandOp, kv.shardAccess)
				kv.mu.Lock()
				if kv.isReConf && commandOp.Config == kv.configNum+1 && !kv.shardAccess[commandOp.Shard.ID] {

					kv.shardAccess[commandOp.Shard.ID] = true
					kv.db[commandOp.Shard.ID] = commandOp.Shard
					// update record
					successRecordUnion(kv.successRecords, commandOp.ClientSuccess)

					// if all shard received update cfg
					allUpdated()

				} else {
					// dup cfg done is possible
					kv.LogPrintf("old config done. do nothing")
				}
				kv.lastMsgIndex = applyMsg.LogIndex

				kv.mu.Unlock()
				continue
			}

			ret := NotifyMsg{"", ""} //default ret ok

			kv.mu.Lock()
			// stop serving which is not in nextCfg
			shard := key2shard(commandOp.Key)
			if !kv.shardAccess[shard] {
				ret.Info = NotifyMsgInfoWrongGroup // server does not store record
			} else {

				// logic for updating database
				shardDB := kv.db[shard].DB
				switch commandOp.OpType {
				case OpTypeGet:
					value, ok := shardDB[commandOp.Key] // if no key, empty
					if ok {
						ret.Value = value
						ret.Info = NotifyMsgInfoGetOK
					} else {
						ret.Info = NotifyMsgInfoNoKey
					}
				case OpTypePut:
					shardDB[commandOp.Key] = commandOp.Value
					ret.Info = NotifyMsgInfoPutOK
				case OpTypeAppend:
					oldValue, _ := shardDB[commandOp.Key]
					shardDB[commandOp.Key] = oldValue + commandOp.Value
					ret.Info = NotifyMsgInfoAppOK
				}
				kv.lastMsgIndex = applyMsg.LogIndex

				// set the request as the latest op for the client, for eleminate dup request
				kv.SaveRecord(commandOp.OpId, ret)
			}
			kv.mu.Unlock()

			// set the max term seem, so that whenever maxterm >= term saw by getState()
			// that means the kvserver has seen all request from last server
			atomic.StoreInt64(&kv.raftNowTerm, int64(applyMsg.Term))

			stateSize := kv.persister.RaftStateSize()
			if kv.maxRaftState > 0 && stateSize > int(float64(kv.maxRaftState)*0.8) { // 0.8 as ratio
				kv.LogPrintf("maxRaftState %d, stateSize %d, do snapshot", kv.maxRaftState, stateSize)
				kv.mu.Lock()
				snapshot := kv.NoLockSnapshot()
				index := kv.lastMsgIndex
				kv.mu.Unlock()
				// block to avoid triggering snapshot manytimes
				kv.rf.ReceiveSnapshot(snapshot, index)
			}
			go kv.NotifyOpDone(commandOp.OpId, applyMsg.CommandIndex, ret, applyMsg.Term)

			// here is the logic to quit
		case <-time.After(time.Millisecond * 100):
			if kv.killed() {
				return
			}
		}

	}
}

func mapUnion(dst map[string]string, src map[string]string) {
	for k, v := range src {
		dst[k] = v // aways use src value since it's from migration
	}
}

func successRecordUnion(dst map[string]OpRecord, src map[string]OpRecord) {
	for k, v := range src {
		if v1, ok := dst[k]; ok {
			// overlap get the latest one
			if v.OpSeq <= v1.OpSeq { // do not update record is not newer than self
				continue
			}
		}
		dst[k] = v
	}

}

func (kv *ShardKV) MigrationLoop() {

	for !kv.killed() {

		msg := <-kv.cfgPendingCh

		kv.mu.Lock()
		currentCfg := kv.mck.Query(kv.configNum)
		if kv.configNum >= msg.Config { // avoid dup cfgdone
			kv.LogPrintf("Migration loop recv old cfg :%+v cfg now %+v. do nothing", msg.Config, kv.configNum)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		nextCfg := kv.mck.Query(msg.Config)
		kv.LogPrintf("Start migration from config %+v to %+v", currentCfg, nextCfg)

		// for all updating shard, receive, if saw new  cfg, abort
		git2Query := make(map[int][]int)

		for shard, gid := range nextCfg.Shards {
			if targetGid := currentCfg.Shards[shard]; gid == kv.gid && targetGid != gid { // shard does not belong to old but belongs to new
				// only ask the server has the shards in currentCfg for data
				if _, ok := git2Query[targetGid]; !ok {
					git2Query[targetGid] = make([]int, 0)
				}
				git2Query[targetGid] = append(git2Query[targetGid], shard)
			}
		}

		wg := sync.WaitGroup{}
		for gid, shardIds := range git2Query {
			for _, shardId := range shardIds {
				wg.Add(1)
				go func(gid int, shardId int) { // do in parallel, so one partion won' affect rest
					reply := kv.sendMigrateShard(currentCfg, gid, shardId)
					if reply.Err == OK {
						_, _, isLeader := kv.rf.Start(Op{OpTypeConfigDone, "", "", nextCfg.Num, reply.Shard, reply.ClientSuccess, randstring(10)})
						kv.LogPrintf("Recevied migration reply %+v from group %d, send cfgdone to raft, is leader %t", reply, gid, isLeader)
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

func (kv *ShardKV) ConfigPoller() {
	for !kv.killed() {
		kv.mu.Lock()
		var nextCfg shardmaster.Config
		nextCfg = kv.mck.Query(kv.configNum + 1)

		submitCfgChange := func() {
			commandOp := Op{OpTypeConfig, "", "", nextCfg.Num, ShardDB{}, nil, randstring(10)}
			kv.LogPrintf("Submiting new cfg to raft. %+v, cfg %v", commandOp, commandOp.Config)
			_, _, leader := kv.rf.Start(commandOp)

			if leader {
				// start a new pending
				if _, ok := kv.cfgPending[nextCfg.Num]; ok {
					// already pending do not start the pending, make sure only one pending for a cfg at a time
					return
				}
				kv.cfgPending[nextCfg.Num] = true
			}

		}
		// either found newer or the same
		if kv.isReConf {
			if nextCfg.Num != kv.configNum+1 {
				panic(fmt.Sprintf("current updating config != nextCfg, next %+v", nextCfg))
			} else {
				// might be replaying, need restart pending
				if _, ok := kv.cfgPending[kv.configNum+1]; ok {
					// not replaying do nothing
				} else {
					// submit config change, possiblly resubmit. need to filter out in applymsg
					kv.LogPrintf("is reconfiguring but no pending, should be replaying or not leader. try to submit new conf change")
					submitCfgChange()
				}
			}
		} else { // not configuring
			if nextCfg.Num == kv.configNum+1 {
				kv.LogPrintf("Found config changes: old %+v new %+v, submit append", kv.configNum, nextCfg)
				submitCfgChange()
			} else if nextCfg.Num == kv.configNum { // no new cfg
			} else {
				panic(fmt.Sprintf("bad config and nextCfg, curent %+v, next %+v", kv.configNum, nextCfg))
			}
		}

		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

type ConfigNumArgs struct {
	GID int
	Num int
}

type ConfigNumReply struct {
}

func (kv *ShardKV) ConfigNum(args *ConfigNumArgs, _ *ConfigNumReply) {
	// kv.LogPrintf("receve config num %+v", args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Num > kv.configNum { // do nothing otherwise will skip the future update
		return
	}
	if _, ok := kv.globalConfigNum[args.GID]; !ok {
		kv.globalConfigNum[args.GID] = args.Num
	}
	cfgNumBefore := kv.globalConfigNum[args.GID]
	if args.Num <= cfgNumBefore { // no update
		return
	}
	cfgNumAfter := args.Num
	kv.globalConfigNum[args.GID] = args.Num
	if args.GID == kv.gid {
		return
	}

	getKeyNum := func() int {
		keyNum := 0
		for _, shardDB := range kv.db {
			keyNum += len(shardDB.DB)
		}
		return keyNum
	}

	kv.LogPrintf("gid %d cfg num has changed. before: %d after %d. current cfg %d.Start shard GC. keys in db %d", args.GID, cfgNumBefore, cfgNumAfter, kv.configNum, getKeyNum())
	kv.LogPrintf("now global confnum %+v", kv.globalConfigNum)
	var shardsend [shardmaster.NShards]int

	var cfgHistory []shardmaster.Config
	for i := cfgNumBefore; kv.isReConf && i <= kv.configNum+1 || i <= kv.configNum; i++ {
		cfgHistory = append(cfgHistory, kv.mck.Query(i))
	}

	// if is reconf should set i < config.num + 1 to avoid delete of the comming shard
	for i := cfgNumBefore; kv.isReConf && i < kv.configNum+1 || i < kv.configNum; i++ {
		kv.LogPrintf("cfg %d %+v, nextCfg %+v", i, cfgHistory[i-cfgNumBefore].Shards, cfgHistory[i+1-cfgNumBefore].Shards)
		shardI := cfgHistory[i-cfgNumBefore].Shards
		shardI1 := cfgHistory[i+1-cfgNumBefore].Shards
		for j := 0; j < shardmaster.NShards; j++ {
			if i < cfgNumAfter {
				if shardI[j] == kv.gid && shardI1[j] == args.GID { // goes out
					shardsend[j]++
				}
			}
			if shardI[j] != kv.gid && shardI1[j] == kv.gid { // goes in
				if shardsend[j] > 0 {
					shardsend[j]--
				}
			}
		}
	}
	shardNoNeed := make(map[int]int)
	for shard, send := range shardsend {
		if send > 0 {
			shardNoNeed[shard] = 1
		}
	}
	kv.LogPrintf("shards no need any longer: %+v", shardNoNeed)

	// filter out
	for shardId, _ := range shardNoNeed {
		delete(kv.db, shardId)
	}

	if kv.lastMsgIndex != -1 && kv.maxRaftState > 0 { // do snapshot right now , just to pass the test
		go func() {
			kv.mu.Lock() // block to make sure snapshot and index are consistent
			snapshot := kv.NoLockSnapshot()
			index := kv.lastMsgIndex
			kv.mu.Unlock()
			// block to avoid triggering snapshot manytimes
			kv.rf.ReceiveSnapshot(snapshot, index)
		}()
	}
	kv.LogPrintf("keys after gc %d", getKeyNum())
}

func (kv *ShardKV) ConfigBroadcastLoop() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		var args ConfigNumArgs
		var reply ConfigNumReply
		var servers []string // all servers in current cfg
		args.GID = kv.gid
		kv.mu.Lock()
		groups := kv.mck.Query(kv.configNum).Groups
		for _, srv := range groups {
			servers = append(servers, srv...)
		}
		args.Num = kv.configNum
		kv.mu.Unlock()
		for _, server := range servers {
			go func(s string) {
				kv.make_end(s).Call("ShardKV.ConfigNum", &args, &reply)
				// kv.LogPrintf("configNum %d send to server %s", args.Num, s)
			}(server) // if not ok, doesn't matter
		}
	}
}

func (kv *ShardKV) PendingGC(currentTerm int) {
	// gc for pending only called when saw op not self

	// calling getstate in server is dangerous for deadlock
	// currentTerm, _ := kv.rf.GetState()
	kv.LogPrintf("Starting GC: current term is %d", currentTerm)
	var pendingsToFail []*Pending

	kv.mu.Lock()
	if len(kv.pendingByOpId) != 0 {
		for _, p := range kv.pendingByOpId {
			// old term pending exists means failure
			// should never gc term >= current even is not leader at line 252
			// since it can become leader immediately with a higer term > currentTerm
			if p.term < currentTerm {
				pendingsToFail = append(pendingsToFail, p)
			}
		}
		for _, p := range pendingsToFail {
			kv.DeletePending(p) // delete all pending
		}
	}
	kv.mu.Unlock()
	for _, p := range pendingsToFail {
		kv.LogPrintf("Pending %+v is marked as fail since it's term too old. current term %d", p, currentTerm)
		p.notifyCh <- NotifyMsg{NotifyMsgInfoNotLeader, ""}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxRaftState bytes, in order to allow Raft to garbage-collect its
// log. if maxRaftState is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mu.Lock()
	kv.pendingByOpId = make(map[string]*Pending)
	kv.raftNowTerm = 0
	kv.successRecords = make(map[string]OpRecord)
	kv.mu.Unlock()

	kv.db = make(map[int]ShardDB)

	// buffer with fix size, exceed will block raft
	kv.bufferChan = make(chan raft.ApplyMsg, 10000)

	// about config changes
	kv.isReConf = false
	kv.cfgPending = make(map[int]bool)
	kv.cfgPendingCh = make(chan CfgNotifyMsg)

	kv.globalConfigNum = make(map[int]int)

	kv.lastMsgIndex = -1

	kv.InstallSnapshot(persister.ReadSnapshot())

	go kv.BufferedLogReader()
	go kv.LogApplier()
	go kv.ConfigPoller()
	go kv.MigrationLoop()
	go kv.ConfigBroadcastLoop()

	return kv
}
