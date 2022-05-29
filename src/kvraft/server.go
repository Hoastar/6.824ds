package kvraft

import (
	"6.824ds/src/labgob"
	"6.824ds/src/labrpc"
	"6.824ds/src/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// 消息通知常量
const (
	NotifyMsgInfoNoKey     = "noKey"
	NotifyMsgInfoGetOK     = "getOk"
	NotifyMsgInfoPutOK     = "putOk"
	NotifyMsgInfoAppendOK  = "appendOk"
	NotifyMsgInfoNotLeader = "notLeader"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op operation
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	OpId   string
}

type NotifyMsg struct {
	Info  string
	Value string
}

// Pending 代办的operation信息
type Pending struct {
	OpId  string
	index int
	term  int
	// 通知
	notifyCh chan NotifyMsg
}

// OpRecord clerk id operation result
type OpRecord struct {
	// clerk request counter
	OpSeq int
	// reply msg
	NtfMsg NotifyMsg
}

// KVServer 结构体
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big；threshold 阈值

	// Your definitions here.

	// 持久化存储
	persister *raft.Persister

	// To determine is still leader
	// raftIndex   int
	// latest term server can observe from applyMsg
	// 当前leader raft实例的任期
	raftNowTerm int64

	// notification when command applied
	pendingByOpId map[string]*Pending

	// kv database,数据库存储
	db map[string]string

	// op record clientId -> client op
	// operation 记录；保证线性化处理operation
	successRecords map[string]OpRecord

	// silly way to avoid blocking the chan
	// 避免阻塞的bufferChan
	bufferChan chan raft.ApplyMsg
}

// MakePending 创建一个待处理或者待回复的事项针对此次op
func (kv *KVServer) MakePending(opId string, index int, term int) *Pending {
	p := &Pending{}
	p.OpId = opId
	p.index = index
	p.term = term
	p.notifyCh = make(chan NotifyMsg)

	kv.pendingByOpId[opId] = p
	return p
}

func (kv *KVServer) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if raft.Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Printf("[Server %d]: %s", kv.me, s)
	}
	return
}

// FindRecordAndSet 发现是否已经是被成功记录的op
func (kv *KVServer) FindRecordAndSet(opId string, reply *OperationReply, setReplyOK func(*OperationReply, NotifyMsg)) bool {

	found := false
	// return clerkId and seq（请求序列号）
	cl, seq := splitId(opId)
	kv.mu.Lock()
	clientRecord, ok := kv.successRecords[cl]
	if ok && clientRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d", cl, clientRecord.OpSeq, clientRecord.NtfMsg, seq)
	} else if ok && clientRecord.OpSeq == seq { // found existing record ,this is dup request 这是已经被处理的请求
		found = true
		setReplyOK(reply, clientRecord.NtfMsg)
	} else {
		// nothing found
	}
	kv.mu.Unlock()
	return found
}

func (kv *KVServer) SaveRecord(opId string, notifyMsg NotifyMsg) {
	// only save success op
	kv.LogPrintf("Saving the completed record id %s msg %+v", opId, notifyMsg)
	cl, seq := splitId(opId)
	kv.mu.Lock()
	clientRecord, ok := kv.successRecords[cl]
	if ok && clientRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d %+v", cl, clientRecord.OpSeq, clientRecord.NtfMsg, seq, notifyMsg)
	}
	kv.successRecords[cl] = OpRecord{seq, notifyMsg}
	kv.mu.Unlock()
}

// DoOp 处理这条operation
func (kv *KVServer) DoOp(args *OperationArgs, reply *OperationReply, setReplyOK func(*OperationReply, NotifyMsg)) {
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
	// 加载当前KV-Server存储的Leader term
	nowTerm := atomic.LoadInt64(&kv.raftNowTerm)

	// 如果不是leader，reply err
	if !isLeader {
		kv.LogPrintf("Not leader, reject with ErrWronleader")
		rejectNotLeader = true
	} else if nowTerm < int64(term) { // leader haven't seen all the applys from prev term; 存储的任期小于当前绑定的raft的term
		rejectNotLeader = true
		kv.LogPrintf("New leader haven't seen it's current term: %d, max term saw %d", term, nowTerm)
	}
	kv.mu.Unlock()
	if rejectNotLeader {
		// return err
		setReplyErr(reply)
		return
	}

	// check if is dup record
	// clerk request 去重操作，如果处理过就直接返回之前处理的结果
	if kv.FindRecordAndSet(args.Id, reply, setReplyOK) {
		kv.LogPrintf("Found args %+v called previously, return %+v", args, reply)
		return
	}

	// lock to make sure server won't try to find the pending(fast receive from raft) before the pending is created
	kv.mu.Lock()

	// CAUTION: Start will block! should avoid dead lock!
	index, submitTerm, isLeader := kv.rf.Start(commandOp)

	if !isLeader {
		kv.LogPrintf("Not leader when calling Start(), reject with ErrWronleader")
		setReplyErr(reply)
		kv.mu.Unlock()
		return
	}
	kv.LogPrintf("Receive op %+v from client", commandOp)

	// 创建待处理事项
	pending := kv.MakePending(commandOp.OpId, index, submitTerm)
	kv.mu.Unlock()

	// 阻塞，等待返回该条op的通知消息
	notifyMsg := <-pending.notifyCh // must wait for the notification, even lose the leader state
	if notifyMsg.Info == NotifyMsgInfoNotLeader {
		setReplyErr(reply)
		kv.LogPrintf("Op %+v failed since no longer leader: %+v", commandOp, notifyMsg)
	} else {
		setReplyOK(reply, notifyMsg)
		kv.LogPrintf("Got notification op %+v success: %+v", commandOp, notifyMsg)
	}
}

// Get RPC handle
func (kv *KVServer) Get(args *OperationArgs, reply *OperationReply) {
	// Your code here.
	// Get 方法内部抽象的预处理replyOK
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

func (kv *KVServer) PutAppend(args *OperationArgs, reply *OperationReply) {
	// Your code here.

	kv.DoOp(args, reply, func(reply *OperationReply, _ NotifyMsg) { reply.Err = OK })
}

// DeletePending 删除事项
func (kv *KVServer) DeletePending(p *Pending) { // should be called when holding mu
	delete(kv.pendingByOpId, p.OpId)
}

// NotifyOpDone 给创建的待处理事项进行结果通知
func (kv *KVServer) NotifyOpDone(opId string, index int, successMsg NotifyMsg, opTerm int) {
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
	// 判断是否存在key为opId的pending事项
	if p, ok = kv.pendingByOpId[opId]; ok {
		kv.LogPrintf("Notifying pending %+v since op success", p)
		notifyMsg = successMsg
		// 删除pending事项
		kv.DeletePending(p)
		issuedBySelf = true
	} else { // not found, not this server's pending
		kv.LogPrintf("Pending opId %s index %d not found in pending table", opId, index)
		p = nil
	}
	kv.mu.Unlock()
	if p != nil {
		// 异步通知结果
		go func() { p.notifyCh <- notifyMsg }() // non block for receive from apply
	}

	if !issuedBySelf {
		kv.PendingGC(opTerm) //seems to be ok to run async. but I don't understand
	}

}

func (kv *KVServer) BufferedLogReader() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		kv.bufferChan <- applyMsg
	}
}

// LogApplier daemon
func (kv *KVServer) LogApplier() {

	for { // this for should never block!
		select {
		case applyMsg := <-kv.bufferChan: //
			kv.LogPrintf("Receiving ApplyMsg from raft %+v", applyMsg)

			if !applyMsg.CommandValid { // not real command, set the term and skip
				// 不是可用的command，set new term
				atomic.StoreInt64(&kv.raftNowTerm, int64(applyMsg.Term))
				if applyMsg.CommandType == raft.CommandTypeNewLeader { // new leader, need to gc the pending, otherwise test will block forever when only 1 client
					// 旧任期的pending意味着失败
					go kv.PendingGC(applyMsg.Term)
				}
				if applyMsg.CommandType == raft.CommandTypeUpdateStateMachine {
					data := applyMsg.Command.([]byte)
					// gc before snapshot is installed
					kv.PendingGC(applyMsg.Term)

					// 安装快照
					kv.InstallSnapshot(data)
				}
				continue
			}

			commandOp := applyMsg.Command.(Op)

			ret := NotifyMsg{"", ""} //default ret ok

			// logic for updating database
			switch commandOp.OpType {
			case OpTypeGet:
				value, ok := kv.db[commandOp.Key] // if no key, empty
				if ok {
					ret.Value = value
					ret.Info = NotifyMsgInfoGetOK
				} else {
					ret.Info = NotifyMsgInfoNoKey
				}
			case OpTypePut:
				kv.db[commandOp.Key] = commandOp.Value
				ret.Info = NotifyMsgInfoPutOK
			case OpTypeAppend:
				oldValue, _ := kv.db[commandOp.Key]
				kv.db[commandOp.Key] = oldValue + commandOp.Value
				ret.Info = NotifyMsgInfoAppendOK
			}

			// set the request as the latest op for the client, Remove duplicate dup requests
			// 去重
			kv.SaveRecord(commandOp.OpId, ret)

			// set the max term seem, so that whenever maxTerm >= term saw by getState()
			// that means the kv server has seen all request from last server
			atomic.StoreInt64(&kv.raftNowTerm, int64(applyMsg.Term))

			// 3B: log compression
			stateSize := kv.persister.RaftStateSize()
			if kv.maxraftstate > 0 && stateSize > int(float64(kv.maxraftstate)*0.8) { // 0.8 as ratio
				kv.rf.LogPrintf("maxraftstate %d, stateSize %d, do snapshot", kv.maxraftstate, stateSize)

				snapshot := kv.Snapshot()
				index := applyMsg.LogIndex
				// block to avoid triggering snapshot many times
				kv.rf.ReceiveSnapshot(snapshot, index)
			}

			// 异步对处理完的operation进行通知
			go kv.NotifyOpDone(commandOp.OpId, applyMsg.CommandIndex, ret, applyMsg.Term)

			// here is the logic to quit
		case <-time.After(time.Millisecond * 100): //TODO possible raft is tring to apply
			if kv.killed() {
				return
			}
		}

	}
}

// PendingGC 释放之前新建的待处理事项
func (kv *KVServer) PendingGC(currentTerm int) {
	// gc for pending only called when saw op not self
	kv.LogPrintf("Starting GC: current term is %d", currentTerm)
	var pendingsToFail []*Pending

	kv.mu.Lock()
	if len(kv.pendingByOpId) != 0 {
		for _, p := range kv.pendingByOpId {
			// old term pending exists means failure
			// should never gc term >= current even is not leader at line 252
			// since it can become leader immediately with a higer term > currentTerm

			// 如果存在旧任期的pending，那么就意味这在currentTerm之前的pending皆失败
			if p.term < currentTerm {
				pendingsToFail = append(pendingsToFail, p)
			}
		}
		for _, p := range pendingsToFail {
			kv.DeletePending(p) // delete all pending record
		}
	}
	kv.mu.Unlock()
	for _, p := range pendingsToFail {
		kv.LogPrintf("Pending %+v is marked as fail since it's term too old. current term %d", p, currentTerm)
		// 将原因写入这些被删除的pending的通道
		p.notifyCh <- NotifyMsg{NotifyMsgInfoNotLeader, ""}
	}
}

// Snapshot 快照存储需要持久化的属性
func (kv *KVServer) Snapshot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	// 编码当前raft leader instance term
	e.Encode(kv.raftNowTerm)
	// 当前db存储
	e.Encode(kv.db)

	// 成功op记录
	e.Encode(kv.successRecords)
	kv.mu.Unlock()

	return w.Bytes()
}

// InstallSnapshot 安装kv-Server的快照
func (kv *KVServer) InstallSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	// supress warning of decoding into non zero value
	kv.raftNowTerm = 0
	kv.db = nil
	kv.successRecords = nil

	// 解码到该raft的需要持久化的对象属性
	if d.Decode(&kv.raftNowTerm) != nil ||
		d.Decode(&kv.db) != nil || d.Decode(&kv.successRecords) != nil {
		log.Fatalf("Decode persistent error!")
	} else {
		kv.LogPrintf("Reading from snapshot, raftNowTerm %d, db %+v, successRecords %+v", kv.raftNowTerm, kv.db, kv.successRecords)
	}
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

// StartKVServer 启动 KV-Server
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// 初始化一个KVServer instance
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	// The communication channel between kv-server and raft instance
	// kv-server与raft通信的通道
	kv.applyCh = make(chan raft.ApplyMsg)

	// 给kv Server绑定一个raft instance
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mu.Lock()

	// 待通知的op记录表
	kv.pendingByOpId = make(map[string]*Pending)

	// kv默认的leader的任期
	kv.raftNowTerm = 0
	kv.mu.Unlock()

	kv.db = make(map[string]string)

	// 已完成的op记录表
	kv.successRecords = make(map[string]OpRecord)

	// buffer with fix size, exceed will block raft
	kv.bufferChan = make(chan raft.ApplyMsg, 10000)

	kv.InstallSnapshot(persister.ReadSnapshot())

	go kv.BufferedLogReader()
	go kv.LogApplier()
	go func() {
		for !kv.killed() {
			time.Sleep(time.Millisecond * 100)
			kv.mu.Lock()
			kv.LogPrintf("--%v am not in deadlock--\n", kv.me)
			kv.mu.Unlock()
		}
	}()

	return kv
}
