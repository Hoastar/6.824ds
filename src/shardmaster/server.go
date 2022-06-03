package shardmaster

import (
	"6.824ds/src/labgob"
	"6.824ds/src/labrpc"
	"6.824ds/src/raft"
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpTypeJoin  = "join"
	OpTypeLeave = "leave"
	OpTypeMove  = "move"
	OpTypeQuery = "query"
)

const (
	NotifyMsgInfoQueryOK   = "queryOk"
	NotifyMsgInfoJoinOK    = "joinOk"
	NotifyMsgInfoLeaveOK   = "leaveOk"
	NotifyMsgInfoMoveOK    = "moveOk"
	NotifyMsgInfoNotLeader = "notLeader"
)

type OpMoveArgs struct {
	Shard int
	GID   int
}

type Op struct {
	// Your data here.
	OpType    string
	JoinArgs  map[int][]string
	LeaveArgs []int
	MoveArgs  OpMoveArgs
	QueryArgs int
	OpId      string
}

type NotifyMsg struct {
	Info     string
	RetValue interface{}
}

type Pending struct {
	OpId     string
	index    int
	term     int
	notifyCh chan NotifyMsg
}

type OpRecord struct {
	OpSeq  int
	NtfMsg NotifyMsg
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	// To determine is still leader
	// raftIndex   int
	// latest term server can observe from applyMsg
	raftNowTerm int64

	// notification when command applied
	pendingByOpId map[string]*Pending

	configs []Config // indexed by config num

	// op record clientid -> client op
	successRecords map[string]OpRecord

	// silly way to avoid blocking the chan
	bufferChan chan raft.ApplyMsg
}

func (sm *ShardMaster) MakePending(opId string, index int, term int) *Pending {
	p := &Pending{}
	p.OpId = opId
	p.index = index
	p.term = term
	p.notifyCh = make(chan NotifyMsg)

	sm.pendingByOpId[opId] = p
	return p
}

func (sm *ShardMaster) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Printf("[ShardMaster %d]: %s", sm.me, s)
	}
	return
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

func (sm *ShardMaster) FindRecordAndSet(opId string, setReplyOk func(NotifyMsg)) bool {

	found := false
	cl, seq := splitId(opId)
	sm.mu.Lock()
	clRecord, ok := sm.successRecords[cl]
	if ok && clRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d", cl, clRecord.OpSeq, clRecord.NtfMsg, seq)
	} else if ok && clRecord.OpSeq == seq { // found existing record ,this is dup request
		found = true
		setReplyOk(clRecord.NtfMsg)
	} else {
		// nothing found
	}
	sm.mu.Unlock()
	return found
}

func (sm *ShardMaster) SaveRecord(opId string, notifyMsg NotifyMsg) {
	// only save success op
	sm.LogPrintf("Saving the completed record id %s msg %+v", opId, notifyMsg)
	cl, seq := splitId(opId)
	sm.mu.Lock()
	clRecord, ok := sm.successRecords[cl]
	if ok && clRecord.OpSeq > seq {
		log.Fatalf("client id %s: stored opId > current opId. stored %d %+v, current %d %+v", cl, clRecord.OpSeq, clRecord.NtfMsg, seq, notifyMsg)
	}
	sm.successRecords[cl] = OpRecord{seq, notifyMsg}
	sm.mu.Unlock()
}

func (sm *ShardMaster) DoOp(commandOp Op, setReplyErr func(), setReplyOk func(NotifyMsg)) {
	// setReplyErr: err since not leader

	sm.mu.Lock()
	term, ld := sm.rf.GetState()
	rejectNotLeader := false
	nowTerm := atomic.LoadInt64(&sm.raftNowTerm)

	if !ld {
		sm.LogPrintf("Not leader, reject with ErrWronleader")
		rejectNotLeader = true
	} else if nowTerm < int64(term) { // leader haven't seen all the applys from prev term
		rejectNotLeader = true
		sm.LogPrintf("New leader haven't seen it's current term: %d, max term saw %d", term, nowTerm)
	}
	sm.mu.Unlock()
	if rejectNotLeader {
		setReplyErr()
		return
	}

	// check if is dup record
	if sm.FindRecordAndSet(commandOp.OpId, setReplyOk) {
		sm.LogPrintf("Found op %+v called previously, return directly", commandOp)
		return
	}

	// lock to make sure server won't try to find the pending(fast receive from raft) before the pending is created
	sm.mu.Lock()

	// CAUTION: Start will block! should avoid dead lock!
	index, submitTerm, isLeader := sm.rf.Start(commandOp)

	if !isLeader {
		sm.LogPrintf("Not leader when calling Start(), reject with ErrWronleader")
		setReplyErr()
		sm.mu.Unlock()
		return
	}
	sm.LogPrintf("Receive op %+v from client", commandOp)

	pending := sm.MakePending(commandOp.OpId, index, submitTerm)
	sm.mu.Unlock()

	notifyMsg := <-pending.notifyCh // must wait for the notification, even lose the leader state
	if notifyMsg.Info == NotifyMsgInfoNotLeader {
		setReplyErr()
		sm.LogPrintf("Op %+v failed since no longer leader: %+v", commandOp, notifyMsg)
	} else {
		setReplyOk(notifyMsg)
		sm.LogPrintf("Got notification op %+v success: %+v", commandOp, notifyMsg)
	}
}

func (sm *ShardMaster) DeletePending(p *Pending) { // should be called when holding mu
	delete(sm.pendingByOpId, p.OpId)
}

func (sm *ShardMaster) NotifyOpDone(opId string, index int, successmsg NotifyMsg, opterm int) {
	sm.mu.Lock()
	if len(sm.pendingByOpId) == 0 { // nobody is waiting for that command
		sm.mu.Unlock()
		sm.LogPrintf("Nothing pending, quit for op %s, index %d", opId, index)
		return
	}

	var p *Pending
	var ok bool
	var notifyMsg NotifyMsg
	issuedBySelf := false
	if p, ok = sm.pendingByOpId[opId]; ok {
		sm.LogPrintf("Notifying pending %+v since op success", p)
		notifyMsg = successmsg
		sm.DeletePending(p)
		issuedBySelf = true
	} else { // not found, not this server's pendingsm.LogPrintf("Pending opId %s index %d not found in pending table", opId, index)
		p = nil
	}
	sm.mu.Unlock()
	if p != nil {
		go func() { p.notifyCh <- notifyMsg }() // non blocking for receive fromm apply
	}

	if !issuedBySelf {
		sm.PendingGC(opterm) //seems to be ok to run async. but i don't
	}
}

func (sm *ShardMaster) BufferedLogReader() {
	for !sm.killed() {
		applyMsg := <-sm.applyCh
		sm.bufferChan <- applyMsg
	}
}

func (sm *ShardMaster) LogApplier() {

	for { // this for should never block!
		select {
		case applyMsg := <-sm.bufferChan:
			sm.LogPrintf("Receiving ApplyMsg from raft %+v", applyMsg)

			if !applyMsg.CommandValid { // not real command, set the term and skip
				atomic.StoreInt64(&sm.raftNowTerm, int64(applyMsg.Term))
				if applyMsg.CommandType == raft.CommandTypeNewLeader { // new leader, need to gc the pending, otherwise test will block forever when only 1 client
					go sm.PendingGC(applyMsg.Term)
				}
				if applyMsg.CommandType == raft.CommandTypeUpdateStateMachine {
					data := applyMsg.Command.([]byte)
					// gc before snapshot is installed
					sm.PendingGC(applyMsg.Term)
					sm.InstallSnapshot(data)
				}
				continue
			}

			commandOp := applyMsg.Command.(Op)

			ret := NotifyMsg{"", nil} //default ret ok

			// logic for updating database
			sm.mu.Lock()
			applyCommand(sm, commandOp, &ret)
			sm.mu.Unlock()

			// set the request as the latest op for the client, for eleminate dup request
			sm.SaveRecord(commandOp.OpId, ret)

			// set the max term seem, so that whenever maxterm >= term saw by getState()
			// that means the kv-server has seen all request from last server
			atomic.StoreInt64(&sm.raftNowTerm, int64(applyMsg.Term))

			stateSize := sm.persister.RaftStateSize()
			if sm.maxRaftState > 0 && stateSize > int(float64(sm.maxRaftState)*0.8) { // 0.8 as ratio
				sm.rf.LogPrintf("maxRaftState %d, stateSize %d, do snapshot", sm.maxRaftState, stateSize)
				snapshot := sm.Snapshot()
				index := applyMsg.LogIndex
				// block to avoid triggering snapshot many times
				sm.rf.ReceiveSnapshot(snapshot, index)
			}
			go sm.NotifyOpDone(commandOp.OpId, applyMsg.CommandIndex, ret, applyMsg.Term)

			// here is the logic to quit
		case <-time.After(time.Millisecond * 100):
			if sm.killed() {
				return
			}
		}

	}
}

func (sm *ShardMaster) PendingGC(currentTerm int) {
	// gc for pending only called when saw op not self

	// calling getstate in server is dangerous for deadlock
	// currentTerm, _ := sm.rf.GetState()
	sm.LogPrintf("Starting GC: current term is %d", currentTerm)
	var pendingsToFail []*Pending

	sm.mu.Lock()
	if len(sm.pendingByOpId) != 0 {
		for _, p := range sm.pendingByOpId {
			// old term pending exists means failure
			// should never gc term >= current even is not leader at line 252
			// since it can become leader immediately with a higer term > currentTerm
			if p.term < currentTerm {
				pendingsToFail = append(pendingsToFail, p)
			}
		}
		for _, p := range pendingsToFail {
			sm.DeletePending(p) // delete all pending
		}
	}
	sm.mu.Unlock()
	for _, p := range pendingsToFail {
		sm.LogPrintf("Pending %+v is marked as fail since it's term too old. current term %d", p, currentTerm)
		p.notifyCh <- NotifyMsg{NotifyMsgInfoNotLeader, nil}
	}
}

func (sm *ShardMaster) Snapshot() []byte {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	sm.mu.Lock()
	e.Encode(sm.raftNowTerm)
	e.Encode(sm.configs)
	e.Encode(sm.successRecords)
	sm.mu.Unlock()

	return w.Bytes()
}

func (sm *ShardMaster) InstallSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	sm.mu.Lock()
	// supress warning of decoding into non zero value
	sm.raftNowTerm = 0
	sm.configs = nil
	sm.successRecords = nil
	if d.Decode(&sm.raftNowTerm) != nil ||
		d.Decode(&sm.configs) != nil || d.Decode(&sm.successRecords) != nil {
		log.Fatalf("Decode persistent error!")
	} else {
		sm.LogPrintf("Reading from snapshot, raftNowTerm %d, db %+v, successRecords %+v", sm.raftNowTerm, sm.configs, sm.successRecords)
	}
	sm.mu.Unlock()

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var commandOp Op
	commandOp.JoinArgs = args.Servers
	commandOp.OpType = OpTypeJoin
	commandOp.OpId = args.Id

	setReplyErr := func() {
		reply.WrongLeader = true
	}

	setReplyOk := func(nmsg NotifyMsg) {
		if nmsg.Info != NotifyMsgInfoJoinOK {
			panic(fmt.Sprintf("bad notify %+v", nmsg))
		}
		reply.Err = OK
	}
	sm.DoOp(commandOp, setReplyErr, setReplyOk)

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var commandOp Op
	commandOp.LeaveArgs = args.GIDs
	commandOp.OpType = OpTypeLeave
	commandOp.OpId = args.Id

	setReplyErr := func() {
		reply.WrongLeader = true
	}

	setReplyOk := func(nmsg NotifyMsg) {
		if nmsg.Info != NotifyMsgInfoLeaveOK {
			panic(fmt.Sprintf("bad notify %+v", nmsg))
		}
		reply.Err = OK
	}
	sm.DoOp(commandOp, setReplyErr, setReplyOk)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var commandOp Op
	commandOp.MoveArgs = OpMoveArgs{args.Shard, args.GID}
	commandOp.OpType = OpTypeMove
	commandOp.OpId = args.Id

	setReplyErr := func() {
		reply.WrongLeader = true
	}

	setReplyOk := func(nmsg NotifyMsg) {
		if nmsg.Info != NotifyMsgInfoMoveOK {
			panic(fmt.Sprintf("bad notify %+v", nmsg))
		}
		reply.Err = OK
	}
	sm.DoOp(commandOp, setReplyErr, setReplyOk)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var commandOp Op
	commandOp.QueryArgs = args.Num
	commandOp.OpType = OpTypeQuery
	commandOp.OpId = args.Id

	// query optimization if query num < cfg len retrun directly
	sm.mu.Lock()
	if args.Num < len(sm.configs) && args.Num >= 0 {
		CopyConfig(&reply.Config, &sm.configs[args.Num])
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	setReplyErr := func() {
		reply.WrongLeader = true
	}

	setReplyOk := func(nmsg NotifyMsg) {
		if nmsg.Info != NotifyMsgInfoQueryOK {
			panic(fmt.Sprintf("bad notify %+v", nmsg))
		}
		retconfig := nmsg.RetValue.(Config)
		reply.Config = retconfig
		reply.Err = OK
	}
	sm.DoOp(commandOp, setReplyErr, setReplyOk)
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	return false
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.persister = persister

	sm.mu.Lock()
	sm.pendingByOpId = make(map[string]*Pending)
	sm.raftNowTerm = 0
	sm.mu.Unlock()

	sm.successRecords = make(map[string]OpRecord)

	// buffer with fix size, exceed will block raft
	sm.bufferChan = make(chan raft.ApplyMsg, 10000)

	sm.InstallSnapshot(persister.ReadSnapshot())

	go sm.BufferedLogReader()
	go sm.LogApplier()
	go func() {
		for !sm.killed() {
			time.Sleep(time.Millisecond * 100)
			sm.mu.Lock()
			sm.LogPrintf("===I am not in deadlock===")
			sm.mu.Unlock()
		}
	}()

	return sm
}
