package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.824ds/src/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	id             string
	lastLeader     int64
	requestCounter int64
	mu             sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.lastLeader = 0

	// gen unique id by now % seconds and obj addr
	ck.id = fmt.Sprintf("%d%p", time.Now().UnixNano()%10e9, &ck)
	atomic.StoreInt64(&ck.requestCounter, 0)
	return ck
}

func (ck *Clerk) GenRequestId() string {
	rid := atomic.AddInt64(&ck.requestCounter, 1)
	return fmt.Sprintf("%s:%d", ck.id, rid)
}

const Debug = 0

func (ck *Clerk) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (ck *Clerk) DoRequest(servicename string, args interface{}, reply interface{}, resetReply func(), isWrongLeader func() bool) {
	// args and reply are pointers

	// serialized request

	ck.mu.Lock()
	lastLeader := atomic.LoadInt64(&ck.lastLeader)
	for {
		resetReply()
		ck.LogPrintf("Calling %s to server %d with args %+v", servicename, lastLeader, args)
		ok := ck.servers[lastLeader].Call(servicename, args, reply)
		ck.LogPrintf("Calls return ok: %t reply: %+v", ok, reply)
		// unaccessible or wrong leader
		if ok && isWrongLeader() {
			atomic.StoreInt64(&ck.lastLeader, lastLeader)
			break
		} else { // next server if return fail or wrong server. An unacesssible server will return fail
			time.Sleep(time.Millisecond * 20) // don't ask next server too quick
			lastLeader = (lastLeader + 1) % int64(len(ck.servers))
		}
		// not ok, should retry the same leader
	}
	ck.mu.Unlock()

	ck.LogPrintf("%s request %+v got reply from server %d: %+v", servicename, args, lastLeader, reply)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.Id = ck.GenRequestId()

	var reply QueryReply
	resetReply := func() {
		reply = QueryReply{}
	}
	isWrongLeader := func() bool {
		return reply.WrongLeader == false
	}

	ck.DoRequest("ShardMaster.Query", args, &reply, resetReply, isWrongLeader)

	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Id = ck.GenRequestId()

	var reply JoinReply
	resetReply := func() {
		reply = JoinReply{}
	}
	isWrongLeader := func() bool {
		return reply.WrongLeader == false
	}

	ck.DoRequest("ShardMaster.Join", args, &reply, resetReply, isWrongLeader)

}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Id = ck.GenRequestId()

	var reply LeaveReply
	resetReply := func() {
		reply = LeaveReply{}
	}
	isWrongLeader := func() bool {
		return reply.WrongLeader == false
	}

	ck.DoRequest("ShardMaster.Leave", args, &reply, resetReply, isWrongLeader)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.Id = ck.GenRequestId()

	var reply MoveReply
	resetReply := func() {
		reply = MoveReply{}
	}
	isWrongLeader := func() bool {
		return reply.WrongLeader == false
	}

	ck.DoRequest("ShardMaster.Move", args, &reply, resetReply, isWrongLeader)
}
