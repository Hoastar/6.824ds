package kvraft

import (
	"6.824ds/src/raft"
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
	// You will have to modify this struct.
	id             string // unique id
	lastLeader     int64  // LeaderId in servers
	requestCounter int64  // request counter 请求计数器（序列号）,
	mu             sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk 初始化Clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.lastLeader = 0

	// gen unique id by now % seconds and obj addr
	ck.id = fmt.Sprintf("%d%p", time.Now().UnixNano()%10e9, &ck)
	atomic.StoreInt64(&ck.requestCounter, 0)

	return ck
}

// GenRequestId 在当前clerk的基础之上（在请求序列号原子加一）生成带有新的请求序列号的请求id
func (ck *Clerk) GenRequestId() string {
	rid := atomic.AddInt64(&ck.requestCounter, 1)
	return fmt.Sprintf("%s:%d", ck.id, rid)
}

func (ck *Clerk) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if raft.Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// DoRequest 经过封装的发起请求操作
func (ck *Clerk) DoRequest(serviceName string, args *OperationArgs, reply *OperationReply) {
	// args and reply are pointers

	// serialized request

	ck.mu.Lock()
	// 获取当前Leader id
	lastLeader := atomic.LoadInt64(&ck.lastLeader)
	// 原子操作生成当前clerk的请求 id
	args.Id = ck.GenRequestId()

	// 循环发起一个请求，
	for {
		*reply = OperationReply{} // set reply to 2 everytime
		ck.LogPrintf("Calling %s to server %d with args %+v", serviceName, lastLeader, args)
		// 远程调用
		ok := ck.servers[lastLeader].Call(serviceName, args, reply)
		ck.LogPrintf("Calls return ok: %t reply: %+v", ok, reply)

		// unaccessible or wrong leader
		if ok && reply.Err != ErrWrongLeader { // 正常相应成功。
			atomic.StoreInt64(&ck.lastLeader, lastLeader)
			break
		} else { // next server if return fail or wrong server. An unacesssible server will return fail；
			// 有可能超时，server lost the leadership, server crash
			// 换取server id 发起重试
			time.Sleep(time.Millisecond * 20) // don't ask next server too quick
			lastLeader = (lastLeader + 1) % int64(len(ck.servers))
		}
		// not ok, should retry the same leader
	}
	ck.mu.Unlock()

	ck.LogPrintf("request %+v got reply from server %d: %+v", args, lastLeader, reply)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

// Get clerk的get请求
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// 准备请求参数与相应参数
	args := OperationArgs{}
	args.Key = key
	args.Op = OpTypeGet

	var reply OperationReply

	// 发起get请求
	ck.DoRequest("KVServer.Get", &args, &reply)

	if reply.Err != ErrNoKey {
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

// PutAppend clerk的Put/Append请求封装
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// 准备参数请求与响应参数
	args := OperationArgs{}
	args.Key = key
	args.Value = value
	// 请求类型
	args.Op = op

	var reply OperationReply
	ck.DoRequest("KVServer.PutAppend", &args, &reply)

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
