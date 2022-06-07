package shardkv

import (
	"6.824ds/src/labrpc"
	"6.824ds/src/shardmaster"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	lastleaders    map[int]int
	id             string
	requestcounter int64

	mu sync.Mutex
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.lastleaders = make(map[int]int)
	// gen unique id by now % seconds and obj addr
	// ck.id = fmt.Sprintf("", time.Now().UnixNano()%10e9, &ck)
	ck.id = randstring(5)
	atomic.StoreInt64(&ck.requestcounter, 0)

	return ck
}

func (ck *Clerk) GenRequestId() string {
	rid := atomic.AddInt64(&ck.requestcounter, 1)
	return fmt.Sprintf("%s:%d", ck.id, rid)
}

func (ck *Clerk) LogPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Printf("[client %s] %s", ck.id, s)
	}
	return
}

func (ck *Clerk) DoRequest(servicename string, args *OperationArgs, reply *OperationReply) {
	// args and reply are pointers

	// serialized request

	ck.mu.Lock()
	args.Id = ck.GenRequestId()
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]

		if _, ok := ck.lastleaders[gid]; !ok {
			ck.lastleaders[gid] = 0
		}
		lastleader := ck.lastleaders[gid]

		if servers, ok := ck.config.Groups[gid]; ok {

			for notok := 0; notok < len(servers); lastleader = (lastleader + 1) % len(servers) {
				srv := ck.make_end(servers[lastleader])

				*reply = OperationReply{} // set reply to emtpy everytime

				ck.LogPrintf("Calling %s to gid %d server %d with shard %d args %+v", servicename, gid, lastleader, shard, args)
				ok := srv.Call(servicename, args, reply)
				ck.LogPrintf("Calls return ok: %t reply: %+v", ok, reply)
				// unaccessible or wrong leader
				if ok && reply.Err == ErrWrongLeader {
					// try next leader
				} else if ok && reply.Err == ErrWrongGroup { // next server if return fail or wrong server. An unacesssible server will return fail
					//TODO need to inc reques id?
					break
				} else if ok { // good!
					ck.lastleaders[gid] = lastleader
					ck.LogPrintf("request %+v got reply from server %d: %+v", args, lastleader, reply)
					ck.mu.Unlock()
					return
				} else { // not ok try next, maybe group has shutdown
					notok++
				}
				time.Sleep(time.Millisecond * 20) // don't ask next server too quick
			}
		}

		time.Sleep(100 * time.Millisecond)

		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)

	}

}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := OperationArgs{}
	args.Key = key
	args.Op = OpTypeGet

	var reply OperationReply

	ck.DoRequest("ShardKV.Get", &args, &reply)

	return reply.Value
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := OperationArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	var reply OperationReply
	ck.DoRequest("ShardKV.PutAppend", &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
