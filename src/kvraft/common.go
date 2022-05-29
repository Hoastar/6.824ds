package kvraft

import (
	"log"
	"strconv"
	"strings"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// OperationArgs 请求参数
// Put or Append or Get
type OperationArgs struct {
	Key   string
	Value string
	// 请求操作类型
	Op string // "Put" or "Append" or "Get"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 请求唯一id
	Id string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

// OperationReply 请求响应
type OperationReply struct {
	// 错误类型
	Err Err
	// 响应值
	Value string
}

// 预先顶一个的操作类型常量
const (
	OpTypeGet    = "get"
	OpTypePut    = "put"
	OpTypeAppend = "append"
)

// splitId 切分请求id,返回clerk id与 clerk 请求序列号
func splitId(opId string) (string, int) {
	ss := strings.Split(opId, ":")
	client := ss[0]
	seq, err := strconv.Atoi(ss[1])
	if err != nil {
		log.Fatalf("Parse int fail %s", client)
	}
	return client, seq
}
