package raft

import "fmt"

const (
	CommandTypeCommand            = 1
	CommandTypeNewLeader          = 2
	CommandTypeNoop               = 3
	CommandTypeSnapshot           = 4
	CommandTypeUpdateStateMachine = 5
)

type LogEntry struct {
	CommandType  int
	Command      commandT
	CommandIndex int
	Term         int
}

type Log struct { // allow gc for trimmed log
	Entries   []LogEntry
	BaseIndex int
}

func (rl *Log) Get(index int) *LogEntry {
	if index < rl.BaseIndex {
		panic(fmt.Sprintf("Accessing discarded index : %d, first availalbe index %d", index, rl.BaseIndex))
	}
	lastIndex := rl.LastIndex()
	if index > lastIndex {
		panic(fmt.Sprintf("index out of range: %d, last index %d", index, lastIndex))
	}
	return &rl.Entries[index-rl.BaseIndex]
}

// LastIndex returns the index value of the last log entry for the current raft instance
func (rl *Log) LastIndex() int {
	return len(rl.Entries) - 1 + rl.BaseIndex
}

func (rl *Log) Append(entry LogEntry) {
	rl.Entries = append(rl.Entries, entry)
}

func (rl *Log) DiscardBefore(index int) {
	if index < rl.BaseIndex {
		panic(fmt.Sprintf("Try to discard index : %d, but first availalbe index %d", index, rl.BaseIndex))
	}
	lastIndex := rl.LastIndex()
	if index > lastIndex {
		panic(fmt.Sprintf("Try to discard: %d, but last index %d", index, lastIndex))
	}
	// 确定entries长度
	newEntries := make([]LogEntry, len(rl.Entries)+rl.BaseIndex-index)
	// index与l.BaseIndex相差多少，那就将rf.Entries前提多少开始裁剪
	copy(newEntries, rl.Entries[index-rl.BaseIndex:])
	rl.BaseIndex = index
	rl.Entries = newEntries
}

func (rl *Log) SliceFrom(index int) []LogEntry { // return Entries[index:]
	if index < rl.BaseIndex {
		panic(fmt.Sprintf("slice start index : %d, first availalbe index %d", index, rl.BaseIndex))
	}
	lastIndex := rl.LastIndex()
	if index > lastIndex+1 { // the same nextIndex for folllower and leader
		panic(fmt.Sprintf("slice start index: %d, last index %d", index, lastIndex))
	}
	return rl.Entries[index-rl.BaseIndex:]
}

func (rl *Log) TruncateAt(index int) { //discard tail start at index
	if index < rl.BaseIndex {
		panic(fmt.Sprintf("truncate index : %d, first availalbe index %d", index, rl.BaseIndex))
	}
	lastIndex := rl.LastIndex()
	if index > lastIndex+1 { // the same nextIndex for folllower and leader
		panic(fmt.Sprintf("truncate index: %d, last index %d", index, lastIndex))
	}
	rl.Entries = rl.Entries[:index-rl.BaseIndex]
}
