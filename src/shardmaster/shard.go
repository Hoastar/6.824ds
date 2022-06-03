package shardmaster

import (
	"fmt"
	"sort"
)

// CopyLatestConfig return latest config
func CopyLatestConfig(sm *ShardMaster) Config {
	var newCfg Config
	currentCfg := sm.configs[len(sm.configs)-1]
	newCfg.Num = currentCfg.Num + 1
	newCfg.Shards = currentCfg.Shards
	newCfg.Groups = make(map[int][]string)
	for k, v := range currentCfg.Groups {
		newCfg.Groups[k] = v
	}
	return newCfg
}

// MinimalReBalance minimal ReBalance alog
func MinimalReBalance(cfg *Config, moveOut []int) {
	// moveOut shards that need to assign

	// cfg.Shard need to rebalance

	// gidHasShard
	gidHasShard := make(map[int][]int) // gid, shards

	// init gidHasShard: cfg.Groups all available
	for gid, _ := range cfg.Groups {
		gidHasShard[gid] = make([]int, 0)
	}

	//
	// moveOut is marked to be removed from cfg.Shards; At this point cfg.groups also contains moveOut
	for i, gid := range cfg.Shards {
		if _, ok := cfg.Groups[gid]; ok { //only check available gid, since shards are all 0s at first
			gidHasShard[gid] = append(gidHasShard[gid], i)
		}
	}

	// numbers of raft-cluster group
	totalServerNum := len(cfg.Groups)

	if totalServerNum == 0 { // all shards left
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	// number of shards after average： upperNum >= 0
	upperNum := NShards % totalServerNum

	// average shard
	upper := NShards / totalServerNum

	if upperNum != 0 {
		// average increasing
		upper++
	}

	adjustUpper := func() {
		if upperNum > 0 {
			upperNum--
			// 如果需要调整的数量已经为零，那么便将平均值减-
			if upperNum == 0 {
				upper--
			}
		}
	}

	var gids []int
	for gid, _ := range cfg.Groups {
		gids = append(gids, gid)
	}

	// Sort by gid in ascending order
	sort.Ints(gids)

	// More refunds: 多退
	for _, gid := range gids { // moveOut
		if len(gidHasShard[gid]) > upper {
			moveOut = append(moveOut, gidHasShard[gid][upper:]...)
			gidHasShard[gid] = gidHasShard[gid][:upper]
			adjustUpper()
		}
	}

	// less compensation: 少补
	for _, gid := range gids { // moveOut
		if len(gidHasShard[gid]) < upper {
			need := upper - len(gidHasShard[gid])
			if need > len(moveOut) { // not enough remain to add
				need = len(moveOut)
			}
			// 补充
			gidHasShard[gid] = append(gidHasShard[gid], moveOut[:need]...)
			moveOut = moveOut[need:]
			adjustUpper()
		}
	}

	for gid, shards := range gidHasShard {
		for _, shard := range shards {
			// 重置cfg.Shards
			cfg.Shards[shard] = gid
		}
	}
}

// ConfigJoin join servers type of map[int][]string
// map[int][]string: key is gid; v is dynamic array of string(server addr) type
func ConfigJoin(sm *ShardMaster, servers map[int][]string) {
	newCfg := CopyLatestConfig(sm)

	var moveOut []int
	if len(newCfg.Groups) == 0 { // no group
		for i := 0; i < NShards; i++ {
			moveOut = append(moveOut, i)
		}
	}

	// add groups
	var newGids []int
	for k, v := range servers {
		newGids = append(newGids, k)
		newCfg.Groups[k] = v
		if _, ok := sm.configs[len(sm.configs)-1].Groups[k]; ok {
			panic(fmt.Sprintf("gid %d has been in the config %+v", k, sm.configs[len(sm.configs)-1]))
		}
	}

	MinimalReBalance(&newCfg, moveOut)
	sm.LogPrintf("after join %+v: %+v", servers, newCfg)
	sm.configs = append(sm.configs, newCfg)
}

// ConfigQuery get latest config, args is num
func ConfigQuery(sm *ShardMaster, num int) Config {
	if num == -1 || num >= len(sm.configs) {
		num = len(sm.configs) - 1
	}

	var cfg Config
	CopyConfig(&cfg, &sm.configs[num]) // copy to avoid race
	return cfg
}

// ConfigMove move shard to other groups
func ConfigMove(sm *ShardMaster, shard int, gid int) {
	newCfg := CopyLatestConfig(sm)
	if _, ok := sm.configs[len(sm.configs)-1].Groups[gid]; !ok {
		panic(fmt.Sprintf("gid %d not in current configs %+v", gid, sm.configs[len(sm.configs)-1]))
	}
	newCfg.Shards[shard] = gid
	sm.configs = append(sm.configs, newCfg)
}

// ConfigLeave remove some groups and move shard to other groups.
func ConfigLeave(sm *ShardMaster, gids []int) {
	newCfg := CopyLatestConfig(sm)

	var moveOut []int

	for _, gid := range gids {
		for shard, gid1 := range newCfg.Shards {
			if gid == gid1 {
				moveOut = append(moveOut, shard) // init move out
			}
		}
		delete(newCfg.Groups, gid)
	}

	MinimalReBalance(&newCfg, moveOut)
	// rm unused gid
	sm.LogPrintf("after leave %+v: %+v", gids, newCfg)
	sm.configs = append(sm.configs, newCfg)
}

// applyCommand apply command
func applyCommand(sm *ShardMaster, commandOp Op, ret *NotifyMsg) {
	switch commandOp.OpType {
	case OpTypeJoin:
		servers := commandOp.JoinArgs
		ConfigJoin(sm, servers)
		ret.Info = NotifyMsgInfoJoinOK
	case OpTypeLeave:
		gids := commandOp.LeaveArgs
		ConfigLeave(sm, gids)
		ret.Info = NotifyMsgInfoLeaveOK
	case OpTypeMove:
		moveArgs := commandOp.MoveArgs
		ConfigMove(sm, moveArgs.Shard, moveArgs.GID)
		ret.Info = NotifyMsgInfoMoveOK
	case OpTypeQuery:
		num := commandOp.QueryArgs
		var cfg Config
		cfg = ConfigQuery(sm, num)
		ret.Info = NotifyMsgInfoQueryOK
		ret.RetValue = cfg
	}
}
