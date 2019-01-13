package shardmaster

import (
	"hash/fnv"
	"strconv"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClientId int64
	RequestId int
}

//type JoinReply struct {
//	WrongLeader bool
//	Err         Err
//}

type LeaveArgs struct {
	GIDs []int
	ClientId int64
	RequestId int
}

//type LeaveReply struct {
//	WrongLeader bool
//	Err         Err
//}

type MoveArgs struct {
	Shard int
	GID   int
	ClientId int64
	RequestId int
}

//type MoveReply struct {
//	WrongLeader bool
//	Err         Err
//}

type QueryArgs struct {
	Num int // desired config number
	ClientId int64
	RequestId int
}

//type QueryReply struct {
//	WrongLeader bool
//	Err         Err
//	Config      Config
//}

type ShardMasterReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (cf *Config) getGIDs() []int {
	var GIDs []int
	for gid := range cf.Groups {
		GIDs = append(GIDs, gid)
	}
	return GIDs
}

func groupHash(gid int) int {
	st := strconv.Itoa(gid)
	h := fnv.New32a()
	h.Write([]byte(st))
	return int(h.Sum32() % NShards)
}

type GroupHash struct {
	GID int
	Hash int
}

type ByHash []GroupHash

func (s ByHash) Len() int {
	return len(s)
}

func (s ByHash) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByHash) Less(i, j int) bool {
	return s[i].Hash < s[j].Hash
}