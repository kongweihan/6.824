package shardmaster

import (
	"fmt"
	"log"
	"raft"
	"sort"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

func (sm *ShardMaster) debugLeader(format string, a ...interface{}) {
	if Debug > 0 && !sm.killed {
		if _, isLeader := sm.rf.GetState(); isLeader {
			sm.debug(format, a...)
		}
	}
}

func (sm *ShardMaster) debug(format string, a ...interface{}) {
	if Debug > 0 && !sm.killed {
		prefix := fmt.Sprintf(" --- SM %2v --- ", sm.me)
		log.Printf(prefix+format, a...)
	}
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int
	killed bool

	configs []Config // indexed by config num

	lastRequest         map[int64]int
	lastRequestConfigId map[int64]int
}

type Action int

const (
	Join Action = iota
	Leave
	Move
	Query
)

type Op struct {
	// Your data here.
	Action Action
	Servers map[int][]string  // Join
	GIDs []int  // Leave
	Shard int  // Move
	GID int  // Move
	Num int  // Query

	ClientId int64
	RequestId int
}

func (op *Op) action() string {
	switch op.Action {
	case Join:
		return "Join"
	case Leave:
		return "Leave"
	case Move:
		return "Move"
	case Query:
		return "Query"
	default:
		panic("Should not have any other Action")
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *ShardMasterReply) {
	// Your code here.
	op := Op{Action: Join, Servers: args.Servers, ClientId: args.ClientId, RequestId: args.RequestId}
	sm.debugLeader("%-9v for  %2v start    RequestId:%4v\n", op.action(), op.ClientId, op.RequestId)
	sm.processOp(op, reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *ShardMasterReply) {
	// Your code here.
	op := Op{Action: Leave, GIDs: args.GIDs, ClientId: args.ClientId, RequestId: args.RequestId}
	sm.debugLeader("%-9v for  %2v start    RequestId:%4v\n", op.action(), op.ClientId, op.RequestId)
	sm.processOp(op, reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *ShardMasterReply) {
	// Your code here.
	op := Op{Action: Move, Shard: args.Shard, GID: args.GID, ClientId: args.ClientId, RequestId: args.RequestId}
	sm.debugLeader("%-9v for  %2v start    RequestId:%4v\n", op.action(), op.ClientId, op.RequestId)
	sm.processOp(op, reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *ShardMasterReply) {
	// Your code here.
	op := Op{Action: Query, Num: args.Num, ClientId: args.ClientId, RequestId: args.RequestId}
	sm.processOp(op, reply)
}

func (sm *ShardMaster) processOp(op Op, reply *ShardMasterReply) {
	sm.mu.Lock()
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	ok := sm.receive(index, term)

	sm.mu.Lock()
	if ok {
		if requestId, present := sm.lastRequest[op.ClientId]; present && requestId == op.RequestId {
			if op.Action == Query {
				reply.Config = sm.configs[sm.lastRequestConfigId[op.ClientId]]
				//sm.debug("Query config %v\n", reply.Config)
			}
			//sm.debug("%-9v for  %2v return   RequestId:%4v\n", op.action(), op.ClientId, op.RequestId)
		} else {
			reply.Err = "fail or old duplicate request"
		}
	} else {
		reply.Err = "term changed"
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) receive(index int, term int) bool {
	for {
		sm.mu.Lock()
		if sm.lastApplied >= index {
			sm.mu.Unlock()
			return true
		}
		if curTerm, _ := sm.rf.GetState(); curTerm != term {
			sm.mu.Unlock()
			return false
		}
		sm.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (sm *ShardMaster) apply() {
	for {
		cmd := <- sm.applyCh

		sm.mu.Lock()

		op, ok := cmd.Command.(Op)
		//sm.debug("apply op %v\n", op)
		if !ok {
			panic("applyCh should always produce Op!")
		}
		if sm.lastRequest[op.ClientId] < op.RequestId {
			switch op.Action {
			case Join:
				sm.join(op.Servers)
			case Leave:
				sm.leave(op.GIDs)
			case Move:
				sm.move(op.Shard, op.GID)
			case Query:
				if op.Num == -1 || op.Num >= len(sm.configs) {
					sm.lastRequestConfigId[op.ClientId] = len(sm.configs) - 1
				} else {
					sm.lastRequestConfigId[op.ClientId] = op.Num
				}
			default:
				panic("ShardMaster unknown operation")
			}
			sm.lastRequest[op.ClientId] = op.RequestId
		} else if sm.lastRequest[op.ClientId] > op.RequestId {
			panic("New RequestId should always > old RequestId")
		} else {
			// duplicate operation
		}
		sm.lastApplied = cmd.CommandIndex

		sm.mu.Unlock()
	}
}

// Assume holding lock of sm
func (sm *ShardMaster) join(m map[int][]string) {
	// Generate new gid list
	oldConfig := sm.lastConfig()
	GIDs := oldConfig.getGIDs()
	for gid := range m {
		GIDs = append(GIDs, gid)
	}

	// Copy gid -> server[] map
	groups := make(map[int][]string)
	for gid, servers := range sm.lastConfig().Groups {
		groups[gid] = servers
	}
	for gid, servers := range m {
		groups[gid] = servers
	}

	config := Config{Num: len(sm.configs), Shards: generateShards(GIDs), Groups: groups}
	sm.debugLeader("New Join config: %v\n", config)
	sm.configs = append(sm.configs, config)
}

// Assume holding lock of sm
func (sm *ShardMaster) leave(leavingGIDs []int) {
	// Generate new gid list
	oldConfig := sm.lastConfig()
	GIDs := oldConfig.getGIDs()
	for _, gid := range leavingGIDs {
		for i := range GIDs {
			if GIDs[i] == gid {
				GIDs = deleteElement1(GIDs, i)
				break
			}
		}
	}

	// Copy gid -> server[] map
	groups := make(map[int][]string)
	for gid, servers := range sm.lastConfig().Groups {
		groups[gid] = servers
	}
	for _, gid := range leavingGIDs {
		delete(groups, gid)
	}

	config := Config{Num: len(sm.configs), Shards: generateShards(GIDs), Groups: groups}
	sm.debugLeader("New Leave config: %v\n", config)
	sm.configs = append(sm.configs, config)
}

// Assume holding lock of sm
func (sm *ShardMaster) move(shard int, gid int) {
	// Copy gid -> server[] map
	groups := make(map[int][]string)
	for gid, servers := range sm.lastConfig().Groups {
		groups[gid] = servers
	}

	var shards [NShards]int
	for i := range sm.lastConfig().Shards {
		shards[i] = sm.lastConfig().Shards[i]
	}
	shards[shard] = gid

	config := Config{Num: len(sm.configs), Shards: shards, Groups: groups}
	sm.debugLeader("New Move config: %v\n", config)
	sm.configs = append(sm.configs, config)
}

func generateShards(GIDs []int) [NShards]int {
	var shards [NShards]int
	if len(GIDs) == 0 {
		return shards
	}

	sort.Ints(GIDs)
	shardsPerGroup := NShards / len(GIDs)
	if shardsPerGroup == 0 {
		shardsPerGroup = 1
	}
	for i := 0; i < NShards; i++ {
		shards[i] = GIDs[(i / shardsPerGroup) % len(GIDs)]
	}

	//// Consistent hashing
	//var hashes []GroupHash
	//for gid := range GIDs {
	//	hashes = append(hashes, GroupHash{GID: gid, Hash: groupHash(gid)})
	//}
	//sort.Sort(ByHash(hashes))
	//hashes = deduplicateGroupHash(hashes)
	//var shards [NShards]int
	//for i := 0; i < NShards; i++ {
	//	shards[i] = findGID(hashes, i)
	//}
	return shards
}

//func findGID(groupHashes []GroupHash, hash int) int {
//	if hash < 0 {
//		panic("hash should never < 0")
//	}
//	for i, groupHash := range groupHashes {
//		if groupHash.Hash > hash {
//			if i == 0 {
//				return groupHashes[len(groupHashes) - 1].GID
//			} else {
//				return groupHashes[i - 1].GID
//			}
//		}
//	}
//	return groupHashes[len(groupHashes) - 1].GID
//}
//
//func deduplicateGroupHash(groupHashes []GroupHash) []GroupHash {
//	for i := 0; i < len(groupHashes) - 1; i++ {
//		if i + 1 < len(groupHashes) && groupHashes[i].Hash == groupHashes[i + 1].Hash {
//			groupHashes = deleteElement2(groupHashes, i)
//			i--
//		}
//	}
//	return groupHashes
//}

func deleteElement1(slice []int, i int) []int {
	return append(slice[:i], slice[i + 1:]...)
}

//func deleteElement2(slice []GroupHash, i int) []GroupHash {
//	return append(slice[:i], slice[i + 1:]...)
//}

// Assume holding lock of sm
func (sm *ShardMaster) lastConfig() Config {
	return sm.configs[len(sm.configs) - 1]
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.mu.Lock()
	sm.killed = true
	sm.mu.Unlock()
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
	sm.lastRequest = make(map[int64]int)
	sm.lastRequestConfigId = make(map[int64]int)
	go sm.apply()

	sm.debug("Start ShardMaster server\n")
	return sm
}
