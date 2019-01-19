package shardkv


import (
	"bytes"
	"fmt"
	"labrpc"
	"log"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

const Debug = 0

// Assume holding the lock of kv.mu
func (kv *ShardKV) debugLeader(format string, a ...interface{}) {
	if Debug > 0 && !kv.killed {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.debug(format, a...)
		}
	}
}

func (kv *ShardKV) debug(format string, a ...interface{}) {
	if Debug > 0 && !kv.killed {
		prefix := fmt.Sprintf(" --- ShardKV  %3v,%2v --- ", kv.gid, kv.me)
		log.Printf(prefix+format, a...)
	}
}

type Action int

const (
	GetOp Action = iota
	PutOp
	AppendOp
	ReConfigStart
	ShardIn
	ShardOut
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	RequestId int

	Action Action
	Key string
	Value string

	Config           	shardmaster.Config // ReConfigStart
	ConfigNum		 	int // ShardIn / ShardOut
	Shards           	map[int]map[string]string // ShardIn / ShardOut
	LastRequest      	map[int64]int // ShardIn
	LastRequestValue 	map[int64]string // ShardIn
}

func (op *Op) action() string {
	switch op.Action {
	case GetOp:
		return "Get"
	case PutOp:
		return "Put"
	case AppendOp:
		return "Append"
	case ReConfigStart:
		return "ReConfigStart"
	case ShardIn:
		return "ShardIn"
	case ShardOut:
		return "ShardOut"
	default:
		panic("Should not have any other Action")
	}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	make_end     func(string) *labrpc.ClientEnd
	gid          int

	// Your definitions here.
	m [shardmaster.NShards]map[string]string // snapshoted
	lastApplied int
	killed bool

	sm       	*shardmaster.Clerk
	config   	shardmaster.Config 	// snapshoted
	newConfig	*shardmaster.Config // snapshoted, if !nil, reconfiguration in-progress

	lastRequest      map[int64]int 		// snapshoted
	lastRequestValue map[int64]string 	// snapshoted

	persister *raft.Persister

	shardOutCalled bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetPutAppendReply) {
	// Your code here.
	op := Op{Action: GetOp, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId}
	kv.processGetPutAppend(op, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *GetPutAppendReply) {
	// Your code here.
	var op Op
	if args.Op == "Put" {
		op = Op{Action: PutOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	} else {
		op = Op{Action: AppendOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	}
	kv.processGetPutAppend(op, reply)
}

func (kv *ShardKV) processGetPutAppend(op Op, reply *GetPutAppendReply) {
	kv.mu.Lock()
	shard := key2shard(op.Key)

	if (!kv.isReConfiguring() && kv.config.Shards[shard] != kv.gid) ||
		(kv.isReConfiguring() && kv.newConfig.Shards[shard] != kv.gid) {
		reply.Err = ErrWrongGroup
		kv.debug("%-9v for    %4v WrongGID Key:%4v(%v) RequestId:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId)
		kv.mu.Unlock()
		return
	}

	if kv.isReConfiguring() && kv.newConfig.Shards[shard] == kv.gid && kv.m[shard] == nil {
		reply.Err = NotInstallYet
		kv.debug("%-9v for    %4v NotInstallYet  Key:%4v(%v) RequestId:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId)
		kv.mu.Unlock()
		return
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("%-9v for    %4v start    Key:%4v(%v) RequestId:%4v Index:%4v Term:%3v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId, index, term)
	kv.mu.Unlock()

	ok := kv.receive(index, term)

	kv.mu.Lock()
	if ok {
		if requestId, present := kv.lastRequest[op.ClientId]; present && requestId == op.RequestId {
			reply.Value = kv.lastRequestValue[op.ClientId]
			reply.Err = OK
			kv.debug("%-9v for    %4v success  Key:%4v(%v) RequestId:%4v Index:%4v LastApplied:%4v Value:%v Result Value:%v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId, index, kv.lastApplied, op.Value, reply.Value)
		} else {
			reply.Err = "fail to create Raft log entry, or old duplicate request, or wrong group"
			kv.debug("%-9v for    %4v fail     Key:%4v(%v) RequestId:%4v Index:%4v LastApplied:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId, index, kv.lastApplied)
		}
	} else {
		reply.WrongLeader = true
		kv.debug("%-9v for    %4v term chg Key:%4v(%v) RequestId:%4v Index:%4v LastApplied:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId, index, kv.lastApplied)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) receive(index int, term int) bool {
	for {
		kv.mu.Lock()
		if kv.lastApplied >= index {
			kv.mu.Unlock()
			return true
		}
		if curTerm, _ := kv.rf.GetState(); curTerm != term {
			kv.mu.Unlock()
			return false
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) apply() {
	for {
		cmd := <- kv.applyCh

		kv.mu.Lock()

		if cmd.Snapshot != nil {
			kv.readSnapshot(cmd.Snapshot)
			kv.lastApplied = cmd.CommandIndex // Used CommandIndex to pass in LastSnapshotIndex
			// Here we don't need to save snapshot because Raft has already done it in InstallSnapshot()
			kv.debug("Installed snapshot lastApplied:%v\n", kv.lastApplied)
			kv.mu.Unlock()
			continue
		}

		op, ok := cmd.Command.(Op)
		if !ok {
			panic("applyCh should always produce Op!")
		}

		// Only start the ReConfig if the new config num is current config num + 1
		// It's possible that when ReConfigStart with config X was put into log,
		// the server was still replaying the log and thought X is the latest config to apply,
		// but as the log is replayed, config X might be applied, so now this ReConfigStart for
		// config X should be ignored
		if op.Action == ReConfigStart && op.Config.Num == kv.config.Num + 1 {
			kv.newConfig = &op.Config
			kv.debug("Apply ReConfigStart for config %v Index:%v\n", kv.newConfig, cmd.CommandIndex)
			// Setup initial shards
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] == 0 && kv.newConfig.Shards[shard] == kv.gid {
					kv.m[shard] = make(map[string]string)
				}
			}
			if kv.isReConfigDone() {
				kv.debug("ReConfig for config %v is done\n", kv.newConfig.Num)
				kv.config = *kv.newConfig
				kv.newConfig = nil
				kv.shardOutCalled = false
			} else {
				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.shardOut()
				}
			}
		} else if op.Action == ShardIn {
			// ShardIn could be duplicated, and need to be ignored
			if kv.isReConfiguring() && op.ConfigNum == kv.newConfig.Num {
				kv.debug("Apply ShardIn  for config %v, #shards %v Index:%v\n", kv.newConfig, len(op.Shards), cmd.CommandIndex)
				for shard := range op.Shards {
					if kv.m[shard] == nil {
						kv.m[shard] = copyMap(op.Shards[shard])
					}
				}
				// For cross-configuration client request de-duplication
				kv.mergeLastRequest(op.LastRequest, op.LastRequestValue)

				if kv.isReConfigDone() {
					kv.debug("ReConfig to %v is done\n", kv.newConfig.Num)
					kv.config = *kv.newConfig
					kv.newConfig = nil
					kv.shardOutCalled = false
				}
			}
		} else if op.Action == ShardOut {
			// ShardOut could be duplicated, and need to be ignored
			if kv.isReConfiguring() && op.ConfigNum == kv.newConfig.Num {
				kv.debug("Apply ShardOut for config %v, #shards %v Index:%v\n", kv.newConfig, len(op.Shards), cmd.CommandIndex)
				for shard := range op.Shards {
					kv.m[shard] = nil
				}
				if kv.isReConfigDone() {
					kv.debug("ReConfig to %v is done\n", kv.newConfig.Num)
					kv.config = *kv.newConfig
					kv.newConfig = nil
					kv.shardOutCalled = false
				}
			}
		} else {
			value, ok := kv.lastRequest[op.ClientId]
			kv.debug("lastRequest client:%v  value:%v  ok:%v\n", op.ClientId, value, ok)
			if kv.lastRequest[op.ClientId] < op.RequestId {
				shard := key2shard(op.Key)
				// Not doing re-config and has the shard, or doing re-config but has received the shard
				if (!kv.isReConfiguring() && kv.config.Shards[shard] == kv.gid) ||
					(kv.isReConfiguring() && kv.newConfig.Shards[shard] == kv.gid && kv.m[shard] != nil) {
					if op.Action == GetOp {
						if v, present := kv.m[shard][op.Key]; present {
							kv.lastRequestValue[op.ClientId] = v
						} else {
							kv.lastRequestValue[op.ClientId] = ""
						}
					} else {
						if v, present := kv.m[shard][op.Key]; present && op.Action == AppendOp {
							kv.m[shard][op.Key] = v + op.Value
						} else {
							kv.m[shard][op.Key] = op.Value
						}
						kv.lastRequestValue[op.ClientId] = kv.m[shard][op.Key]
					}
					kv.lastRequest[op.ClientId] = op.RequestId
				} else {
					// Wrong group now
				}
			} else if kv.lastRequest[op.ClientId] > op.RequestId {
				panic("New RequestId should always > old RequestId")
			} else {
				// duplicate operation
			}
		}
		kv.lastApplied = cmd.CommandIndex

		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			// This could be skipped under-the-hood, if a newer snapshot was just installed in Raft
			// and thus the current lastSnapshotIndex is even bigger than lastApplied
			// The whole server state will be replaced shortly as it receive the install snapshot cmd
			kv.debug("Save snapshot #config:%v re-configing:%v lastApplied:%v\n", kv.config.Num, kv.isReConfiguring(), kv.lastApplied)
			kv.saveSnapshot()
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	for {
		time.Sleep(100 * time.Microsecond)
		kv.mu.Lock()
		if kv.isReConfiguring() {
			// Reconfiguration in-progress, but leader may have changed or crashed-and-rebooted
			if _, isLeader := kv.rf.GetState(); isLeader && !kv.shardOutCalled {
				kv.shardOut()
			}
			// Wait until reconfiguration is done
			kv.mu.Unlock()
			continue
		}

		nextConfigNum := kv.config.Num + 1
		kv.mu.Unlock()
		// Query could block indefinitely because of network failure, so don't lock around it,
		// if it's locked and blocked, this server can't be Kill()ed
		config := kv.sm.Query(nextConfigNum)

		kv.mu.Lock()
		if config.Num <= kv.config.Num { // could be <, in extreme case, if config is by leader after the Query
			kv.mu.Unlock()
			continue
		}

		kv.debugLeader("Query had old config:%v\n", kv.config)
		kv.debugLeader("Query got new config:%v\n", config)
		op := Op{Action: ReConfigStart, Config: config}
		index, term, isLeader := kv.rf.Start(op)

		if !isLeader {
			kv.mu.Unlock() // Find new config, but I'm not leader, let leader do it
			continue
		}
		kv.debug("Start %-8v for config %v Index:%4v Term:%3v\n", op.action(), config.Num, index, term)
		kv.mu.Unlock()

		kv.receive(index, term)
	}
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) isReConfiguring() bool {
	return kv.newConfig != nil
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) shardOut() {
	kv.shardOutCalled = true
	// Send shards that are not sent yet
	// Since this is non-blocking, there won't be deadlocks among groups
	out := kv.getShardOutMap()
	lastRequest := copyMap2(kv.lastRequest)
	lastRequestValue := copyMap3(kv.lastRequestValue)
	for gid, shards := range out {
		kv.debug("sendInstallShard to gid %v #shards:%v\n", gid, len(shards))
		go kv.sendInstallShard(kv.newConfig.Num, shards, lastRequest, lastRequestValue, kv.newConfig.Groups[gid])
	}
}

// Assume holding the lock of kv.mu
// Return a map from gid to to-be-sent shards for that gid
// shards is a map from shard number to shard data(which is a map)
// Only include shards that are not sent yet(kv.m[s] != nil)
func (kv *ShardKV) getShardOutMap() map[int]map[int]map[string]string {
	m := make(map[int]map[int]map[string]string)
	for s := 0; s < shardmaster.NShards; s++ {
		if kv.config.Shards[s] == kv.gid && kv.config.Shards[s] != kv.newConfig.Shards[s] && kv.m[s] != nil {
			targetGid := kv.newConfig.Shards[s]
			if _, present := m[targetGid]; present {
				m[targetGid][s] = copyMap(kv.m[s])
			} else {
				m[targetGid] = make(map[int]map[string]string)
				m[targetGid][s] = copyMap(kv.m[s])
			}
		}
	}
	return m
}

// Assume m is locked or not changing
func copyMap(m map[string]string) map[string]string {
	n := make(map[string]string)
	for k, v := range m {
		n[k] = v
	}
	return n
}

// Assume m is locked or not changing
func copyMap2(m map[int64]int) map[int64]int {
	n := make(map[int64]int)
	for k, v := range m {
		n[k] = v
	}
	return n
}

// Assume m is locked or not changing
func copyMap3(m map[int64]string) map[int64]string {
	n := make(map[int64]string)
	for k, v := range m {
		n[k] = v
	}
	return n
}

//// Assume holding the lock of kv.mu
//// Return a map of gid to to-be-received shards for that gid
//// Only include shards that are not received yet(kv.m[s] == nil)
//func (kv *ShardKV) getShardInMap() map[int][]int {
//	// Get list of shards to receive
//	m := make(map[int][]int)
//	for s := 0; s < shardmaster.NShards; s++ {
//		if kv.config.Shards[s] != kv.gid && kv.newConfig.Shards[s] == kv.gid && kv.m[s] == nil {
//			if _, present := m[kv.gid]; present {
//				m[kv.gid] = []int{s}
//			} else {
//				m[kv.gid] = append(m[kv.gid], s)
//			}
//		}
//	}
//	return m
//}

type InstallShardArgs struct {
	ConfigNum int
	Shards map[int]map[string]string // Map of shard to shard data(a kv-map)
	LastRequest map[int64]int
	LastRequestValue map[int64]string
}

type InstallShardReply struct {
	WrongLeader bool
	NotReConfigYet bool
}

// NOT assume holding the lock of kv.mu
func (kv *ShardKV) sendInstallShard(configNum int, shards map[int]map[string]string,
		lastRequest map[int64]int, lastRequestValue map[int64]string, servers []string) {
	args := InstallShardArgs{ConfigNum: configNum, Shards: shards,
		LastRequest: lastRequest, LastRequestValue: lastRequestValue}
	si := 0
	for {
		srv := kv.make_end(servers[si])
		var reply InstallShardReply
		kv.debug("sendInstallShard send %v\n", args)
		ok := srv.Call("ShardKV.InstallShard", &args, &reply)
		if ok {
			if reply.WrongLeader {
				si = (si + 1) % len(servers)
			} else if reply.NotReConfigYet {
				time.Sleep(100 * time.Millisecond)
			} else {
				// Shard sent, start ShardOut operation
				// Because we only leader send out shards,
				// followers don't know when to delete old shards
				kv.mu.Lock()
				op := Op{Action: ShardOut, ConfigNum: configNum, Shards: args.Shards}
				index, term, isLeader := kv.rf.Start(op)
				kv.mu.Unlock()
				if !isLeader {
					return
				}
				kv.debug("Start %-8v #shards:%v Index:%4v Term:%3v\n", op.action(), len(args.Shards), index, term)

				ok := kv.receive(index, term)
				if ok {
					return
				}
				newTerm, isStillLeader := kv.rf.GetState()
				kv.debug("sendInstallShard receive fail term:%v new-term:%v leader:%v\n", term, newTerm, isStillLeader)
			}
		} else {
			si = (si + 1) % len(servers)
		}
	}
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	kv.mu.Lock()
	kv.debug("InstallShard receive %v\n", args)
	defer kv.debug("InstallShard reply %v\n", reply)
	// Re-config is already done
	if kv.config.Num >= args.ConfigNum {
		kv.mu.Unlock()
		return
	}
	// Re-config has not started yet, please try again later
	if !kv.isReConfiguring() || kv.newConfig.Num < args.ConfigNum {
		reply.NotReConfigYet = true
		kv.debug("isReConfiguring:%v newConfig:%v\n", kv.isReConfiguring(), kv.newConfig)
		kv.mu.Unlock()
		return
	}
	// Re-config in-progress, but this set of shards is already installed, return success
	var anyNewShard int
	for k := range args.Shards {
		anyNewShard = k
	}
	if kv.isReConfiguring() && kv.newConfig.Num == args.ConfigNum && kv.m[anyNewShard] != nil {
		kv.mu.Unlock()
		return
	}

	// Re-config in-progress, install shards
	op := Op{Action: ShardIn, ConfigNum: args.ConfigNum, Shards: args.Shards,
		LastRequest: args.LastRequest, LastRequestValue: args.LastRequestValue}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("Start %-8v #shards:%v Index:%4v Term:%3v\n", op.action(), len(args.Shards), index, term)
	kv.mu.Unlock()

	ok := kv.receive(index, term)
	if !ok {
		reply.WrongLeader = true
		return
	}
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) isReConfigDone() bool {
	for s := 0; s < shardmaster.NShards; s++ {
		if (kv.newConfig.Shards[s] == kv.gid && kv.m[s] == nil) ||
			(kv.newConfig.Shards[s] != kv.gid && kv.m[s] != nil) {
			return false
		}
	}
	return true
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) mergeLastRequest(lastRequest map[int64]int, lastRequestValue map[int64]string) {
	for clientId, requestId := range lastRequest {
		if v, present := kv.lastRequest[clientId]; !present || v < requestId {
			kv.lastRequest[clientId] = requestId
			kv.lastRequestValue[clientId] = lastRequestValue[clientId]
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.persister = persister

	// Your initialization code here.
	kv.readSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.debug("Start ShardKV server, current config:%v, newConfig:%v\n", kv.config, kv.newConfig)

	go kv.updateConfig()
	go kv.apply()


	return kv
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) saveSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.lastRequest)
	e.Encode(kv.lastRequestValue)
	e.Encode(kv.config)

	// nil needs special handling
	if kv.newConfig != nil {
		e.Encode(*kv.newConfig)
	} else {
		e.Encode(shardmaster.Config{Num: -1}) // Means newConfig is nil
	}

	// nil in slice of map needs special handling. If we encode a slice of nil map, we will decode out a slice of empty map, not nil map
	var isNil [shardmaster.NShards]bool
	for s := range kv.m {
		if kv.m[s] == nil {
			isNil[s] = true
		}
	}
	e.Encode(kv.m)
	e.Encode(isNil)

	kv.rf.Snapshot(w.Bytes(), kv.lastApplied)
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state
		// m doesn't need initialization because the server doesn't have any shard initially, should be nil
		kv.lastRequest = make(map[int64]int)
		kv.lastRequestValue = make(map[int64]string)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var lastRequest map[int64]int
	var lastRequestValue map[int64]string

	var config shardmaster.Config
	var newConfig shardmaster.Config

	var m [shardmaster.NShards]map[string]string
	var isNil [shardmaster.NShards]bool

	if d.Decode(&lastRequest) != nil || d.Decode(&lastRequestValue) != nil || d.Decode(&config) != nil || d.Decode(&newConfig) != nil || d.Decode(&m) != nil || d.Decode(&isNil) != nil {
		panic(fmt.Sprintf("readSnapshot: error decode"))
	} else {
		kv.lastRequest = lastRequest
		kv.lastRequestValue = lastRequestValue

		kv.config = config
		if newConfig.Num != -1 {
			kv.newConfig = &newConfig
		} else {
			kv.newConfig = nil
		}

		for s := range m {
			if isNil[s] {
				m[s] = nil
			}
		}
		kv.m = m
	}
}
