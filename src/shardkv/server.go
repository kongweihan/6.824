package shardkv


// import "shardmaster"
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
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action Action
	Key string
	Value string
	ClientId int64
	RequestId int
}

func (op *Op) action() string {
	switch op.Action {
	case GetOp:
		return "Get"
	case PutOp:
		return "Put"
	case AppendOp:
		return "Append"
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

	//masters      []*labrpc.ClientEnd
	make_end     func(string) *labrpc.ClientEnd
	gid          int

	// Your definitions here.
	m [shardmaster.NShards]map[string]string
	lastApplied int
	killed bool

	sm       *shardmaster.Clerk
	config   shardmaster.Config

	lastRequest      map[int64]int
	lastRequestValue map[int64]string

	persister *raft.Persister
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
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.debug("%-9v for    %4v WrongGID    Key:%4v(%v) RequestId:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId)
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
			reply.Err = "fail or old duplicate request"
			kv.debug("%-9v for    %4v fail     Key:%4v(%v) RequestId:%4v Index:%4v LastApplied:%4v\n", op.action(), op.ClientId % 10000, op.Key, shard, op.RequestId, index, kv.lastApplied)
		}
	} else {
		reply.Err = "term changed"
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
		time.Sleep(50 * time.Millisecond)
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
		} else {
			op, ok := cmd.Command.(Op)
			if !ok {
				panic("applyCh should always produce Op!")
			}
			if kv.lastRequest[op.ClientId] < op.RequestId {
				shard := key2shard(op.Key)
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
			} else if kv.lastRequest[op.ClientId] > op.RequestId {
				panic("New RequestId should always > old RequestId")
			} else {
				// duplicate operation
			}
			kv.lastApplied = cmd.CommandIndex

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.saveSnapshot()
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig() {
	for {
		config := kv.sm.Query(-1)
		kv.mu.Lock()
		kv.config = config
		kv.mu.Unlock()
		time.Sleep(100 * time.Microsecond)
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
	//kv.masters = masters

	// Your initialization code here.
	kv.readSnapshot(persister.ReadSnapshot())

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.updateConfig()
	go kv.apply()

	kv.debug("Start ShardKV server\n")
	return kv
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) saveSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.m)
	e.Encode(kv.lastRequest)
	e.Encode(kv.lastRequestValue)
	kv.rf.Snapshot(w.Bytes(), kv.lastApplied)
}

// Assume holding the lock of kv.mu
func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		for i := range kv.m {
			kv.m[i] = make(map[string]string)
		}
		kv.lastRequest = make(map[int64]int)
		kv.lastRequestValue = make(map[int64]string)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m [shardmaster.NShards]map[string]string
	var lastRequest map[int64]int
	var lastRequestValue map[int64]string
	if d.Decode(&m) != nil || d.Decode(&lastRequest) != nil || d.Decode(&lastRequestValue) != nil {
		panic("readSnapshot: error decode\n")
	} else {
		kv.m = m
		kv.lastRequest = lastRequest
		kv.lastRequestValue = lastRequestValue
	}
}
