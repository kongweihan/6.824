package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const DebugServer = 0
const DebugClient = 0

func (kv *KVServer) debug(format string, a ...interface{}) {
	if DebugServer > 0 {
		if !kv.killed {
			prefix := fmt.Sprintf(" --- KVServer %v --- ", kv.me)
			log.Printf(prefix+format, a...)
		}
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

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m map[string]string
	lastApplied int
	killed bool

	lastRequest map[int64]int
	lastGetValue map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Action: GetOp, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("%v\tKey: %v\tstart\tindex: %v\tValue: %v\tclient: %v\trequest: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId)
	kv.mu.Unlock()

	ok := kv.receive(index, term)

	kv.mu.Lock()
	if ok {
		if v, present := kv.lastRequest[op.ClientId]; present && op.RequestId == v {
			kv.debug("%v\tsuccess\tKey: %v\tindex: %v\tValue: %v\tclient: %v\trequest: %v\tlastApplied: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId, kv.lastApplied)
			reply.Value = kv.lastGetValue[op.ClientId]
		} else {
			kv.debug("%v\tfail\tKey: %v\tindex: %v\tValue: %v\tclient: %v\trequest: %v\tlastApplied: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId, kv.lastApplied)
			reply.Err = "fail"
		}
	} else {
		kv.debug("%v\tKey:%v term changed\tindex: %v\tValue: %v\tclient: %v\trequest: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId)
		reply.Err = "fail"
	}
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// No need to PutAppend the same finished request again
	kv.mu.Lock()
	if kv.lastRequest[args.ClientId] == args.RequestId {
		kv.debug("PutAppend\tduplicate client: %v request: %v\n", args.ClientId, args.RequestId)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var op Op
	if args.Op == "Put" {
		op = Op{Action: PutOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	} else {
		op = Op{Action: AppendOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("%v\tKey: %v\tstart\tindex: %v\tValue: %v\tclient: %v\trequest: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId)
	kv.mu.Unlock()

	ok := kv.receive(index, term)

	kv.mu.Lock()
	if ok {
		if v, present := kv.lastRequest[op.ClientId]; present && op.RequestId == v {
			kv.debug("%v\tsuccess\tKey: %v\tindex: %v\tValue: %v\tclient: %v\trequest: %v\tlastApplied: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId, kv.lastApplied)
		} else {
			kv.debug("%v\tfail\tKey: %v\tindex: %v\tValue: %v\tclient: %v\trequest: %v\tlastApplied: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId, kv.lastApplied)
			reply.Err = "fail"
		}
	} else {
		kv.debug("%v\tKey:%v term changed\tindex: %v\tValue: %v\tclient: %v\trequest: %v\n", op.action(), op.Key, index, op.Value, op.ClientId, op.RequestId)
		reply.Err = "fail"
	}
	kv.mu.Unlock()
}

func (kv *KVServer) receive(index int, term int) bool {
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

func (kv *KVServer) apply() {
	for {
		cmd := <- kv.applyCh

		kv.mu.Lock()

		op, ok := cmd.Command.(Op)
		if ok {
			if !cmd.CommandValid {
				// TODO
			}
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.debug("%v\tapply\tindex: %v\tKey: %v\tValue: %v\tclient: %v\trequest: %v\n", op.action(), cmd.CommandIndex, op.Key, op.Value, op.ClientId, op.RequestId)
			}
			if kv.lastRequest[op.ClientId] != op.RequestId {
				if op.Action == GetOp {
					if v, present := kv.m[op.Key]; present {
						kv.lastGetValue[op.ClientId] = v
					} else {
						kv.lastGetValue[op.ClientId] = ""
					}
				} else {
					if v, present := kv.m[op.Key]; present && op.Action == AppendOp {
						kv.m[op.Key] = v + op.Value
					} else {
						kv.m[op.Key] = op.Value
					}
				}
			}
			kv.lastRequest[op.ClientId] = op.RequestId
			kv.lastApplied = cmd.CommandIndex
		} else {
			panic("applyCh should always produce Op!")
		}

		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	kv.killed = true
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.m = make(map[string]string)
	kv.lastRequest = make(map[int64]int)
	kv.lastGetValue = make(map[int64]string)
	go kv.apply()
	return kv
}
