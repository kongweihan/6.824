package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const Print = 0

func (kv *KVServer) debug(format string, a ...interface{}) {
	if Debug > 0 && !kv.killed {
		kv.print(format, a...)
	}
}

func (kv *KVServer) debugLeader(format string, a ...interface{}) {
	if Debug > 0 && !kv.killed {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.print(format, a...)
		}
	}
}

func (kv *KVServer) print(format string, a ...interface{}) {
	if Print > 0 && !kv.killed {
		prefix := fmt.Sprintf(" --- KVServer %2v --- ", kv.me)
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

	lastRequest      map[int64]int
	lastRequestValue map[int64]string

	persister *raft.Persister
}


func (kv *KVServer) Get(args *GetArgs, reply *GetPutAppendReply) {
	// Your code here.
	op := Op{Action: GetOp, Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId}

	kv.processGetPutAppend(op, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *GetPutAppendReply) {
	// Your code here.
	var op Op
	if args.Op == "Put" {
		op = Op{Action: PutOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	} else {
		op = Op{Action: AppendOp, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId}
	}

	kv.processGetPutAppend(op, reply)
}

func (kv *KVServer) processGetPutAppend(op Op, reply *GetPutAppendReply) {
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}
	kv.debug("%-9v for  %2v start    Key:%4v RequestId:%4v Index:%4v Term:%3v\n", op.action(), op.ClientId, op.Key, op.RequestId, index, term)
	kv.mu.Unlock()

	ok := kv.receive(index, term)

	kv.mu.Lock()
	if ok {
		if requestId, present := kv.lastRequest[op.ClientId]; present && requestId == op.RequestId {
			reply.Value = kv.lastRequestValue[op.ClientId]
			kv.debug("%-9v for  %2v success  Key:%4v RequestId:%4v Index:%4v LastApplied:%4v Value:%v Result Value:%v\n", op.action(), op.ClientId, op.Key, op.RequestId, index, kv.lastApplied, op.Value, reply.Value)
		} else {
			reply.Err = "fail or old duplicate request"
			kv.debug("%-9v for  %2v fail     Key:%4v RequestId:%4v Index:%4v LastApplied:%4v\n", op.action(), op.ClientId, op.Key, op.RequestId, index, kv.lastApplied)
		}
	} else {
		kv.debug("%-9v for  %2v term chg Key:%4v RequestId:%4v Index:%4v LastApplied:%4v\n", op.action(), op.ClientId, op.Key, op.RequestId, index, kv.lastApplied)
		reply.Err = "term changed"
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

		if cmd.Snapshot != nil {
			kv.debug("Install snapshot, lastApplied set to %v\n", cmd.CommandIndex)
			kv.readSnapshot(cmd.Snapshot)
			kv.lastApplied = cmd.CommandIndex // Used CommandIndex to pass in LastSnapshotIndex
			// Here we don't need to save snapshot because Raft has already done it in InstallSnapshot()
		} else {
			op, ok := cmd.Command.(Op)
			if !ok {
				panic("applyCh should always produce Op!")
			}
			if kv.lastRequest[op.ClientId] < op.RequestId {
				if op.Action == GetOp {
					if v, present := kv.m[op.Key]; present {
						kv.lastRequestValue[op.ClientId] = v
					} else {
						kv.lastRequestValue[op.ClientId] = ""
					}
				} else {
					if v, present := kv.m[op.Key]; present && op.Action == AppendOp {
						kv.m[op.Key] = v + op.Value
					} else {
						kv.m[op.Key] = op.Value
					}
					kv.lastRequestValue[op.ClientId] = kv.m[op.Key]
				}
				kv.debugLeader("%-9v for  %2v apply    Key:%4v RequestId:%4v Index:%4v Value:%v  LastRequestId:%v  \n", op.action(), op.ClientId, op.Key, op.RequestId, cmd.CommandIndex, kv.lastRequestValue[op.ClientId], kv.lastRequest[op.ClientId])
				kv.lastRequest[op.ClientId] = op.RequestId
			} else if kv.lastRequest[op.ClientId] > op.RequestId {
				panic("New RequestId should always > old RequestId")
			} else {
				kv.debugLeader("%-9v for  %2v applydup Key:%4v RequestId:%4v Index:%4v Value:%v  LastRequestId:%v  \n", op.action(), op.ClientId, op.Key, op.RequestId, cmd.CommandIndex, kv.lastRequestValue[op.ClientId], kv.lastRequest[op.ClientId])
			}
			kv.lastApplied = cmd.CommandIndex

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.saveSnapshot()
			}
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
	kv.persister = persister

	// You may need initialization code here.
	kv.print("StartKVServer, reading snapshot\n")
	kv.readSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	return kv
}

// Assume holding the lock of kv.mu
func (kv *KVServer) saveSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.m)
	e.Encode(kv.lastRequest)
	e.Encode(kv.lastRequestValue)
	kv.rf.Snapshot(w.Bytes(), kv.lastApplied)
	kv.print("Saved snapshot\n\t\tk/v:\n%v\n\t\tlastRequest:\n%v\n\t\t lastRequestValue:\n%v\n", mapToString1(kv.m), mapToString2(kv.lastRequest), mapToString3(kv.lastRequestValue))
}

// Assume holding the lock of kv.mu
func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		kv.m = make(map[string]string)
		kv.lastRequest = make(map[int64]int)
		kv.lastRequestValue = make(map[int64]string)
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[string]string
	var lastRequest map[int64]int
	var lastRequestValue map[int64]string
	if d.Decode(&m) != nil || d.Decode(&lastRequest) != nil || d.Decode(&lastRequestValue) != nil {
		panic("readSnapshot: error decode\n")
	} else {
		kv.m = m
		kv.lastRequest = lastRequest
		kv.lastRequestValue = lastRequestValue
	}
	kv.print("Read snapshot\n\t\tk/v:\n%v\n\t\tlastRequest:\n%v\n\t\t lastRequestValue:\n%v\n", mapToString1(kv.m), mapToString2(kv.lastRequest), mapToString3(kv.lastRequestValue))
}

func mapToString1(m map[string]string) string {
	var ret string
	for k, v := range m {
		ret += fmt.Sprintf("\t\t%3v: %v", k, v) + "\n"
	}
	return ret
}

func mapToString2(m map[int64]int) string {
	var ret string
	for k, v := range m {
		ret += fmt.Sprintf("\t\t%3v: %v", k, v) + "\n"
	}
	return ret
}

func mapToString3(m map[int64]string) string {
	var ret string
	for k, v := range m {
		ret += fmt.Sprintf("\t\t%3v: %v", k, v) + "\n"
	}
	return ret
}