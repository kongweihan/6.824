package raftkv

import (
	"fmt"
	"labrpc"
	"log"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId int64
	leaderId int
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) debug(format string, a ...interface{}) {
	if DebugClient > 0 {
		prefix := fmt.Sprintf(" --- Clerk %v --- ", ck.clientId)
		log.Printf(prefix+format, a...)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.requestId++
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		reply := GetReply{}
		ck.debug("Get leader:%v send args:%v\n", ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.WrongLeader || reply.Err != "" {
				ck.debug("Get leader:%v fail args:%v wrongLeader:%v\n", ck.leaderId, args, reply.WrongLeader)
				ck.nextLeader()
			} else {
				ck.debug("Get leader:%v success args:%v\n", ck.leaderId, args)
				return reply.Value
			}
		} else {
			ck.debug("Get leader:%v RPC lost args:%v\n", ck.leaderId, args)
			ck.nextLeader()
		}
	}
}

func (ck *Clerk) nextLeader() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestId++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		reply := PutAppendReply{}
		ck.debug("Get leader:%v send args:%v\n", ck.leaderId, args)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader || reply.Err != "" {
				ck.debug("PutAppend leader:%v fail args:%v wrongleader:%v\n", ck.leaderId, args, reply.WrongLeader)
				ck.nextLeader()
			} else {
				ck.debug("PutAppend leader:%v success args:%v\n", ck.leaderId, args)
				return
			}
		} else {
			ck.debug("PutAppend leader:%v RPC lost args:%v\n", ck.leaderId, args)
			ck.nextLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
