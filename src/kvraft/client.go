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
	requestId int  // Will start at 1, initial lastRequest[clientId] will be 0
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
	if Debug > 0 {
		prefix := fmt.Sprintf(" --- Clerk    %2v --- ", ck.clientId)
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
		reply := GetPutAppendReply{}
		ck.debug("Get       to   %2v send     Key:%4v RequestId:%4v\n", ck.leaderId, args.Key, args.RequestId)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.WrongLeader {
				ck.debug("Get       from %2v wrong    Key:%4v RequestId:%4v\n", ck.leaderId, args.Key, args.RequestId)
			} else if reply.Err != "" {
				ck.debug("Get       from %2v fail     Key:%4v RequestId:%4v\n", ck.leaderId, args.Key, args.RequestId)
			} else {
				ck.debug("Get       from %2v success  Key:%4v RequestId:%4v value: %v\n", ck.leaderId, args.Key, args.RequestId, reply.Value)
				return reply.Value
			}
		} else {
			ck.debug("Get       from %2v RPC lost Key:%4v RequestId:%4v\n", ck.leaderId, args.Key, args.RequestId)
		}
		ck.nextLeader()
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
		reply := GetPutAppendReply{}
		ck.debug("PutAppend to   %2v send     Key:%4v RequestId:%4v Value: %v\n", ck.leaderId, args.Key, args.RequestId, args.Value)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.WrongLeader {
				ck.debug("PutAppend from %2v wrong    Key:%4v RequestId:%4v\n", ck.leaderId, args.Key, args.RequestId)
			} else if reply.Err != "" {
				ck.debug("PutAppend from %2v fail     Key:%4v RequestId:%4v Value: %v\n", ck.leaderId, args.Key, args.RequestId, args.Value)
			} else {
				ck.debug("PutAppend from %2v success  Key:%4v RequestId:%4v Value: %v Result Value: %v\n", ck.leaderId, args.Key, args.RequestId, args.Value, reply.Value)
				return
			}
		} else {
			ck.debug("PutAppend from %2v RPC lost Key:%4v RequestId:%4v Value: %v\n", ck.leaderId, args.Key, args.RequestId, args.Value)
		}
		ck.nextLeader()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
