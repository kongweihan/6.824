package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
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
	// Your code here.
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.requestId++
	args := QueryArgs{Num: num, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply ShardMasterReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.requestId++
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply ShardMasterReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	ck.requestId++
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply ShardMasterReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	ck.requestId++
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply ShardMasterReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
