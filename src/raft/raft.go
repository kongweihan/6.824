package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	currentTerm int
	timeout time.Time  // point in time when the server should timeout and start a new election
	leader int  // the known leader in the current term
	votedFor int  // whom this server has voted for in the current term
	votes int  // number of received granted votes in the current term, if this server is a candidate

	log []Entry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Entry struct {
	Command interface{}
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	// To make sure the state is returned for the correct corresponding term, need to lock and store both values and
	// return them together.
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int  // candidate's term
	CandidateId int  // candidate requesting vote
	LastLogIndex int  // index of candidate's last log entry
	LastLogTerm int  // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int  // currentTerm from the voting server, for candidate to update itself
	VoteGranted bool  // true means candidate received vote from the voting server
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.state == Follower {
			if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.logIsLatest(args.LastLogTerm, args.LastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.resetTimeout() // because I know there's at least 1 server who has
									// more up-to-date log than me, I should wait more
				reply.VoteGranted = true
			} else {
				// Alternative: should I become candidate and start election immediately, or wait for timeout?
				reply.VoteGranted = false
			}
		} else {
			// Leader state: reject vote
			// Candidate state: I have started election for this term and voted for myself, reject vote
			reply.VoteGranted = false
		}
	} else {  // new term
		rf.becomeFollower(args.Term)  // no mater what state I'm in, be a follower
		if rf.logIsLatest(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			// timeout just reset, no need to reset again
		} else {
			// Alternative: should I become candidate and start election immediately, or wait for timeout?
			reply.VoteGranted = false
		}
	}

	reply.Term = rf.currentTerm

	rf.mu.Unlock()
}

// Assume holding the lock of rf.mu
func (rf *Raft) logIsLatest(term int, index int) bool {
	if rf.lastLogTerm() > term {
		return false
	} else if rf.lastLogTerm() < term {
		return true
	} else {
		return len(rf.log) - 1 <= index
	}
}

func (rf *Raft) resetTimeout() {
	rf.timeout = time.Now().Add(time.Duration(1000 + rand.Intn(1000)) * time.Millisecond)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()

	if reply.Term > rf.currentTerm {
		rf.debug("RequestVote reply from %v:  see newer term:%v, step down\n", server, reply.Term)
		rf.becomeFollower(args.Term)
	} else {
		if reply.VoteGranted {
			if rf.state != Leader {
				rf.debug("RequestVote reply from %v:  vote granted,  current vote:%v\n", server, rf.votes)
				rf.votes++
				if rf.hasMajority(rf.votes) {
					rf.becomeLeader()
					rf.debug("RequestVote: took leadership,  current vote:%v\n", rf.votes)
					rf.sendHeartbeat()
				}
			} else {
				rf.debug("RequestVote reply from %v:  vote granted, but already is Leader\n", server)
			}
		} else {
			rf.debug("RequestVote reply from %v:  vote rejected,  current vote:%v\n", server, rf.votes)
		}
	}

	rf.mu.Unlock()
	return ok
}

// Assume holding the lock of rf.mu
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term  // update my current term
	rf.resetTimeout()
	rf.leader = -1
	rf.votedFor = -1
	rf.votes = 0
}

// Assume holding the lock of rf.mu
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.leader = rf.me
	for p := 0; p < len(rf.peers); p++ {
		rf.nextIndex[p] = len(rf.log)
		rf.matchIndex[p] = 0
	}
}

// Assume holding the lock of rf.mu
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.resetTimeout()
	rf.leader = -1
	rf.votedFor = rf.me
	rf.votes = 1
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	if rf.state != Leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, Entry{Command: command, Term: rf.currentTerm})
		index = rf.lastLogIndex()
		term = rf.lastLogTerm()
		rf.debug("Start() new entry %v, index:%v, term:%v\n", command, index, term)
		rf.sendHeartbeat()
	}

	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("%v Make(): #peers=%v\n", rf.me, len(rf.peers))
	rf.applyCh = applyCh
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	go rf.timer()
	go rf.heartbeat()
	go rf.applyMsg()
	DPrintf("%v initialization finished\n", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) timer() {
	for {
		rf.mu.Lock()

		if rf.state != Leader {
			if time.Now().After(rf.timeout) {
				rf.becomeCandidate()
				rf.debug("timer() new term candidate, going to start election\n")
				go rf.election(rf.currentTerm) // Start election for the specified term
			}
		}

		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) election(term int) {
	//rf.debug("election() Lock\n")
	rf.mu.Lock()

	if rf.currentTerm > term {
		// Term has advanced even before the election started for the specified term
		rf.debug("election() Unlock: term advanced, election did not start\n")
		rf.mu.Unlock()
		return
	}

	rf.debug("election() start new election, sending RequestVote\n")
	requestVoteArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log), LastLogTerm: rf.lastLogTerm()}
	requestVoteReplies := make([]RequestVoteReply, len(rf.peers))
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			go rf.sendRequestVote(p, &requestVoteArgs, &requestVoteReplies[p])
		}
	}
	//rf.debug("election() Unlock: RequestVote sent\n")
	rf.mu.Unlock()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) heartbeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.sendHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(105 * time.Millisecond)
	}
}

// Assume holding lock of rf.mu
func (rf *Raft) sendHeartbeat() {
	rf.debug("Sending heartbeat\n")
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			go rf.sendAppendEntries(p)
		}
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool  // if false, either follower has newer term, or has conflict term, use ConflictTerm and FirstIndex
	ConflictTerm int  // conflict term of the follower, -1 means the entry at PrevLogIndex doesn't exist in follower's log
	FirstIndex int  // first index of the conflict term of the follower
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false  // not necessary
		rf.debug("AppendEntries reject old term from server %v\n", args.LeaderId)
	} else {
		rf.becomeFollower(args.Term)
		rf.leader = args.LeaderId
		reply.Term = rf.currentTerm  // same as args.Term

		if rf.lastLogIndex() < args.PrevLogIndex {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.FirstIndex = rf.lastLogIndex() + 1
		} else {
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				rf.debug("AppendEntries ack Leader:%v, log inconsistent index:%v term:%v leader term:%v  lastLogIndex:%v\n", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, rf.lastLogIndex())
				reply.Success = false
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				reply.FirstIndex = rf.findFirstIndex(reply.ConflictTerm)
				rf.log = rf.log[:args.PrevLogIndex]  // truncate known invalid entries (with conflict term)
			} else {
				rf.debug("AppendEntries ack Leader:%v, log ok up to %v  lastLogIndex:%v  append entries:%v\n", args.LeaderId, args.PrevLogIndex, rf.lastLogIndex(), args.Entries)
				rf.log = rf.log[:args.PrevLogIndex + 1]
				rf.log = append(rf.log, args.Entries...)
				if args.LeaderCommit > rf.commitIndex {
					rf.debug("AppendEntries update commitIndex, current:%v leader:%v lastLogIndex:%v\n", rf.commitIndex, args.LeaderCommit, rf.lastLogIndex())
					if args.LeaderCommit < rf.lastLogIndex() {
						rf.commitIndex = args.LeaderCommit
					} else {
						rf.commitIndex = rf.lastLogIndex()
					}
				}
				reply.Success = true
			}
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int) {
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm: rf.log[rf.nextIndex[server] - 1].Term,
		LeaderCommit: rf.commitIndex}
	if rf.nextIndex[server] <= rf.lastLogIndex() {
		args.Entries = rf.log[rf.nextIndex[server]:]
	}
	reply := AppendEntriesReply{}
	rf.debug("sendAppendEntires to server %v  PrevLogIndex:%v  PrevLogTerm:%v  LeaderCommit:%v  Entries:%v", server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if ok {
		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
		} else if reply.Term == args.Term && reply.Term == rf.currentTerm {
			if rf.state == Leader && args.PrevLogIndex == rf.nextIndex[server] - 1 {
				if reply.Success {
					// 3 possible cases: 	both nextIndex and matchIndex changed
					//						nextIndex doesn't change, matchIndex changed
					//						neither changed
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
					oldMatch := rf.matchIndex[server]
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					if rf.matchIndex[server] > oldMatch {
						rf.updateCommitIndex()  // only need to update commitIndex if matchIndex changed
					}
				} else {
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.FirstIndex
					} else if last := rf.findLastIndex(reply.ConflictTerm); last != -1 {
						rf.nextIndex[server] = last + 1
					} else {
						rf.nextIndex[server] = reply.FirstIndex
					}
					go rf.sendAppendEntries(server)
				}
			}
		}

		rf.mu.Unlock()
	}
}

// Assume holding the lock of rf.mu
func (rf *Raft) findFirstIndex(term int) int {
	lastIndex := rf.findLastIndex(term)
	for i := lastIndex; i >= 0; i-- {
		if rf.log[i].Term < term {
			return i + 1
		}
	}
	panic("findFirstIndex should never take a non-exist term")
}

// Assume holding the lock of rf.mu
func (rf *Raft) findLastIndex(term int) int {
	for i := rf.lastLogIndex(); i >= 0; i-- {
		if rf.log[i].Term < term {
			return -1
		} else if rf.log[i].Term == term {
			return i
		}
	}
	return -1
}

// Assume holding the lock of rf.mu
func (rf *Raft) updateCommitIndex() {
	for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
		if rf.log[i].Term == rf.currentTerm {
			count := 1  // leader itself has the log for sure
			for p := 0; p < len(rf.peers); p++ {
				if p != rf.me {
					if rf.matchIndex[p] >= i {
						count++
					}
				}
			}
			if rf.hasMajority(count) {
				rf.commitIndex = i
				break
			}
		} else {
			break
		}
	}


	//for i := rf.commitIndex + 1; i < len(rf.log); i++ {
	//	count := 1  // leader itself has the log for sure
	//	for p := 0; p < len(rf.peers); p++ {
	//		if p != rf.me {
	//			if rf.matchIndex[p] >= i {
	//				count++
	//			}
	//		}
	//	}
	//	if rf.hasMajority(count) && {
	//		rf.commitIndex = i
	//	} else {
	//		return
	//	}
	//}
}

func (rf *Raft) applyMsg() {
	for {
		var commitIndex int
		rf.mu.Lock()
		commitIndex = rf.commitIndex
		rf.mu.Unlock()

		if rf.lastApplied < commitIndex {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			rf.debug("ApplyMsg  command:%v  index:%v  commitIndex:%v\n", rf.log[rf.lastApplied], rf.lastApplied, rf.commitIndex)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) hasMajority(count int) bool {
	return count >= len(rf.peers)/2 + 1
}

func (rf *Raft) debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf(" --- Server %v Term %v State %v --- ", rf.me, rf.currentTerm, rf.state)
	DPrintf(prefix + format, a...)
}