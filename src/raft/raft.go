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
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const Debug = 0
const Print = 0

func (rf *Raft) debug(format string, a ...interface{}) {
	if Debug > 0 && !rf.killed {
		rf.print(format, a...)
	}
}

func (rf *Raft) print(format string, a ...interface{}) {
	if Print > 0 && !rf.killed {
		prefix := fmt.Sprintf(" --- Server %v Term %v State %v --- ", rf.me, rf.currentTerm, rf.state)
		log.Printf(prefix+format, a...)
	}
}

const (
	TimeoutLower int = 500
	TimeoutUpper int = 2000
	HeartbeatSleep time.Duration = 150
)

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
	CommandIndex int  // used as lastIncludedIndex if the msg sends a snapshot
	Snapshot 	 []byte
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

	currentTerm int  // persisted
	votedFor int  // persisted, whom this server has voted for in the current term
	log Log  // persisted

	timeout time.Time  // point in time when the server should timeout and start a new election
	leader int  // the known leader in the current term
	votes int  // number of received granted votes in the current term, if this server is a candidate

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg

	snapshot []byte

	killed bool
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)


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
	DPrintf("server %v Make(): #peers=%v\n", rf.me, len(rf.peers))
	rf.applyCh = applyCh
	rf.log = MakeLog()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.print("Make Rafe, reading state\n")
	rf.readPersist(persister.ReadRaftState())

	go rf.timer()
	go rf.heartbeat()
	go rf.applyMsg()
	DPrintf("server %v initialization finished\n", rf.me)

	return rf
}

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

// Assume holding the lock of rf.mu
func (rf *Raft) resetTimeout() {
	rf.timeout = time.Now().Add(time.Duration(TimeoutLower + rand.Intn(TimeoutUpper - TimeoutLower)) * time.Millisecond)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// Assume holding the lock of rf.mu
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	rf.debug("persist: term:%v vote:%v LastSnapshotIndex:%v LastSnapshotTerm:%v log:%v\n", rf.currentTerm, rf.votedFor, rf.log.LastSnapshotIndex, rf.log.LastSnapshotTerm, rf.log.toString())
	rf.persister.SaveRaftState(rf.encodeState())
}

// Assume holding the lock of rf.mu
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
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

	rf.mu.Lock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		panic("readPersist: error decode\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}

	rf.print("Read Raft state: term:%v vote:%v log:\n%v\n", rf.currentTerm, rf.votedFor, rf.log.toString())
	rf.mu.Unlock()
}

func (rf *Raft) Snapshot(snapshot []byte, lastSnapshotIndex int) {
	rf.mu.Lock()

	rf.debug("Compact log lastLogIndex: %v LastSnapshotIndex: %v\n", rf.log.lastLogIndex(), lastSnapshotIndex)
	rf.log.compactUpTo(lastSnapshotIndex)
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	rf.debug("Saved snapshot(%v) and state(%v)\n", rf.persister.SnapshotSize(), rf.persister.RaftStateSize())
	rf.print("Saved Raft state: term:%v vote:%v log:\n%v\n", rf.currentTerm, rf.votedFor, rf.log.toString())

	rf.mu.Unlock()
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
		rf.debug("RequestVote from %v: reject, old term:%v lastLogTerm:%v lastLogIndex:%v  my lastLogTerm:%v  my lastLogIndex:%v\n", args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
	} else if args.Term == rf.currentTerm {
		if rf.leader == -1 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.log.isLessOrEqualUpToDateThan(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			rf.resetTimeout() // because I know there's at least 1 server who has
								// more up-to-date log than me, I should wait more
			reply.VoteGranted = true
			rf.debug("RequestVote from %v: grant, term:%v lastLogTerm:%v lastLogIndex:%v  my lastLogTerm:%v  my lastLogIndex:%v\n", args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
		} else {
			reply.VoteGranted = false
			rf.debug("RequestVote from %v: reject, already voted or log not up-to-day term:%v lastLogTerm:%v lastLogIndex:%v  my lastLogTerm:%v  my lastLogIndex:%v\n", args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
		}
	} else {  // new term
		rf.becomeFollower(args.Term)  // no mater what state I'm in, be a follower
		if rf.log.isLessOrEqualUpToDateThan(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.debug("RequestVote from %v: grant, term:%v lastLogTerm:%v lastLogIndex:%v  my lastLogTerm:%v  my lastLogIndex:%v\n", args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
			// timeout just reset, no need to reset again
		} else {
			reply.VoteGranted = false
			rf.debug("RequestVote from %v: reject, log not up-to-date, term:%v lastLogTerm:%v lastLogIndex:%v  my lastLogTerm:%v  my lastLogIndex:%v\n", args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.log.lastLogTerm(), rf.log.lastLogIndex())
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()

	rf.mu.Unlock()
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

	if ok {
		rf.mu.Lock()

		if args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				rf.debug("reply RequestVote from %v:  see newer term:%v, become follower\n", server, reply.Term)
				rf.becomeFollower(reply.Term)
			} else if reply.Term == rf.currentTerm {
				if reply.VoteGranted {
					if rf.state != Leader {
						rf.debug("reply RequestVote from %v:  vote granted,  current vote:%v\n", server, rf.votes)
						rf.votes++
						if rf.hasMajority(rf.votes) {
							rf.becomeLeader()
							rf.debug("took leadership,  current vote:%v, sending heartbeat\n", rf.votes)
							rf.sendHeartbeat()
						}
					} else {
						rf.debug("reply RequestVote from %v:  vote granted, but already is Leader\n", server)
					}
				} else {
					rf.debug("reply RequestVote from %v:  vote rejected,  current vote:%v\n", server, rf.votes)
				}
			}
		}

		rf.mu.Unlock()
	}
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
		rf.nextIndex[p] = rf.log.len()
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
		rf.log.append(Entry{Command: command, Term: rf.currentTerm})
		index = rf.log.lastLogIndex()
		term = rf.log.lastLogTerm()
		rf.debug("Start() new entry %v, index:%v, term:%v, sending hearbeat\n", command, index, term)
		rf.persist()
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
	rf.mu.Lock()
	rf.killed = true
	rf.mu.Unlock()
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
	rf.mu.Lock()

	if rf.currentTerm > term {
		// Term has advanced even before the election started for the specified term
		rf.debug("election() Unlock: term advanced, election did not start\n")
		rf.mu.Unlock()
		return
	}

	rf.debug("election() start new election, sending RequestVote\n")
	requestVoteArgs := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.log.lastLogIndex(), LastLogTerm: rf.log.lastLogTerm()}
	requestVoteReplies := make([]RequestVoteReply, len(rf.peers))
	for p := 0; p < len(rf.peers); p++ {
		if p != rf.me {
			go rf.sendRequestVote(p, &requestVoteArgs, &requestVoteReplies[p])
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartbeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.debug("Sending periodic heartbeat\n")
			rf.sendHeartbeat()
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatSleep * time.Millisecond)
	}
}

// Assume holding lock of rf.mu
func (rf *Raft) sendHeartbeat() {
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			rf.sendAppendEntriesOrInstallSnapshot(server)
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
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		rf.debug("AppendEntries reject old term %v from server %v\n", args.Term, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false // not necessary
		return
	}

	rf.becomeFollower(args.Term)
	rf.leader = args.LeaderId
	reply.Term = rf.currentTerm  // same as args.Term

	if args.PrevLogIndex > rf.log.lastLogIndex() {
		// If follower's log is shorter than leader's nextIndex
		// Index          	0 1 2 3 4 5 6 ...
		// Leader Term    	0 1 1 1 1 2 2 ...
		// Follower Term  	0 1 1
		// PrevLogIndex                 ^
		// FirstIndex		      ^
		// New nextIndex		  ^
		rf.debug("AppendEntries ack Leader:%v, log shorter than leader's PrevLogIndex:%v  lastLogIndex:%v", args.LeaderId, args.PrevLogIndex, rf.log.lastLogIndex())
		reply.ConflictTerm = -1
		reply.FirstIndex = rf.log.lastLogIndex() + 1 // Leader should directly set nextIndex to this
		reply.Success = false
	} else if args.PrevLogIndex < rf.commitIndex {
		// Leader need not check consistency below rf.commitIndex, because it might already be applied and must be consistent
		rf.debug("AppendEntries ack leader:%v, PrevLogIndex %v < commitIndex %v, ask leader to set nextIndex to commitIndex+1", args.LeaderId, args.PrevLogIndex, rf.commitIndex)
		reply.ConflictTerm = -1
		reply.FirstIndex = rf.commitIndex + 1 // Leader should directly set nextIndex to this
		reply.Success = false
	} else if args.PrevLogTerm != rf.log.getTerm(args.PrevLogIndex) {
		// If leader has the conflict term (here is 1), set nextIndex = leader's last-index-of-conflict-term + 1
		// Index          	0 1 2 3 4 5 6 7 ...
		// Leader Term    	0 1 1 2 2 2 2 2 ...
		// Follower Term  	0 1 1 1 1 1 1
		// PrevLogIndex               ^
		// FirstIndex		  ^
		// New nextIndex		  ^

		// If leader doesn't have the conflict term (here is 1), set nextIndex = follower's first-index-of-conflict-term
		// Index          	0 1 2 3 4 5 6 ...
		// Leader Term    	0 1 1 1 3 3 3 ...
		// Follower Term  	0 1 2 2 2 2
		// PrevLogIndex             ^
		// FirstIndex		    ^
		// New nextIndex		^
		rf.debug("AppendEntries ack Leader:%v, log inconsistent index:%v term:%v leader term:%v  lastLogIndex:%v\n", args.LeaderId, args.PrevLogIndex, rf.log.get(args.PrevLogIndex).Term, args.PrevLogTerm, rf.log.lastLogIndex())
		reply.ConflictTerm = rf.log.getTerm(args.PrevLogIndex)
		// If snapshot contains any entry of conflict term, that means leader has this term in log,
		// and will ignore reply.FirstIndex, so we can leave reply.FirstIndex as -1
		reply.FirstIndex = rf.log.findFirstIndexOfSameTermAt(args.PrevLogIndex)
		//rf.log.truncateAt(args.PrevLogIndex)  /// truncate known invalid entries of conflict term
		reply.Success = false
		//rf.persist()
	} else {
		// Log is consistent up to args.PrevLogIndex
		// If args.Entries contains any conflict entry, truncate and append args.Entries
		// Index          	0 1 2 3 4 5 6 ...
		// Leader Term    	0 1 2(2 2 3 3)...  args.Entries in ()
		// Follower Term  	0 1 2 2 2 2
		// PrevLogIndex         ^
		//							  ^ Different from args.Entries

		// If no conflict in args.Entries, truncate if leader's log in the RPC is longer than the follower
		// Index          	0 1 2 3 4 5 6 ...
		// Leader Term    	0 1 2(2 2 3 3)...  args.Entries in ()
		// Follower Term  	0 1 2 2 2 3
		// PrevLogIndex         ^

		// Leader's log in the RPC is shorter than follower, do not truncate
		// It's possible that follower receive old RPC from leader with valid but shorter entries
		// Index          	0 1 2 3 4 5 6 ...
		// Leader Term    	0 1 2(2 2 3 3)...  args.Entries in ()
		// Follower Term  	0 1 2 2 2 3 3 3 3
		// PrevLogIndex         ^
		if rf.log.hasConflict(args.PrevLogIndex + 1, args.Entries) || args.PrevLogIndex + len(args.Entries) + 1 > rf.log.len() {
			rf.debug("AppendEntries ack Leader:%v, log ok up to %v  lastLogIndex:%v  append Entries:%v\n", args.LeaderId, args.PrevLogIndex, rf.log.lastLogIndex(), args.Entries)
			rf.log.truncateAt(args.PrevLogIndex + 1)
			rf.log.append(args.Entries...)
		} else {
			rf.debug("AppendEntries ack Leader:%v, log ok up to %v  lastLogIndex:%v  but follower already have everything in the entries, no append\n", args.LeaderId, args.PrevLogIndex, rf.log.lastLogIndex())
		}
		reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			rf.debug("AppendEntries update commitIndex, current:%v leader:%v lastLogIndex:%v\n", rf.commitIndex, args.LeaderCommit, rf.log.lastLogIndex())
			if args.LeaderCommit < rf.log.lastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.log.lastLogIndex()
			}
		}
	}
	rf.persist() // persist for all cases because currentTerm might have changed
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	rf.debug("send AppendEntries to server %v  term:%v  PrevLogIndex:%v  PrevLogTerm:%v  LeaderCommit:%v  Entries:%v\n", server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		// Old RPC, the reply is too late, ignore
		return
	}

	if reply.Term > rf.currentTerm {
		rf.debug("reply AppendEntries from %v, newer term:%v, step down", server, reply.Term)
		rf.becomeFollower(reply.Term)
		return
	}

	// Otherwise reply.Term must == rf.currentTerm, because I have stayed leader and if your term is smaller, you would have adopted my term
	if args.PrevLogIndex != rf.nextIndex[server] - 1 {
		// Old RPC, the reply is too late, ignore
		return
	}
	if reply.Success {
		// 3 possible cases: 	both nextIndex and matchIndex changed
		//						nextIndex doesn't change, matchIndex changed
		//						neither changed
		rf.debug("reply AppendEntries from %v, success, curNextIndex:%v newNextIndex:%v oldMatchIndex:%v newMatchIndex:%v\n",
			server, rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1, rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))

		oldMatch := rf.matchIndex[server]
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		if rf.matchIndex[server] > oldMatch {
			rf.updateCommitIndex() // update commitIndex only if matchIndex changed
		}
	} else {
		// See comments in AppendEntries()
		if reply.ConflictTerm == -1 {
			rf.debug("reply AppendEntries from %v, follower's log shorter than PrevLogIndex %v, follower's last index:%v\n", server, args.PrevLogIndex, reply.FirstIndex)
			rf.nextIndex[server] = reply.FirstIndex
		} else if last := rf.log.findLastIndexOfTerm(reply.ConflictTerm); last != -1 {
			rf.debug("reply AppendEntries from %v, conflict term, leader has the term, last index for the term:%v\n", server, last)
			rf.nextIndex[server] = last + 1
		} else {
			// Either leader doesn't have the conflict term, or it's hidden in snapshot
			rf.debug("reply AppendEntries from %v, conflict term, leader doesn't have the term or it's hidden in snapshot, first index of the follower in that term:%v\n", server, reply.FirstIndex)
			rf.nextIndex[server] = reply.FirstIndex
		}

		rf.sendAppendEntriesOrInstallSnapshot(server)
	}
}

// Assume holding the lock of rf.mu
func (rf *Raft) sendAppendEntriesOrInstallSnapshot(server int) {
	if rf.nextIndex[server] <= rf.log.LastSnapshotIndex {
		rf.nextIndex[server] = rf.log.LastSnapshotIndex + 1
		installSnapshotArgs := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LastSnapshotIndex: rf.log.LastSnapshotIndex,
			LastSnapshotTerm:  rf.log.LastSnapshotTerm,
			Snapshot:          rf.persister.ReadSnapshot()}
		go rf.sendInstallSnapshot(server, &installSnapshotArgs)
	} else {
		newArgs := AppendEntriesArgs{
			Term:         	rf.currentTerm,
			LeaderId:     	rf.me,
			PrevLogIndex: 	rf.nextIndex[server] - 1,
			PrevLogTerm:  	rf.log.getTerm(rf.nextIndex[server] - 1),
			Entries:      	rf.log.getFrom(rf.nextIndex[server]),
			LeaderCommit: 	rf.commitIndex}
		go rf.sendAppendEntries(server, &newArgs)
	}
}

// Assume holding the lock of rf.mu
func (rf *Raft) updateCommitIndex() {
	for i := rf.log.lastLogIndex(); i > rf.commitIndex; i-- {
		if rf.log.get(i).Term == rf.currentTerm {
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
}

func (rf *Raft) applyMsg() {
	for {
		var msg *ApplyMsg = nil

		rf.mu.Lock()
		if rf.snapshot != nil {  // First check if there's a snapshot to install, send other commands later
			msg = &ApplyMsg{CommandValid: false, Snapshot: rf.snapshot, CommandIndex: rf.lastApplied}
			rf.snapshot = nil
		} else if rf.lastApplied < rf.log.LastSnapshotIndex {
			rf.lastApplied = rf.log.LastSnapshotIndex // Invariant: lastApplied always >= LastSnapshotIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied  // Invariant: lastApplied always <= commitIndex
			}
		} else if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg = &ApplyMsg{CommandValid: true, Command: rf.log.get(rf.lastApplied).Command, CommandIndex: rf.lastApplied}
			rf.debug("ApplyMsg  lastLogIndex:%v  applyingIndex:%v  Raft commitIndex:%v  command:%v\n", rf.log.lastLogIndex(), msg.CommandIndex, rf.commitIndex, msg.Command)
		}
		rf.mu.Unlock()

		if msg != nil {
			rf.applyCh <- *msg
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) hasMajority(count int) bool {
	return count >= len(rf.peers)/2 + 1
}

type Log struct {
	Log               []Entry
	LastSnapshotIndex int // Only modified when creating snapshot or installing snapshot
	LastSnapshotTerm  int // Only modified when creating snapshot or installing snapshot
}

func MakeLog() Log {
	var log Log
	log.LastSnapshotIndex = 0
	log.LastSnapshotTerm = 0
	//log.append(Entry{Term: 0})
	return log
}

func (lg *Log) len() int {
	return lg.LastSnapshotIndex + len(lg.Log) + 1
}

func (lg *Log) lastLogIndex() int {
	return lg.LastSnapshotIndex + len(lg.Log)
}

func (lg *Log) get(i int) Entry {
	if i <= lg.LastSnapshotIndex {
		panic(fmt.Sprintf("Can't get log entry %v, LastSnapshotIndex: %v", i, lg.LastSnapshotIndex))
	} else if i > lg.lastLogIndex() {
		panic(fmt.Sprintf("Log entry %v out of range, lastLogIndex: %v", i, lg.lastLogIndex()))
	} else {
		return lg.Log[i - lg.LastSnapshotIndex- 1]
	}
}

func (lg *Log) getFrom(i int) []Entry {
	if i <= lg.LastSnapshotIndex {
		panic(fmt.Sprintf("Can't getFrom log entry %v, LastSnapshotIndex: %v", i, lg.LastSnapshotIndex))
	} else if i > lg.len() {
		panic(fmt.Sprintf("Log entry %v out of range, lastLogIndex: %v", i, lg.lastLogIndex()))
	} else {
		return lg.Log[i - lg.LastSnapshotIndex- 1:]
	}
}

// This func can get term for LastSnapshotIndex
func (lg *Log) getTerm(i int) int {
	if i < lg.LastSnapshotIndex {
		panic(fmt.Sprintf("Can't getTerm for log entry %v, LastSnapshotIndex: %v", i, lg.LastSnapshotIndex))
	} else if i >= lg.len() {
		panic(fmt.Sprintf("getTerm for %v out of range, lastLogIndex: %v", i, lg.lastLogIndex()))
	} else if i == lg.LastSnapshotIndex {
		return lg.LastSnapshotTerm
	} else {
		return lg.get(i).Term
	}
}

// This func can get term at LastSnapshotIndex
func (lg *Log) lastLogTerm() int {
	return lg.getTerm(lg.lastLogIndex())
}

func (lg *Log) append(entries ...Entry) {
	lg.Log = append(lg.Log, entries...)
}

func (lg *Log) truncateAt(i int) {
	if i <= lg.LastSnapshotIndex {
		panic(fmt.Sprintf("Can't truncate log entry %v, LastSnapshotIndex: %v", i, lg.LastSnapshotIndex))
	//} else if i > lg.lastLogIndex() {
	//	panic(fmt.Sprintf("Log entry %v out of range, lastLogIndex: %v", i, lg.lastLogIndex()))
	} else {
		lg.Log = lg.Log[:i - lg.LastSnapshotIndex - 1]
	}
}

func (lg *Log) toString() string {
	ret := fmt.Sprintf("\t\tSnapshot lastIndex: %v lastTerm: %v\n\t\t", lg.LastSnapshotIndex, lg.LastSnapshotTerm)
	for i := lg.LastSnapshotIndex + 1; i <= lg.lastLogIndex(); i++ {
		if i % 10 == 0 {
			ret += "\n\t\t"
		}
		ret += fmt.Sprintf("%v: %v ", i, lg.get(i).Command)
	}
	return ret
}

func (lg *Log) isLessOrEqualUpToDateThan(term int, index int) bool {
	if lg.lastLogTerm() > term {
		return false
	} else if lg.lastLogTerm() < term {
		return true
	} else {
		return lg.lastLogIndex() <= index
	}
}

// 						0 1 2 3 4 5 6 ...
// log						  0 1 2 3 ...
// lastSnapshotIndex        ^
// lastApplied					  ^
func (lg *Log) compactUpTo(lastApplied int) {
	if lastApplied < 0 {
		panic(fmt.Sprintf("Can't snapshot at index %v", lastApplied))
	} else if lastApplied > lg.lastLogIndex() {
		panic(fmt.Sprintf("Can't snapshot at index %v, log lastIndex:%v", lastApplied, lg.lastLogIndex()))
	} else {
		// Note: the order matters here
		lg.LastSnapshotTerm = lg.getTerm(lastApplied)
		lg.Log = lg.Log[lastApplied - lg.LastSnapshotIndex:]
		lg.LastSnapshotIndex = lastApplied
	}
}

// Return -1 if term-at-index-i == LastSnapshotTerm, because we can't know what's the first index of the term
func (lg *Log) findFirstIndexOfSameTermAt(i int) int {
	term := lg.getTerm(i)
	for ; i >= lg.LastSnapshotIndex; i-- {
		if lg.getTerm(i) != term {
			return i + 1
		}
	}
	return -1
}

func (lg *Log) hasConflict(index int, entries []Entry) bool {
	for i, j := index, 0; i <= lg.lastLogIndex() && j < len(entries); i, j = i + 1, j + 1 {
		if lg.getTerm(i) != entries[j].Term {
			return true
		}
	}
	return false
}

func (lg *Log) findLastIndexOfTerm(term int) int {
	for i := lg.lastLogIndex(); i >= lg.LastSnapshotIndex; i-- {
		if lg.getTerm(i) < term {
			return -1
		} else if lg.getTerm(i) == term {
			return i
		}
	}
	return -1
}

type InstallSnapshotArgs struct {
	Term              int
	LastSnapshotIndex int
	LastSnapshotTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	}

	if args.LastSnapshotIndex <= rf.log.lastLogIndex() {
		return
	}

	rf.debug("InstallSnapshot from Term:%v LastSnapshotIndex:%v\n", args.Term, args.LastSnapshotIndex)
	// From now on, Raft will behave as if kv server has installed the snapshot, even if it might be still on the way
	// through the command channel
	rf.log.Log = []Entry{}
	rf.log.LastSnapshotIndex = args.LastSnapshotIndex
	rf.log.LastSnapshotTerm = args.LastSnapshotTerm

	// Next applyMsg() will see the snapshot and first send snapshot through the command channel,
	// even if new log entries are committed between finish of InstallSnapshot() and begin of applyMsg()
	rf.lastApplied = args.LastSnapshotIndex
	rf.commitIndex = rf.lastApplied
	rf.snapshot = args.Snapshot
	// Snapshot and state must be persisted now instead of on server side, because we've set the log so that it seems to
	// leader it has persisted the snapshot and state, and the server might commit entries. If this follower crashes
	// before the snapshot is actually persisted, we can lose data.
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Snapshot)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	rf.debug("send InstallSnapshot to server %v  Term:%v  LastSnapshotIndex:%v  LastSnapshotTerm:%v\n", server, args.Term, args.LastSnapshotIndex, args.LastSnapshotTerm)
	rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
}