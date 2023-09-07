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
	//	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
func (r *Raft) Dolog(index int, i ...interface{}) {
	if index == -1 {
		log.Println(append([]interface{}{interface{}(fmt.Sprintf("[T:%d]{Level:%d}Server[%d]-[nil]", atomic.LoadInt32(&r.Term), atomic.LoadInt32(&r.Level), r.me))}, i...)...)
	} else {
		log.Println(append([]interface{}{interface{}(fmt.Sprintf("[T:%d]{Level:%d}Server[%d]-[%d]", atomic.LoadInt32(&r.Term), atomic.LoadInt32(&r.Level), r.me, index))}, i...)...)
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type RaftPeer struct {
	C              *labrpc.ClientEnd
	BeginHeartBeat chan struct{}
	StopHeartBeat  chan struct{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	IsLeaderAlive   int32 //
	Follower        int32
	Level           int32 //
	LogIndex        int32 //
	LogIndexLock    sync.Locker
	Term            int32 //
	TermLock        sync.Locker
	TimeOutChan     chan struct{}
	LevelChangeChan chan struct{}
	RaftPeers       []RaftPeer
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

var (
	LevelLeader    = int32(3)
	LevelCandidate = int32(2)
	LevelFollower  = int32(1)
)

func (r *Raft) GetLogIndex() int32 {
	return atomic.LoadInt32(&r.LogIndex)
}
func (r *Raft) LoadLogIndex(i int32) {
	atomic.StoreInt32(&r.LogIndex, i)
}
func (r *Raft) GetTerm() int32 {
	r.TermLock.Lock()
	defer r.TermLock.Unlock()
	return r.Term
}
func (r *Raft) SetTerm(i int32) {
	r.TermLock.Lock()
	defer r.TermLock.Unlock()
	r.Term = i
}
func (r *Raft) BeginSendHeartBeat() {
	for i := range r.RaftPeers {
		if i != r.me {
			select {
			case r.RaftPeers[i].BeginHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (r *Raft) StopSendHeartBeat() {
	for i := range r.RaftPeers {
		if i != r.me {
			select {
			case r.RaftPeers[i].StopHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (r *Raft) registeHeartBeat(index int) {
	arg := RequestVoteArgs{SelfIndex: int32(r.me)}
	for !r.killed() {
	restart:
		<-r.RaftPeers[index].BeginHeartBeat
		for {
			arg.LogIndex = r.GetLogIndex()
			arg.Term = atomic.LoadInt32(&r.Term)
			arg.Time = time.Now()
			rpl := RequestVoteReply{}
			select {
			case <-r.RaftPeers[index].StopHeartBeat:
				goto restart
			case <-time.NewTimer(25 * time.Millisecond).C:
				if r.GetLevel() != LevelLeader {
					goto restart
				}
				ok := r.RaftPeers[index].C.Call("Raft.HeartBeat", &arg, &rpl)
				// ok := r.peers[index].Call("Raft.HeartBeat", &arg, &rpl)
				r.Dolog(index, "Raft.HeartBeat", ok, arg.String(), rpl.String())
				if ok && !rpl.IsAgree {
					r.ChangeToFollower(&rpl)
				}
			}
		}
	}
}
func (r *Raft) ChangeToLeader() {
	r.Dolog(-1, "Going to Be Leader")
	r.SetLevel(LevelLeader)
	select {
	case r.LevelChangeChan <- struct{}{}:
	default:
	}
	r.BeginSendHeartBeat()
}
func (r *Raft) ChangeToCandidate() {
	r.Dolog(-1, "Going to Be Candidate")
	// atomic.StoreInt32(&r.Level, LevelCandidate)
	r.SetLevel(LevelCandidate)
	select {
	case r.LevelChangeChan <- struct{}{}:
	default:
	}
}
func (r *Raft) ChangeToFollower(rpl *RequestVoteReply) {
	r.Dolog(-1, "Going to Be Follower")
	if atomic.LoadInt32(&r.Level) == LevelLeader {
		r.StopSendHeartBeat()
	}
	if rpl != nil {
		if rpl.PeerTerm > r.GetTerm() {
			r.SetTerm(rpl.PeerTerm)
		}
	}
	r.SetLevel(LevelFollower)
	select {
	case r.LevelChangeChan <- struct{}{}:
	default:
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func (r *Raft) HeartBeat(arg *RequestVoteArgs, rpl *RequestVoteReply) {
	r.Dolog(int(arg.SelfIndex), "RECVED heatbeat")
	defer func() {
		r.TimeOutChan <- struct{}{}
	}()
	rpl.PeerLogIndex = r.GetLogIndex()
	rpl.PeerTerm = r.GetTerm()
	rpl.ReturnTime = time.Now()
	if arg.LogIndex < rpl.PeerLogIndex || arg.Term < rpl.PeerTerm {
		rpl.IsAgree = false
		return
	} else {
		rpl.IsAgree = true
	}
	if r.GetLevel() == LevelLeader {
		rpll := rpl
		rpll.PeerTerm = arg.Term
		r.ChangeToFollower(rpll)
	}

}
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.Level = LevelFollower
	rf.IsLeaderAlive = 1
	rf.TimeOutChan = make(chan struct{}, 1)
	rf.LevelChangeChan = make(chan struct{}, 1)
	rf.RaftPeers = make([]RaftPeer, len(rf.peers))
	for i, _ := range rf.peers {
		if i != rf.me {
			rf.RaftPeers[i] = RaftPeer{C: rf.peers[i], BeginHeartBeat: make(chan struct{}), StopHeartBeat: make(chan struct{})}
			go rf.registeHeartBeat(i)
		}
	}
	rf.LogIndexLock = &sync.Mutex{}
	rf.TermLock = &sync.Mutex{}
	file, _ := os.Create(fmt.Sprintf("/tmp/raft_%d", os.Getpid()))
	log.SetOutput(file)
	log.SetFlags(log.Lmicroseconds)
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).

	return int(rf.GetTerm()), rf.GetLevel() == LevelLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term      int32
	LogIndex  int32
	Time      time.Time
	SelfIndex int32
}

func (r *RequestVoteArgs) String() string {
	return fmt.Sprintf("%+v ", *r)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	PeerTerm     int32
	PeerLogIndex int32
	ReturnTime   time.Time
	IsAgree      bool
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("%+v ", *r)
}

func (r *Raft) RequestPreVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.PeerLogIndex = r.GetLogIndex()
	reply.PeerTerm = r.GetTerm()
	reply.ReturnTime = time.Now()
	reply.IsAgree = atomic.LoadInt32(&r.IsLeaderAlive) == 0
	r.Dolog(int(args.SelfIndex), "get RequestPreVote REQUEST", args, reply)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.TermLock.Lock()
	defer rf.TermLock.Unlock()
	reply.PeerTerm = rf.Term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Term < args.Term && args.LogIndex >= rf.GetLogIndex() {
		reply.IsAgree = true
		rf.Term = args.Term
	} else {
		reply.IsAgree = false
	}
	reply.PeerLogIndex = rf.GetLogIndex()
	reply.ReturnTime = time.Now()
	rf.Dolog(int(args.SelfIndex), "Been Called RequestVote", args, reply)
	// Your code here (2A, 2B).
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.Dolog(-1, "killdead")
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) GetLevel() int32 {
	return atomic.LoadInt32(&rf.Level)
}

func (rf *Raft) SetLevel(i int32) {
	atomic.StoreInt32(&rf.Level, i)
	rf.Dolog(-1, "Level set to ", i)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.GetLevel() {
		case LevelFollower:
			select {
			case <-rf.LevelChangeChan:
			case <-rf.TimeOutChan:
				atomic.StoreInt32(&rf.IsLeaderAlive, 1)
			case <-time.NewTimer(time.Duration((150 + rand.Int63()%150) * time.Hour.Milliseconds())).C:
				rf.Dolog(-1, "TimeOut")
				atomic.StoreInt32(&rf.IsLeaderAlive, 0)
				go TryToBecomeCandidate(rf)
			}
		case LevelCandidate:
			select {
			case <-rf.LevelChangeChan:
			case <-rf.TimeOutChan:
			}
		case LevelLeader:
			select {
			case <-rf.LevelChangeChan:
			case <-rf.TimeOutChan:
			}
		}
	}
}

func TryToBecomeCandidate(rf *Raft) {
	rf.ChangeToCandidate()
	arg := RequestVoteArgs{Term: rf.GetTerm() + 1, LogIndex: rf.GetLogIndex(), Time: time.Now(), SelfIndex: int32(rf.me)}
	rpl := make([]RequestVoteReply, len(rf.peers))
	wg := &sync.WaitGroup{}
	for i := range rf.RaftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				rf.Dolog(index, "Raft.RequestPreVote  GO ", index, arg.String())
				// ok := rf.RaftPeers[index].C.Call("Raft.RequestPreVote", &arg, &rpl[index])
				ok := rf.Call(index, "Raft.RequestPreVote", &arg, &rpl[index])
				rf.Dolog(index, "Raft.RequestPreVote RETURN ", index, ok, rpl[index].String())
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	count := 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerTerm >= arg.Term || rpl[i].PeerLogIndex > rf.GetLogIndex() {
				rf.ChangeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count > len(rf.peers)/2 {
		rf.SetTerm(rf.GetTerm() + 1)
	} else {
		rf.ChangeToFollower(nil)
		return
	}
	for i := range rf.RaftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				rf.Dolog(index, "Raft.RequestVote", arg.String())
				// ok := rf.RaftPeers[index].C.Call("Raft.RequestVote", &arg, &rpl[index])
				ok := rf.Call(index, "Raft.RequestVote", &arg, &rpl[index])
				rf.Dolog(index, "Raft.RequestVote", ok, rpl[index].String())
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	count = 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerTerm >= rf.GetTerm() || rpl[i].PeerLogIndex > rf.GetLogIndex() {
				rf.ChangeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count < len(rf.peers)/2 {
		rf.ChangeToFollower(nil)
		return
	} else {
		rf.ChangeToLeader()
	}
}

func (r *Raft) Call(index int, FuncName string, arg *RequestVoteArgs, rpl *RequestVoteReply) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	i := false
	go func() {
		i = r.peers[index].Call(FuncName, arg, rpl)
		cancel()
	}()
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond * 100):
		r.Dolog(index, "Rpc Timeout ", FuncName, arg.String(), rpl.String())
	}
	return i
}
