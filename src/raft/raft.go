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
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

func (r *Raft) checkFuncDone(FuncName string) func() {
	t := time.Now().UnixMilli()
	i := make(chan bool, 1)
	i2 := make(chan bool, 1)
	go func() {
		r.Dolog(-1, t, FuncName+" GO")
		i2 <- true
		for {
			select {
			case <-time.After(HeartbeatTimeout * 2 * time.Millisecond):
				r.Dolog(-1, "\n", t, "!!!!\t", FuncName+" MayLocked\n")
			case <-i:
				close(i)
				return
			}
		}
	}()
	<-i2
	close(i2)
	return func() {
		i <- true
		r.Dolog(-1, t, FuncName+" return", time.Now().UnixMilli()-t, "ms")
	}
}

var (
	LevelLeader    = int32(3)
	LevelCandidate = int32(2)
	LevelFollower  = int32(1)

	commitChanSize   = int32(100)
	HeartbeatTimeout = 40 * time.Millisecond
	voteTimeOut      = 100

	LogCheckBeginOrReset = 0
	LogCheckAppend       = 1
	LogCheckStore        = 2
	LogCheckIgnore       = 3
	LogCheckReLoad       = 4
	LogCheckSnap         = 5

	UpdateLogLines = 200

	LogStateNormal = int32(0)
	LogUpdateIng   = int32(1)
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
		log.Println(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[nil]", atomic.LoadInt32(&r.level), atomic.LoadInt32(&r.term), r.me)))}, i...)...)
	} else {
		log.Println(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[%d]", atomic.LoadInt32(&r.level), atomic.LoadInt32(&r.term), r.me, index)))}, i...)...)
	}

	if index == -1 {
		r.DebugLoger.Println(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[nil]", atomic.LoadInt32(&r.level), atomic.LoadInt32(&r.term), r.me)))}, i...)...)
	} else {
		r.DebugLoger.Println(append([]interface{}{interface{}(fmt.Sprintf("%-35s", fmt.Sprintf("{Level:%d}[T:%d]Server[%d]-[%d]", atomic.LoadInt32(&r.level), atomic.LoadInt32(&r.term), r.me, index)))}, i...)...)
	}

}

type RequestArgs struct {
	// Your data here (2A, 2B).
	SelfTerm     int32
	LastLogIndex int32
	LastLogTerm  int32
	Time         time.Time
	SelfIndex    int32
	CommitIndex  int32
	// If there is this field,
	// it means that this request will use to
	// synchronize logs.
	Msg []*LogData
}
type RequestReply struct {
	// Your data here (2A).
	PeerSelfTerm     int32
	PeerLastLogIndex int32
	PeerLastLogTerm  int32
	ReturnTime       time.Time
	IsAgree          bool
	PeerCommitIndex  int32
	// If there is this field,
	// it means that this reply will use to
	// request more LogData.
	LogDataMsg *LogData
}

// save in Sequence table
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	SnapshotTerm  int
	SnapshotIndex int
	Snapshot      []byte
}

// a pieces of Server Talk
type LogData struct {
	Msg           *ApplyMsg
	LastTimeIndex int
	LastTimeTerm  int
	SelfIndex     int
	SelfTermNow   int
}

func (a *ApplyMsg) string() string {
	return fmt.Sprintf("%+v", *a)
}
func (a *LogData) string() string {
	return fmt.Sprintf(" %+v[this.Msg:%s] ", *a, a.Msg.string())
}

// a piece of Raft
type Log struct {
	Msgs    []*ApplyMsg
	MsgRwMu sync.RWMutex
}

// A Go object implementing a single Raft peer.
type RaftPeer struct {
	C                *labrpc.ClientEnd
	modeLock         sync.Mutex
	BeginHeartBeat   chan struct{}
	StopHeartBeat    chan struct{}
	JumpHeartBeat    chan struct{}
	SendHeartBeat    chan struct{}
	logIndexTermLock sync.Mutex
	logIndex         int32
	lastLogTerm      int32
	lastTalkTime     int64
	commitIndex      int32
}

func (R *RaftPeer) updateLastTalkTime() {
	atomic.StoreInt64(&R.lastTalkTime, time.Now().UnixMicro())
}
func (R *RaftPeer) isTimeOut() bool {
	return time.Now().UnixMicro()-atomic.LoadInt64(&R.lastTalkTime) > (HeartbeatTimeout*3/2).Microseconds()
}
func (rf *Raft) checkOutLeaderOnline() bool {
	Len := len(rf.raftPeers)
	count := 1
	for i := range rf.raftPeers {
		if i != rf.me && !rf.raftPeers[i].isTimeOut() {
			count++
			if count > Len/2 {
				return true
			}
		}
	}
	return false
}

// tmp stuct to update Log data
type MsgStore struct {
	msgs  []*LogData
	owner int
	term  int32
	mu    sync.Mutex
}

func (s *MsgStore) string() string {
	var str []byte
	for _, ld := range s.msgs {
		str = append(str, []byte(ld.string())...)
		str = append(str, []byte("\n\t")...)
	}
	str = append(str, []byte(fmt.Sprintf("\n\towner: %d", s.owner))...)
	return string(str)
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	isLeaderAlive int32 //
	level         int32 //
	// logIndex      int32 //
	// logIndexLock  sync.Locker

	commitIndex      int32      //
	commitIndexMutex sync.Mutex //

	term            int32 //
	termLock        sync.Mutex
	timeOutChan     chan struct{}
	levelChangeChan chan struct{}
	raftPeers       []RaftPeer
	// Your data here (2A, 2B, 2C).
	commandLog Log
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	commitChan          chan int32
	pMsgStore           *MsgStore // nil
	pMsgStoreCreateLock sync.Mutex

	KilledChan chan bool
	DebugLoger *log.Logger

	applyChan chan ApplyMsg

	logSize int64 //
}

func (r *Raft) getCommitIndex() int32 {
	r.commitIndexMutex.Lock()
	defer r.commitIndexMutex.Unlock()
	return r.getCommitIndexUnsafe()
}
func (r *Raft) getCommitIndexUnsafe() int32 {
	return atomic.LoadInt32(&r.commitIndex)
}
func (r *Raft) tryUpdateCommitIndex(i int32) {
	r.commitChan <- i
}

// because of persistent The caller needs exclusive rf.commands.msgs (lock)
func (r *Raft) justSetCommitIndex(i int32) {
	r.commitIndexMutex.Lock()
	defer r.commitIndexMutex.Unlock()
	r.commitIndex = i
}

func (r *Raft) setCommitIndex(i int32) {
	r.commitIndexMutex.Lock()
	defer r.commitIndexMutex.Unlock()
	r.setCommitIndexUnsafe(i)
}

func (r *Raft) setCommitIndexUnsafe(i int32) {
	r.commitIndex = i
	// r.registPersist()
}
func (r *Raft) GetSnapshot() *ApplyMsg {
	r.commandLog.MsgRwMu.RLock()
	defer r.commandLog.MsgRwMu.RUnlock()
	return r.getSnapshotUnsafe()
}

// unsafe
func (r *Raft) getSnapshotUnsafe() *ApplyMsg {
	r.Dolog(-1, fmt.Sprintf("Get SnapShot index[%d] , term[%d]", r.commandLog.Msgs[0].SnapshotIndex, r.commandLog.Msgs[0].SnapshotTerm))
	return r.commandLog.Msgs[0]
}

// func (r *Raft) tryUpdateCommitIndexUnsafe(i int32) {
// r.commitIndex = i
// r.Dolog(-1, "tryUpdateCommitIndex", i, "submit in commitChan")
// r.commitChan <- i
// }

func (r *Raft) getLogIndex() int32 {
	r.commandLog.MsgRwMu.RLock()
	defer r.commandLog.MsgRwMu.RUnlock()
	return r.getLogIndexUnsafe()
}
func (r *Raft) getLogIndexUnsafe() int32 {
	if r.commandLog.Msgs[len(r.commandLog.Msgs)-1].SnapshotValid {
		return int32(r.commandLog.Msgs[len(r.commandLog.Msgs)-1].SnapshotIndex)
	} else {
		return int32(r.commandLog.Msgs[len(r.commandLog.Msgs)-1].CommandIndex)
	}
}

func (r *Raft) getTerm() int32 {
	r.termLock.Lock()
	defer r.termLock.Unlock()
	return r.term
}
func (r *Raft) setTerm(i int32) {
	r.termLock.Lock()
	defer r.termLock.Unlock()
	r.setTermUnsafe(i)
}
func (r *Raft) setTermUnsafe(i int32) {
	r.term = i
	// r.registPersist()
}
func (r *Raft) beginSendHeartBeat() {
	for i := range r.raftPeers {
		if i != r.me {
			select {
			case r.raftPeers[i].BeginHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (r *Raft) stopSendHeartBeat() {
	for i := range r.raftPeers {
		if i != r.me {
			select {
			case r.raftPeers[i].StopHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (r *Raft) registeHeartBeat(index int) {
	for !r.killed() {
	restart:
		select {
		case <-r.KilledChan:
			return
		case <-r.raftPeers[index].BeginHeartBeat:
		}
		for {
			select {
			case <-r.KilledChan:
				return
			case <-r.raftPeers[index].StopHeartBeat:
				goto restart
			case <-r.raftPeers[index].JumpHeartBeat:
				continue
			case <-time.After(HeartbeatTimeout):
			case <-r.raftPeers[index].SendHeartBeat:
			}
			if r.getLevel() != LevelLeader {
				goto restart
			}
			r.goSendHeartBeat(index)
		}
	}
}
func (r *Raft) goSendHeartBeat(index int) bool {
	arg := RequestArgs{SelfIndex: int32(r.me)}
	rpl := RequestReply{}
	arg.LastLogIndex, arg.LastLogTerm = r.getLastLogData()
	arg.SelfTerm = r.getTerm()
	arg.Time = time.Now()
	arg.CommitIndex = r.getCommitIndex()
	arg.Msg = nil
	// do call
	ok := r.call(index, "Raft.HeartBeat", &arg, &rpl)
	if !ok {
		return false
	}
	if ok && !rpl.IsAgree && (rpl.PeerSelfTerm > r.getTerm() || rpl.PeerLastLogIndex > r.getLogIndex() || rpl.PeerLastLogTerm > arg.LastLogTerm) {

		r.commandLog.MsgRwMu.RLock()
		r.DebugLoger.Println("\nHeartBeat Error:\n " + showMsgS(r.commandLog.Msgs))
		r.commandLog.MsgRwMu.RUnlock()

		r.Dolog(index, "r.HeartBeatErrr", r.getLogIndex(), "heartbeat return false Going to be Follower", arg.string(), rpl.string())
		r.changeToFollower(&rpl)
	} else {
		func() {
			r.raftPeers[index].logIndexTermLock.Lock()
			defer r.raftPeers[index].logIndexTermLock.Unlock()
			if rpl.PeerLastLogTerm > r.raftPeers[index].lastLogTerm {
				r.raftPeers[index].lastLogTerm = rpl.PeerLastLogTerm
				r.raftPeers[index].logIndex = rpl.PeerLastLogIndex
			} else if rpl.PeerLastLogTerm == r.raftPeers[index].lastLogTerm {
				r.raftPeers[index].logIndex = rpl.PeerLastLogIndex
			}
			r.raftPeers[index].commitIndex = rpl.PeerCommitIndex
		}()
		if rpl.PeerLastLogIndex < arg.LastLogIndex || rpl.PeerLastLogTerm < arg.LastLogTerm {
			r.tryleaderUpdatePeer(index, &LogData{Msg: nil, LastTimeIndex: int(arg.LastLogIndex), LastTimeTerm: int(arg.LastLogTerm)})
		}
	}
	return true
}
func (r *Raft) Ping() bool {
	if r.getLevel() != LevelLeader {
		return false
	}
	finish := make(chan bool, len(r.raftPeers))
	for index := range r.raftPeers {
		if index != r.me {
			go func(i int) {
				select {
				case r.raftPeers[i].JumpHeartBeat <- struct{}{}:
				default:
				}
				finish <- r.goSendHeartBeat(i)
				r.Dolog(i, "ping received Peer Alive")
			}(index)
		}
	}
	result := make(chan bool, len(r.raftPeers))
	go func() {
		flag := 1
		IsSend := false
		for i := 0; i < len(r.raftPeers)-1; i++ {
			if <-finish {
				flag++
			}
			if !IsSend && flag > len(r.raftPeers)/2 {
				result <- true
				IsSend = true
			}
		}
		close(finish)
		close(result)
	}()
	return <-result
}
func (r *Raft) changeToLeader() {
	r.Dolog(-1, "Going to Be Leader")
	atomic.StoreInt32(&r.isLeaderAlive, 1)
	r.setLevel(LevelLeader)
	select {
	case r.levelChangeChan <- struct{}{}:
	default:
	}
	for i := range r.raftPeers {
		if i == r.me {
			r.raftPeers[i].logIndexTermLock.Lock()
			r.raftPeers[r.me].logIndex, r.raftPeers[r.me].lastLogTerm = r.getLastLogData()
			r.raftPeers[i].logIndexTermLock.Unlock()
		} else {
			r.raftPeers[i].logIndexTermLock.Lock()
			r.raftPeers[i].lastLogTerm, r.raftPeers[i].logIndex = 0, 0
			r.raftPeers[i].logIndexTermLock.Unlock()
		}
	}
	r.beginSendHeartBeat()
}
func (r *Raft) changeToCandidate() {
	r.Dolog(-1, "Going to Be Candidate")
	r.setLevel(LevelCandidate)
	select {
	case r.levelChangeChan <- struct{}{}:
	default:
	}
}
func (r *Raft) changeToFollower(rpl *RequestReply) {
	r.Dolog(-1, "Going to Be Follower")
	if r.getLevel() == LevelLeader {
		r.stopSendHeartBeat()
	}
	r.setLevel(LevelFollower)
	if rpl != nil {
		if rpl.PeerSelfTerm > r.getTerm() {
			r.setTerm(rpl.PeerSelfTerm)
		}
	}
	select {
	case r.levelChangeChan <- struct{}{}:
	default:
	}
}

func (r *Raft) HeartBeat(arg *RequestArgs, rpl *RequestReply) {
	if r.killed() {
		return
	}
	defer func(tmpArg *RequestArgs, tmpRpl *RequestReply) {
		r.Dolog(int(tmpArg.SelfIndex), "REPLY Heartbeat", "\t\n Arg:", tmpArg.string(), "\t\n Rpl:", tmpRpl.string())
	}(arg, rpl)

	rpl.PeerLastLogIndex, rpl.PeerLastLogTerm = r.getLastLogData()
	rpl.PeerSelfTerm = r.getTerm()
	rpl.ReturnTime = time.Now()

	rpl.IsAgree = arg.LastLogTerm >= rpl.PeerLastLogTerm && (arg.LastLogTerm > rpl.PeerLastLogTerm || arg.LastLogIndex >= rpl.PeerLastLogIndex)
	if !rpl.IsAgree {
		return
	}
	if arg.SelfTerm > rpl.PeerSelfTerm {
		rpl.PeerSelfTerm = arg.SelfIndex
		r.setTerm(arg.SelfTerm)
	}
	select {
	case r.timeOutChan <- struct{}{}:
	default:
	}

	if r.getLevel() == LevelLeader {
		rpll := *rpl
		rpll.PeerSelfTerm = arg.SelfTerm
		r.changeToFollower(&rpll)
	}
	if arg.Msg == nil || len(arg.Msg) == 0 {
	} else {
		for i := range arg.Msg {
			r.Dolog(int(arg.SelfIndex), "Try LOAD-Log", arg.Msg[i].string())
		}
		rpl.LogDataMsg = r.updateMsgs(arg.Msg)
		r.Dolog(-1, "Request Update Log Data To Leader", rpl.LogDataMsg)
		rpl.PeerLastLogIndex, rpl.PeerLastLogTerm = r.getLastLogData()
	}
	if arg.LastLogTerm > rpl.PeerLastLogTerm {
	} else if arg.CommitIndex > r.getCommitIndex() {
		r.tryUpdateCommitIndex(arg.CommitIndex)
	}
	rpl.PeerCommitIndex = r.getCommitIndex()
}

func (rf *Raft) GetState() (int, bool) {
	return int(rf.getTerm()), rf.getLevel() == LevelLeader
}

func (rf *Raft) persist(snapshot *ApplyMsg) {
	defer rf.checkFuncDone("persist")()

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()
	rf.persistUnsafe(snapshot)

}

type CommandPersistNode struct {
	Term    int64
	Command interface{}
	Index   int64
}

// 此函数不保证对log的操作的原子性
func (rf *Raft) persistUnsafe(snapshot *ApplyMsg) {
	defer rf.checkFuncDone("persistUnsafe")()
	if snapshot != nil {
		if snapshot.SnapshotIndex < rf.commandLog.Msgs[0].SnapshotIndex {
			return
		}
	}

	buffer := bytes.Buffer{}
	encoder := labgob.NewEncoder(&buffer)
	err := encoder.Encode(rf.getCommitIndexUnsafe())
	if err != nil {
		log.Fatal("Failed to encode CommitIndex: ", err)
	}
	err = encoder.Encode(atomic.LoadInt32(&rf.term))
	if err != nil {
		log.Fatal("Failed to encode Term: ", err)
	}
	i := rf.commandLog.Msgs[0].Snapshot
	rf.commandLog.Msgs[0].Snapshot = nil
	err = encoder.Encode(rf.commandLog.Msgs)
	if err != nil {
		log.Fatal("Failed to encode Msgs: ", err)
	}
	rf.commandLog.Msgs[0].Snapshot = i
	encodedData := buffer.Bytes() // 获取编码后的数据

	if rf.commandLog.Msgs[0] != nil && rf.commandLog.Msgs[0].Snapshot != nil {
		rf.persister.Save(encodedData, rf.commandLog.Msgs[0].Snapshot) // 保存数据到持久化存储
	} else {
		rf.persister.Save(encodedData, nil) // 保存数据到持久化存储
	}

	// 记录输出
	output := fmt.Sprintf("Encoded CommitIndex: %v, Term: %v, \n(Len:%d)Msgs: %#v",
		rf.getSnapshotUnsafe().SnapshotIndex,
		atomic.LoadInt32(&rf.term), len(rf.commandLog.Msgs),
		*rf.commandLog.Msgs[0])

	rf.DebugLoger.Println("\nPersist Save - " + output)
	atomic.StoreInt64(&rf.logSize, int64(rf.persister.RaftStateSize()))
}

// Unsafe
func showMsgS(rf []*ApplyMsg) string {
	str := "\n"
	for i := range rf {
		str += rf[i].string() + "\n"
	}
	return str
}
func (rf *Raft) RaftSize() int64 {
	return atomic.LoadInt64(&rf.logSize)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	defer rf.checkFuncDone("readPersist")()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int32
	var commitedIndex int32
	var msgs []*ApplyMsg

	if err := d.Decode(&commitedIndex); err != nil {
		log.Fatal("Failed to decode CommitIndex: ", err)
	}
	if err := d.Decode(&term); err != nil {
		log.Fatal("Failed to decode Term: ", err)
	}
	if err := d.Decode(&msgs); err != nil {
		log.Fatal("Failed to decode Msgs: ", err)
	}

	rf.term = term
	if commitedIndex > 0 {
		rf.commitIndex = commitedIndex
	} else {
		rf.commitIndex = 0
	}
	rf.commandLog.Msgs = msgs

	// 记录输出
	output := fmt.Sprintf("Decoded CommitIndex: %v, Term: %v, Msgs: %v",
		rf.commitIndex,
		rf.term,
		rf.commandLog.Msgs)
	rf.Dolog(-1, "Persist Load - "+output)
	rf.commandLog.Msgs[0].Snapshot = rf.persister.ReadSnapshot()
}

// unsafe
func (rf *Raft) GetTargetCacheIndex(index int) int {
	if len(rf.commandLog.Msgs) == 0 {
		// return -1
		panic("len(rf.commandLog.Msgs) == 0")
	}
	LastLog := rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1]
	IndexReturn := 0
	if LastLog.SnapshotValid {
		IndexReturn = len(rf.commandLog.Msgs) - (LastLog.SnapshotIndex - index) - 1
	} else {
		IndexReturn = len(rf.commandLog.Msgs) - (LastLog.CommandIndex - index) - 1
	}
	if !(IndexReturn > 0 && IndexReturn < len(rf.commandLog.Msgs)) {
		rf.Dolog(-1, fmt.Sprintf("Try to get index %d return %d rf.Msgs(len(%d) , lastIndex(%d))", index, IndexReturn, len(rf.commandLog.Msgs), (rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex)))
	}
	return IndexReturn
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	defer rf.checkFuncDone("Snapshot")()

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()
	rf.SnapshotUnsafe(index, -1, snapshot)
}

func (rf *Raft) SnapshotUnsafe(index int, InputSnapShotTerm int, snapshot []byte) {
	// Your code here (2D).
	defer rf.checkFuncDone("SnapshotUnsafe")()
	rf.Dolog(-1, fmt.Sprintf("Try save snapshot Index:%v  data :%v", index, snapshot))
	if (InputSnapShotTerm != -1 && InputSnapShotTerm < rf.getSnapshotUnsafe().SnapshotTerm) || index <= rf.getSnapshotUnsafe().SnapshotIndex {
		return
	}

	inputIndexSCacheIndex := rf.GetTargetCacheIndex(index)

	var newMagsHead *ApplyMsg
	if inputIndexSCacheIndex < 0 {
		panic("Snapshot Called, But Not found correspond Log")
	} else if inputIndexSCacheIndex >= len(rf.commandLog.Msgs) {
		// rf.Dolog(-1, "Snapshot called but not found correspond Log , inputIndexSCacheIndex >= len(rf.commandLog.Msgs)", "index", index, "inputIndexSCacheIndex", inputIndexSCacheIndex, "len(rf.commandLog.Msgs)", len(rf.commandLog.Msgs))
		if InputSnapShotTerm == -1 {
			return
		} else {
			newMagsHead = &ApplyMsg{CommandValid: false, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: InputSnapShotTerm, SnapshotIndex: index}
		}
	} else {
		term := rf.commandLog.Msgs[inputIndexSCacheIndex].CommandTerm
		if InputSnapShotTerm != -1 && InputSnapShotTerm > term {
			term = InputSnapShotTerm
		}
		newMagsHead = &ApplyMsg{CommandValid: false, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: term, SnapshotIndex: index}
	}
	oldMsgs := rf.commandLog.Msgs
	rf.commandLog.Msgs = make([]*ApplyMsg, 0)
	if inputIndexSCacheIndex >= len(oldMsgs) {
		rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
	} else {
		if len(oldMsgs) > 1 {
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, oldMsgs[inputIndexSCacheIndex+1:]...)
		} else {
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMagsHead)
		}
	}
	// why here need send msg , i forget
	// go func() {
	// rf.applyChan <- *rf.commandLog.Msgs[0]
	// }()
	rf.persistUnsafe(rf.getSnapshotUnsafe())
	rf.Dolog(-1, fmt.Sprintf("Saved snapshot Index:%v  data :%v", index, snapshot))
	// os.Stdout.WriteString(fmt.Sprintf("\t F[%d] CallSnapShot:%#v\n", rf.me, *rf.getSnapshotUnsafe()))
}

func (r *RequestArgs) string() string {
	if r.Msg == nil || len(r.Msg) == 0 {
		return fmt.Sprintf("%+v ", *r)
	} else {
		str := ""
		for i := 0; i < len(r.Msg); i++ {
			str += fmt.Sprintf("Msg:(%s)", r.Msg[i].string())
		}
		str += fmt.Sprintf("\n\t%+v", *r)
		return str
	}
}
func (r *RequestReply) string() string {
	if r.LogDataMsg == nil {
		return fmt.Sprintf("%+v ", *r)
	} else {
		return fmt.Sprintf("Msg:(%s) %+v", r.LogDataMsg.string(), *r)
	}
}

func (r *Raft) RequestPreVote(args *RequestArgs, reply *RequestReply) {
	if r.killed() {
		return
	}
	defer r.checkFuncDone("RequestPreVote")()
	reply.PeerLastLogIndex, reply.PeerLastLogTerm = r.getLastLogData()
	reply.PeerSelfTerm = r.getTerm()
	reply.ReturnTime = time.Now()
	selfCommitINdex := r.getCommitIndex()
	reply.PeerCommitIndex = selfCommitINdex
	// reply.IsAgree = atomic.LoadInt32(&r.isLeaderAlive) == 0 && ((reply.PeerLastLogTerm < args.LastLogTerm) || (reply.PeerLastLogIndex <= args.LastLogIndex && reply.PeerLastLogTerm == args.LastLogTerm))
	reply.IsAgree = args.CommitIndex >= selfCommitINdex && (atomic.LoadInt32(&r.isLeaderAlive) == 0 && args.LastLogTerm >= reply.PeerLastLogTerm && (args.LastLogTerm > reply.PeerLastLogTerm || args.LastLogIndex >= reply.PeerLastLogIndex) && reply.PeerSelfTerm < args.SelfTerm)
}

func (rf *Raft) RequestVote(args *RequestArgs, reply *RequestReply) {
	if rf.killed() {
		return
	}
	defer rf.checkFuncDone("RequestVote")()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.termLock.Lock()
	defer rf.termLock.Unlock()
	reply.PeerSelfTerm = rf.term
	reply.PeerLastLogIndex, reply.PeerLastLogTerm = rf.getLastLogData()
	//	选举投票与否的标准是 各个节点的commit程度 各个节点的日志的新旧程度 当新旧程度一样时 再比较投票的任期
	reply.IsAgree = true
	selfCommitINdex := rf.getCommitIndex()
	if args.CommitIndex > selfCommitINdex {
		reply.IsAgree = true
	} else if args.CommitIndex < selfCommitINdex {
		reply.IsAgree = false
	} else {
		if args.LastLogTerm < reply.PeerLastLogTerm {
			reply.IsAgree = false
			rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.LastLogTerm < reply.PeerLastLogTerm")
		} else if args.LastLogTerm == reply.PeerLastLogTerm {
			if args.LastLogIndex < reply.PeerLastLogIndex {
				reply.IsAgree = false
				rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.LastLogIndex < reply.PeerLastLogIndex")
			} else if args.LastLogIndex == reply.PeerLastLogIndex {
				if args.SelfTerm <= reply.PeerSelfTerm {
					reply.IsAgree = false
					rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.SelfTerm <= reply.PeerSelfTerm")
				}
			}
		}
	}
	if args.SelfTerm <= reply.PeerSelfTerm {
		reply.IsAgree = false
		rf.Dolog(int(args.SelfIndex), "Disagree Vote Because of "+"args.SelfTerm <= reply.PeerSelfTerm")
	}
	if reply.IsAgree {
		if rf.getLevel() == LevelLeader {
			rf.changeToFollower(nil)
		}
		rf.setTermUnsafe(args.SelfTerm)
		atomic.StoreInt32(&rf.isLeaderAlive, 1)
	}
	reply.ReturnTime = time.Now()
	reply.PeerCommitIndex = selfCommitINdex
	rf.Dolog(int(args.SelfIndex), "answer RequestVote", args.string(), reply.string())
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

// func (rf *Raft) sendRequestVote(server int, args *RequestArgs, reply *RequestReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

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

func (rf *Raft) checkMsg(data *LogData) int {
	if data == nil {
		return -1
	}
	// check过程中进行拿锁

	if len(rf.commandLog.Msgs) == 0 {
		panic("len(rf.commandLog.Msgs) == 0")
	}

	rf.pMsgStore.mu.Lock()
	defer rf.pMsgStore.mu.Unlock()
	if rf.pMsgStore.term < int32(data.SelfTermNow) {
		rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: data.SelfIndex, mu: sync.Mutex{}, term: int32(data.SelfTermNow)}
	}
	if data.Msg.SnapshotValid {
		return LogCheckSnap
	}

	// 将 snapindex 记录成 commandindex 去进行Check
	lastRfLog := *rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1]
	if lastRfLog.SnapshotValid {
		lastRfLog.CommandIndex, lastRfLog.CommandTerm = lastRfLog.SnapshotIndex, lastRfLog.SnapshotTerm
	}
	rf.Dolog(-1, "Will check ", data.string())

	switch {
	// 如果已经提交过的 忽略
	case data.Msg.CommandIndex <= int(rf.getCommitIndexUnsafe()):
		return LogCheckIgnore

	// 没有快照之前的 第一项log
	case data.LastTimeIndex == 0 && data.LastTimeTerm == -1:
		if len(rf.commandLog.Msgs) == 1 || rf.commandLog.Msgs[1].CommandTerm != data.Msg.CommandTerm {
			return LogCheckBeginOrReset
		} else {
			return LogCheckIgnore
		}

	case data.LastTimeIndex == lastRfLog.CommandIndex:
		if data.LastTimeTerm == lastRfLog.CommandTerm {
			// prelog和现有log最后一项 完全相同 append
			return LogCheckAppend
		} else {
			if lastRfLog.SnapshotValid {
				panic("\nsnapshot Dis-agreement \n" + lastRfLog.string())
			}
			// 否则 store
			return LogCheckStore
		}
	// 传入数据 索引元高于本地
	case data.LastTimeIndex > lastRfLog.CommandIndex:
		// 进行(同步)缓存
		return LogCheckStore

	// store
	case data.LastTimeIndex < lastRfLog.CommandIndex:
		if data.LastTimeIndex <= 0 {
			panic("requeste update command index is out of range [<=0]")
		} else if i := rf.GetTargetCacheIndex(data.Msg.CommandIndex); i <= 0 {
			if rf.getSnapshotUnsafe().SnapshotValid && rf.getSnapshotUnsafe().SnapshotIndex >= data.Msg.CommandIndex {
				return LogCheckIgnore
			} else {
				panic("requeste update command index is out of range[<=0]\n" + data.string() + "\n" + rf.getSnapshotUnsafe().string())
			}
		} else if rf.commandLog.Msgs[rf.GetTargetCacheIndex(data.Msg.CommandIndex)].CommandTerm == data.Msg.CommandTerm && rf.commandLog.Msgs[rf.GetTargetCacheIndex(data.LastTimeIndex)].CommandTerm == data.LastTimeTerm {
			// [S][C][C][C][C][C][][][][]
			//    -------^-
			//   (check)[C] --> same
			return LogCheckIgnore
		}
		return LogCheckStore
	default:
	}
	return -1
}
func (rf *Raft) appendMsg(msg *LogData) {
	if msg == nil || rf.getLevel() == LevelLeader {
		return
	}
	rf.commandLog.Msgs = append(rf.commandLog.Msgs, msg.Msg)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	defer rf.raftPeers[rf.me].logIndexTermLock.Unlock()
	rf.raftPeers[rf.me].logIndex = int32(msg.Msg.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(msg.Msg.CommandTerm)
	rf.Dolog(-1, "LogAppend", msg.Msg.string())
}
func (rf *Raft) getLastLogData() (int32, int32) {

	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	if rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotValid {
		return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotTerm)
	}
	return int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandTerm)
}

func (m *MsgStore) insert(target *LogData) {
	if len(m.msgs) == 0 {
		m.msgs = []*LogData{target}
		return
	}
	if target.Msg.CommandIndex < m.msgs[0].Msg.CommandIndex {
		m.msgs = append([]*LogData{target}, m.msgs...)
		return
	}
	if target.Msg.CommandIndex > m.msgs[len(m.msgs)-1].Msg.CommandIndex {
		m.msgs = append(m.msgs, target)
		return
	}
	index := 0
	right := len(m.msgs)
	for index < right {
		mid := index + (right-index)/2
		if m.msgs[mid].Msg.CommandIndex < target.Msg.CommandIndex {
			index = mid + 1
		} else {
			right = mid
		}
	}
	if m.msgs[index].Msg.CommandIndex == target.Msg.CommandIndex {
		if m.msgs[index].Msg.CommandTerm != target.Msg.CommandTerm {
			m.msgs[index] = target
		}
	} else {
		m.msgs = append(m.msgs, nil)
		copy(m.msgs[index+1:], m.msgs[index:])
		m.msgs[index] = target
	}
	// log.Printf("m: %v\n", m)
}

// expect a orderly list in this
// when rf.msgs's tail  ==  this list 's head
// do update And Del this list 's head step by step
// to rf.msgs's tail != this list 's head || this list 's len == 0

func (rf *Raft) saveMsg() (*LogData, bool) {

	if func() *MsgStore {
		rf.pMsgStoreCreateLock.Lock()
		defer rf.pMsgStoreCreateLock.Unlock()
		if rf.pMsgStore == nil {
			return nil
		} else {
			rf.pMsgStore.mu.Lock()
			defer rf.pMsgStore.mu.Unlock()
			if len(rf.pMsgStore.msgs) == 0 {
				return nil
			}
			return rf.pMsgStore
		}
	}() == nil {
		return nil, false
	}
	IsChangeMsg := false

	rf.pMsgStore.mu.Lock()
	defer rf.pMsgStore.mu.Unlock()
	// store的 更新到头了
	if rf.pMsgStore.msgs == nil || len(rf.pMsgStore.msgs) == 0 {
		panic("rf.pMsgStore.msgs is not initialized , May race state result")

		// 有快照之前的追加
	} else if !rf.getSnapshotUnsafe().SnapshotValid && rf.pMsgStore.msgs[0].LastTimeTerm == -1 {
		rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), rf.commandLog.Msgs[:1]...)
		rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
		rf.pMsgStore.msgs = rf.pMsgStore.msgs[1:]
		// rf.pMsgStore.msgs = make([]*LogData, 0)
		// return nil, true
		IsChangeMsg = true
	}

	for {
		if len(rf.pMsgStore.msgs) == 0 {
			break
		}
		rf.Dolog(-1, "Try Save "+rf.pMsgStore.msgs[0].string())
		index := rf.GetTargetCacheIndex(rf.pMsgStore.msgs[0].LastTimeIndex)
		if index >= len(rf.commandLog.Msgs) {
			break
		} else if index <= 0 {
			if rf.getSnapshotUnsafe().SnapshotValid && rf.pMsgStore.msgs[0].LastTimeIndex == rf.getSnapshotUnsafe().SnapshotIndex && rf.pMsgStore.msgs[0].LastTimeTerm == rf.getSnapshotUnsafe().SnapshotTerm {
				if len(rf.commandLog.Msgs) > 2 && (rf.pMsgStore.msgs[0].Msg.CommandIndex == rf.commandLog.Msgs[1].CommandIndex && rf.pMsgStore.msgs[0].Msg.CommandTerm == rf.commandLog.Msgs[1].CommandTerm) {
				} else {
					rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), rf.commandLog.Msgs[:1]...)
					rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
					IsChangeMsg = true
				}
			} else if !rf.getSnapshotUnsafe().SnapshotValid || rf.pMsgStore.msgs[0].Msg.CommandIndex > rf.getSnapshotUnsafe().SnapshotIndex {
				log.Panic("require snapshot ? Access out of bounds")
			} else {
			}
		} else {
			if rf.pMsgStore.msgs[0].LastTimeTerm == rf.commandLog.Msgs[index].CommandTerm {
				if index+1 == len(rf.commandLog.Msgs) {
					rf.commandLog.Msgs = append(rf.commandLog.Msgs, rf.pMsgStore.msgs[0].Msg)
				} else {
					rf.commandLog.Msgs[index+1] = rf.pMsgStore.msgs[0].Msg
					if len(rf.commandLog.Msgs) >= index+1 {
						rf.commandLog.Msgs = rf.commandLog.Msgs[:index+2]
					}
				}
				IsChangeMsg = true
			} else {
				break
			}
		}
		rf.Dolog(-1, "Saved "+rf.pMsgStore.msgs[0].string())
		rf.pMsgStore.msgs = rf.pMsgStore.msgs[1:]
	}
	if len(rf.pMsgStore.msgs) != 0 {
		return rf.pMsgStore.msgs[0], IsChangeMsg
	} else {
		rf.pMsgStore.msgs = make([]*LogData, 0)
		return nil, IsChangeMsg
	}
}
func (rf *Raft) storeMsg(msg *LogData) {
	defer rf.checkFuncDone("storeMsg")()
	rf.pMsgStoreCreateLock.Lock()
	defer rf.pMsgStoreCreateLock.Unlock()
	if msg == nil {
		return
	}
	if rf.pMsgStore == nil || rf.pMsgStore.owner != msg.SelfIndex || rf.pMsgStore.term < int32(msg.Msg.CommandTerm) {
		if rf.pMsgStore != nil {
			rf.Dolog(-1, "'rf.pMsgStore' has been overwritten", "brfore Leader:", rf.pMsgStore.owner, "Now:", msg.SelfIndex)
		} else {
			rf.Dolog(-1, "'rf.pMsgStore' has been overwritten", "brfore Leader:", nil, "Now:", msg.SelfIndex)
		}
		if rf.pMsgStore != nil {
			rf.pMsgStore.mu.Lock()
			defer rf.pMsgStore.mu.Unlock()
		}
		rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: msg.SelfIndex, mu: sync.Mutex{}, term: int32(msg.Msg.CommandTerm)}
	}
	rf.pMsgStore.insert(msg)
}
func (rf *Raft) logBeginOrResetMsg(log *LogData) {

	if len(rf.commandLog.Msgs) > 1 {
		rf.commandLog.Msgs = rf.commandLog.Msgs[:1]
	}

	rf.commandLog.Msgs = append(rf.commandLog.Msgs, log.Msg)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	rf.raftPeers[rf.me].logIndex = int32(log.Msg.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(log.Msg.CommandTerm)
	rf.raftPeers[rf.me].logIndexTermLock.Unlock()

	rf.Dolog(-1, "LogAppend", log.Msg.string())
	rf.Dolog(-1, "Log", log.Msg.string())
}
func (rf *Raft) LoadSnap(data *LogData) {
	if data.Msg.SnapshotIndex < rf.getSnapshotUnsafe().SnapshotIndex || (data.Msg.CommandIndex == rf.getSnapshotUnsafe().SnapshotIndex && data.Msg.CommandTerm == rf.getSnapshotUnsafe().SnapshotTerm) {
		return
	} else {
		// rf.SnapshotUnsafe(data.Msg.SnapshotIndex, data.Msg.SnapshotTerm, data.Msg.Snapshot)
		oldmsg := rf.commandLog.Msgs
		cacheIndex := rf.GetTargetCacheIndex(data.Msg.SnapshotIndex)
		if cacheIndex < 0 {
			return
		} else if cacheIndex >= len(rf.commandLog.Msgs) {
			rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), data.Msg)
		} else {
			rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), data.Msg)
			rf.commandLog.Msgs = append(rf.commandLog.Msgs, oldmsg[cacheIndex+1:]...)
		}
		rf.persistUnsafe(rf.getSnapshotUnsafe())
		// os.Stdout.WriteString(fmt.Sprintf("\t F[%d] LoadSnapShot:%#v\n", rf.me, *rf.getSnapshotUnsafe()))
	}
}
func (rf *Raft) updateMsgs(msg []*LogData) *LogData {
	defer rf.checkFuncDone("updateMsgs")()

	var IsStoreMsg bool = false
	var IsChangeMsg bool = false
	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()

	for i := 0; i < len(msg); i++ {
		result := rf.checkMsg(msg[i])
		switch result {
		case LogCheckSnap:
			rf.Dolog(-1, "LogCheckSnap", msg[i].string())
			rf.LoadSnap(msg[i])
		case LogCheckBeginOrReset:
			rf.Dolog(-1, "LogCheckBeginOrReset", msg[i].string())
			rf.logBeginOrResetMsg(msg[i])
			IsChangeMsg = true
		case LogCheckAppend:
			rf.Dolog(-1, "LogCheckAppend", msg[i].string())
			rf.appendMsg(msg[i])
			IsChangeMsg = true
		case LogCheckIgnore:
			rf.Dolog(-1, "LogCheckIgnore", msg[i].string())
		case LogCheckStore:
			IsStoreMsg = true
			rf.Dolog(-1, "LogCheckStore", msg[i].string())
			rf.storeMsg(msg[i])
			// IsChangeMsg = true
		default:
			rf.pMsgStore.mu.Lock()
			defer rf.pMsgStore.mu.Unlock()
			rf.Dolog(-1, "The requested data is out of bounds ", rf.pMsgStore.string(), "RequestIndex:>", rf.pMsgStore.msgs[0].LastTimeIndex, "process will be killed")
			log.Panic(-1, "The requested data is out of bounds ", rf.pMsgStore.string(), "RequestIndex:>", rf.pMsgStore.msgs[0].LastTimeIndex, "process will be killed")
		}
	}
	i, IsSave := rf.saveMsg()
	if IsStoreMsg {
		rf.DebugLoger.Printf("Pmsg:> \n%s", rf.pMsgStore.string())
	}
	if IsChangeMsg || IsSave {
		// rf.registPersist()
	}
	// if i != nil {
	// 	log.Printf("Return rpl To request msg : %+v\n \nMsg Now:>\n", i.string())
	// 	for i2, am := range rf.commandLog.Msgs {
	// 		log.Printf("rf.CommandLog.Msgs[%d]: %+v\n", i2, am.string())
	// 	}
	// 	for i2, am := range rf.pMsgStore.msgs {
	// 		log.Printf("rf.pMsgStore.msgs[%d]: %+v\n", i2, am.string())
	// 	}

	// }
	return i
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	defer rf.checkFuncDone("Start")()
	rf.Dolog(-1, "Start Called ")

	TermNow := rf.getTerm()
	LevelNow := rf.getLevel()
	lastLogIndex := rf.getLogIndex()

	// No idea about whether use Ping()
	// if i, m, checkOutLeaderOnline := int(lastLogIndex), int(TermNow), rf.checkOutLeaderOnline(); LevelNow != LevelLeader || (!checkOutLeaderOnline && !rf.Ping()) {
	if i, m, checkOutLeaderOnline := int(lastLogIndex), int(TermNow), rf.checkOutLeaderOnline(); LevelNow != LevelLeader || (!checkOutLeaderOnline) {
		rf.Dolog(-1, "Start return ", "LogIndex", i, "Term", m, "Level", LevelNow)
		return i, m, false
	}
	if command == nil {
		return int(lastLogIndex), int(TermNow), true
	}

	rf.commandLog.MsgRwMu.Lock()
	defer rf.commandLog.MsgRwMu.Unlock()

	var arg *RequestArgs
	newMessage := &ApplyMsg{
		CommandTerm:  int(TermNow),
		CommandValid: true,
		Command:      command,
		CommandIndex: int(rf.getLogIndexUnsafe()) + 1}

	rf.commandLog.Msgs = append(rf.commandLog.Msgs, newMessage)
	rf.raftPeers[rf.me].logIndexTermLock.Lock()
	rf.raftPeers[rf.me].logIndex = int32(newMessage.CommandIndex)
	rf.raftPeers[rf.me].lastLogTerm = int32(newMessage.CommandTerm)
	rf.raftPeers[rf.me].logIndexTermLock.Unlock()

	rf.Dolog(-1, "Start TO LogAppend", newMessage.string())
	// rf.registPersist()

	arg = &RequestArgs{
		SelfTerm:  TermNow,
		Time:      time.Now(),
		SelfIndex: int32(rf.me),
		Msg: append(make([]*LogData, 0),
			&LogData{
				Msg:         newMessage,
				SelfIndex:   rf.me,
				SelfTermNow: int(TermNow)}),
	}

	if rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotValid {
		arg.LastLogIndex, arg.LastLogTerm = int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].SnapshotTerm)
	} else {
		arg.LastLogIndex, arg.LastLogTerm = int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandIndex), int32(rf.commandLog.Msgs[len(rf.commandLog.Msgs)-1].CommandTerm)
	}

	// if len(rf.commandLog.Msgs) > 1 {
	// 	arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandIndex
	// 	arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandTerm
	// } else {
	// 	if rf.commandLog.Msgs[0].SnapshotValid {
	// 		arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[0].SnapshotIndex
	// 		arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[0].SnapshotTerm
	// 	} else {
	// 		arg.Msg[0].LastTimeIndex = 0
	// 		arg.Msg[0].LastTimeTerm = -1
	// 	}
	// }
	if len(rf.commandLog.Msgs) == 2 && rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].SnapshotValid {
		arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[0].SnapshotIndex
		arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[0].SnapshotTerm
	} else {
		arg.Msg[0].LastTimeIndex = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandIndex
		arg.Msg[0].LastTimeTerm = rf.commandLog.Msgs[len(rf.commandLog.Msgs)-2].CommandTerm
	}

	for i := range rf.raftPeers {
		if i != rf.me {
			go func(index int) {
				rpl := &RequestReply{}
				select {
				case rf.raftPeers[index].JumpHeartBeat <- struct{}{}:
				default:
				}
				rf.Dolog(index, "Raft.Heartbeat[LoadMsgBegin]", arg.string())
				ok := rf.call(index, "Raft.HeartBeat", arg, rpl)
				switch {
				case !ok:
					rf.Dolog(index, "Raft.HeartBeat(sendMsg)", "Timeout")
				case !rpl.IsAgree:
					rf.Dolog(index, "Raft.HeartBeat(sendMsg)", "Peer DisAgree", rpl.string())
				case rpl.LogDataMsg != nil:
					rf.tryleaderUpdatePeer(index, rpl.LogDataMsg)
				default:
					rf.raftPeers[index].logIndexTermLock.Lock()
					rf.raftPeers[index].lastLogTerm = rpl.PeerLastLogTerm
					rf.raftPeers[index].logIndex = rpl.PeerLastLogIndex
					rf.raftPeers[index].commitIndex = rpl.PeerCommitIndex
					rf.raftPeers[index].logIndexTermLock.Unlock()
				}
			}(i)
		} else {
			continue
		}
	}
	i, m, l := int(rf.getLogIndexUnsafe()), int(TermNow), LevelNow == LevelLeader
	rf.Dolog(-1, "Start return ", "LogIndex", i, "Term", m, "Level", l)
	return i, m, l
}

func (r *Raft) getLogs(index int, Len int) []*LogData {
	defer r.checkFuncDone("getLogs")()
	termNow := r.getTerm()
	r.commandLog.MsgRwMu.RLock()
	defer r.commandLog.MsgRwMu.RUnlock()
	targetLogIndexEnd := r.GetTargetCacheIndex(index)
	if targetLogIndexEnd < 0 {
		// panic("Request target index < 0 ")
		targetLogIndexEnd = 0
	} else if targetLogIndexEnd >= len(r.commandLog.Msgs) {
		panic("Request target index out of range ")
	}
	targetLogIndexBegin := targetLogIndexEnd - Len
	result := make([]*LogData, 0)
	if targetLogIndexBegin <= 0 {
		if r.commandLog.Msgs[0].SnapshotValid {
			result = append(result, &LogData{SelfIndex: r.me, SelfTermNow: int(termNow)})
			i := *r.commandLog.Msgs[0]
			result[0].Msg = &i
		}
		targetLogIndexBegin = 1
	}
	for targetLogIndexBegin <= targetLogIndexEnd {
		result = append(result, &LogData{
			SelfTermNow: int(termNow),
			Msg:         r.commandLog.Msgs[targetLogIndexBegin],
			SelfIndex:   r.me})
		if r.commandLog.Msgs[targetLogIndexBegin-1].SnapshotValid {
			result[len(result)-1].LastTimeIndex, result[len(result)-1].LastTimeTerm = r.commandLog.Msgs[targetLogIndexBegin-1].SnapshotIndex, r.commandLog.Msgs[targetLogIndexBegin-1].SnapshotTerm
		} else if r.commandLog.Msgs[targetLogIndexBegin-1].CommandValid {
			result[len(result)-1].LastTimeIndex, result[len(result)-1].LastTimeTerm = r.commandLog.Msgs[targetLogIndexBegin-1].CommandIndex, r.commandLog.Msgs[targetLogIndexBegin-1].CommandTerm
		}
		targetLogIndexBegin++
	}
	return result

}
func (r *Raft) tryleaderUpdatePeer(index int, msg *LogData) {
	if r.raftPeers[index].modeLock.TryLock() {
		go r.leaderUpdatePeer(index, msg)
	}
}
func (rf *Raft) isInLog(index int, Term int) bool {
	rf.commandLog.MsgRwMu.RLock()
	defer rf.commandLog.MsgRwMu.RUnlock()
	i := rf.GetTargetCacheIndex(index)
	if i < 0 || i >= len(rf.commandLog.Msgs) {
		return false
	} else if (rf.commandLog.Msgs[i].SnapshotValid && rf.commandLog.Msgs[i].SnapshotTerm == Term) || (rf.commandLog.Msgs[i].CommandValid && rf.commandLog.Msgs[i].CommandTerm == Term) {
		return true
	}
	return false
}
func (r *Raft) leaderUpdatePeer(peerIndex int, msg *LogData) {
	defer r.checkFuncDone("leaderUpdatePeer")()
	defer r.raftPeers[peerIndex].modeLock.Unlock()
	r.Dolog(peerIndex, "leaderUpdate: leaderUpdatePeer Get RQ")
	arg := RequestArgs{
		SelfTerm:  r.getTerm(),
		SelfIndex: int32(r.me),
		// Msg:       append(make([]*LogData, 0), &LogData{SelfIndex: r.me}),
	}
	for {
		arg.LastLogIndex, arg.LastLogTerm = r.getLastLogData()
		arg.Time = time.Now()
		if r.getLevel() != LevelLeader {
			break
		}
		r.raftPeers[peerIndex].logIndexTermLock.Lock()
		peerLastLogIndex, peerLastLogTerm := r.raftPeers[peerIndex].logIndex, r.raftPeers[peerIndex].lastLogTerm
		r.raftPeers[peerIndex].logIndexTermLock.Unlock()
		if ok := r.isInLog(int(peerLastLogIndex), int(peerLastLogTerm)); ok {
			getLen := msg.LastTimeIndex - int(peerLastLogIndex)
			arg.Msg = r.getLogs(msg.LastTimeIndex, getLen)
		} else {
			arg.Msg = r.getLogs(msg.LastTimeIndex, UpdateLogLines)
		}
		arg.CommitIndex = r.getCommitIndex()
		rpl := RequestReply{}
		r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) Go ", arg.string())

		select {
		case r.raftPeers[peerIndex].JumpHeartBeat <- struct{}{}:
		default:
		}

		// 没有超时机制
		ok := r.raftPeers[peerIndex].C.Call("Raft.HeartBeat", &arg, &rpl)
		if ok {
			r.raftPeers[peerIndex].updateLastTalkTime()
			r.raftPeers[peerIndex].logIndexTermLock.Lock()
			r.raftPeers[peerIndex].commitIndex = rpl.PeerCommitIndex
			r.raftPeers[peerIndex].lastLogTerm = rpl.PeerLastLogTerm
			r.raftPeers[peerIndex].logIndex = rpl.PeerLastLogIndex
			r.raftPeers[peerIndex].logIndexTermLock.Unlock()
		}

		r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) return ", rpl.string())

		if !ok {
			r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) Timeout CallFalse", arg.string())
			break
		} else if !rpl.IsAgree {
			r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) DisAgree", rpl.string())
			break
		} else if rpl.LogDataMsg != nil {
			r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) ReWriteCacheToGetNextOne", rpl.string())
			msg = rpl.LogDataMsg
		} else {
			r.Dolog(peerIndex, "leaderUpdate: HeartBeat(Update) UpdateDone", rpl.string())
			break
		}
	}
	r.Dolog(peerIndex, "leaderUpdate: Update Done")
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
	for i := 0; i < 100; i++ {
		rf.KilledChan <- true
	}
	close(rf.KilledChan)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLevel() int32 {
	return atomic.LoadInt32(&rf.level)
}

func (rf *Raft) setLevel(i int32) {
	atomic.StoreInt32(&rf.level, i)
	rf.Dolog(-1, "Level set to ", i)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		switch rf.getLevel() {
		case LevelFollower:
			select {
			case <-rf.levelChangeChan:
			case <-rf.KilledChan:
				return
			case <-rf.timeOutChan:
				atomic.StoreInt32(&rf.isLeaderAlive, 1)
			case <-time.NewTimer(time.Duration((int64(voteTimeOut) + rand.Int63()%150) * time.Hour.Milliseconds())).C:
				rf.Dolog(-1, "TimeOut")
				atomic.StoreInt32(&rf.isLeaderAlive, 0)
				go TryToBecomeLeader(rf)
			}
		case LevelCandidate:
			select {
			case <-rf.levelChangeChan:
			case <-rf.KilledChan:
				return
			case <-rf.timeOutChan:
			}
		case LevelLeader:
			select {
			case <-rf.KilledChan:
				return
			case <-rf.levelChangeChan:
			case <-rf.timeOutChan:
			}
		}
	}
}

func TryToBecomeLeader(rf *Raft) {
	defer rf.checkFuncDone("TryToBecomeLeader")()
	rf.changeToCandidate()
	arg := RequestArgs{SelfTerm: rf.getTerm() + 1, Time: time.Now(), SelfIndex: int32(rf.me), CommitIndex: rf.getCommitIndex()}

	// 这里要拿日志锁
	arg.LastLogIndex, arg.LastLogTerm = rf.getLastLogData()

	rpl := make([]RequestReply, len(rf.peers))
	wg := &sync.WaitGroup{}
	for i := range rf.raftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				rf.Dolog(index, "Raft.RequestPreVote  GO ", index, arg.string())
				ok := rf.call(index, "Raft.RequestPreVote", &arg, &rpl[index])
				rf.Dolog(index, "Raft.RequestPreVote RETURN ", ok, rpl[index].IsAgree, rpl[index].string())
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	count := 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerLastLogTerm > arg.LastLogTerm || (rpl[i].PeerLastLogTerm == arg.LastLogTerm && rpl[i].PeerLastLogIndex > rf.getLogIndex()) {
				// timeout case
				if rpl[i].PeerLastLogTerm == 0 && rpl[i].PeerLastLogIndex == 0 {
					continue
				}
				rf.changeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count > len(rf.peers)/2 {
		// 在这之前对其他的投票了
		// arg.SelfTerm = rf.getTerm()
		rf.setTerm(arg.SelfTerm)
	} else {
		rf.changeToFollower(nil)
		return
	}
	rpl = make([]RequestReply, len(rf.peers))

	for i := range rf.raftPeers {
		if i != rf.me {
			wg.Add(1)
			go func(index int) {
				rf.Dolog(index, "Raft.RequestVote GO ", arg.string())
				ok := rf.call(index, "Raft.RequestVote", &arg, &rpl[index])
				rf.Dolog(index, "Raft.RequestVote RETUEN ", ok, rpl[index].IsAgree, rpl[index].string())
				wg.Done()
			}(i)
		}
	}
	wg.Wait()
	count = 1
	for i := range rpl {
		if i != rf.me {
			if rpl[i].PeerLastLogTerm > arg.LastLogTerm || (rpl[i].PeerLastLogTerm == arg.LastLogTerm && rpl[i].PeerLastLogIndex > rf.getLogIndex()) {
				// timeout case
				if rpl[i].PeerLastLogTerm == 0 && rpl[i].PeerLastLogIndex == 0 {
					continue
				}
				rf.changeToFollower(nil)
				return
			}
			if rpl[i].IsAgree {
				count++
			}
		}
	}
	if count > len(rf.peers)/2 {
		for i := range rpl {
			if i != rf.me && rpl[i].IsAgree {
				rf.raftPeers[i].logIndexTermLock.Lock()
				rf.raftPeers[i].lastLogTerm, rf.raftPeers[i].logIndex = rpl[i].PeerLastLogIndex, rpl[i].PeerLastLogTerm
				rf.raftPeers[i].logIndexTermLock.Unlock()
			}
		}
		rf.changeToLeader()
	} else {
		rf.changeToFollower(nil)
		return
	}
}

func (r *Raft) call(index int, FuncName string, arg *RequestArgs, rpl *RequestReply) bool {
	asdf := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), HeartbeatTimeout)
	defer cancel()
	i := false
	go func() {
		if arg.Msg != nil && len(arg.Msg) > 1 {
			r.Dolog(-1, asdf, "UPdate rpcGo", arg.string())
		}
		i = r.peers[index].Call(FuncName, arg, rpl)
		if arg.Msg != nil && len(arg.Msg) > 1 {
			r.Dolog(-1, asdf, "UPdate rpcReturn", rpl.string())
		}
		cancel()
	}()
	select {
	case <-ctx.Done():
		r.raftPeers[index].updateLastTalkTime()
	case <-time.After(HeartbeatTimeout):
		r.Dolog(index, "Rpc Timeout ", FuncName, arg.string(), rpl.string())
	}
	return i
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// os.Stderr.WriteString("Raft Make \n")
	rf := &Raft{}
	rf.pMsgStore = &MsgStore{msgs: make([]*LogData, 0), owner: -1, term: -1, mu: sync.Mutex{}}
	rf.commitChan = make(chan int32, commitChanSize)
	rf.applyChan = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.level = LevelFollower
	rf.isLeaderAlive = 0
	rf.term = 0
	rf.timeOutChan = make(chan struct{}, 1)
	rf.levelChangeChan = make(chan struct{}, 1)
	rf.raftPeers = make([]RaftPeer, len(rf.peers))
	rf.commandLog.Msgs = append(make([]*ApplyMsg, 0), &ApplyMsg{CommandValid: true, Command: nil, CommandIndex: 0, CommandTerm: -1, SnapshotValid: false, Snapshot: nil, SnapshotTerm: 0, SnapshotIndex: 0})
	rf.commandLog.MsgRwMu = sync.RWMutex{}
	rf.pMsgStoreCreateLock = sync.Mutex{}
	rf.KilledChan = make(chan bool, 100)
	rf.termLock = sync.Mutex{}

	file, err := os.OpenFile(fmt.Sprintf("/home/wang/raftLog/raft_%d.R", os.Getpid()), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("err.Error(): %v\n", err.Error()))
		os.Exit(1)
	}
	log.SetOutput(file)
	log.SetFlags(log.Lmicroseconds)

	file2, err := os.OpenFile(fmt.Sprintf("/home/wang/raftLog/raft_%d_%d.R", os.Getpid(), rf.me), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("err.Error(): %v\n", err.Error()))
		log.Fatal(err)
	}

	rf.DebugLoger = log.New(file2, "", log.LstdFlags)
	rf.DebugLoger.SetFlags(log.Lmicroseconds)
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	for i := range rf.peers {
		rf.raftPeers[i] = RaftPeer{C: rf.peers[i], SendHeartBeat: make(chan struct{}), JumpHeartBeat: make(chan struct{}, 1), BeginHeartBeat: make(chan struct{}), StopHeartBeat: make(chan struct{})}
		rf.raftPeers[i].modeLock = sync.Mutex{}
		if i != rf.me {
			go rf.registeHeartBeat(i)
		}
	}

	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.commandLog.MsgRwMu.RLock()
	go func() {
		defer rf.commandLog.MsgRwMu.RUnlock()
		if rf.commandLog.Msgs[0].SnapshotValid {
			rf.applyChan <- *rf.commandLog.Msgs[0]
		}
		asdf := rf.GetTargetCacheIndex(int(rf.getCommitIndexUnsafe()))
		for i := 1; i <= asdf && i < len(rf.commandLog.Msgs); i++ {
			rf.applyChan <- *rf.commandLog.Msgs[i]
		}
		go rf.ticker()
		// go rf.persisterTicker()
		go rf.committer(applyCh)
	}()
	return rf
}

type peersLogSlice []peersLog
type peersLog struct {
	term        int32
	index       int32
	commitIndex int32
}

func (s peersLogSlice) Len() int {
	return len(s)
}

func (s peersLogSlice) Less(i, j int) bool {
	if s[i].term == s[j].term {
		return s[i].index < s[j].index
	} else {
		return s[i].term < s[j].term
	}
}

func (s peersLogSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (rf *Raft) sendHeartBeat() {
	for i := range rf.raftPeers {
		if i != rf.me {
			select {
			case rf.raftPeers[i].SendHeartBeat <- struct{}{}:
			default:
			}
		}
	}
}
func (rf *Raft) committer(applyCh chan ApplyMsg) {
	rf.Dolog(-1, "Committer Create \n")
	var ToLogIndex int32
	var LogedIndex int32
	var ok bool
	// init If restart
	LogedIndex = rf.getCommitIndex()
	ToLogIndex = LogedIndex

	for {
		peerLogedIndexs := make([]peersLog, len(rf.raftPeers))
		select {
		// leader scan followers to deside New TologIndex
		case <-rf.KilledChan:
			return
		case <-time.After(5 * time.Millisecond):
			if rf.getLevel() == LevelLeader {
				halfLenPeersCommitted := 0
				for i := range rf.raftPeers {
					if i == rf.me {
						continue
					}
					rf.raftPeers[i].logIndexTermLock.Lock()
					peerLogedIndexs[i].index = rf.raftPeers[i].logIndex
					peerLogedIndexs[i].term = rf.raftPeers[i].lastLogTerm
					peerLogedIndexs[i].commitIndex = rf.raftPeers[i].commitIndex
					rf.raftPeers[i].logIndexTermLock.Unlock()
				}
				sort.Sort(peersLogSlice(peerLogedIndexs))
				{
					asdf := make([]int, 0)
					for i := range peerLogedIndexs {
						asdf = append(asdf, int(peerLogedIndexs[i].commitIndex))
					}
					sort.Ints(asdf)
					halfLenPeersCommitted = asdf[len(asdf)/2+1]
				}
				ToLogHalfIndex := peerLogedIndexs[(len(peerLogedIndexs))/2+1]
				_, SelfNowLastLogTerm := rf.getLastLogData()
				if SelfNowLastLogTerm > ToLogHalfIndex.term {
					continue
				}
				if ToLogHalfIndex.index > ToLogIndex {
					ToLogIndex = int32(halfLenPeersCommitted)
					rf.Dolog(-1, "Committer: Update CommitIndex", ToLogIndex)
					rf.justSetCommitIndex(ToLogHalfIndex.index)
					rf.persist(rf.GetSnapshot())
					rf.sendHeartBeat()
				}
			}

			// follower get new TologIndex from Leader heartbeat
		case ToLogIndex, ok = <-rf.commitChan:
			if rf.getLevel() == LevelLeader {
				rf.Dolog(-1, "Committer: FALAT err: leader Get CommitChan returned")
				os.Exit(1)
			}
			if !ok {
				return
			} else {
				rf.Dolog(-1, "Committer: Get TologIndex ", ToLogIndex)
			}
		}
		// check
		rf.Dolog(-1, "Committer: ", "ToLogIndex ", ToLogIndex, "<= LogedIndex", LogedIndex)
		if ToLogIndex <= LogedIndex {
			ToLogIndex = LogedIndex
			continue
		} else {
			findLogSuccess := false
			expectedLogMsgCacheIndex, exceptLogMsgIndex := int32(0), int32(0)
			for {
				rf.commandLog.MsgRwMu.RLock()
				findLogSuccess, expectedLogMsgCacheIndex, exceptLogMsgIndex =
					func() (bool, int32, int32) {
						defer rf.commandLog.MsgRwMu.RUnlock()
						for exceptLogMsgIndex := LogedIndex + 1; exceptLogMsgIndex <= ToLogIndex; exceptLogMsgIndex++ {
							rf.DebugLoger.Println("1")

							// get cache index
							expectedLogMsgCacheIndex := int32(rf.GetTargetCacheIndex(int(exceptLogMsgIndex)))
							rf.DebugLoger.Println("2")
							if expectedLogMsgCacheIndex <= 0 {
								i := rf.getSnapshotUnsafe()
								if i.SnapshotValid && i.SnapshotIndex >= int(exceptLogMsgIndex) {
									exceptLogMsgIndex = int32(i.SnapshotIndex)
									expectedLogMsgCacheIndex = 0
								}
							}
							// out of range
							if expectedLogMsgCacheIndex >= int32(len(rf.commandLog.Msgs)) {
								return false, expectedLogMsgCacheIndex, exceptLogMsgIndex
							} else {
								rf.DebugLoger.Println("3")
								// commit operation
								select {
								case applyCh <- *rf.commandLog.Msgs[expectedLogMsgCacheIndex]:
								// 阻塞占有锁 不呢超过5ms
								case <-time.After(5 * time.Millisecond):
									goto done
								}
								rf.Dolog(-1, fmt.Sprintf("Committer: Commit log message CacheIndex:[%d] Index[%d] %s", expectedLogMsgCacheIndex, exceptLogMsgIndex, rf.commandLog.Msgs[expectedLogMsgCacheIndex].string()))
								rf.DebugLoger.Println("4")
								LogedIndex = exceptLogMsgIndex
								rf.DebugLoger.Println("5")
							}
						}
					done:
						return true, -1, -1
					}()
				if rf.getLevel() != LevelLeader {
					rf.setCommitIndex(LogedIndex)
				}
				if !findLogSuccess || LogedIndex == ToLogIndex {
					rf.persist(rf.GetSnapshot())
					break
				}
			}
			if !findLogSuccess {
				//log
				rf.Dolog(-1, fmt.Sprintf("Committer:  Trying to Log Message[%d] But failed(OutOfRange[%d])", exceptLogMsgIndex, expectedLogMsgCacheIndex))
			}
		}
	}
}
