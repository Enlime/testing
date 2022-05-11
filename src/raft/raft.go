package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"labrpc"
	"sync"
	"time"
	"math/rand"
	//"fmt"
//	"log"

)
// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type STATE int
const(
	FOLLOWER STATE = iota
	CANDIDATE
	LEADER
)



//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg         // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int				//lastest term server has seen
	votedFor int				//candidatedId that received vote in current term 
	voteCnt int
	voteWinMajority chan bool
	state STATE 				//follower, candidate, or leader	
	heartBeatTimer *time.Timer	
	electionTimer *time.Timer

	//log entries, add later in proj4

}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3).
	//TODO: may be need to be locked
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (4).
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
	// Your code here (4).
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
	// Your data here (3, 4).
	Term int
	CandidateId int 

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	//fmt.Println(rf.me,"request vote for " , args.CandidateId)
	//log.Printf("request vote for")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//when term > currentTerm, set currentTerm to term, convert to follower
	if args.Term > rf.currentTerm {
		//fmt.Println("in request vote term > current term, ", rf.me)
		//rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != FOLLOWER{
			//fmt.Println("term > current term, ", rf.me, "turn to follower")
			rf.state = FOLLOWER
			rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
		}
		
	}

	reply.Term = rf.currentTerm
	//TODO: may need to check when voteFor == candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		//fmt.Println(rf.me,"vote for " , rf.votedFor)
 
	} else{
		reply.VoteGranted = false
	}


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
	return ok
}



type AppendEntriesArgs struct {
	Term int
	LeaderId int 
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if rf.state == LEADER 
	if rf.state == FOLLOWER {
		rf.electionTimer.Reset(time.Millisecond*(time.Duration(500+rand.Intn(150))))
	}

	if args.Term > rf.currentTerm {
		//fmt.Println(rf.me, "append entries args term > current term, turn to follower, was ", rf.state)
		rf.currentTerm = args.Term
		if rf.state != FOLLOWER{
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
		}

	}

	if args.Term == rf.currentTerm && rf.state == CANDIDATE {
		//fmt.Println(rf.me, "args term == current term, turn to follower")
		
		if rf.state != FOLLOWER{
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
		}
	}
	
	//term := rf.currentTerm

	reply.Term = 0

	//rf.mu.Unlock()
	//may be TODO:turn candidate to follower if needed 


}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (4).

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

/*
func (rf *Raft) LeaderHeartBeat() {

	for true {
		if rf.state == LEADER {

		}
	}
}
*/

func (rf *Raft) BeingLeader(){
	//fmt.Println(rf.me, "being a leader ")
	rf.mu.Lock()
	rf.state = LEADER
	rf.electionTimer.Stop()
	//fmt.Println(rf.me, "being a leader in term", rf.currentTerm)
	rf.mu.Unlock()

	var current_state STATE
	rf.mu.Lock()
	current_state = rf.state
	rf.mu.Unlock()

	for current_state == LEADER {
		rf.mu.Lock()
		apply_entries_args := AppendEntriesArgs{Term:rf.currentTerm, LeaderId:rf.me}
		apply_entries_reply := AppendEntriesReply{}
		rf.mu.Unlock()

		for i :=0; i < len(rf.peers); i++ {
			if i == rf.me{
				continue
			}
			go func(i int){
				rf.sendAppendEntries(i, &apply_entries_args, &apply_entries_reply)
				//fmt.Println(apply_entries_reply)
				rf.mu.Lock()
				if apply_entries_reply.Term > rf.currentTerm {
					//rf.mu.Lock()
					//fmt.Println(rf.me, "leader turn to follower")
					rf.state = FOLLOWER
					rf.currentTerm = apply_entries_reply.Term
					rf.votedFor = -1
					rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
					rf.mu.Unlock()
					return 
				}
				rf.mu.Unlock()
			}(i)
		}
		time.Sleep(time.Millisecond*100)
		rf.mu.Lock()
		current_state = rf.state
		rf.mu.Unlock()
	}
	return
}


func (rf *Raft) BeingCandidate(){
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.mu.Unlock()

	//fmt.Println(rf.me)
	//fmt.Println(rf.me, "being candidate")

	var current_state STATE
	rf.mu.Lock()
	current_state = rf.state
	rf.mu.Unlock()

	for current_state == CANDIDATE {
		//fmt.Println(rf.me, "candidate to election")

		//TODO: add timer to signal candidate timeout 
		rf.mu.Lock()
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		vote_args := RequestVoteArgs{Term:rf.currentTerm, CandidateId:rf.me}
		rf.mu.Unlock()

		//vote_cnt := 1

		rf.mu.Lock()
		rf.voteCnt = 1
		rf.mu.Unlock()

		electionTimer := time.NewTimer(time.Millisecond*(time.Duration(500+rand.Intn(150))))

		for i :=0; i < len(rf.peers); i++ {
			if i == rf.me{
				continue
			}
			go func(i int){
				vote_reply := RequestVoteReply{}
				rf.sendRequestVote(i, &vote_args, &vote_reply)
				//if !res {
					//fmt.Println(rf.me, " send request failed ", i)
					//fmt.Println("reply term ", vote_reply.Term, " VoteGranted", vote_reply.VoteGranted)
				//}
				rf.mu.Lock()
				//defer rf.mu.Unlock()
				if vote_reply.Term > rf.currentTerm {
					//fmt.Println(rf.me, "reply term > current term, turn to follower, peer is ", i)
					rf.state = FOLLOWER
					rf.currentTerm = vote_reply.Term
					rf.votedFor = -1
					rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
					rf.mu.Unlock()			
					return 
				}
				if vote_reply.VoteGranted {
					rf.voteCnt = rf.voteCnt + 1
				}
				if  rf.state==CANDIDATE && rf.voteCnt > int(0.5* float32(len(rf.peers))){
					rf.voteWinMajority <- true
					rf.mu.Unlock()
					//fmt.Println("vote win")
					rf.BeingLeader()
					return
				}
				rf.mu.Unlock()
			}(i)

		}
		//time.Sleep(time.Millisecond*(time.Duration(800)))
		//if !rf.electionTimer.Stop() {
		//<-rf.electionTimer.C
		//}
		//rf.mu.Lock()
		select {
		case <- rf.voteWinMajority:
		case <- electionTimer.C:
		}


		rf.mu.Lock()
		current_state = rf.state
		//rf.mu.Lock()
		//fmt.Println(rf.me, "term ", rf.currentTerm, " has vote cnt ", vote_cnt)
		//rf.mu.Unlock()
		rf.mu.Unlock()
	}
	return 

}



func (rf *Raft) ElectionTimeoutMonitor() {
	for true {
		var current_state STATE
		rf.mu.Lock()
		current_state = rf.state
		rf.mu.Unlock()
	
		for current_state == FOLLOWER {
			rf.mu.Lock()
			rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
			
			rf.mu.Unlock()

		}

		//time.Sleep(time.Second * 5)
		//fmt.Println("return from candidate")
	}
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

	// Your initialization code here (3, 4).

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.voteWinMajority = make(chan bool)
	rf.state = FOLLOWER

	rf.heartBeatTimer = nil 
	rf.electionTimer = nil

	//go rf.ElectionTimeoutMonitor()
	rf.mu.Lock()
	rf.electionTimer = time.AfterFunc(time.Millisecond*(time.Duration(500+rand.Intn(150))), rf.BeingCandidate)
	
	rf.mu.Unlock()



	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
