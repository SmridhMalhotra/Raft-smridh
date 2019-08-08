package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (

	Stopped ="stopped"
	Initialized = "initialized"
	Follower = "follower"
	Candidate = "candidate"
	Leader = "leader"
)

const (
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultElectionTimeout = 200 * time.Millisecond
)

const (
	MaxLogEntriesPerRequest         = 2000

)

const ElectionTimeoutThresholdPercent = 0.8

var NotLeaderError =errors.New("raft.Server: This is not the current leader")
var DuplicatePeerError = errors.New("raft.Server: Duplicate Peer Exists")
var CommandTimeoutError = errors.New("raft.Server: Command timed out")
var StopError = errors.New("raft: Terminated")


type Server interface {
	Name() string
	Context() interface{}
	Leader() string
	State() string
	Path() string
	LogPath() string
	Term() uint64
	CommitIndex() uint64
	VotedFor() string
	MemberCount() int
	QuorumSize() int
	IsLogEmpty() bool
	LogEntries() []*LogEntry
	LastCommandName() string
	GetState() string
	ElectionTimeout() time.Duration
	SetElectionTimeout(duration time.Duration)
	HeartbeatInterval() time.Duration
	SetHeartbeatInterval(duration time.Duration)
	Transporter() Transporter
	SetTransporter(t Transporter)
	AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	RequestVote(req *RequestVoteRequest) *RequestVoteResponse
	AddPeer(name string, connectionString string) error
	RemovePeer(name string) error
	Peers() map[string]*Peer
	Init() error
	Start() error
	Stop()
	Running() bool
	AddEventListener(string, EventListener)
	FlushCommitIndex()
	Do(command Command) (interface{},error)
}

type server struct {

	*eventDispatcher
	name                    string
	path                    string
	state                   string
	string                  string
	transporter             Transporter
	context                 interface{}
	currentTerm             uint64
	votedFor                string
	log                     *Log
	leader                  string
	peers                   map[string]*Peer
	mutex                   sync.RWMutex
	syncedPeer              map[string]bool
	stopped                 chan bool
	c                       chan *ev
	electionTimeout         time.Duration
	heartbeatInterval       time.Duration
	maxLogEntriesPerRequest uint64
	connectionString        string
	routineGroup            sync.WaitGroup
}

func NewServer(name string, path string,transporter Transporter,ctx interface{},connectionString string ) (Server,error) {
	if name == "" {
		return nil, errors.New("server name cannot be blank")
	}
	if transporter == nil {
		panic("Transporter required")
	}
	s := &server{
		name:                    name,
		path:                    path,
		transporter:             transporter,

		context:                 ctx,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),
		electionTimeout:         DefaultElectionTimeout,
		heartbeatInterval:       DefaultHeartbeatInterval,
		maxLogEntriesPerRequest: MaxLogEntriesPerRequest,
		connectionString:        connectionString,
	}
	s.eventDispatcher = newEventDispatcher(s)

	// Setup apply function.
	s.log.ApplyFunc = func(e *LogEntry, c Command) (interface{}, error) {
		// Dispatch commit event.
		s.DispatchEvent(newEvent(CommitType, e, nil))

		// Apply command to the state machine.
		switch c := c.(type) {
		case CommandApply:
			return c.Apply(&context{
				server:       s,
				currentTerm:  s.currentTerm,
				currentIndex: s.log.internalCurrentIndex(),
				commitIndex:  s.log.commitIndex,
			})
		case deprecatedCommandApply:
			return c.Apply(s)

		default:
			return nil, fmt.Errorf("command does not implement Apply()")
		}
	}

	return s, nil
}

func (s *server) Name() string {
	return s.name
}

func (s *server) Do(command Command) (interface{},error) {
	return s.send(command)
}

func (s *server) Context() interface{} {
	return s.context
}

func (s *server) Leader() string {
	return s.leader
}

func (s *server) State() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

func (s *server) Path() string {
	return s.path
}

func (s *server) LogPath() string {
	return path.Join(s.path,"log")
}

func (s *server) Term() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.currentTerm
}

func (s *server) CommitIndex() uint64 {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return  s.log.commitIndex
}

func (s *server) VotedFor() string {
	return s.votedFor
}

func (s *server) MemberCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.peers)+1
}

func (s *server) QuorumSize() int {
	s.mutex.RLock()
	defer  s.mutex.RUnlock()
	return (s.MemberCount()/2) + 1
}

func (s *server) IsLogEmpty() bool {
	return s.log.isEmpty()
}

func (s *server) LogEntries() []*LogEntry {
	s.log.mutex.RLock()
	s.log.mutex.RUnlock()
	return s.log.entries
}

func (s *server) LastCommandName() string {
	return s.log.lastCommandName()
}

func (s *server) GetState() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, CommitedIndex: %v ", s.name, s.state, s.currentTerm, s.log.commitIndex)
}

func (s *server) ElectionTimeout() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.electionTimeout

}

func (s *server) SetElectionTimeout(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.electionTimeout = duration
}

func (s *server) HeartbeatInterval() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.heartbeatInterval
}

func (s *server) SetHeartbeatInterval(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.heartbeatInterval = duration
	for _,p := range s.peers{
		p.heartbeatInterval = duration
	}
}

func (s *server) Transporter() Transporter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.transporter
}

func (s *server) SetTransporter(t Transporter) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.transporter = t
}

func (s *server) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	ret,_ := s.send(req)
	resp,_ := ret.(*AppendEntriesResponse)
	return resp

}

func (s *server) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	ret,_ := s.send(req)
	resp,_ := ret.(*RequestVoteResponse)
	return resp
}

func (s *server) AddPeer(name string, connectionString string) error {

	if s.peers[name] !=nil {
		return nil
	}
	if s.name !=name {
		peer := newPeer(s,name,connectionString,s.heartbeatInterval)
		if s.state == Leader{
			peer.startHeartbeat()

		}
		s.peers[peer.Name] = peer
		s.debugln("server.peer.add: ", name, len(s.peers))
		s.DispatchEvent(newEvent(AddPeer,name,nil))
	}
	s.writeConf()
	return nil

}
// TODO remove?
func ( server) RemovePeer(name string) error {
	// TODO writeconf?
	panic("implement me")
}


func (s *server) Peers() map[string]*Peer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	peers := make(map[string]*Peer)
	for name,peer:= range s.peers{
		peers[name] = peer.clone()
	}
	return peers
}

func (s *server) Init() error {
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}
	if s.state == Initialized||s.log.initialized{
		s.state=Initialized
		return nil
	}
	if err := s.readConf(); err != nil {
		s.debugln("raft: Conf file error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}
	if err := s.log.open(s.LogPath()); err !=nil{
		s.debugln("raft: Log error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}
	_,s.currentTerm =s.log.lastInfo()
	s.state = Initialized
	return nil
}

func init() {
	RegisterCommand(&NOPCommand{})
	RegisterCommand(&DefaultJoinCommand{})
	RegisterCommand(&DefaultLeaveCommand{})
	RegisterCommand(&writeCommand{})
}

func (s  *server) Start() error {
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}
	if err := s.Init();err !=nil{
		return err
	}
	s.stopped = make(chan bool)
	s.setState(Follower)
	if !s.promotable() {
		s.debugln("start as a new raft server")

		// If log entries exist then allow promotion to candidate
		// if no AEs received.
	} else {
		s.debugln("start from previous saved state")
	}

	debugln(s.GetState())
	s.routineGroup.Add(1)
	go func(){
		defer s.routineGroup.Done()
		s.loop()
	}()
	return nil

}

func(s *server) setState(state string){
	s.mutex.Lock()
	defer s.mutex.Unlock()
	prevState := s.state
	prevLeader := s.leader
	s.state = state
	if s.state == Leader{
		s.leader = s.Name()
		s.syncedPeer = make(map[string]bool)
	}
	s.DispatchEvent(newEvent(StateChange,s.state,prevState))

	if s.leader != prevLeader {
		s.DispatchEvent(newEvent(LeaderChange,s.leader,prevState))
	}
}

func (s *server) promotable() bool{
	return s.log.CurrentIndex() > 0
}

func (s *server) Stop() {
	if s.state==Stopped {
		return
	}
	close(s.stopped)
	s.routineGroup.Wait()
	s.log.close()
	s.setState(Stopped)
}

func (s *server) Running() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state != Stopped && s.state!=Initialized
}


type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}

func (s *server) updateCurrentTerm(term uint64, leaderName string){
	if s.currentTerm< term {
		prevterm := s.currentTerm
		prevLeader := s.leader
		if s.state == Leader{
			for _, peer :=range s.peers{
				peer.stopHeartbeat(false)
			}
		}
		if s.state != Follower{
			s.setState(Follower)
		}
		s.mutex.Lock()
		s.currentTerm= term
		s.leader = leaderName
		s.votedFor = ""
		s.mutex.Unlock()
		s.DispatchEvent(newEvent(TermChange,s.currentTerm,prevterm))
		if s.leader != prevLeader{
			s.DispatchEvent(newEvent(LeaderChange,s.leader,prevLeader))
		}
	}
}

func (s *server) loop(){
	defer s.debugln("server loop ended")
	state := s.State()
	for state != Stopped{
		s.debugln("server loop running",state)
		switch state {
		case Follower:
			s.followerLoop()
		case Candidate:
			s.candidateLoop()
		case Leader:
			s.leaderLoop()
		}
		state = s.State()
	}

}

func (s *server) send(value interface{}) (interface{},error){
	if !s.Running() {
		return nil,StopError
	}
	event := &ev{target:value,c : make(chan error,1)}
	select {
	case s.c <-event:
	case <-s.stopped:
		return nil, StopError
	}
	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
	}
}

func (s *server) sendAsync(value interface{}){
	if !s.Running() {
		return
	}
	event := &ev{target:value,c : make(chan error,1)}
	select {
	case s.c <-event:
		return
	default:
	}
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		select {
		case s.c <- event:
		case <-s.stopped :
		}
	}()
}

func (s *server) followerLoop(){
	debugln("inside follower loop")
	since := time.Now()
	electionTimeout := s.ElectionTimeout()
	timeOutChan := afterBetween(s.ElectionTimeout(),2*s.ElectionTimeout())
	for s.State() == Follower{
		var err error
		update := false
		select{
		case <-s.stopped:
			s.setState(Stopped)
			return
		case e:= <-s.c:
			switch req := e.target.(type) {
			case JoinCommand:
				if s.log.CurrentIndex() == 0 && req.NodeName() == s.Name() {
					s.setState(Leader)
					s.processCommand(req,e)
				} else{
					err = NotLeaderError
			}

			case *AppendEntriesRequest:
				elapsedTime := time.Now().Sub(since)
				if elapsedTime > time.Duration(float64(electionTimeout)*ElectionTimeoutThresholdPercent) {
					s.DispatchEvent(newEvent(ElectionTimeout, elapsedTime, nil))
				}
				e.returnValue, update = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, update = s.processRequestVoteRequest(req)
			default:
				err = NotLeaderError
			}
			e.c <-err
			case <-timeOutChan:
				if s.promotable() {
					s.setState(Candidate)
				}else {
					update = true
				}

		}
		if update {
			since = time.Now()
			timeOutChan = afterBetween(s.ElectionTimeout(),2*s.ElectionTimeout())
		}
	}
}

func (s *server) candidateLoop() {
	prevLeader := s.leader
	s.leader = ""
	if prevLeader != s.leader{
		s.DispatchEvent(newEvent(LeaderChange,s.leader,prevLeader))
	}

	lastLogIndex,lastLogTerm := s.log.lastInfo()
	initElec :=true
	votes := 0
	var timeoutChan <- chan time.Time
	var respChan chan *RequestVoteResponse

	for s.State() == Candidate{
		if initElec{
			s.currentTerm++
			s.votedFor = s.name
			respChan = make(chan *RequestVoteResponse,len(s.peers))
			for _,p := range s.peers{
				s.routineGroup.Add(1)
				go func(p *Peer){
					defer s.routineGroup.Done()
					p.sendVoteRequest(newRequestVoteRequest(s.currentTerm,lastLogIndex,lastLogTerm,s.name),respChan)
				}(p)
			}
			votes = 1
			timeoutChan = afterBetween(s.ElectionTimeout(),2*s.ElectionTimeout())
			initElec = false

		}
		if votes == s.QuorumSize(){
			s.debugln("candidate received enough votes")
			s.setState(Leader)
			return
		}
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return
		case resp := <- respChan:
			if success := s.processVoteResponse(resp);success{
				votes++
				s.debugln("server.candidate.vote.granted: ", votes)
			}
		case e := <-s.c:
			var err error
			switch req := e.target.(type) {
			case Command:
				err =  NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}
			e.c <- err

		case <-timeoutChan:
			initElec = true
		}
	}
}

func (s *server) leaderLoop(){
	logIndex,_ := s.log.lastInfo()
	for _,peer := range s.peers{
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.Do(NOPCommand{})
	}()

	for s.State() == Leader {
		var err error
		select {
		case <-s.stopped:
			for _,peer := range s.peers {
				peer.stopHeartbeat(false)
			}
			s.setState(Stopped)
			return
		case e := <- s.c:
			switch req := e.target.(type){
			case Command:
				s.processCommand(req,e)
				continue
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}
			e.c <- err

			}
			
		}
		s.syncedPeer = nil

}

func (s *server) processCommand(command Command,e *ev){
	entry,err := s.log.createEntry(s.currentTerm,command,e)

	if err != nil {
		s.debugln("server.command.log.entry.error:", err)
		e.c <- err
		return
	}

	if err := s.log.appendEntry(entry);err != nil{
		s.debugln("server.command.log.error:", err)
		e.c <- err
		return
	}

	s.syncedPeer[s.Name()] = true
	if len(s.peers) == 0 {
		commitIndex := s.log.CurrentIndex()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}


func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse,bool){
	if req.Term < s.currentTerm{
		s.debugln("server.ae.error: stale term")
		return newAppendEntriesResponse(s.currentTerm, false, s.log.CurrentIndex(), s.log.CommitIndex()), false
	}
	if req.Term == s.currentTerm {
		_assert(s.State() != Leader, "leader.elected.at.same.term.%d\n", s.currentTerm)
		if s.State() == Candidate{
			s.setState(Follower)
		}

		s.leader = req.LeaderName
	} else{
		s.updateCurrentTerm(req.Term,req.LeaderName)
	}
	if err := s.log.truncate(req.PrevLogIndex,req.PrevLogTerm); err != nil {
		s.debugln("server.ae.truncate.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.CurrentIndex(), s.log.CommitIndex()), true
	}
	if err := s.log.appendEntries(req.Entries); err != nil {
		s.debugln("server.ae.append.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.CurrentIndex(), s.log.CommitIndex()), true
	}


	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		s.debugln("server.ae.commit.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.CurrentIndex(), s.log.CommitIndex()), true
	}
	//s.debugln(s.Context().(*DB))
	return newAppendEntriesResponse(s.currentTerm, true, s.log.CurrentIndex(), s.log.CommitIndex()), true
}

func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse){
	if resp.Term() > s.Term(){
		s.updateCurrentTerm(resp.Term(),"")
		return
	}
	if !resp.Success(){
		return
	}
	if resp.append == true {
		s.syncedPeer[resp.peer] = true
	}

	if(len(s.syncedPeer) < s.QuorumSize()){
		return
	}
	var indices []uint64
	indices = append(indices, s.log.CurrentIndex())
	for _,peer := range  s.peers{
		indices = append(indices,peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(uint64Slice(indices)))

	commitIndex := indices[s.QuorumSize()-1]

	commitedIndex := s.log.commitIndex

	if commitIndex > commitedIndex {
		s.log.sync()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}

}

func (s *server) processVoteResponse(resp *RequestVoteResponse) bool{
	if resp.VoteGranted && resp.Term == s.currentTerm{
		return true
	}
	if resp.Term > s.currentTerm{
		s.debugln("server.candidate.vote.failed")
		s.updateCurrentTerm(resp.Term,"")
	} else{
		s.debugln("server.candidate.vote: denied")
	}
	return false
}

func (s *server) processRequestVoteRequest(req *RequestVoteRequest) (*RequestVoteResponse,bool){
	if req.Term < s.Term(){
		s.debugln("server.rv.deny.vote: cause stale term")
		return  newRequestVoteResponse(s.currentTerm,false), false
	}
	if req.Term > s.Term() {
		s.updateCurrentTerm(req.Term,"")
	}else if s.votedFor != "" && s.votedFor != req.CandidateName {
		s.debugln("server.deny.vote: cause duplicate vote: ", req.CandidateName,
			" already vote for ", s.votedFor)
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	lastIndex, lastTerm := s.log.lastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm{
		s.debugln("server.deny.vote: cause out of date log: ", req.CandidateName,
			"Index :[", lastIndex, "]", " [", req.LastLogIndex, "]",
			"Term :[", lastTerm, "]", " [", req.LastLogTerm, "]")
		return newRequestVoteResponse(s.currentTerm, false), false

	}

	s.votedFor = req.CandidateName
	s.debugln("server.rv.vote: ", s.name, " votes for", req.CandidateName, "at term", req.Term)
	return newRequestVoteResponse(s.currentTerm,true) , true
}

func (s *server) FlushCommitIndex() {
	s.debugln("server.conf.update")
	// Write the configuration to file.
	s.writeConf()
}

func (s *server) writeConf() {

	peers := make([]*Peer, len(s.peers))

	i := 0
	for _, peer := range s.peers {
		peers[i] = peer.clone()
		i++
	}

	r := &Config{
		CommitIndex: s.log.commitIndex,
		Peers:       peers,
	}

	b, _ := json.Marshal(r)

	confPath := path.Join(s.path, "conf")
	tmpConfPath := path.Join(s.path, "conf.tmp")

	err := writeFileSynced(tmpConfPath, b, 0600)

	if err != nil {
		panic(err)
	}

	os.Rename(tmpConfPath, confPath)
}

// Read the configuration for the server.
func (s *server) readConf() error {
	confPath := fmt.Sprintf("%s\\%s",s.path,"conf")

	s.debugln("readConf.open ", confPath)

	// open conf file
	b, err := ioutil.ReadFile(confPath)

	if err != nil {
		return nil
	}

	conf := &Config{}

	if err = json.Unmarshal(b, conf); err != nil {
		return err
	}

	s.log.updateCommitIndex(conf.CommitIndex)

	return nil
}






func (s *server) debugln(v ...interface{}) {
	if logLevel > Debug {
		debugf("[%s Term:%d] %s", s.name, s.Term(), fmt.Sprintln(v...))
	}
}

func (s *server) traceln(v ...interface{}) {
	if logLevel > Trace {
		tracef("[%s] %s", s.name, fmt.Sprintln(v...))
	}
}







