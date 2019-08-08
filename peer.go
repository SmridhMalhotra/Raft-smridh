package main

import (
	"sync"
	"time"
)

type Peer struct {
	server            *server
	Name              string
	ConnectionString  string
	prevLogIndex      uint64
	stopChan          chan bool
	heartbeatInterval time.Duration
	lastActivity      time.Time
	sync.RWMutex
}

func newPeer(server *server, name string, connectionstring string, heartbeatInterval time.Duration) *Peer{
	return &Peer{
		server:            server,
		Name:              name,
		ConnectionString:  connectionstring,
		heartbeatInterval: heartbeatInterval,
	}
}

func (p *Peer) setHeartBeatInterval(interval time.Duration){
	p.heartbeatInterval = interval
}

func (p *Peer) getPrevLogIndex() uint64{
	p.RLock()
	defer p.RUnlock()
	return p.prevLogIndex
}

func (p *Peer) setPrevLogIndex(index uint64){
	p.Lock()
	defer p.Unlock()
	p.prevLogIndex = index
}

func (p *Peer) setLastActivity(time time.Time){
	p.Lock()
	defer p.Unlock()
	p.lastActivity = time
}

func (p *Peer) startHeartbeat(){
	p.stopChan = make(chan bool)
	c := make(chan bool)
	p.setLastActivity(time.Now())
	p.server.routineGroup.Add(1)
	go func() {
		defer p.server.routineGroup.Done()
		p.heartbeat(c)
	}()
	<-c
}

func (p *Peer)	stopHeartbeat(flush bool){
	p.setLastActivity(time.Time{})
	p.stopChan <- flush

}

func (p *Peer) LastActivity() time.Time{
	p.RLock()
	defer p.RUnlock()
	return p.lastActivity
}

func (p *Peer) clone() *Peer{
	p.Lock()
	defer p.Unlock()
	return &Peer{
		Name:             p.Name,
		ConnectionString: p.ConnectionString,
		lastActivity:     p.lastActivity,
		prevLogIndex:     p.prevLogIndex,
	}
}

func (p *Peer) heartbeat(c chan bool){
	stopChan:= p.stopChan
	c <-true
	ticker := time.Tick(p.heartbeatInterval)
	debugln("peer.heartbeat: ", p.Name, p.heartbeatInterval)
	for{
		select{
		case flush := <-stopChan:
			if flush{
				p.flush()
				debugln("peer.heartbeat.stop.with.flush: ", p.Name)
				return
			} else{
			debugln("peer.heartbeat.stop: ", p.Name)
			return
			}
		case <-ticker:
			start :=time.Now()
			p.flush()
			duration := time.Now().Sub(start)
			p.server.DispatchEvent(newEvent(Heartbeat,duration,nil))
		}
	}

}

func (p *Peer) flush(){
	debugln("peer.heartbeat.flush: ", p.Name)
	debugln("prevlogindflush direct",p.prevLogIndex)
	debugln("prevlogindflush indirect",p.getPrevLogIndex())
	prevLogIndex := p.getPrevLogIndex()
	term := p.server.currentTerm
	entries, prevLogTerm := p.server.log.getEntriesAfter(prevLogIndex,p.server.maxLogEntriesPerRequest)
	if entries != nil {
		p.sendAppendEntriesRequest(newAppendEntriesRequest(term,prevLogIndex,prevLogTerm,p.server.log.CommitIndex(),p.server.name,entries))
	}
}

func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	tracef("peer.append.send: %s->%s [prevLog:%v length: %v]\n",
		p.server.Name(), p.Name, req.PrevLogIndex, len(req.Entries))
	resp := p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {
		p.server.DispatchEvent(newEvent(HearbeatInterval, p, nil))
		debugln("peer.append.timeout: ", p.server.Name(), "->", p.Name)
		return
	}
	traceln("peer.append.resp: ", p.server.Name(), "<-", p.Name)
	p.setLastActivity(time.Now())
	p.Lock()
	if resp.Success() {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].GetIndex()
			if req.Entries[len(req.Entries)-1].GetTerm() == p.server.currentTerm {
				resp.append = true
			}
		}
		traceln("peer.append.resp.success: ", p.Name, "; idx =", p.prevLogIndex)
	} else {
		if resp.Term() > p.server.Term() {
			debugln("peer.append.resp.not.update: new.leader.found")
		} else if resp.Term() == req.Term && resp.CommitIndex() >= p.prevLogIndex {
			p.prevLogIndex = resp.CommitIndex()
			debugln("peer.append.resp.update: ", p.Name, "; idx =", p.prevLogIndex)
		} else if p.prevLogIndex > 0 {
			p.prevLogIndex--
			if p.prevLogIndex > resp.Index() {
				p.prevLogIndex = resp.Index()
			}
			debugln("peer.append.resp.decrement: ", p.Name, "; idx =", p.prevLogIndex)
		}
	}
	p.Unlock()
	resp.peer = p.Name
	debugln("prevlogind direct",p.prevLogIndex)
	debugln("prevlogind indirect",p.getPrevLogIndex())
	p.server.sendAsync(resp)
}

func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse){
	debugln("peer.vote: ", p.server.Name(), "->", p.Name)
	req.peer = p
	if resp := p.server.Transporter().SendVoteRequest(p.server, p, req); resp !=nil{
		debugln("peer.vote.recv: ", p.server.Name(), "<-", p.Name)
		p.setLastActivity(time.Now())
		resp.peer = p
		c <- resp
	}else{
		debugln("peer.vote.failed: ", p.server.Name(), "<-", p.Name)
	}
}