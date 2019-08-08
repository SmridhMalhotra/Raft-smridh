package main

type Transporter interface {
		SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	    SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
}





