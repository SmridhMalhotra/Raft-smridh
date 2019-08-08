package main

import (
	"github.com/gogo/protobuf/proto"
	"io"
	"io/ioutil"
	"raft-smridh/RPCs"
)

type RequestVoteRequest struct {
	peer 			*Peer
	Term 			uint64
	LastLogIndex 	uint64
	LastLogTerm 	uint64
	CandidateName 	string

}

type RequestVoteResponse struct {

	peer 			*Peer
	Term 			uint64
	VoteGranted 	bool
}

func newRequestVoteRequest(term uint64, lastLogIndex uint64, lastLogTerm uint64, candidateName string) *RequestVoteRequest{
	return &RequestVoteRequest{
		Term:          term,
		LastLogIndex:  lastLogIndex,
		LastLogTerm:   lastLogTerm,
		CandidateName: candidateName,
	}

}

func (req *RequestVoteRequest) Encode(w io.Writer) (int, error){
	pb := &RPCs.RequestVoteRequest{
		Term:          proto.Uint64(req.Term),
		LastLogIndex:  proto.Uint64(req.LastLogIndex),
		LastLogTerm:   proto.Uint64(req.LastLogTerm),
		CandidateName: proto.String(req.CandidateName),
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

func (req *RequestVoteRequest) Decode (r io.Reader) (int,error) {
	data,err := ioutil.ReadAll(r)

	if err != nil {
		return -1,err
	}
	pb := new(RPCs.RequestVoteRequest)
	if err := proto.Unmarshal(data,pb); err != nil{
		return -1,err
	}

	req.Term = pb.GetTerm()
	req.LastLogTerm = pb.GetLastLogTerm()
	req.LastLogTerm = pb.GetLastLogTerm()
	req.CandidateName = pb.GetCandidateName()


	return len(data), nil
}

func newRequestVoteResponse(term uint64,voteGranted bool) *RequestVoteResponse{
	return &RequestVoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
	}
}

func (res *RequestVoteResponse) Encode(w io.Writer) (int, error){
	pb := &RPCs.RequestVoteResponse{
		Term:          proto.Uint64(res.Term),
		VoteGranted:   proto.Bool(res.VoteGranted),
	}
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

func (res *RequestVoteResponse) Decode (r io.Reader) (int,error) {
	data,err := ioutil.ReadAll(r)

	if err != nil {
		return -1,err
	}
	pb := new(RPCs.RequestVoteResponse)
	if err := proto.Unmarshal(data,pb); err != nil{
		return -1,err
	}

	res.Term = pb.GetTerm()
	res.VoteGranted = pb.GetVoteGranted()

	return len(data), nil
}





