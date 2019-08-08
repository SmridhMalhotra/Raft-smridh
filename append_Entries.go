package main

import (
	"github.com/gogo/protobuf/proto"
	"io"
	"io/ioutil"

	"raft-smridh/RPCs"
)

type AppendEntriesRequest struct {
	Term uint64
	PrevLogIndex uint64
	PrevLogTerm uint64
	CommitIndex uint64
	LeaderName string
	Entries []*RPCs.LogEntry
}

type AppendEntriesResponse struct {
	pb *RPCs.AppendEntriesResponse
	peer string
	append bool
}



func newAppendEntriesRequest(term uint64, prevLogIndex uint64, prevLogTerm uint64, commitIndex uint64,
	leaderName string, entries []*LogEntry) *AppendEntriesRequest{

	pbEntries := make([]*RPCs.LogEntry, len(entries))
	for i := range entries{
		pbEntries[i] = entries[i].pb
	}

	return &AppendEntriesRequest{
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderName:   leaderName,
		Entries:      pbEntries,
	}
}

func (req *AppendEntriesRequest) Encode(w io.Writer) (int, error){
	pb := &RPCs.AppendEntriesRequest{
		Term:         proto.Uint64(req.Term),
		PrevLogIndex: proto.Uint64(req.PrevLogIndex),
		PrevLogTerm:  proto.Uint64(req.PrevLogTerm),
		CommitIndex:  proto.Uint64(req.CommitIndex),
		LeaderName:   proto.String(req.LeaderName),
		Entries:      req.Entries,
	}
	debugln("before encoding in encoding" ,req.PrevLogIndex)
	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	if err != nil {
		return -1, err
	}
	debugln("after encoding in encoding" ,pb.GetPrevLogIndex())
	return w.Write(p)
}

func (req *AppendEntriesRequest) Decode (r io.Reader) (int,error) {
	data,err := ioutil.ReadAll(r)

	if err != nil {
		return -1,err
	}
	pb := new(RPCs.AppendEntriesRequest)
	if err := proto.Unmarshal(data,pb); err != nil{
		return -1,err
	}
	debugln("after decoding in decoding" ,pb.GetPrevLogIndex())
	req.Term = pb.GetTerm()
	req.PrevLogIndex = pb.GetPrevLogIndex()
	req.PrevLogTerm = pb.GetPrevLogTerm()
	req.CommitIndex = pb.GetCommitIndex()
	req.LeaderName = pb.GetLeaderName()
	req.Entries = pb.GetEntries()

	return len(data), nil
}

func newAppendEntriesResponse(term uint64, success bool, index uint64, commitIndex uint64) *AppendEntriesResponse {
	pb := &RPCs.AppendEntriesResponse{
		Term:        proto.Uint64(term),
		Index:       proto.Uint64(index),
		Success:     proto.Bool(success),
		CommitIndex: proto.Uint64(commitIndex),
	}

	return &AppendEntriesResponse{
		pb : pb,
	}
}

func (res *AppendEntriesResponse) Index() uint64 {
	return res.pb.GetIndex()
}
func (res *AppendEntriesResponse) CommitIndex() uint64 {
	return res.pb.GetCommitIndex()
}
func (res *AppendEntriesResponse) Term() uint64 {
	return res.pb.GetTerm()
}
func (res *AppendEntriesResponse) Success() bool {
	return res.pb.GetSuccess()
}

func (res *AppendEntriesResponse) Encode(w io.Writer) (int,error){
	b, err := proto.Marshal(res.pb)
	if err != nil {
		return -1,err
	}

	return w.Write(b)
}

func ( res *AppendEntriesResponse) Decode(r io.Reader) (int,error){
	data,err := ioutil.ReadAll(r)
	if err != nil {
		return -1, err
	}

	res.pb = new(RPCs.AppendEntriesResponse)
	if err := proto.Unmarshal(data,res.pb); err != nil {
		return -1, err
	}

	return len(data), nil
}



