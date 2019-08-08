package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"io"
	"raft-smridh/RPCs"
)

type LogEntry struct {
	pb       *RPCs.LogEntry
	Position int64
	log      *Log
	event    *ev
}

func newLogEntry(log *Log, event *ev, index uint64,term uint64,command Command)(*LogEntry,error){
	var buf bytes.Buffer
	var commandName string
	if command != nil {
		commandName = command.CommandName()
		if encoder,ok := command.(CommandEncoder);ok{
			if err := encoder.Encode(&buf); err != nil {
				return nil,err
			}
		}else{
			if err := json.NewEncoder(&buf).Encode(command); err !=nil{
				return nil,err
			}
		}
	}
	pb := &RPCs.LogEntry{
		Index: proto.Uint64(index),
		Term: proto.Uint64(term),
		CommandName: proto.String(commandName),
		Command: buf.Bytes(),
	}

	e := &LogEntry{
		pb:    pb,
		log:   log,
		event: event,
	}
	return e,nil

}
func (e *LogEntry) Index() uint64 {
	return e.pb.GetIndex()
}

func (e *LogEntry) Term() uint64 {
	return e.pb.GetTerm()
}

func (e *LogEntry) CommandName() string {
	return e.pb.GetCommandName()
}

func (e *LogEntry) Command() []byte {
	return e.pb.GetCommand()
}

func (e *LogEntry) Encode(w io.Writer) (int, error) {
	b, err := proto.Marshal(e.pb)
	if err != nil {
		return -1, err
	}

	if _, err = fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return -1, err
	}

	return w.Write(b)
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
func (e *LogEntry) Decode(r io.Reader) (int, error) {

	var length int
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(r, data)

	if err != nil {
		return -1, err
	}

	if err = proto.Unmarshal(data, e.pb); err != nil {
		return -1, err
	}

	return length + 8 + 1, nil
}
