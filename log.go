package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"raft-smridh/RPCs"
	"sync"
)

type Log struct {
	ApplyFunc  func(*LogEntry, Command) (interface{}, error)
	file *os.File
	path string
	entries []*LogEntry
	commitIndex uint64
	mutex sync.RWMutex
	startIndex uint64
	startTerm uint64
	initialized bool

}

func newLog() *Log{
	return &Log{
		entries: make([]*LogEntry, 0),
	}
}

func (l *Log) CommitIndex() uint64{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.commitIndex
}

func (l *Log) CurrentIndex() uint64{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.internalCurrentIndex()
}

func (l *Log) internalCurrentIndex() uint64{
	if len(l.entries)==0{
		return l.startIndex
	}
	return l.entries[len(l.entries)-1].Index()
}

func (l *Log) nextIndex() uint64{

	return l.CurrentIndex() + 1
}

func (l *Log) isEmpty() bool{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return len(l.entries) == 0 && l.startIndex == 0
}

func (l *Log) lastCommandName() string{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if len(l.entries) > 0{
		if entry := l.entries[len(l.entries)-1]; entry !=nil{
			return entry.CommandName()
		}
	}
	return ""
}

func (l *Log) currentTerm() uint64{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if len(l.entries)==0{
		return l.startTerm
	}
	return l.entries[len(l.entries)-1].Term()

}

func (l *Log) open(path string) error{
	var ReadBytes int64
	var err error
	l.file, err = os.OpenFile(path,os.O_RDWR,0600)
	l.path = path
	if err != nil{
		if os.IsNotExist(err){
			l.file,err = os.OpenFile(path,os.O_WRONLY|os.O_CREATE,0600)
			if err == nil {
				l.initialized = true;
			}
			return err
		}
		return err
	}

	for {
		entry, _ := newLogEntry(l, nil, 0, 0, nil)
		entry.Position, _ = l.file.Seek(0, io.SeekCurrent)

		n, err := entry.Decode(l.file)

		if err != nil {
			if err == io.EOF {
				debugln("openfile.append.log: finished")
			} else {
				if err = os.Truncate(path, ReadBytes); err != nil{
					return fmt.Errorf("raft.Log: Unable to recover: %v", err)
				}

			}
			break
		}
		if entry.Index()>l.startIndex{
			l.entries = append(l.entries,entry)
			if entry.Index() <= l.commitIndex{
				command,err := newCommand(entry.CommandName(),entry.Command())
				if err != nil {
					continue
				}
				l.ApplyFunc(entry,command)

			}
			debugln("open.log.append log index ", entry.Index())
		}
		ReadBytes += int64(n)
	}
	debugln("open.log.recovery number of log ", len(l.entries))
	l.initialized = true
	return nil
}

func (l *Log) close() {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if l.file !=nil{
		l.file.Close()
		l.file = nil
	}
	l.entries = make([]*LogEntry,0)

}

func (l *Log) sync() error{
	return l.file.Sync()
}

func (l *Log) createEntry(term uint64,command Command,e *ev) (*LogEntry,error){
		return newLogEntry(l,e,l.nextIndex(),term,command)
}

func (l *Log) getEntry(index uint64) *LogEntry{
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if index <= l.startIndex || index > l.startIndex + uint64(len(l.entries)){
		return nil
	}
	return l.entries[index - l.startIndex  -1]
}

func (l *Log) containsEntry(index uint64,term uint64) bool{
	entry := l.getEntry(index)
	return entry != nil && entry.Term() == term
}

func (l *Log) getEntriesAfter(index uint64,max uint64) ([]*LogEntry,uint64){
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if index < l.startIndex{
		return nil,0
	}
	if index > l.startIndex + uint64(len(l.entries)){
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}

	if index == l.startIndex {
		traceln("log.entriesAfter.beginning: ", index, " ", l.startIndex)
		return l.entries, l.startTerm
	}

	entries := l.entries[index-l.startIndex :]
	length := len(entries)

	traceln("log.entriesAfter: startIndex:", l.startIndex, " length", len(l.entries))

	if	uint64(length) < max {
		return entries, l.entries[index-l.startIndex-1].Term()
	}else{
		return entries[: max], l.entries[index-l.startIndex-1].Term()
	}

}

func (l *Log)commitInfo() (index uint64,term uint64){
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if l.commitIndex == 0 {
		return 0,0
	}
	
	if l.commitIndex == l.startIndex{
		return l.startIndex, l.startTerm
	}

	debugln("commitInfo.get.[", l.commitIndex, "/", l.startIndex, "]")

	entry := l.entries[l.commitIndex - l.startIndex - 1]
	return entry.Index(),entry.Term()

}

func (l *Log)lastInfo() (index uint64,term uint64){
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.entries) ==0 {
		return l.startIndex, l.startTerm
	}
	entry := l.entries[len(l.entries)-1]
	return entry.Index(),entry.Term()

}

func (l *Log) updateCommitIndex(index uint64){
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if(index > l.commitIndex){
		l.commitIndex = index
	}
}

func(l *Log)setCommitIndex(index uint64) error{
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if index > l.startIndex + uint64(len(l.entries)){
		index = l.startIndex + uint64(len(l.entries))
	}

	if index < l.commitIndex{
		return nil
	}
	for i:= l.commitIndex + 1 ; i <= index;i++{
		entry := l.entries[i - l.startIndex - 1]

		l.commitIndex = entry.Index()
		command,err := newCommand(entry.CommandName(),entry.Command())
		if err != nil{
			return err
		}
		returnValue, err := l.ApplyFunc(entry,command)

		if entry.event !=nil{
			entry.event.returnValue = returnValue
			entry.event.c  <- err
		}

		// _, isJoinCommand := command.(JoinCommand)
		_, isJoinCommand := command.(JoinCommand)

		// we can only commit up to the most recent join command
		// if there is a join in this batch of commands.
		// after this commit, we need to recalculate the majority.
		if isJoinCommand {
			return nil
		}





	}
	return nil
}

func (l *Log) flushCommitIndex(){
	l.file.Seek(0, io.SeekStart)
	fmt.Fprintf(l.file, "%8x\n", l.commitIndex)
	l.file.Seek(0, io.SeekEnd)
}

func (l *Log) appendEntries(entries []*RPCs.LogEntry) error{
	startPosition, _ := l.file.Seek(0,io.SeekCurrent)
	w := bufio.NewWriter(l.file)
	var size int64
	var err error
	for i:= range entries{
		logEntry := &LogEntry{
			log:      l,
			Position: startPosition,
			pb:       entries[i],
		}
		if size,err = l.writeEntry(logEntry,w); err != nil{
			return err
		}
		startPosition += size
	}
	w.Flush()
	err = l.sync()
	if err != nil {
		panic(err)
	}

	return nil

}

func (l *Log) appendEntry(entry *LogEntry) error{
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file == nil {
		return errors.New("raft.Log: Log is not open")
	}
	if(len(l.entries)>0){
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term() < lastEntry.Term(){
			return fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}

	}
	position,_ := l.file.Seek(0,io.SeekCurrent)
	entry.Position = position
	if _, err := entry.Encode(l.file); err != nil {
		return err
	}


	l.entries = append(l.entries, entry)

	return nil

}

func (l *Log) writeEntry(entry *LogEntry,w io.Writer) (int64,error){
	if l.file == nil {
		return -1, errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term() < lastEntry.Term() {
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		} else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() {
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}
	}

	// Write to storage.
	size, err := entry.Encode(w)
	if err != nil {
		return -1, err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return int64(size), nil

}

func (l* Log) truncate(index uint64,term uint64) error{
	l.mutex.Lock()
	defer l.mutex.Unlock()
	debugln("log.truncate: ", index)
	if index < l.commitIndex{
		debugln("log.truncate.before")
		return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)
	}

	if index > l.startIndex + uint64(len(l.entries)){
		debugln("log.truncate.after")
		return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	if index == l.startIndex{
		l.file.Truncate(0)
		l.file.Seek(0,io.SeekCurrent)

		for _,entry :=range l.entries {
			if entry.event != nil {
				entry.event.c <- errors.New("command failed to be committed due to node failure")
			}
		}

		l.entries = []*LogEntry{}

	}else{
		entry := l.entries[index - l.startIndex-1]
		if len(l.entries) > 0 && entry.Term() != term{
			debugln("log.truncate.termMismatch")
			return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term(), index, term)
		}
		if index < l.startIndex + uint64(len(l.entries)){
			position := l.entries[index-l.startIndex-1].Position
			l.file.Truncate(position)
			l.file.Seek(position,io.SeekCurrent)
			for i := index - l.startIndex; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				if entry.event != nil {
					entry.event.c <- errors.New("command failed to be committed due to node failure")
				}
			}

			l.entries = l.entries[0 : index-l.startIndex]

		}
	}
	return nil
}








