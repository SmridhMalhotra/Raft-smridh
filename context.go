package main



type Context interface {
	Server() Server
	CurrentTerm() uint64
	CurrentIndex() uint64
	CommitIndex() uint64
}

type context struct {

	server Server
	currentTerm uint64
	currentIndex uint64
	commitIndex uint64


}

func (c* context) Server() Server {
	return c.server
}

func (c* context) CurrentTerm() uint64 {
	return c.currentTerm
}

func (c* context) CurrentIndex() uint64 {
	return c.currentIndex
}

func (c* context) CommitIndex() uint64 {
	return c.commitIndex
}







