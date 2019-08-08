package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

type Command interface {
	CommandName() string
}

var commandType map[string]Command

func init(){
	commandType = map[string]Command{}
}

type CommandApply interface {
	Apply(Context)  (interface{},error)

}

type deprecatedCommandApply interface {
	Apply(Server) (interface{}, error)
}



type CommandEncoder interface {
	Encode(writer io.Writer) error
	Decode(reader io.Reader) error
}

func newCommand(name string, data []byte) (Command, error){
	command := commandType[name]
	if command == nil {
		return nil, fmt.Errorf("raft.Command: Unregistered command type: %s", name)
	}
	v:= reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy,ok := v.(Command);
	if !ok {
		panic(fmt.Sprint("raft: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()));
	}
	if data != nil {
		if encoder, ok :=copy.(CommandEncoder); ok {
			if err := encoder.Decode(bytes.NewReader(data)); err !=nil{
				return nil,err
			}
		}else{
			if err := json.NewDecoder(bytes.NewReader(data)).Decode(copy); err != nil{
				return nil,err
			}
		}
	}
	return copy,nil
}

func RegisterCommand(command Command) {
	if command == nil {
		panic(fmt.Sprintf("raft: Cannot register nil"))
	} else if commandType[command.CommandName()] != nil {
		panic(fmt.Sprintf("raft: Duplicate registration: %s", command.CommandName()))
	}
	commandType[command.CommandName()] = command
}
