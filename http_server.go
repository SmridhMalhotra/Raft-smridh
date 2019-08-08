package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

type HTTPServer struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer Server
	httpServer *http.Server
	db         *DB
	mutex      sync.Mutex
}

func (s *HTTPServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

func newHTTPServer(path string,host string, port int) *HTTPServer{
	s := &HTTPServer{
		host:   host,
		path:   path,
		port:   port,
		db:     newDB(),
		router: mux.NewRouter(),
	}
	if b,err := ioutil.ReadFile(filepath.Join(path,"name"));err==nil{
		s.name = string(b)
	}else{
		s.name = fmt.Sprintf("%07x",rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path,"name"),[]byte(s.name),0644);err !=nil{
			panic(err)
		}
	}
	return s

}

func (s *HTTPServer) connectionString() string  {
	return fmt.Sprintf("http://%s:%d",s.host,s.port)
}

func (s *HTTPServer) ListenAndServer(leader string) error{
	var err error
	log.Printf("Initializing Raft server %s",s.path)
	transporter := NewHTTPTransporter("/raft",200*time.Millisecond)

	s.raftServer,err = NewServer(s.name,s.path,transporter,s.db,"")
	if err !=nil{
		log.Fatal(err)
	}
	transporter.Init(s.raftServer,s)
	s.raftServer.Start()

	if leader != "" {
		log.Println("Attempting to join leader:", leader)
		if !s.raftServer.IsLogEmpty(){
			log.Fatal("Cannot join with an existing log")
		}
		if err := s.Join(leader);err!=nil{
			log.Fatal(err)
		}
	}else if s.raftServer.IsLogEmpty() {
		log.Println("Initializing new cluster")
		_,err := s.raftServer.Do(&DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err !=nil{
			log.Fatal(err)
		}
	} else{
		log.Println("recovered from log")
	}
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}
	s.router.HandleFunc("/db/{key}",s.readHandler).Methods("GET")
	s.router.HandleFunc("/db/{key}",s.writeHandler).Methods("POST")
	s.router.HandleFunc("/join",s.joinHandler).Methods("POST")
	log.Println("listening at: ",s.connectionString())
	return s.httpServer.ListenAndServe()
}

func(s *HTTPServer) Join(leader string) error{
	command := &DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}
	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp,err :=http.Post(fmt.Sprintf("%s/join",leader),"application/json",&b)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s *HTTPServer) joinHandler(w http.ResponseWriter,r *http.Request){
	command := &DefaultJoinCommand{}
	if err := json.NewDecoder(r.Body).Decode(command); err !=nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}
	if _,err := s.raftServer.Do(command);err != nil{
		http.Error(w,err.Error(),http.StatusInternalServerError)
		return
	}
}

func (s *HTTPServer) writeHandler(w http.ResponseWriter,r *http.Request){
	vars := mux.Vars(r)
	b,err:= ioutil.ReadAll(r.Body)
	if err !=nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	value := string(b)

	_,err = s.raftServer.Do(newWriteCommand(vars["key"],value))
	if err != nil {
		http.Error(w,err.Error(),http.StatusBadRequest)
	}
}

func (s *HTTPServer) readHandler(w http.ResponseWriter,r *http.Request){
	vars := mux.Vars(r)
	value:= s.db.Get(vars["key"])
	w.Write([]byte(value))
}

