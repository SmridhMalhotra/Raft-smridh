package main

import (
	"log"
	"os"
)




func main() {


	// Setup commands.


	// Set the data directory.

	path := "C:\\Users\\smrid\\Go\\src\\raft-smridh\\logs\\1"
	host := "127.0.0.1"
	port := 4001
	SetLogLevel(Trace)

	if err := os.MkdirAll(path, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}

	log.SetFlags(log.LstdFlags)

	s := newHTTPServer(path, host, port)

	go func() {

		log.Println(s.ListenAndServer(""))
	}()

	log.Println("executing server 2")
	path1 := "C:\\Users\\smrid\\Go\\src\\raft-smridh\\logs\\2"

	port1 := 4002
	if err := os.MkdirAll(path1, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}
	s1 := newHTTPServer(path1,host,port1)

	go func(){
		log.Println(s1.ListenAndServer(s.connectionString()))
	}()
	log.Println("executing server 3")
	path2 := "C:\\Users\\smrid\\Go\\src\\raft-smridh\\logs\\3"

	port2 := 4003
	if err := os.MkdirAll(path2, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}
	s2 := newHTTPServer(path2,host,port2)
	log.Println(s2.ListenAndServer(s.connectionString()))

	/*log.Println("executing server 3")
	path3 := "C:\\Users\\smrid\\Go\\src\\raft-smridh\\logs\\4"

	port3 := 4004
	if err := os.MkdirAll(path3, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}
	s3 := newHTTPServer(path3,host,port3)
	go func() {
		log.Println(s3.ListenAndServer(s.connectionString()))
	}()

	log.Println("executing server 3")
	path4 := "C:\\Users\\smrid\\Go\\src\\raft-smridh\\logs\\5"

	port4 := 4005
	if err := os.MkdirAll(path4, 0744); err != nil {
		log.Fatalf("Unable to create path: %v", err)
	}
	s4 := newHTTPServer(path4,host,port4)
	log.Println(s4.ListenAndServer(s.connectionString()))*/

}

