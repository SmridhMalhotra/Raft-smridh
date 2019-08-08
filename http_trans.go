package main

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	path2 "path"
	"time"
)

type HTTPtransporter struct {
	DisableKeepAlives bool
	prefix string
	appendEntriesPath string
	requestVotePath string
	httpClient http.Client
	Transport *http.Transport
}



type HTTPMuxer interface {
	HandleFunc(string, func(http.ResponseWriter,*http.Request))
}

func NewHTTPTransporter(prefix string, timeout time.Duration) *HTTPtransporter{
	t := &HTTPtransporter{
		DisableKeepAlives: false,
		prefix:            prefix,
		appendEntriesPath: joinPath(prefix, "/appendEntries"),
		requestVotePath:   joinPath(prefix, "/requestVote"),
		Transport:         &http.Transport{DisableKeepAlives: false},
	}
	t.httpClient.Transport =t.Transport
	t.Transport.ResponseHeaderTimeout = timeout
	t.Transport.MaxIdleConns = 500
	return  t
}

func joinPath(connectionString string, pathSuffix string) string{
	u,err := url.Parse(connectionString)
	if err !=nil{
		panic(err)
	}
	u.Path = path2.Join(u.Path,pathSuffix)
	return u.String()

}

func(t *HTTPtransporter) Prefix() string {
	return t.prefix
}

func(t *HTTPtransporter) AppendEntriesPath() string {
	return t.appendEntriesPath
}

func(t *HTTPtransporter) RequestVotePath() string {
	return t.requestVotePath
}

func (t *HTTPtransporter) Init(server Server,mux HTTPMuxer){
	mux.HandleFunc(t.appendEntriesPath,t.appendEntriesHandler(server))
	mux.HandleFunc(t.requestVotePath,t.requestVoteHandler(server))

}

func (t *HTTPtransporter) SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
	var b bytes.Buffer
	if _,err := req.Encode(&b);err !=nil{
		traceln("transporter.rv.encoding.error")
		return  nil
	}
	url := joinPath(peer.ConnectionString,t.RequestVotePath())
	traceln(server.Name(),"POST",url)
	httpResp,err := t.httpClient.Post(url,"application/protobuf",&b)
	if httpResp == nil || err !=nil{
		traceln("transporter.rv.response.error:", err)
		return nil
	}
	defer httpResp.Body.Close()
	resp := &RequestVoteResponse{}
	if _,err := resp.Decode(httpResp.Body);err!=nil&&err!=io.EOF{
		traceln("transporter.rv.decoding.error:", err)
		return nil
	}
	return resp
}

func (t *HTTPtransporter) SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
	var b bytes.Buffer
	debugln("before transporting" ,req.PrevLogIndex)
	if _,err := req.Encode(&b);err !=nil{
		traceln("transporter.ae.encoding.error")
		return  nil
	}
	url := joinPath(peer.ConnectionString,t.AppendEntriesPath())
	traceln(server.Name(),"POST",url)
	httpResp,err := t.httpClient.Post(url,"application/protobuf",&b)
	if httpResp == nil || err !=nil{
		traceln("transporter.ae.response.error:", err)
		return nil
	}
	defer httpResp.Body.Close()
	resp := &AppendEntriesResponse{}
	if _,err := resp.Decode(httpResp.Body);err!=nil{
		traceln("transporter.ae.decoding.error:", err)
		return nil
	}
	return resp
}

func (t *HTTPtransporter) appendEntriesHandler(server Server) http.HandlerFunc{
	return func(writer http.ResponseWriter, request *http.Request) {
		 traceln(server.Name(),"RECV /appendEntries")
		 req := &AppendEntriesRequest{}
		 if _,err := req.Decode(request.Body);err !=nil{
			http.Error(writer,"",http.StatusBadRequest)
			 return
		 }
		debugln("after transporting" ,req.PrevLogIndex)
		 resp := server.AppendEntries(req)
		 if resp == nil {
			http.Error(writer,"No response came",http.StatusInternalServerError)
			 return
		 }
		if _, err := resp.Encode(writer); err != nil {
			http.Error(writer, "", http.StatusInternalServerError)
			return
		}
	}
}

func (t *HTTPtransporter) requestVoteHandler(server Server) http.HandlerFunc{
	return func(writer http.ResponseWriter, request *http.Request) {
		traceln(server.Name(),"RECV /requestVote")
		req := &RequestVoteRequest{}
		if _,err := req.Decode(request.Body);err !=nil{
			http.Error(writer,"",http.StatusBadRequest)
			return
		}
		resp := server.RequestVote(req)
		if resp == nil {
			http.Error(writer,"No response came",http.StatusInternalServerError)
			return
		}
		if _, err := resp.Encode(writer); err != nil {
			http.Error(writer, "", http.StatusInternalServerError)
			return
		}
	}
}



