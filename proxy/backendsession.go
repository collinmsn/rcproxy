package proxy

import (
	"bufio"
	"container/list"
	"net"

	"github.com/collinmsn/resp"
	"github.com/fatih/pool"
	log "github.com/ngaut/logging"
)

type BackendSession struct {
	conn         net.Conn
	requestQueue <-chan *PipelineRequest
	notifyExit   chan<- struct{}
	inflight     *list.List
	out          chan *resp.Object
}

func NewBackendSession(conn net.Conn, requestQueue <-chan *PipelineRequest, notifyExit chan<- struct{}) *BackendSession {
	s := &BackendSession{
		conn:         conn,
		requestQueue: requestQueue,
		notifyExit:   notifyExit,
		inflight:     list.New(),
		out:          make(chan *resp.Object, 1000),
	}
	return s
}

func (s *BackendSession) Start() {
	go s.readingLoop()
	go s.writingLoop()
}

func (s *BackendSession) readingLoop() {
	reader := bufio.NewReader(s.conn)
	for {
		obj := resp.NewObject()
		if err := resp.ReadDataBytes(reader, obj); err != nil {
			log.Error(err)
			close(s.out)
			return
		} else {
			s.out <- obj
		}
	}
}

func (s *BackendSession) writingLoop() {
	var err error
	defer func() {
		s.conn.(*pool.PoolConn).MarkUnusable()
		s.conn.Close()
		messageForInflight := "pending request is cleared"
		if err != nil {
			messageForInflight = err.Error()
		}
		obj := resp.NewObjectFromData(&resp.Data{
			T:      resp.T_Error,
			String: []byte(messageForInflight),
		})
		for e := s.inflight.Front(); e != nil; e = e.Next() {
			plReq := e.Value.(*PipelineRequest)
			plRsp := &PipelineResponse{
				req: plReq,
				obj: obj,
			}
			plReq.backQ <- plRsp
		}
		s.notifyExit <- struct{}{}
	}()
	for {
		select {
		case req, ok := <-s.requestQueue:
			if !ok {
				log.Info("closed by backend")
				return
			}
			if err := s.handleReq(req); err != nil {
				return
			}
		case rsp, ok := <-s.out:
			if !ok {
				log.Info("exit triggered by reading loop")
				return
			}
			s.handleRsp(rsp)
		}
	}
}

func (s *BackendSession) handleReq(plReq *PipelineRequest) (err error) {
	// always put req into inflight list first
	s.inflight.PushBack(plReq)

	buf := plReq.cmd.Format()
	if _, err = s.conn.Write(buf); err != nil {
		log.Error(err)
	}
	return
}

func (s *BackendSession) handleRsp(obj *resp.Object) {
	if s.inflight.Len() == 0 {
		panic("should never happer")
	}

	plReq := s.inflight.Remove(s.inflight.Front()).(*PipelineRequest)
	plRsp := &PipelineResponse{
		req: plReq,
		obj: obj,
	}
	plReq.backQ <- plRsp
}
