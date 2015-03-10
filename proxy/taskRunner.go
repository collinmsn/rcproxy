package proxy

import (
	"container/list"
	log "github.com/Sirupsen/logrus"
	"github.com/walu/resp"
	"net"
	"time"
)

// TaskRunner assure every request will be responded
type TaskRunner struct {
	in       chan interface{}
	out      chan interface{}
	inflight *list.List
	server   string
	conn     net.Conn
	connPool *ConnPool
	closed   bool
}

func NewTaskRunner(server string, connPool *ConnPool) (*TaskRunner, error) {
	tr := &TaskRunner{
		in:       make(chan interface{}, 10000),
		out:      make(chan interface{}, 10000),
		server:   server,
		connPool: connPool,
		inflight: list.New(),
	}

	if conn, err := connPool.GetConn(server); err != nil {
		return nil, err
	} else {
		tr.conn = conn
	}

	go tr.writingLoop()
	go tr.readingLoop()

	return tr, nil
}

func (tr *TaskRunner) readingLoop() {
	log.Debugf("begin reading loop, server=%s", tr.server)
	for {
		if data, err := resp.ReadData(tr.conn); err != nil {
			tr.out <- err
			log.Errorf("exit reading loop, server=%s", tr.server)
			return
		} else {
			tr.out <- data
		}
	}
}

func (tr *TaskRunner) writingLoop() {
	log.Debugf("begin writing loop, server=%s", tr.server)
	var err error
	for {
		if tr.closed && tr.inflight.Len() == 0 {
			tr.conn.Close()
			log.Errorf("exit writing loop, server=%s", tr.server)
			return
		}
		if err != nil {
			err = tr.tryRecover(err)
			if err != nil {
				continue
			}
		}
		select {
		case req := <-tr.in:
			err = tr.handleReq(req)
		case resp := <-tr.out:
			err = tr.handleResp(resp)
		}
	}
}

func (tr *TaskRunner) handleReq(req interface{}) error {
	var err error
	switch req.(type) {
	case *PipelineRequest:
		plReq := req.(*PipelineRequest)
		err = tr.writeToBackend(plReq)
		if err != nil {
			log.Errorf("write to backend failed, server=%s,err=%s", tr.server, err)
		}
	case struct{}:
		log.Infof("close task runner, server=%s", tr.server)
		tr.closed = true
	}
	return err
}

func (tr *TaskRunner) handleResp(rsp interface{}) error {
	plReq := tr.inflight.Remove(tr.inflight.Front()).(*PipelineRequest)
	plResp := &PipelineResponse{
		ctx: plReq,
	}
	var err error
	switch rsp.(type) {
	case *resp.Data:
		plResp.rsp = rsp.(*resp.Data)
	case error:
		err = rsp.(error)
		plResp.err = err
	}
	plReq.backQ <- plResp
	return err
}

func (tr *TaskRunner) tryRecover(err error) error {
	log.Warn("try recover from ", err)
	tr.cleanupInflight(err)

	//try to recover
	if conn, err := tr.connPool.GetConn(tr.server); err != nil {
		log.Errorf("try to recover failed, server=%s,err=%s", tr.server, err)
		time.Sleep(1 * time.Second)
		return err
	} else {
		tr.conn.Close()
		tr.conn = conn
		go tr.readingLoop()
	}

	return nil
}

func (tr *TaskRunner) cleanupInflight(err error) {
	for e := tr.inflight.Front(); e != nil; {
		plReq := e.Value.(*PipelineRequest)
		log.Error("clean up", plReq)
		plResp := &PipelineResponse{
			ctx: plReq,
			err: err,
		}
		plReq.backQ <- plResp
		next := e.Next()
		tr.inflight.Remove(e)
		e = next
	}
}

func (tr *TaskRunner) cleanupReqQueue() {
	for {
		select {
		case req := <-tr.in:
			tr.handleReq(req)
		default:
			return
		}
	}
}

func (tr *TaskRunner) writeToBackend(plReq *PipelineRequest) error {
	log.Debugf("req=%d", plReq.seq)
	tr.inflight.PushBack(plReq)
	if _, err := tr.conn.Write(plReq.cmd.Format()); err != nil {
		log.Error(err)
		return err
	} else {
		return nil
	}
}

func (tr *TaskRunner) Exit() {
	tr.in <- struct{}{}
}
