package proxy

import (
	"bufio"
	"container/list"
	"net"
	"time"

	"github.com/collinmsn/resp"
	"github.com/fatih/pool"
	log "github.com/ngaut/logging"
)

// TaskRunner assure every request will be responded
type TaskRunner struct {
	in       chan interface{}
	out      chan interface{}
	inflight *list.List
	server   string
	conn     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	connPool *ConnPool
	closed   bool
}

func NewTaskRunner(server string, connPool *ConnPool) (*TaskRunner, error) {
	tr := &TaskRunner{
		in:       make(chan interface{}, 1000),
		out:      make(chan interface{}, 1000),
		inflight: list.New(),
		server:   server,
		connPool: connPool,
	}

	if conn, err := connPool.GetConn(server); err != nil {
		return nil, err
	} else {
		tr.initRWConn(conn)
	}

	go tr.writingLoop()
	go tr.readingLoop()

	return tr, nil
}

func (tr *TaskRunner) readingLoop() {
	for {
		if data, err := resp.ReadData(tr.r); err != nil {
			tr.out <- err
			log.Errorf("exit reading loop, server=%s", tr.server)
			return
		} else {
			tr.out <- data
		}
	}
}

func (tr *TaskRunner) writingLoop() {
	var err error
	for {
		if tr.closed && tr.inflight.Len() == 0 {
			tr.conn.(*pool.PoolConn).MarkUnusable()
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
		case rsp := <-tr.out:
			err = tr.handleResp(rsp)
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
	if tr.inflight.Len() == 0 {
		// this would occur when reader returned from blocking reading
		log.Warningf("no inflight request to response, rsp=%s", rsp)
		if err, ok := rsp.(error); ok {
			return err
		} else {
			return nil
		}
	}

	plReq := tr.inflight.Remove(tr.inflight.Front()).(*PipelineRequest)
	plRsp := &PipelineResponse{
		ctx: plReq,
	}
	var err error
	switch rsp.(type) {
	case *resp.Data:
		plRsp.rsp = rsp.(*resp.Data)
	case error:
		err = rsp.(error)
		plRsp.err = err
	}
	plReq.backQ <- plRsp
	plReq.wg.Done()
	return err
}

func (tr *TaskRunner) tryRecover(err error) error {
	log.Warning("try recover from ", err)
	tr.cleanupInflight(err)

	//try to recover
	if conn, err := tr.connPool.GetConn(tr.server); err != nil {
		log.Errorf("try to recover failed, server=%s,err=%s", tr.server, err)
		time.Sleep(1 * time.Second)
		return err
	} else {
		tr.conn.(*pool.PoolConn).MarkUnusable()
		tr.conn.Close()
		tr.initRWConn(conn)
		go tr.readingLoop()
	}

	return nil
}

func (tr *TaskRunner) cleanupInflight(err error) {
	for e := tr.inflight.Front(); e != nil; {
		plReq := e.Value.(*PipelineRequest)
		log.Error("clean up", plReq)
		plRsp := &PipelineResponse{
			ctx: plReq,
			err: err,
		}
		plReq.backQ <- plRsp
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
	tr.inflight.PushBack(plReq)
	buf := plReq.cmd.Format()
	if _, err := tr.w.Write(buf); err != nil {
		log.Error(err)
		return err
	} else {
		if len(tr.in) == 0 {
			tr.w.Flush()
		}
		return nil
	}
}

func (tr *TaskRunner) initRWConn(conn net.Conn) {
	tr.conn = conn
	tr.r = bufio.NewReader(tr.conn)
	tr.w = bufio.NewWriter(tr.conn)
}

func (tr *TaskRunner) Exit() {
	tr.in <- struct{}{}
}
