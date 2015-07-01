package proxy

import (
	"bufio"
	"container/list"
	"errors"
	"net"
	"time"

	"github.com/collinmsn/resp"
	"github.com/fatih/pool"
	log "github.com/ngaut/logging"
)

var (
	initTaskRunnerConnErr = errors.New("init task runner connection error")
	writeToBackendErr     = errors.New("write to backend error")
	recoverFailedErr      = errors.New("try to recover from error failed")
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

func NewTaskRunner(server string, connPool *ConnPool) *TaskRunner {
	tr := &TaskRunner{
		in:       make(chan interface{}, 5000),
		out:      make(chan interface{}, 5000),
		inflight: list.New(),
		server:   server,
		connPool: connPool,
	}

	if conn, err := connPool.GetConn(server); err != nil {
		log.Error(initTaskRunnerConnErr, tr.server, err)
	} else {
		tr.initRWConn(conn)
	}

	go tr.writingLoop()
	go tr.readingLoop()

	return tr
}

func (tr *TaskRunner) readingLoop() {
	var err error
	defer func() {
		log.Error("exit reading loop", tr.server, err)
	}()
	if tr.r == nil {
		err = initTaskRunnerConnErr
		return
	}
	// reading loop
	for {
		obj := resp.NewObject()
		if err = resp.ReadDataBytes(tr.r, obj); err != nil {
			tr.out <- err
			return
		} else {
			tr.out <- obj
		}
	}
}

func (tr *TaskRunner) writingLoop() {
	var err error
	for {
		if tr.closed && tr.inflight.Len() == 0 {
			if tr.conn != nil {
				tr.conn.(*pool.PoolConn).MarkUnusable()
				tr.conn.Close()
			}
			log.Error("exit writing loop", tr.server, err)
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
			log.Error(writeToBackendErr, tr.server, err)
		}
	case struct{}:
		log.Info("close task runner", tr.server)
		tr.closed = true
	}
	return err
}

func (tr *TaskRunner) handleResp(rsp interface{}) error {
	if tr.inflight.Len() == 0 {
		// this would occur when reader returned from blocking reading
		log.Info("no inflight requests", rsp)
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
	case *resp.Object:
		plRsp.rsp = rsp.(*resp.Object)
	case error:
		err = rsp.(error)
		plRsp.err = err
	}
	plReq.backQ <- plRsp
	return err
}

func (tr *TaskRunner) tryRecover(err error) error {
	tr.cleanupInflight(err)

	//try to recover
	if conn, err := tr.connPool.GetConn(tr.server); err != nil {
		tr.cleanupReqQueue()
		log.Error(recoverFailedErr, tr.server, err)
		time.Sleep(100 * time.Millisecond)
		return err
	} else {
		log.Info("recover success", tr.server)
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
	var err error
	// always put req into inflight list first
	tr.inflight.PushBack(plReq)

	if tr.w == nil {
		err = initTaskRunnerConnErr
		log.Error(err)
		return err
	}
	buf := plReq.cmd.Format()
	if _, err = tr.w.Write(buf); err != nil {
		log.Error(err)
		return err
	}
	if len(tr.in) == 0 {
		err = tr.w.Flush()
		if err != nil {
			log.Error("flush error", err)
		}
	}
	return err
}

func (tr *TaskRunner) initRWConn(conn net.Conn) {
	if tr.conn != nil {
		tr.conn.(*pool.PoolConn).MarkUnusable()
		tr.conn.Close()
	}
	tr.conn = conn
	tr.r = bufio.NewReader(tr.conn)
	tr.w = bufio.NewWriter(tr.conn)
}

func (tr *TaskRunner) Exit() {
	tr.in <- struct{}{}
}
