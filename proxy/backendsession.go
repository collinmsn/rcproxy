package proxy

import (
	"bufio"
	"container/list"
	"errors"
	"net"
	"time"

	"fmt"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
	"gopkg.in/fatih/pool.v2"
)

const (
	REQUEST_QUEUE_SIZE  = 50000
	RESPONSE_QUEUE_SIZE = 20000
)

var (
	ERR_BACKEND_SESSION_OVERFLOW       = errors.New("Backend session overflow")
	ERR_BACKEND_SESSION_EXITING        = errors.New("Backend session exiting")
	ERR_BACKEND_SESSION_READER_EXITING = errors.New("Backend session reader exiting")
)

type BackendSession interface {
	Schedule(req *Request)
	Start()
	Exit()
}

type DefaultBackendSession struct {
	addr          string
	conn          net.Conn
	requestQueue  chan *Request
	responseQueue chan *resp.Object
	connPool      *ConnPool
	r             *bufio.Reader
	w             *bufio.Writer
	inflight      *list.List
	closed        bool
	exitChan      chan struct{}
}

func NewDefaultBackendSession(addr string, connPool *ConnPool) *DefaultBackendSession {
	return &DefaultBackendSession{
		addr:          addr,
		requestQueue:  make(chan *Request, REQUEST_QUEUE_SIZE),
		responseQueue: make(chan *resp.Object, REQUEST_QUEUE_SIZE),
		connPool:      connPool,
		inflight:      list.New(),
		exitChan:      make(chan struct{}),
	}
}

// Schedule adds request to backend-session's request queue
// it will never block even if request queue is overflow
func (s *DefaultBackendSession) Schedule(req *Request) {
	select {
	case s.requestQueue <- req:
		log.Debug("Backend session schedule")
	default:
		req.err = ERR_BACKEND_SESSION_OVERFLOW
		req.Done()
	}
}

func (s *DefaultBackendSession) Start() {
	if conn, err := s.connPool.GetConn(s.addr); err != nil {
		log.Error("failed to connect to backend", s.addr, err)
	} else {
		s.initRWConn(conn)
	}

	go s.writingLoop()
	go s.readingLoop(s.responseQueue)

}

// readingLoop 一直从连接中读取response object,如果出错则退出,通过关闭responseQueue通知writtingLoop
func (s *DefaultBackendSession) readingLoop(responseQueue chan *resp.Object) {
	var err error
	defer func() {
		log.Error("exit reading loop", s.addr, err)
		close(responseQueue)
	}()

	if s.r == nil {
		return
	}
	// reading loop
	for {
		obj := resp.NewObject()
		if err = resp.ReadDataBytes(s.r, obj); err != nil {
			log.Error("Failed to read response object", s.addr, err)
			return
		} else {
			responseQueue <- obj
		}
	}
}

// writtingLoop只有在被要求退出时才会退出
func (s *DefaultBackendSession) writingLoop() {
	var err error
	defer func() {
		// 退出前将inflight和request queue处理完
		s.cleanupInflight(ERR_BACKEND_SESSION_EXITING)
		s.cleanupRequestQueue(ERR_BACKEND_SESSION_EXITING)
		s.conn.Close()
	}()
	for {
		select {
		case <-s.exitChan:
			log.Info("exiting writing loop")
			return
		default:
		}

		if err != nil {
			err = s.tryRecover(err)
			if err != nil {
				continue
			}
		}

		select {
		case req := <-s.requestQueue:
			err = s.handleRequest(req)
		case obj, ok := <-s.responseQueue:
			if ok {
				s.handleResponse(obj)
			} else {
				err = ERR_BACKEND_SESSION_READER_EXITING
			}
		}
	}
}

func (s *DefaultBackendSession) initRWConn(conn net.Conn) {
	if s.conn != nil {
		s.conn.(*pool.PoolConn).MarkUnusable()
		s.conn.Close()
	}
	s.conn = conn
	s.r = bufio.NewReader(s.conn)
	s.w = bufio.NewWriter(s.conn)
}

// tryRecover首先将inflight request都ack一下,然后尝试重连后端,如果失败,则当request queue也进行清理
// 如果重连成功, 重置responseQueue并重新启动reading goroutine
// 因为重连成功时会关掉旧的连接,所以原来的reading loop会自然退出
func (s *DefaultBackendSession) tryRecover(err error) error {
	s.cleanupInflight(err)
	if conn, err := s.connPool.GetConn(s.addr); err != nil {
		s.cleanupRequestQueue(err)
		log.Error(err)
		time.Sleep(100 * time.Millisecond)
		return err
	} else {
		log.Info("recover success", s.addr)
		s.initRWConn(conn)
		s.resetOutChannel()
		go s.readingLoop(s.responseQueue)
	}

	return nil
}

func (s *DefaultBackendSession) handleRequest(req *Request) error {
	return s.writeToBackend(req)
}

func (s *DefaultBackendSession) writeToBackend(req *Request) error {
	var err error
	// always put req into inflight list first
	s.inflight.PushBack(req)

	if s.w == nil {
		err = fmt.Errorf("Faild to connect to: %s", s.addr)
		log.Error(err)
		return err
	}
	buf := req.cmd.Format()
	if _, err = s.w.Write(buf); err != nil {
		log.Error(err)
		return err
	}
	if len(s.requestQueue) == 0 {
		err = s.w.Flush()
		if err != nil {
			log.Error("flush error", err)
		}
	}
	return err
}

func (s *DefaultBackendSession) cleanupInflight(err error) {
	for e := s.inflight.Front(); e != nil; e = e.Next() {
		req := e.Value.(*Request)
		req.err = err
		req.Done()
	}
	s.inflight.Init()
}

func (s *DefaultBackendSession) cleanupRequestQueue(err error) {
	for {
		select {
		case req, ok := <-s.requestQueue:
			if ok {
				req.err = err
				req.Done()
			} else {
				return
			}
		default:
			return
		}
	}
}

func (s *DefaultBackendSession) resetOutChannel() {
	s.responseQueue = make(chan *resp.Object, RESPONSE_QUEUE_SIZE)
}

func (s *DefaultBackendSession) Exit() {
	close(s.exitChan)
}

func (s *DefaultBackendSession) handleResponse(obj *resp.Object) {
	req := s.inflight.Remove(s.inflight.Front()).(*Request)
	req.obj = obj
	req.Done()
}
