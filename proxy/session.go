package proxy

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"gopkg.in/fatih/pool.v2"

	"fmt"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
)

const (
	CMD_NAME_EVAL    = "EVAL"
	CMD_NAME_EVALSHA = "EVALSHA"
	CMD_EVAL_KEY_POS = 3
)

var (
	MOVED                 = []byte("-MOVED")
	ASK                   = []byte("-ASK")
	ASK_CMD_BYTES         = []byte("ASKING\r\n")
	BLACK_CMD_ERR         = []byte("unsupported command")
	BLACK_CMD_RESP_DATA   = &resp.Data{T: resp.T_Error, String: BLACK_CMD_ERR}
	BLACK_CMD_RESP_OBJECT = resp.NewObjectFromData(BLACK_CMD_RESP_DATA)
)

type ClientReadWriter interface {
	ReadCommand() (*resp.Command, error)
	WriteObject(*resp.Object) error
	Close() error
	RemoteAddr() net.Addr
}

/**
* SessionReadWriter负责client端读写,
* 写出没有使用buffer io
 */
type ClientSessionReadWriter struct {
	net.Conn
	reader *bufio.Reader
}

func NewClientSessionReadWriter(conn net.Conn) *ClientSessionReadWriter {
	return &ClientSessionReadWriter{
		Conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

func (s *ClientSessionReadWriter) ReadCommand() (cmd *resp.Command, err error) {
	if cmd, err = resp.ReadCommand(s.reader); err != nil {
		log.Error("read from client", err, s.RemoteAddr())
	}
	return
}

func (s *ClientSessionReadWriter) WriteObject(obj *resp.Object) (err error) {
	if _, err = s.Write(obj.Raw()); err != nil {
		log.Error("write to client", err, s.RemoteAddr())
	}
	return
}

// Session represents a connection between client and proxy
// connection reading and writing are executed individually in go routines
// since requests are sent to different backends, response may come back unordered
// a heap is used to reorder the pipeline responses
type Session struct {
	io              ClientReadWriter
	pendingRequests chan *Request
	closeWg         *WaitGroupWrapper
	reqWg           *sync.WaitGroup
	connPool        *ConnPool
	dispatcher      Dispatcher
}

func NewSession(io ClientReadWriter, connPool *ConnPool, dispatcher Dispatcher) *Session {
	session := &Session{
		io:              io,
		pendingRequests: make(chan *Request, 5000),
		closeWg:         &WaitGroupWrapper{},
		reqWg:           &sync.WaitGroup{},
		connPool:        connPool,
		dispatcher:      dispatcher,
	}
	return session
}

func (s *Session) Run() {
	s.closeWg.Wrap(s.WritingLoop)
	s.closeWg.Wrap(s.ReadingLoop)
	s.closeWg.Wait()
}

// WritingLoop consumes rspQueue and send response to client
// It close the connection to notify reader on error
// and continue loop until the reader has told it to exit
func (s *Session) WritingLoop() {
	defer func() {
		s.io.Close()
		for {
			if _, ok := <-s.pendingRequests; !ok {
				break
			}
		}
		log.Info("exit writing loop", s.io.RemoteAddr())
	}()
	for {
		select {
		case req, ok := <-s.pendingRequests:
			if !ok {
				return
			}
			req.Wait()

			if err := s.handleRespPipeline(req); err != nil {
				return
			}
		}
	}
}

func (s *Session) ReadingLoop() {
	defer func() {
		// it's safe to close rspCh only after all requests have done
		s.reqWg.Wait()
		close(s.pendingRequests)
		log.Info("exit reading loop", s.io.RemoteAddr())
	}()
	for {
		cmd, err := s.io.ReadCommand()
		if err != nil {
			break
		}

		// convert all command name to upper case
		cmd.Args[0] = strings.ToUpper(cmd.Args[0])

		// check if command is supported
		cmdFlag := CmdFlag(cmd)
		if cmdFlag&CMD_FLAG_BLACK != 0 {
			req := &Request{
				cmd:       cmd,
				obj:       BLACK_CMD_RESP_OBJECT,
				WaitGroup: &sync.WaitGroup{},
			}
			s.pendingRequests <- req
			continue
		}

		// check if is multi key cmd
		if yes, numKeys := IsMultiCmd(cmd); yes && numKeys > 1 {
			s.handleMultiKeyCmd(cmd, numKeys)
		} else {
			if err = s.handleGenericCmd(cmd, cmdFlag); err != nil {
				break
			}
		}
	}

}

// 将resp写出去。如果是multi key command，只有在全部完成后才汇总输出
func (s *Session) handleRespMulti(req *Request) error {
	var obj *resp.Object
	if parentCmd := req.parentCmd; parentCmd != nil {
		// sub request
		parentCmd.OnSubCmdFinished(req)
		if !parentCmd.Finished() {
			return nil
		}
		obj = parentCmd.CoalesceRsp()
	} else {
		obj = req.obj
	}
	if n := atomic.AddUint64(&accessLogCount, 1); n%LogEveryN == 0 {
		log.Info("access", s.io.RemoteAddr(), req.cmd.Args[:2])
	}
	return s.io.WriteObject(obj)
}

// redirect send request to backend again to new server told by redis cluster
func (s *Session) redirect(server string, req *Request, ask bool) {
	var conn net.Conn
	var err error

	req.err = nil
	conn, err = s.connPool.GetConn(server)
	if err != nil {
		log.Error(err)
		req.err = err
		return
	}
	defer func() {
		if err != nil {
			log.Error(err)
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	if ask {
		if _, err = conn.Write(ASK_CMD_BYTES); err != nil {
			req.err = err
			return
		}
	}
	if _, err = conn.Write(req.cmd.Format()); err != nil {
		req.err = err
		return
	}
	if ask {
		if _, err = resp.ReadData(reader); err != nil {
			req.err = err
			return
		}
	}
	obj := resp.NewObject()
	if err = resp.ReadDataBytes(reader, obj); err != nil {
		req.err = err
	} else {
		req.obj = obj
	}
}

// handleResp handles MOVED and ASK redirection and call write response
func (s *Session) handleRespRedirect(req *Request) error {
	if req.err != nil {
		s.dispatcher.TriggerReloadSlots()
		rsp := &resp.Data{T: resp.T_Error, String: []byte(req.err.Error())}
		req.obj = resp.NewObjectFromData(rsp)
	} else {
		raw := req.obj.Raw()
		if raw[0] == resp.T_Error {
			if bytes.HasPrefix(raw, MOVED) {
				_, server := ParseRedirectInfo(string(raw))
				s.dispatcher.TriggerReloadSlots()
				s.redirect(server, req, false)
			} else if bytes.HasPrefix(raw, ASK) {
				_, server := ParseRedirectInfo(string(raw))
				s.redirect(server, req, true)
			}
		}
	}

	if req.err != nil {
		return req.err
	}

	if err := s.handleRespMulti(req); err != nil {
		return err
	}

	return nil
}

// handleRespPipeline handles the response if its sequence number is equal to session's
// response sequence number, otherwise, put it to a heap to keep the response order is same
// to request order
func (s *Session) handleRespPipeline(req *Request) error {
	return s.handleRespRedirect(req)
}

func (s *Session) handleMultiKeyCmd(cmd *resp.Command, numKeys int) {
	var subCmd *resp.Command
	var err error
	mc := NewMultiRequest(cmd, numKeys)
	// multi sub cmd share the same seq number, 在处理reorder时有利于提高效率
	readOnly := mc.CmdType() == MGET
	for i := 0; i < numKeys; i++ {
		switch mc.CmdType() {
		case MGET:
			subCmd, err = resp.NewCommand("GET", cmd.Value(i+1))
		case MSET:
			subCmd, err = resp.NewCommand("SET", cmd.Value(2*i+1), cmd.Value((2*i + 2)))
		case DEL:
			subCmd, err = resp.NewCommand("DEL", cmd.Value(i+1))
		}
		if err != nil {
			panic(err)
		}
		key := subCmd.Value(1)
		slot := Key2Slot(key)
		req := &Request{
			cmd:       subCmd,
			readOnly:  readOnly,
			slot:      slot,
			subSeq:    i,
			parentCmd: mc,
			WaitGroup: &sync.WaitGroup{},
		}
		req.Add(1)
		s.dispatcher.Schedule(req)
		s.pendingRequests <- req
	}
}

func (s *Session) handleGenericCmd(cmd *resp.Command, cmdFlag int) (err error) {
	key := cmd.Value(1)
	if cmd.Args[0] == CMD_NAME_EVAL || cmd.Args[0] == CMD_NAME_EVALSHA {
		if len(cmd.Args) >= CMD_EVAL_KEY_POS+1 {
			key = cmd.Value(CMD_EVAL_KEY_POS)
		} else {
			return fmt.Errorf("not enough parameter for command: %s", cmd.Args[0])
		}
	}
	slot := Key2Slot(key)
	req := &Request{
		cmd:       cmd,
		readOnly:  cmdFlag&CMD_FLAG_READONLY != 0,
		slot:      slot,
		WaitGroup: &sync.WaitGroup{},
	}

	req.Add(1)
	s.dispatcher.Schedule(req)
	s.pendingRequests <- req
	return
}

// ParseRedirectInfo parse slot redirect information from MOVED and ASK Error
func ParseRedirectInfo(msg string) (slot int, server string) {
	var err error
	parts := strings.Fields(msg)
	if len(parts) != 3 {
		log.Fatalf("invalid redirect message: %s", msg)
	}
	slot, err = strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("invalid redirect message: %s", msg)
	}
	server = parts[2]
	return
}
