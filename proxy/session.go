package proxy

import (
	"bufio"
	"bytes"
	"container/heap"
	"net"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/fatih/pool.v2"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
)

var (
	MOVED         = []byte("-MOVED")
	ASK           = []byte("-ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
	BLACK_CMD_ERR = []byte("unsupported command")
)

type RespReadWriter interface {
	ReadCommand() (*resp.Command, error)
	WriteObject(*resp.Object) error
	Close() error
	RemoteAddr() net.Addr
}

/**
* SessionReadWriter负责client端读写,
* 写出没有使用buffer io
 */
type SessionReadWriter struct {
	net.Conn
	reader *bufio.Reader
}

func NewSessionReadWriter(conn net.Conn) *SessionReadWriter {
	return &SessionReadWriter{
		Conn: conn,
		// TODO: tune buffer size
		reader: bufio.NewReader(conn),
	}
}

func (s *SessionReadWriter) ReadCommand() (cmd *resp.Command, err error) {
	if cmd, err = resp.ReadCommand(s.reader); err != nil {
		log.Error("read from client", err, s.RemoteAddr())
	}
	return
}

func (s *SessionReadWriter) WriteObject(obj *resp.Object) (err error) {
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
	io         RespReadWriter
	reqSeq     int64
	ackSeq     int64
	rspCh      chan *PipelineResponse
	closeWg    *WaitGroupWrapper
	reqWg      *sync.WaitGroup
	rspHeap    *PipelineResponseHeap
	connPool   *ConnPool
	dispatcher Dispatcher
}

func NewSession(io RespReadWriter, connPool *ConnPool, dispatcher Dispatcher) *Session {
	session := &Session{
		io:         io,
		rspCh:      make(chan *PipelineResponse, 1000),
		closeWg:    &WaitGroupWrapper{},
		reqWg:      &sync.WaitGroup{},
		connPool:   connPool,
		dispatcher: dispatcher,
		rspHeap:    &PipelineResponseHeap{},
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
		// ack all pending requests
		for {
			if rsp, ok := <-s.rspCh; ok {
				rsp.req.wg.Done()
			} else {
				break
			}
		}
		log.Info("exit writing loop", s.io.RemoteAddr())
	}()
	for {
		select {
		case rsp, ok := <-s.rspCh:
			if !ok {
				return
			}

			if err := s.handleRespPipeline(rsp); err != nil {
				return
			}
		}
	}
}

func (s *Session) ReadingLoop() {
	defer func() {
		// it's safe to close rspCh only after all requests have done
		s.reqWg.Wait()
		close(s.rspCh)
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
			plReq := &PipelineRequest{
				seq: s.advanceReqSeq(),
				wg:  s.reqWg,
			}
			s.reqWg.Add(1)
			rsp := &resp.Data{T: resp.T_Error, String: BLACK_CMD_ERR}
			plRsp := &PipelineResponse{
				obj: resp.NewObjectFromData(rsp),
				req: plReq,
			}
			s.rspCh <- plRsp
			continue
		}

		// check if is multi key cmd
		if yes, numKeys := IsMultiCmd(cmd); yes && numKeys > 1 {
			s.handleMultiKeyCmd(cmd, numKeys)
		} else {
			s.handleGenericCmd(cmd, cmdFlag)
		}
	}

}

// 将resp写出去。如果是multi key command，只有在全部完成后才汇总输出
func (s *Session) handleRespMulti(plRsp *PipelineResponse) error {
	var obj *resp.Object
	if parentCmd := plRsp.req.parentCmd; parentCmd != nil {
		// sub request
		parentCmd.OnSubCmdFinished(plRsp)
		if !parentCmd.Finished() {
			return nil
		}
		s.advanceAckSeq()
		obj = parentCmd.CoalesceRsp().obj
	} else {
		obj = plRsp.obj
	}
	return s.io.WriteObject(obj)
}

// redirect send request to backend again to new server told by redis cluster
func (s *Session) redirect(server string, plRsp *PipelineResponse, ask bool) {
	var conn net.Conn
	var err error

	plRsp.err = nil
	conn, err = s.connPool.GetConn(server)
	if err != nil {
		log.Error(err)
		plRsp.err = err
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
			plRsp.err = err
			return
		}
	}
	if _, err = conn.Write(plRsp.req.cmd.Format()); err != nil {
		plRsp.err = err
		return
	}
	if ask {
		if _, err = resp.ReadData(reader); err != nil {
			plRsp.err = err
			return
		}
	}
	obj := resp.NewObject()
	if err = resp.ReadDataBytes(reader, obj); err != nil {
		plRsp.err = err
	} else {
		plRsp.obj = obj
	}
}

// handleResp handles MOVED and ASK redirection and call write response
func (s *Session) handleRespRedirect(plRsp *PipelineResponse) error {
	plRsp.req.wg.Done()
	if plRsp.req.parentCmd == nil {
		s.advanceAckSeq()
	}

	if plRsp.err != nil {
		s.dispatcher.TriggerReloadSlots()
		rsp := &resp.Data{T: resp.T_Error, String: []byte(plRsp.err.Error())}
		plRsp.obj = resp.NewObjectFromData(rsp)
	} else {
		raw := plRsp.obj.Raw()
		if raw[0] == resp.T_Error {
			if bytes.HasPrefix(raw, MOVED) {
				_, server := ParseRedirectInfo(string(raw))
				s.dispatcher.TriggerReloadSlots()
				s.redirect(server, plRsp, false)
			} else if bytes.HasPrefix(raw, ASK) {
				_, server := ParseRedirectInfo(string(raw))
				s.redirect(server, plRsp, true)
			}
		}
	}

	// TODO: 不合理
	if plRsp.err != nil {
		return plRsp.err
	}

	if err := s.handleRespMulti(plRsp); err != nil {
		return err
	}

	return nil
}

// handleRespPipeline handles the response if its sequence number is equal to session's
// response sequence number, otherwise, put it to a heap to keep the response order is same
// to request order
func (s *Session) handleRespPipeline(plRsp *PipelineResponse) error {
	if plRsp.req.seq != s.ackSeq {
		heap.Push(s.rspHeap, plRsp)
		if gap := plRsp.req.seq - s.ackSeq; gap%50 == 0 {
			log.Warningf("resp pipeline gap:%d rsp_seq:%d ack_seq:%d client:%s", gap, plRsp.req.seq, s.ackSeq,
				s.io.RemoteAddr())
		}
		return nil
	}

	if err := s.handleRespRedirect(plRsp); err != nil {
		return err
	}
	// continue to check the heap
	for {
		if rsp := s.rspHeap.Top(); rsp == nil || rsp.req.seq != s.ackSeq {
			return nil
		}
		rsp := heap.Pop(s.rspHeap).(*PipelineResponse)
		if err := s.handleRespRedirect(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) handleMultiKeyCmd(cmd *resp.Command, numKeys int) {
	var subCmd *resp.Command
	var err error
	mc := NewMultiRequest(cmd, numKeys)
	// multi sub cmd share the same seq number, 在处理reorder时有利于提高效率
	seq := s.advanceReqSeq()
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
		plReq := &PipelineRequest{
			cmd:       subCmd,
			readOnly:  readOnly,
			slot:      slot,
			seq:       seq,
			subSeq:    i,
			backQ:     s.rspCh,
			parentCmd: mc,
			wg:        s.reqWg,
		}
		s.reqWg.Add(1)
		s.dispatcher.Schedule(plReq)
	}
}

func (s *Session) handleGenericCmd(cmd *resp.Command, cmdFlag int) {
	key := cmd.Value(1)
	slot := Key2Slot(key)
	plReq := &PipelineRequest{
		cmd:      cmd,
		readOnly: cmdFlag&CMD_FLAG_READONLY != 0,
		slot:     slot,
		seq:      s.advanceReqSeq(),
		backQ:    s.rspCh,
		wg:       s.reqWg,
	}

	s.reqWg.Add(1)
	s.dispatcher.Schedule(plReq)
}

func (s *Session) advanceReqSeq() (seq int64) {
	seq = s.reqSeq
	s.reqSeq++
	return
}

func (s *Session) advanceAckSeq() (seq int64) {
	seq = s.ackSeq
	s.ackSeq++
	return seq
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
