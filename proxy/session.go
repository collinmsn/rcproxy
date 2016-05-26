package proxy

import (
	"bytes"
	"container/heap"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"gopkg.in/fatih/pool.v2"

	"bufio"

	resp "github.com/collinmsn/stvpresp"
	log "github.com/ngaut/logging"
)

var (
	MOVED         = []byte("-MOVED")
	ASK           = []byte("-ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
	BLACK_CMD_ERR = []byte("unsupported command")
)

type ClientReadWriter interface {
	ReadCommand() (*redis.Resp, error)
	WriteObject(*redis.Resp) error
	Close() error
	RemoteAddr() net.Addr
}

/**
* SessionReadWriter负责client端读写,
* 写出没有使用buffer io
 */
type ClientSessionReadWriter struct {
	net.Conn
	reader *redis.Decoder
	writer *redis.Encoder
}

func NewClientSessionReadWriter(conn net.Conn) *ClientSessionReadWriter {
	return &ClientSessionReadWriter{
		Conn:   conn,
		reader: redis.NewDecoder(bufio.NewReader(conn)),
		writer: redis.NewEncoder(bufio.NewWriter(conn)),
	}
}

func (s *ClientSessionReadWriter) ReadCommand() (cmd *redis.Resp, err error) {
	cmd, err = s.reader.Decode()
	if err != nil {
		log.Error("read from client", err, s.RemoteAddr())
	}
	return
}

func (s *ClientSessionReadWriter) WriteObject(obj *redis.Resp) (err error) {
	if err = s.writer.Encode(obj, true); err != nil {
		log.Error("write to client", err, s.RemoteAddr())
	}
	return
}

// Session represents a connection between client and proxy
// connection reading and writing are executed individually in go routines
// since requests are sent to different backends, response may come back unordered
// a heap is used to reorder the pipeline responses
type Session struct {
	io         ClientReadWriter
	reqSeq     int64
	ackSeq     int64
	rspCh      chan *PipelineResponse
	closeWg    *WaitGroupWrapper
	reqWg      *sync.WaitGroup
	rspHeap    *PipelineResponseHeap
	connPool   *ConnPool
	dispatcher Dispatcher
}

func NewSession(io ClientReadWriter, connPool *ConnPool, dispatcher Dispatcher) *Session {
	session := &Session{
		io:         io,
		rspCh:      make(chan *PipelineResponse, 10000),
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

		op := string(bytes.ToUpper(cmd.Array[0].Value))

		// check if command is supported
		cmdFlag := CmdFlag(op)
		if cmdFlag&CMD_FLAG_BLACK != 0 {
			plReq := &PipelineRequest{
				seq: s.advanceReqSeq(),
				wg:  s.reqWg,
			}
			s.reqWg.Add(1)
			plRsp := &PipelineResponse{
				obj: redis.NewError(BLACK_CMD_ERR),
				req: plReq,
			}
			s.rspCh <- plRsp
			continue
		}

		// check if is multi key cmd
		if yes, numKeys := IsMultiCmd(op, cmd); yes && numKeys > 1 {
			s.handleMultiKeyCmd(cmd, op, numKeys)
		} else {
			s.handleGenericCmd(cmd, cmdFlag)
		}
	}

}

// 将resp写出去。如果是multi key command，只有在全部完成后才汇总输出
func (s *Session) handleRespMulti(plRsp *PipelineResponse) error {
	var obj *redis.Resp
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
	if n := atomic.AddUint64(&accessLogCount, 1); n%LogEveryN == 0 {
		log.Info("access", s.io.RemoteAddr(), plRsp.req.cmd)
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

	writer := redis.NewEncoder(bufio.NewWriter(conn))
	if ask {
		if _, err = writer.Write(ASK_CMD_BYTES); err != nil {
			plRsp.err = err
			return
		}
	}

	if err = writer.Encode(plRsp.req.cmd, true); err != nil {
		plRsp.err = err
		return
	}
	reader := redis.NewDecoder(bufio.NewReader(conn))
	if ask {
		if _, err = reader.Decode(); err != nil {
			plRsp.err = err
			return
		}
	}
	if obj, err := reader.Decode(); err != nil {
		plRsp.obj = redis.NewError([]byte(err.Error()))
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
		plRsp.obj = redis.NewError([]byte(plRsp.err.Error()))
		plRsp.err = nil
	} else if plRsp.obj.IsError() {
		// TODO: check if Value contains Error prefix
		raw := plRsp.obj.Value
		if raw[0] == resp.ERROR_PREFIX {
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

func (s *Session) handleMultiKeyCmd(cmd *redis.Resp, op string, numKeys int) {
	panic("not implemented")
	/*
		var subCmd resp.Command
		var key []byte
		mc := NewMultiRequest(cmd, op, numKeys)
		// multi sub cmd share the same seq number, 在处理reorder时有利于提高效率
		seq := s.advanceReqSeq()
		readOnly := mc.CmdType() == MGET
		for i := 0; i < numKeys; i++ {
			switch mc.CmdType() {
			case MGET:
				key = args[i+1]
				subCmd = resp.NewCommand("GET", string(args[i+1]))
			case MSET:
				key = args[2*i+1]
				subCmd = resp.NewCommand("SET", string(args[2*i+1]), string(args[2*i+2]))
			case DEL:
				key = args[i+1]
				subCmd = resp.NewCommand("DEL", string(args[i+1]))
			}
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
	*/
}

func (s *Session) handleGenericCmd(cmd *redis.Resp, cmdFlag int) {
	// TODO: 参数个数
	slot := Key2Slot(cmd.Array[1].Value)
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
