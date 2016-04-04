package proxy

import (
	"bufio"
	"bytes"
	"container/heap"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fatih/pool"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
)

var (
	MOVED         = []byte("-MOVED")
	ASK           = []byte("-ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
	BLACK_CMD_ERR = []byte("unsupported command")
)

// Session represents a connection between client and proxy
// connection reading and writing are executed individually in go routines
// since requests are sent to different backends, response may come back unordered
// a heap is used to reorder the pipeline responses
type Session struct {
	net.Conn
	connClosed int32
	reader     *bufio.Reader
	reqSeq     int64
	rspSeq     int64
	rspCh      chan *PipelineResponse
	closeWg    *WaitGroupWrapper
	reqWg      *sync.WaitGroup
	rspHeap    *PipelineResponseHeap
	connPool   *ConnPool
	dispatcher *Dispatcher
}

func NewSession(conn net.Conn, connPool *ConnPool, dispatcher *Dispatcher) *Session {
	session := &Session{
		Conn:       conn,
		reader:     bufio.NewReader(conn),
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
	s.Close()
}

// WritingLoop consumes rspQueue and send response to client
// It close the connection to notify reader on error
// and continue loop until the reader has told it to exit
func (s *Session) WritingLoop() {
	for {
		select {
		case rsp, ok := <-s.rspCh:
			if !ok {
				log.Info("exit writing loop", s.Conn.RemoteAddr())
				return
			}

			if err := s.handleRespPipeline(rsp); err != nil {
				s.notifyReader()
				continue
			}
		}
	}
}

func (s *Session) ReadingLoop() {
	for {
		cmd, err := resp.ReadCommand(s.reader)
		if err != nil {
			log.Error("read command:", err)
			break
		}
		// convert all command name to upper case
		cmd.Args[0] = strings.ToUpper(cmd.Args[0])

		if n := atomic.AddUint64(&accessLogCount, 1); n%LogEveryN == 0 {
			if len(cmd.Args) > 1 {
				log.Infof("access %s %s %s", s.RemoteAddr(), cmd.Name(), cmd.Args[1])
			} else {
				log.Infof("access %s %s", s.RemoteAddr(), cmd.Name())
			}
		}

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
				rsp: resp.NewObjectFromData(rsp),
				ctx: plReq,
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
	// it's safe to close rspCh only after all requests have done
	s.reqWg.Wait()
	s.notifyWriter()
	log.Info("exit reading loop", s.Conn.RemoteAddr())
}

// Notify reader by closing the underline connection
func (s *Session) notifyReader() {
	s.Close()
}

// Notify writer by closing the response channel
func (s *Session) notifyWriter() {
	close(s.rspCh)
}

// 将resp写出去。如果是multi key command，只有在全部完成后才汇总输出
func (s *Session) writeResp(plRsp *PipelineResponse) error {
	var buf []byte
	if parentCmd := plRsp.ctx.parentCmd; parentCmd != nil {
		// sub request
		parentCmd.OnSubCmdFinished(plRsp)
		if !parentCmd.Finished() {
			return nil
		}
		buf = parentCmd.CoalesceRsp().rsp.Raw()
		s.advanceRspSeq()
	} else {
		buf = plRsp.rsp.Raw()
	}
	// write to client directly with non-buffered io
	if _, err := s.Write(buf); err != nil {
		log.Error(err)
		return err
	}

	return nil
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
	if _, err = conn.Write(plRsp.ctx.cmd.Format()); err != nil {
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
		plRsp.rsp = obj
	}
}

// handleResp handles MOVED and ASK redirection and call write response
func (s *Session) handleResp(plRsp *PipelineResponse) error {
	plRsp.ctx.wg.Done()
	if plRsp.ctx.parentCmd == nil {
		s.advanceRspSeq()
	}

	if plRsp.err != nil {
		s.dispatcher.TriggerReloadSlots()
		rsp := &resp.Data{T: resp.T_Error, String: []byte(plRsp.err.Error())}
		plRsp.rsp = resp.NewObjectFromData(rsp)
	} else {
		raw := plRsp.rsp.Raw()
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

	if plRsp.err != nil {
		return plRsp.err
	}

	if err := s.writeResp(plRsp); err != nil {
		return err
	}

	return nil
}

// handleRespPipeline handles the response if its sequence number is equal to session's
// response sequence number, otherwise, put it to a heap to keep the response order is same
// to request order
func (s *Session) handleRespPipeline(plRsp *PipelineResponse) error {
	if plRsp.ctx.seq != s.rspSeq {
		heap.Push(s.rspHeap, plRsp)
		if (plRsp.ctx.seq-s.rspSeq)%50 == 0 {
			log.Warningf("resp pipeline client:%s rspseq:%d waitforseq:%d", s.Conn.RemoteAddr(), plRsp.ctx.seq, s.rspSeq)
		}
		return nil
	}

	if err := s.handleResp(plRsp); err != nil {
		return err
	}
	// continue to check the heap
	for {
		if rsp := s.rspHeap.Top(); rsp == nil || rsp.ctx.seq != s.rspSeq {
			return nil
		}
		rsp := heap.Pop(s.rspHeap).(*PipelineResponse)
		if err := s.handleResp(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) handleMultiKeyCmd(cmd *resp.Command, numKeys int) {
	var subCmd *resp.Command
	var err error
	mc := NewMultiKeyCmd(cmd, numKeys)
	// multi sub cmd share the same seq number
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

// close underline connection
// it's safe to be called multi times
func (s *Session) Close() {
	if atomic.CompareAndSwapInt32(&s.connClosed, 0, 1) {
		log.Infof("close session %s reqseq:%d rspseq:%d rspheap:%d", s.Conn.RemoteAddr(), s.reqSeq, s.reqSeq, s.rspHeap.Len())
		s.Conn.Close()
	}
}

func (s *Session) Read(p []byte) (int, error) {
	return s.reader.Read(p)
}

func (s *Session) advanceReqSeq() (seq int64) {
	seq = s.reqSeq
	s.reqSeq++
	return
}

func (s *Session) advanceRspSeq() (seq int64) {
	seq = s.rspSeq
	s.rspSeq++
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
