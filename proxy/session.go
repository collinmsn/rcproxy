package proxy

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
)

var (
	MOVED         = []byte("MOVED")
	ASK           = []byte("ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
	OK_BYTES      = []byte("+OK\r\n")
	BLACK_CMD_ERR = []byte("unsupported command")
)

const (
	PIPE_LEN = 16
)

type Session struct {
	net.Conn
	r                     *bufio.Reader
	pipelineSeq           int64
	lastUnsentResponseSeq int64
	backQ                 chan *PipelineResponse
	closed                bool
	closeSignal           *sync.WaitGroup
	connPool              *ConnPool
	dispatcher            *Dispatcher
	mo                    *MultiOperator
	rspHeap               *PipelineResponseHeap
}

func (s *Session) Run() {
	s.closeSignal.Add(1)
	go s.WritingLoop()
	s.ReadLoop()
}

// consume backQ and send response to client
// handle MOVE and ASK redirection
// notify reader to exist on write failed
// writer is responsible for close connection
// the only way the writer can notify reader is close the writer can notify reader
func (s *Session) WritingLoop() {
	for {
		select {
		case rsp, ok := <-s.backQ:
			if !ok {
				// reader has closed backQ
				s.Close()
				s.closeSignal.Done()
				return
			}

			if err := s.handleResp(rsp); err != nil {
				s.Close()
				continue
			}
		}
	}
}

func (s *Session) ReadLoop() {
	for {
		cmd, err := resp.ReadCommand(s.r)
		if err != nil {
			log.Error(err)
			break
		}
		if n := atomic.AddUint32(&accessLogCount, 1); n%LogEveryN == 0 {
			if len(cmd.Args) > 1 {
				log.Infof("access %s %s %s", s.RemoteAddr(), cmd.Name(), cmd.Args[1])
			} else {
				log.Infof("access %s %s", s.RemoteAddr(), cmd.Name())
			}
		}
		// will compare command name later, so convert it to upper case
		cmd.Args[0] = strings.ToUpper(cmd.Args[0])

		// check if command is supported
		if IsBlackListCmd(cmd) {
			plReq := &PipelineRequest{
				seq: s.getNextSeq(),
			}
			rsp := &resp.Data{T: resp.T_Error, String: BLACK_CMD_ERR}
			plRsp := &PipelineResponse{
				rsp: rsp,
				ctx: plReq,
			}
			s.backQ <- plRsp
			continue
		}

		if yes, numKeys := IsMultiOpCmd(cmd); yes && numKeys > 1 {
			plReq := &PipelineRequest{
				seq: s.getNextSeq(),
			}
			plRsp := &PipelineResponse{
				ctx: plReq,
			}
			if rsp, err := s.mo.handleMultiOp(cmd, numKeys); err != nil {
				plRsp.rsp = &resp.Data{T: resp.T_Error, String: []byte(err.Error())}
			} else {
				plRsp.rsp = rsp
			}
			s.backQ <- plRsp
			continue
		}
		key := cmd.Value(1)
		slot := Key2Slot(key)
		plReq := &PipelineRequest{
			cmd:   cmd,
			slot:  slot,
			seq:   s.getNextSeq(),
			backQ: s.backQ,
			wg:    &sync.WaitGroup{},
		}

		plReq.wg.Add(1)
		s.dispatcher.Schedule(plReq)
		plReq.wg.Wait()
	}

	close(s.backQ)
	s.closeSignal.Wait()
}

func (s *Session) writeResp(plRsp *PipelineResponse) error {
	buf := plRsp.rsp.Format()
	// write to client directly with non-buffered io
	if _, err := s.Write(buf); err != nil {
		log.Error(err)
		return err
	}

	return nil
}

func (s *Session) redirect(server string, plRsp *PipelineResponse, ask bool) {
	plRsp.err = nil
	conn, err := s.connPool.GetConn(server)
	if err != nil {
		plRsp.err = err
		return
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if ask {
		if _, err := conn.Write(ASK_CMD_BYTES); err != nil {
			plRsp.err = err
			return
		}
	}
	if _, err := conn.Write(plRsp.ctx.cmd.Format()); err != nil {
		plRsp.err = err
		return
	}
	if ask {
		if _, err := resp.ReadData(reader); err != nil {
			plRsp.err = err
			return
		}
	}
	if data, err := resp.ReadData(reader); err != nil {
		plRsp.err = err
		return
	} else {
		plRsp.rsp = data
	}
}

func (s *Session) handleResp(plRsp *PipelineResponse) error {
	if plRsp.ctx.seq != s.lastUnsentResponseSeq {
		// should never happen
		log.Fatalf("seq not match, respSeq=%d,lastUnsentRespSeq=%d", plRsp.ctx.seq, s.lastUnsentResponseSeq)
	}

	s.lastUnsentResponseSeq++

	if plRsp.err != nil {
		s.dispatcher.TriggerReloadSlots()
		plRsp.rsp = &resp.Data{T: resp.T_Error, String: []byte(plRsp.err.Error())}
	} else if plRsp.rsp.T == resp.T_Error {
		if bytes.HasPrefix(plRsp.rsp.String, MOVED) {
			slot, server := ParseRedirectInfo(string(plRsp.rsp.String))
			s.dispatcher.UpdateSlotInfo(&SlotInfo{
				start:  slot,
				end:    slot,
				master: server,
			})
			s.redirect(server, plRsp, false)
		} else if bytes.HasPrefix(plRsp.rsp.String, ASK) {
			_, server := ParseRedirectInfo(string(plRsp.rsp.String))
			s.redirect(server, plRsp, true)
		}
	}

	if plRsp.err != nil {
		return plRsp.err
	}

	if !s.closed {
		if err := s.writeResp(plRsp); err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) Close() {
	if !s.closed {
		s.closed = true
		s.Conn.Close()
	}
}

func ParseRedirectInfo(msg string) (slot int, server string) {
	parts := strings.Split(msg, " ")
	if len(parts) != 3 {
		log.Fatalf("invalid redirect message: %s", msg)
	}
	var err error
	slot, err = strconv.Atoi(parts[1])
	if err != nil {
		log.Fatalf("invalid redirect message: %s", msg)
	}
	server = parts[2]
	return
}

func (s *Session) Read(p []byte) (int, error) {
	return s.r.Read(p)
}

func (s *Session) getNextSeq() (seq int64) {
	seq = s.pipelineSeq
	s.pipelineSeq++
	return
}
