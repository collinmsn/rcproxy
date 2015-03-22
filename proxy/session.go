package proxy

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/collinmsn/resp"
)

var (
	MOVED         = []byte("MOVED")
	ASK           = []byte("ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
)

const (
	PIPE_LEN = 16
)

type Session struct {
	net.Conn
	r                     *bufio.Reader
	w                     *bufio.Writer
	pipelineSeq           int64
	lastUnsentResponseSeq int64
	backQ                 chan *PipelineResponse
	closed                bool
	closeSignal           *sync.WaitGroup
	connPool              *ConnPool
	dispatcher            *Dispatcher
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
		if err := s.filter(cmd); err != nil {
			rsp := &resp.Data{T: resp.T_Error, String: []byte(err.Error())}
			plRsp := &PipelineResponse{
				rsp: rsp,
			}
			s.backQ <- plRsp
			continue
		}
		key := cmd.Value(1)
		slot := Key2Slot(key)
		plReq := &PipelineRequest{
			cmd:   cmd,
			slot:  slot,
			seq:   s.pipelineSeq,
			backQ: s.backQ,
			wg:    &sync.WaitGroup{},
		}
		s.pipelineSeq++

		plReq.wg.Add(1)
		s.dispatcher.Schedule(plReq)
		plReq.wg.Wait()
	}

	close(s.backQ)
	s.closeSignal.Wait()
}

func (s *Session) filter(cmd *resp.Command) error {
	return nil
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
		log.Panicf("seq not match, respSeq=%d,lastUnsentRespSeq=%d", plRsp.ctx.seq, s.lastUnsentResponseSeq)
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
		s.w.Flush()
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
