package proxy

import (
	"bufio"
	"bytes"
	"net"
	"strconv"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/walu/resp"
)

var (
	MOVED         = []byte("MOVED")
	ASK           = []byte("ASK")
	ASK_CMD_BYTES = []byte("+ASKING\r\n")
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
		case resp, ok := <-s.backQ:
			if !ok {
				// reader has closed backQ
				s.Close()
				s.closeSignal.Done()
				return
			}

			if err := s.handleResp(resp); err != nil {
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
			plResp := &PipelineResponse{
				rsp: rsp,
			}
			s.backQ <- plResp
			continue
		}
		log.Debugf("%s cmd: %s", s.Conn.RemoteAddr(), cmd.Name())
		plReq := &PipelineRequest{
			cmd:   cmd,
			seq:   s.pipelineSeq,
			backQ: s.backQ,
			wg:    &sync.WaitGroup{},
		}
		s.pipelineSeq++

		plReq.wg.Add(1)
		log.Debugf("%#v", plReq.wg)
		s.dispatcher.Schedule(plReq)
		plReq.wg.Wait()
	}

	close(s.backQ)
	s.closeSignal.Wait()
}

func (s *Session) filter(cmd *resp.Command) error {
	return nil
}

func (s *Session) writeResp(plResp *PipelineResponse) error {
	buf := plResp.rsp.Format()
	if _, err := s.w.Write(buf); err != nil {
		log.Error(err)
		return err
	}
	s.w.Flush()
	return nil
}

func (s *Session) redirect(server string, plResp *PipelineResponse, ask bool) {
	plResp.err = nil
	conn, err := s.connPool.GetConn(server)
	defer conn.Close()
	if err != nil {
		plResp.err = err
		return
	}
	if ask {
		if _, err := conn.Write(ASK_CMD_BYTES); err != nil {
			plResp.err = err
			return
		}
	}
	if _, err := conn.Write(plResp.ctx.cmd.Format()); err != nil {
		plResp.err = err
		return
	}
	if ask {
		if _, err := resp.ReadData(conn); err != nil {
			plResp.err = err
			return
		}
	}
	if data, err := resp.ReadData(conn); err != nil {
		plResp.err = err
		return
	} else {
		plResp.rsp = data
	}
}

func (s *Session) handleResp(plResp *PipelineResponse) error {
	if plResp.ctx.seq != s.lastUnsentResponseSeq {
		// should never happen
		log.Panicf("seq not match, respSeq=%d,lastUnsentRespSeq=%d", plResp.ctx.seq, s.lastUnsentResponseSeq)
	}

	s.lastUnsentResponseSeq++
	plResp.ctx.wg.Done()

	if plResp.err != nil {
		if nerr, ok := plResp.err.(net.Error); ok && !nerr.Temporary() {
			log.Warnf("non temporary net work error, remote=%s,err=%#v", s.Conn.RemoteAddr(), nerr)
			s.dispatcher.TriggerReloadSlots()
		}
		plResp.rsp = &resp.Data{T: resp.T_Error, String: []byte(plResp.err.Error())}
	} else if plResp.rsp.T == resp.T_Error {
		if bytes.HasPrefix(plResp.rsp.String, MOVED) {
			slot, server := ParseRedirectInfo(string(plResp.rsp.String))
			s.dispatcher.UpdateSlotInfo(&SlotInfo{
				start:  slot,
				end:    slot,
				master: server,
			})
			s.redirect(server, plResp, false)
		} else if bytes.HasPrefix(plResp.rsp.String, ASK) {
			_, server := ParseRedirectInfo(string(plResp.rsp.String))
			s.redirect(server, plResp, true)
		}
	}
	log.Debugf("%#v", plResp.rsp)

	if plResp.err != nil {
		s.w.Flush()
		return plResp.err
	}

	if !s.closed {
		if err := s.writeResp(plResp); err != nil {
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

func (s *Session) Write(p []byte) (int, error) {
	return s.w.Write(p)
}
