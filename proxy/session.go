package proxy

import (
	"bufio"
	"bytes"
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/walu/resp"
	"net"
	"strconv"
	"strings"
	"sync"
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
	slotTable             *SlotTable
}

func (s *Session) writeResp(plResp *PipelineResponse) error {
	buf := plResp.resp.Format()
	if _, err := s.Write(buf); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *Session) retry(server string, plResp *PipelineResponse, ask bool) {
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
		plResp.resp = data
	}
}

func (s *Session) handleResp(plResp *PipelineResponse) (flush bool, err error) {
	if plResp.ctx.seq != s.lastUnsentResponseSeq {
		log.Fatalf("seq not match: resp seq: %d, lastUnsentSeq %d", plResp.ctx.seq, s.lastUnsentResponseSeq)
	}

	s.lastUnsentResponseSeq++
	plResp.ctx.wg.Done()

	if plResp.err != nil {
		// if connect reset
		// slotTable.Reload()
		plResp.resp = &resp.Data{T: resp.T_Error, String: []byte(plResp.err.Error())}
	} else if plResp.resp.T == resp.T_Error {
		if bytes.HasPrefix(plResp.resp.String, MOVED) {
			slot, server := ParseRedirectInfo(string(plResp.resp.String))
			s.slotTable.Set(slot, server)
			s.retry(server, plResp, false)
		} else if bytes.HasPrefix(plResp.resp.String, ASK) {
			_, server := ParseRedirectInfo(string(plResp.resp.String))
			s.retry(server, plResp, true)
		}
	}
	log.Debugf("%#v", plResp.resp)

	if plResp.err != nil {
		return true, plResp.err
	}

	if !s.closed {
		if err := s.writeResp(plResp); err != nil {
			return false, err
		}
		flush = true
	}

	return
}

func (s *Session) Close() {
	s.closed = true
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

			flush, err := s.handleResp(resp)
			if err != nil {
				log.Warning(s.RemoteAddr(), resp.ctx, errors.ErrorStack(err))
				s.Close() //notify reader to exit
				continue
			}

			if flush && len(s.backQ) == 0 {
				err := s.w.Flush()
				if err != nil {
					s.Close() //notify reader to exit
					log.Warning(s.RemoteAddr(), resp.ctx, errors.ErrorStack(err))
					continue
				}
			}
		}
	}
}

func (s *Session) Read(p []byte) (int, error) {
	return s.r.Read(p)
}

func (s *Session) Write(p []byte) (int, error) {
	return s.w.Write(p)
}
