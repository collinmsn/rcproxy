package proxy

import (
	"bufio"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/howeyc/crc16"
	"github.com/walu/resp"
	"net"
	"strings"
	"sync"
	"time"
)

type Proxy struct {
	port              int
	clientIdleTimeout time.Duration
	readTimeout       time.Duration
	slotTable         *SlotTable
	reqCh             chan *PipelineRequest
	connPool          *ConnPool
	taskRunners       map[string]*TaskRunner
	exitChan          chan struct{}
}

func NewProxy(port int, readTimeout time.Duration, slotTable *SlotTable, connPool *ConnPool) *Proxy {
	p := &Proxy{
		port:              port,
		clientIdleTimeout: 10 * time.Second,
		readTimeout:       readTimeout,
		slotTable:         slotTable,
		reqCh:             make(chan *PipelineRequest, 1000),
		connPool:          connPool,
		taskRunners:       make(map[string]*TaskRunner),
		exitChan:          make(chan struct{}),
	}
	return p
}

func (p *Proxy) Exit() {
	close(p.exitChan)
}

func (p *Proxy) readCommands(ccr *bufio.Reader) ([]*resp.Command, error) {
	cmds := []*resp.Command{}
	if cmd, err := resp.ReadCommand(ccr); err != nil {
		return cmds, err
	} else {
		log.Debugf("read command")
		cmds = append(cmds, cmd)
	}
	for {
		if _, err := ccr.Peek(1); err != nil {
			log.Error(err)
			return cmds, nil
		}
		if cmd, err := resp.ReadCommand(ccr); err != nil {
			return cmds, err
		} else {
			log.Debugf("read command")
			cmds = append(cmds, cmd)
		}
	}
}

func (p *Proxy) handleConnection(cc net.Conn) {
	session := &Session{
		Conn:        cc,
		r:           bufio.NewReader(cc),
		w:           bufio.NewWriter(cc),
		backQ:       make(chan *PipelineResponse, 1000),
		closeSignal: &sync.WaitGroup{},
		connPool:    p.connPool,
		slotTable:   p.slotTable,
	}
	session.closeSignal.Add(1)
	go session.WritingLoop()

	var err error
	defer func() {
		session.closeSignal.Wait()
		cc.Close()
		if err != nil {
			log.Errorf("close connection %v, %+v", cc.RemoteAddr(), session)
		}
	}()

	for {
		err = p.redisTunnel(session)
		if err != nil {
			close(session.backQ)
			return
		}
	}
}

func (p *Proxy) redisTunnel(session *Session) error {
	cmd, err := resp.ReadCommand(session.r)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Debugf("%s cmd: %s", session.Conn.RemoteAddr(), cmd.Name())
	plReq := &PipelineRequest{
		cmd:   cmd,
		seq:   session.pipelineSeq,
		backQ: session.backQ,
		wg:    &sync.WaitGroup{},
	}
	session.pipelineSeq++

	plReq.wg.Add(1)
	log.Debugf("%#v", plReq.wg)
	p.reqCh <- plReq
	plReq.wg.Wait()
	return nil
}

func (p *Proxy) Run() {
	addr := fmt.Sprintf("0.0.0.0:%d", p.port)
	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Infof("proxy listens on port %d", p.port)
	}
	defer listener.Close()

	go p.DispatchLoop()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Error(err)
			continue
		}
		log.Debugf("accept client: %s", conn.RemoteAddr())
		go p.handleConnection(conn)
	}
}

func (p *Proxy) DispatchLoop() {
	log.Debugf("start dispatch loop")
	var err error
	for {
		select {
		case req, ok := <-p.reqCh:
			if !ok {
				log.Infof("exit dispatch loop")
				return
			}
			slot := p.calcSlot(req.cmd.Name())
			server := p.slotTable.Get(slot)
			taskRunner, ok := p.taskRunners[server]
			if !ok {
				log.Infof("create task runner, server=%s", server)
				taskRunner, err = NewTaskRunner(server, p.connPool)
				if err != nil {
				} else {
					p.taskRunners[server] = taskRunner
				}
			}
			taskRunner.in <- req
		}
	}
}

func (p *Proxy) calcSlot(key string) int {
	bytes := []byte(key)
	if pos := strings.IndexByte(key, '{'); pos != -1 {
		if rpos := strings.LastIndex(key, "}"); rpos > pos {
			bytes = []byte(key[pos+1 : rpos])
		}
	}
	slot := crc16.ChecksumCCITT(bytes) % NumSlots
	return int(slot)
}
