package proxy

import (
	"bufio"
	"net"
	"sync"
	"time"

	log "github.com/ngaut/logging"
)

var (
	LogEveryN      uint32
	accessLogCount uint32 // overflow is allowed
)

type Proxy struct {
	addr              string
	clientIdleTimeout time.Duration
	readTimeout       time.Duration
	dispatcher        *Dispatcher
	slotTable         *SlotTable
	connPool          *ConnPool
	mo                *MultiOperator
	exitChan          chan struct{}
}

func NewProxy(addr string, readTimeout time.Duration, dispatcher *Dispatcher, connPool *ConnPool) *Proxy {
	p := &Proxy{
		addr:              addr,
		clientIdleTimeout: 120 * time.Second,
		readTimeout:       readTimeout,
		dispatcher:        dispatcher,
		connPool:          connPool,
		mo:                NewMultiOperator(addr),
		exitChan:          make(chan struct{}),
	}
	return p
}

func (p *Proxy) Exit() {
	close(p.exitChan)
}

func (p *Proxy) handleConnection(cc net.Conn) {
	session := &Session{
		Conn:        cc,
		r:           bufio.NewReader(cc),
		backQ:       make(chan *PipelineResponse, 1000),
		closeSignal: &sync.WaitGroup{},
		connPool:    p.connPool,
		mo:          p.mo,
		dispatcher:  p.dispatcher,
		rspHeap:     &PipelineResponseHeap{},
	}
	session.Run()
}

func (p *Proxy) Run() {
	tcpAddr, err := net.ResolveTCPAddr("tcp", p.addr)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Infof("proxy listens on %s", p.addr)
	}
	defer listener.Close()

	go p.dispatcher.Run()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Error(err)
			continue
		}
		log.Infof("accept client: %s", conn.RemoteAddr())
		go p.handleConnection(conn)
	}
}
