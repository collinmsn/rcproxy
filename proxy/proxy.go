package proxy

import (
	"bufio"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net"
	"os"
	"sync"
	"time"
)

type Proxy struct {
	port              int
	clientIdleTimeout time.Duration
	readTimeout       time.Duration
	dispatcher        *Dispatcher
	slotTable         *SlotTable
	connPool          *ConnPool
	exitChan          chan struct{}
}

func NewProxy(port int, readTimeout time.Duration, dispatcher *Dispatcher, connPool *ConnPool) *Proxy {
	p := &Proxy{
		port:              port,
		clientIdleTimeout: 120 * time.Second,
		readTimeout:       readTimeout,
		dispatcher:        dispatcher,
		connPool:          connPool,
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
		w:           bufio.NewWriter(cc),
		backQ:       make(chan *PipelineResponse, 1000),
		closeSignal: &sync.WaitGroup{},
		connPool:    p.connPool,
		dispatcher:  p.dispatcher,
	}
	session.Run()
}

func (p *Proxy) Run() {
	addr := fmt.Sprintf("0.0.0.0:%d", p.port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Infof("proxy listens on port %d, pid %d", p.port, os.Getpid())
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
