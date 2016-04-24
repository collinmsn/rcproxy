package proxy

import (
	"net"

	log "github.com/ngaut/logging"
)

var (
	LogEveryN      uint64 = 100
	accessLogCount uint64
)

type Proxy struct {
	addr       string
	dispatcher *RequestDispatcher
	slotTable  *SlotTable
	connPool   *ConnPool
	exitChan   chan struct{}
}

func NewProxy(addr string, dispatcher *RequestDispatcher, connPool *ConnPool) *Proxy {
	p := &Proxy{
		addr:       addr,
		dispatcher: dispatcher,
		connPool:   connPool,
		exitChan:   make(chan struct{}),
	}
	return p
}

func (p *Proxy) Exit() {
	close(p.exitChan)
}

func (p *Proxy) handleConnection(conn net.Conn) {
	io := NewClientSessionReadWriter(conn)
	session := NewSession(io, p.connPool, p.dispatcher)
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
		log.Info("proxy listens on", p.addr)
	}
	defer listener.Close()

	go p.dispatcher.Run()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Error(err)
			continue
		}
		log.Info("accept client", conn.RemoteAddr())
		go p.handleConnection(conn)
	}
}
