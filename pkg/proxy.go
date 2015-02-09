package pkg

import (
	"net"
	"log"
	"fmt"
	"time"
	"github.com/walu/resp"
	"github.com/howeyc/crc16"
)

type Proxy struct {
	port     int
	slotMap *SlotMap
	exitChan chan struct{}
}

func NewProxy(port int, timeout time.Duration, connectTimeout, readTimeout time.Duration, slotMap *SlotMap, exitChan chan struct { }) *Proxy {
	p := &Proxy{
		port: port,
		connectTimeout: connectTimeout,
		readTimeout: readTimeout,
		slotMap: slotMap,
		exitChan: exitChan,
	}
	return p
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
		log.Printf("proxy listen on port %d", p.port)
	}
	defer listener.Close()
	for {
		select {
		case <-p.exitChan:
			return
		default:
		}
		// Wait for a connection.
		listener.SetDeadline(time.Now().Add(5 * time.Second))
		conn, err := listener.AcceptTCP()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			} else {
				log.Fatal(err)
				return
			}
		}
		go func(c *net.TCPConn) {
			defer c.Close()
			for {

				cmd, err := resp.ReadCommand(*c)
				slot := crc16.ChecksumCCITT([]byte(cmd)) % NumSlots
				if err != nil {
					log.Print(err)
					return
				}
				op, key = cmd.Name(), cmd.String(1)
				slot :=
			}
		}(conn)
	}
}
