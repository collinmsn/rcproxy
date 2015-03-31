package proxy

// @see https://github.com/wandoulabs/codis/blob/master/pkg/proxy/router/multioperator.go

import (
	"time"

	"bufio"
	"fmt"
	"github.com/collinmsn/resp"
	"github.com/fatih/pool"
	log "github.com/ngaut/logging"
	"net"
)

const (
	NUM_MULTIOP_WORKERS = 64
)

var (
	OK_DATA *resp.Data
)

func init() {
	OK_DATA = &resp.Data{
		T:      resp.T_SimpleString,
		String: []byte("OK"),
	}
}

type MultiOperator struct {
	q chan *MulOp
	p pool.Pool
}

type MulOp struct {
	cmd     *resp.Command
	numKeys int
	result  *resp.Data
	wait    chan error
}

func NewMultiOperator(port int) *MultiOperator {
	p, err := pool.NewChannelPool(0, 5, func() (net.Conn, error) {
		return net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 1*time.Second)
	})
	if err != nil {
		log.Fatal(err)
	}

	mo := &MultiOperator{
		q: make(chan *MulOp, 128),
		p: p,
	}
	for i := 0; i < NUM_MULTIOP_WORKERS; i++ {
		go mo.work()
	}

	return mo
}

func (mo *MultiOperator) handleMultiOp(cmd *resp.Command, numKeys int) (*resp.Data, error) {
	wait := make(chan error, 1)
	op := &MulOp{cmd: cmd, numKeys: numKeys, wait: wait}
	mo.q <- op
	return op.result, <-wait
}

func (mo *MultiOperator) work() {
	for mop := range mo.q {
		switch mop.cmd.Name() {
		case "MGET":
			mo.mget(mop)
		case "MSET":
			mo.mset(mop)
		case "DEL":
			mo.del(mop)
		}
	}
}

func (mo *MultiOperator) mgetResults(mop *MulOp) (*resp.Data, error) {
	var err error
	var cmd *resp.Command
	var conn net.Conn

	conn, err = mo.p.Get()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			log.Error(err)
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	// send request
	for _, key := range mop.cmd.Args[1:] {
		cmd, err = resp.NewCommand("MGET", key)
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(cmd.Format()); err != nil {
			return nil, err
		}
	}
	if err = w.Flush(); err != nil {
		return nil, err
	}

	// read rsp
	rsp := &resp.Data{
		T:     resp.T_Array,
		Array: make([]*resp.Data, mop.numKeys),
	}
	var subRsp *resp.Data
	for i := 0; i < mop.numKeys; i++ {
		if subRsp, err = resp.ReadData(r); err != nil {
			return nil, err
		} else {
			rsp.Array[i] = subRsp
		}
	}

	return rsp, err
}

func (mo *MultiOperator) mget(mop *MulOp) {
	start := time.Now()
	defer func() {
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do mget", sec)
		}
	}()

	rsp, err := mo.mgetResults(mop)
	if err != nil {
		mop.wait <- err
		return
	}
	mop.result = rsp
	mop.wait <- err
}

func (mo *MultiOperator) delResults(mop *MulOp) (*resp.Data, error) {
	var err error
	var cmd *resp.Command
	var conn net.Conn

	conn, err = mo.p.Get()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			log.Error(err)
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	// send request
	for _, key := range mop.cmd.Args[1:] {
		cmd, err = resp.NewCommand("DEL", key)
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(cmd.Format()); err != nil {
			return nil, err
		}
	}
	if err = w.Flush(); err != nil {
		return nil, err
	}

	// read rsp
	rsp := &resp.Data{
		T: resp.T_Integer,
	}
	var subRsp *resp.Data
	for i := 0; i < mop.numKeys; i++ {
		if subRsp, err = resp.ReadData(r); err != nil || subRsp.T != resp.T_Integer {
			return nil, err
		} else {
			rsp.Integer += subRsp.Integer
		}
	}
	return rsp, err
}

func (mo *MultiOperator) msetResults(mop *MulOp) (*resp.Data, error) {
	var err error
	var cmd *resp.Command
	var conn net.Conn

	conn, err = mo.p.Get()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			log.Error(err)
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	// write request
	for i := 1; i < 1+mop.numKeys*2; i += 2 {
		//change mset to set
		if cmd, err = resp.NewCommand("SET", mop.cmd.Args[i], mop.cmd.Args[i+1]); err != nil {
			return nil, err
		}
		if _, err = w.Write(cmd.Format()); err != nil {
			return nil, err
		}
	}
	if err = w.Flush(); err != nil {
		return nil, err
	}
	// read rsp
	for i := 0; i < mop.numKeys; i++ {
		if _, err = resp.ReadData(r); err != nil {
			return nil, err
		}
	}

	return OK_DATA, err
}

func (mo *MultiOperator) mset(mop *MulOp) {
	start := time.Now()
	defer func() {
		//todo:extra function
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do del", sec)
		}
	}()

	rsp, err := mo.msetResults(mop)
	if err != nil {
		mop.wait <- err
		return
	}

	mop.result = rsp
	mop.wait <- err
}

func (mo *MultiOperator) del(mop *MulOp) {
	start := time.Now()
	defer func() {
		//todo:extra function
		if sec := time.Since(start).Seconds(); sec > 2 {
			log.Warning("too long to do del", sec)
		}
	}()

	rsp, err := mo.delResults(mop)
	if err != nil {
		mop.wait <- err
		return
	}

	mop.result = rsp
	mop.wait <- err
}
