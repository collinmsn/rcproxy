package proxy

import (
	"io"
	"net"
	"testing"

	"fmt"
	"math/rand"
	"time"

	"bufio"
	"bytes"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
	"github.com/stretchr/testify/assert"
)

type MockSessionReadWriter struct {
	cmds      chan *resp.Command
	objs      chan *resp.Object
	closeCmds bool
}

func (rw *MockSessionReadWriter) Close() error {
	if rw.closeCmds {
		close(rw.cmds)
	}
	return nil
}
func (rw *MockSessionReadWriter) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

/**
* 用chan close模拟client close
 */
func (rw *MockSessionReadWriter) ReadCommand() (*resp.Command, error) {
	cmd, ok := <-rw.cmds
	if !ok {
		return nil, io.EOF
	} else {
		return cmd, nil
	}
}

func (rw *MockSessionReadWriter) WriteObject(obj *resp.Object) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Error(r)
			err = io.ErrClosedPipe
		}
	}()
	rw.objs <- obj
	return
}

type EchoDispatcher struct {
}

func (d *EchoDispatcher) TriggerReloadSlots() {
}
func (d *EchoDispatcher) Schedule(req *PipelineRequest) {
	rsp := &PipelineResponse{
		req: req,
		obj: resp.NewObjectFromData(&resp.Data{
			T:      resp.T_BulkString,
			String: []byte(req.cmd.Args[1]),
		}),
	}
	req.backQ <- rsp
}

func TestSessionClientReadWrite(t *testing.T) {
	assert := assert.New(t)
	io := &MockSessionReadWriter{
		cmds: make(chan *resp.Command),
		objs: make(chan *resp.Object),
	}
	session := NewSession(io, nil, &EchoDispatcher{})
	go session.Run()
	var N int = 10000
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	reqRsp := make(map[*resp.Command]*resp.Object)
	for _, k := range keys {
		cmd, _ := resp.NewCommand("echo", k)
		reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
			T:      resp.T_BulkString,
			String: []byte(k),
		})
	}
	for k, v := range reqRsp {
		io.cmds <- k
		rsp := <-io.objs
		assert.Equal(string(rsp.Raw()), string(v.Raw()))
	}
}

func TestSessionClientReadError(t *testing.T) {
	assert := assert.New(t)
	io := &MockSessionReadWriter{
		cmds: make(chan *resp.Command),
		objs: make(chan *resp.Object),
	}
	session := NewSession(io, nil, &EchoDispatcher{})
	go session.Run()
	N := 10

	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	reqRsp := make(map[*resp.Command]*resp.Object)
	for _, k := range keys {
		cmd, _ := resp.NewCommand("echo", k)
		reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
			T:      resp.T_BulkString,
			String: []byte(k),
		})
	}
	for k, v := range reqRsp {
		io.cmds <- k
		rsp := <-io.objs
		assert.Equal(string(rsp.Raw()), string(v.Raw()))
	}
	closeFunc := func(d time.Duration) {
		<-time.After(d)
		close(io.cmds)
	}
	d := 3 * time.Second
	go closeFunc(d)
	ts := time.Now()
	session.closeWg.Wait()
	used := time.Since(ts)
	assert.InEpsilon(float64(used), float64(d), float64(5*time.Millisecond))
}

func TestSessionClientWriteError(t *testing.T) {
	assert := assert.New(t)
	io := &MockSessionReadWriter{
		cmds:      make(chan *resp.Command),
		objs:      make(chan *resp.Object),
		closeCmds: true,
	}
	session := NewSession(io, nil, &EchoDispatcher{})
	go session.Run()
	N := 10

	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	reqRsp := make(map[*resp.Command]*resp.Object)
	cmds := make([]*resp.Command, 0, N)
	for _, k := range keys {
		cmd, _ := resp.NewCommand("echo", k)
		reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
			T:      resp.T_BulkString,
			String: []byte(k),
		})
		cmds = append(cmds, cmd)
	}
	for _, cmd := range cmds {
		io.cmds <- cmd
	}
	count := 0
	for _, cmd := range cmds {
		rsp := <-io.objs
		assert.Equal(string(rsp.Raw()), string(reqRsp[cmd].Raw()))
		count++
		if count == N/2 {
			close(io.objs)
			break
		}
	}

	ts := time.Now()
	session.closeWg.Wait()
	used := time.Since(ts)
	assert.InEpsilon(float64(used), 0, float64(5*time.Millisecond))
}

func shuffle(reqs []*PipelineRequest) {
	for i := range reqs {
		j := rand.Intn(i + 1)
		reqs[i], reqs[j] = reqs[j], reqs[i]
	}
}

type OutOfOrderDispatcher struct {
	buf    []*PipelineRequest
	bufLen int
}

func (d *OutOfOrderDispatcher) TriggerReloadSlots() {
}
func (d *OutOfOrderDispatcher) Schedule(req *PipelineRequest) {
	d.buf = append(d.buf, req)
	if len(d.buf) < d.bufLen {
		return
	}
	shuffle(d.buf)
	for _, req := range d.buf {
		rsp := &PipelineResponse{
			req: req,
			obj: resp.NewObjectFromData(&resp.Data{
				T:      resp.T_BulkString,
				String: []byte(req.cmd.Args[1]),
			}),
		}
		req.backQ <- rsp
	}
	d.buf = d.buf[:0]
}

func TestSessionPipelineOrder(t *testing.T) {
	assert := assert.New(t)
	var BUF_LEN int = 110
	var ROUND = 100
	var N int = BUF_LEN * ROUND
	io := &MockSessionReadWriter{
		cmds: make(chan *resp.Command),
		objs: make(chan *resp.Object, N),
	}
	dispatcher := &OutOfOrderDispatcher{
		bufLen: BUF_LEN,
	}
	session := NewSession(io, nil, dispatcher)
	go session.Run()
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	reqRsp := make(map[*resp.Command]*resp.Object)
	cmdOrder := make([]*resp.Command, 0, N)
	for _, k := range keys {
		cmd, _ := resp.NewCommand("echo", k)
		reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
			T:      resp.T_BulkString,
			String: []byte(k),
		})
		cmdOrder = append(cmdOrder, cmd)
	}
	for i := 0; i < ROUND; i++ {
		for j := 0; j < BUF_LEN; j++ {
			io.cmds <- cmdOrder[i*BUF_LEN+j]
		}
		for j := 0; j < BUF_LEN; j++ {
			cmd := cmdOrder[i*BUF_LEN+j]
			rsp := <-io.objs
			assert.Equal(string(rsp.Raw()), string(reqRsp[cmd].Raw()))
		}
	}
}

func TestSessionMultiRequest(t *testing.T) {
	assert := assert.New(t)
	var BUF_LEN int = 110
	var ROUND = 100
	var N int = BUF_LEN * ROUND
	io := &MockSessionReadWriter{
		cmds: make(chan *resp.Command),
		objs: make(chan *resp.Object, BUF_LEN),
	}
	dispatcher := &OutOfOrderDispatcher{
		bufLen: BUF_LEN,
	}
	session := NewSession(io, nil, dispatcher)
	go session.Run()
	keys := make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	// in each round, there are random get command and a mget command
	// mget is started at a random pos
	for i := 0; i < ROUND; i++ {
		reqRsp := make(map[*resp.Command]*resp.Object)
		var cmdOrder []*resp.Command
		numMgetKeys := rand.Int() % BUF_LEN
		if numMgetKeys < 2 {
			// 因为mget如果num key为1,会被转成get处理
			numMgetKeys = 2
		}
		pos := rand.Int() % (BUF_LEN - numMgetKeys)
		for j := 0; j < pos; j++ {
			k := fmt.Sprintf("key%d", i*BUF_LEN+j)
			cmd, _ := resp.NewCommand("get", k)
			reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
				T:      resp.T_BulkString,
				String: []byte(k),
			})
			cmdOrder = append(cmdOrder, cmd)
			io.cmds <- cmd
		}
		var mgetKeys []string
		for j := pos; j < pos+numMgetKeys; j++ {
			k := fmt.Sprintf("key%d", i*BUF_LEN+j)
			mgetKeys = append(mgetKeys, k)
			cmd, _ := resp.NewCommand("get", k)
			reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
				T:      resp.T_BulkString,
				String: []byte(k),
			})
			cmdOrder = append(cmdOrder, cmd)
		}
		var args []string
		args = append(args, "mget")
		args = append(args, mgetKeys...)
		cmd, _ := resp.NewCommand(args...)
		io.cmds <- cmd

		for j := pos + numMgetKeys; j < BUF_LEN; j++ {
			k := fmt.Sprintf("key%d", i*BUF_LEN+j)
			cmd, _ := resp.NewCommand("get", k)
			reqRsp[cmd] = resp.NewObjectFromData(&resp.Data{
				T:      resp.T_BulkString,
				String: []byte(k),
			})
			cmdOrder = append(cmdOrder, cmd)
			io.cmds <- cmd
		}

		// rsp
		for j := 0; j < pos; j++ {
			cmd := cmdOrder[j]
			rsp := <-io.objs
			assert.Equal(string(rsp.Raw()), string(reqRsp[cmd].Raw()))
		}
		// multi
		rsp := <-io.objs
		data, _ := resp.ReadData(bufio.NewReader(bytes.NewReader(rsp.Raw())))
		assert.Equal(len(data.Array), numMgetKeys)
		for j := 0; j < len(data.Array); j++ {
			cmd := cmdOrder[pos+j]
			assert.Equal(string(resp.NewObjectFromData(data.Array[j]).Raw()), string(reqRsp[cmd].Raw()))
		}
		for j := pos + numMgetKeys; j < BUF_LEN; j++ {
			cmd := cmdOrder[j]
			rsp := <-io.objs
			assert.Equal(string(rsp.Raw()), string(reqRsp[cmd].Raw()))
		}
	}
}
