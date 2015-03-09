package proxy

import (
	"container/list"
	log "github.com/Sirupsen/logrus"
	"github.com/walu/resp"
	"net"
	"sync"
)

// TaskRunner assure every request will be responded
type TaskRunner struct {
	in       chan *PipelineRequest
	inflight *list.List
	// terminated by outside
	exitChan chan struct{}
	// read loop exit, notify writer
	exitReaderChan chan struct{}
	// writer wait reader to exit
	wg       *sync.WaitGroup
	server   string
	conn     net.Conn
	connPool *ConnPool
}

func (tr *TaskRunner) readingLoop() {
	for {
		data, err := resp.ReadData(tr.conn)
		if err != nil {
			log.Error("exit read loop", tr.server, err)
			tr.wg.Done()
			tr.exitReaderChan <- struct{}{}
			return
		}
		// TODO: lock
		plReq := tr.inflight.Remove(tr.inflight.Front()).(*PipelineRequest)
		log.Debugf("%#v", plReq.cmd)
		log.Debugf("%#v", plReq.wg)
		plResp := &PipelineResponse{
			resp: data,
			ctx:  plReq,
		}
		plReq.backQ <- plResp
	}
}

func (tr *TaskRunner) writingLoop() {
	for {
		select {
		case <-tr.exitChan:
			log.Infof("exit task runner", tr.server)
			tr.conn.Close()
			tr.wg.Wait()
			return
		case <-tr.exitReaderChan:
			// try recover
			log.Warnf("read loop exit")

		case plReq := <-tr.in:
			log.Debugf("%#v", plReq.wg)
			if err := tr.writeToBackend(plReq); err != nil {
				// try recover
			}
		}
	}
}

func (tr *TaskRunner) writeToBackend(plReq *PipelineRequest) error {
	log.Debugf("req=%d", plReq.seq)
	tr.inflight.PushBack(plReq)
	if _, err := tr.conn.Write(plReq.cmd.Format()); err != nil {
		log.Error(err)
		return err
	} else {
		return nil
	}
}

func NewTaskRunner(server string, connPool *ConnPool) (*TaskRunner, error) {
	tr := &TaskRunner{
		in:       make(chan *PipelineRequest, 1000),
		server:   server,
		connPool: connPool,
		inflight: list.New(),
		wg:       &sync.WaitGroup{},
	}

	if conn, err := connPool.GetConn(server); err != nil {
		return nil, err
	} else {
		tr.conn = conn
	}
	tr.wg.Add(1)

	go tr.writingLoop()
	go tr.readingLoop()

	return tr, nil
}
