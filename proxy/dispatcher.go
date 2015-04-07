package proxy

import (
	"errors"
	"net"
	"time"

	"bufio"

	"github.com/collinmsn/resp"
	"github.com/fatih/pool"
	log "github.com/ngaut/logging"
)

// dispatcher routes requests from all clients to the right backend
// it also maintains the slot table

var (
	CLUSTER_SLOTS        []byte
	ERR_ALL_NODES_FAILED = errors.New("all startup nodes are failed to get cluster slots")
)

func init() {
	cmd, _ := resp.NewCommand("CLUSTER", "SLOTS")
	CLUSTER_SLOTS = cmd.Format()
}

type Dispatcher struct {
	startupNodes       []string
	slotTable          *SlotTable
	slotReloadInterval time.Duration
	reqCh              chan *PipelineRequest
	connPool           *ConnPool
	taskRunners        map[string]*TaskRunner
	// notify slots changed
	slotInfoChan   chan interface{}
	slotReloadChan chan struct{}
}

func NewDispatcher(startupNodes []string, slotReloadInterval time.Duration, connPool *ConnPool) *Dispatcher {
	d := &Dispatcher{
		startupNodes:       startupNodes,
		slotTable:          NewSlotTable(),
		slotReloadInterval: slotReloadInterval,
		reqCh:              make(chan *PipelineRequest, 10000),
		connPool:           connPool,
		taskRunners:        make(map[string]*TaskRunner),
		slotInfoChan:       make(chan interface{}),
		slotReloadChan:     make(chan struct{}, 1),
	}
	return d
}

func (d *Dispatcher) InitSlotTable() error {
	if slotInfos, err := d.reloadTopology(); err != nil {
		return err
	} else {
		for _, si := range slotInfos {
			d.slotTable.SetSlotInfo(si)
		}
	}
	return nil
}

func (d *Dispatcher) Run() {
	go d.slotsReloadLoop()
	for {
		select {
		case req, ok := <-d.reqCh:
			// dispatch req
			if !ok {
				log.Info("exit dispatch loop")
				return
			}
			server := d.slotTable.Get(req.slot)
			taskRunner, ok := d.taskRunners[server]
			if !ok {
				log.Info("create task runner", server)
				taskRunner = NewTaskRunner(server, d.connPool)
				d.taskRunners[server] = taskRunner
			}
			taskRunner.in <- req
		case info := <-d.slotInfoChan:
			d.handleSlotInfoChanged(info)
		}
	}
}

// handle single slot update and batch update
// remove unused task runner
func (d *Dispatcher) handleSlotInfoChanged(info interface{}) {
	switch info.(type) {
	case *SlotInfo:
		d.slotTable.SetSlotInfo(info.(*SlotInfo))
	case []*SlotInfo:
		newServers := make(map[string]bool)
		for _, si := range info.([]*SlotInfo) {
			d.slotTable.SetSlotInfo(si)
			newServers[si.master] = true
		}
		for server, tr := range d.taskRunners {
			if _, ok := newServers[server]; !ok {
				tr.Exit()
				delete(d.taskRunners, server)
			}
		}
	}
}

func (d *Dispatcher) Schedule(req *PipelineRequest) {
	d.reqCh <- req
}

func (d *Dispatcher) UpdateSlotInfo(si *SlotInfo) {
	log.Infof("update slot info: %#v", si)
	d.slotInfoChan <- si
}

// wait for the slot reload chan and reload cluster topology
// at most every slotReloadInterval
func (d *Dispatcher) slotsReloadLoop() {
	for {
		select {
		case <-time.After(d.slotReloadInterval):
			if _, ok := <-d.slotReloadChan; !ok {
				log.Infof("exit reload slot table loop")
				return
			}
			if slotInfos, err := d.reloadTopology(); err != nil {
				log.Errorf("reload slot table failed")
			} else {
				d.slotInfoChan <- slotInfos
			}
		}
	}
}

// request "CLUSTER SLOTS" to retrieve the cluster topology
// try each start up nodes until the first success one
func (d *Dispatcher) reloadTopology() (slotInfos []*SlotInfo, err error) {
	log.Info("reload slot table")
	for _, server := range d.startupNodes {
		if slotInfos, err = d.doReload(server); err == nil {
			break
		}
	}
	return
}

func (d *Dispatcher) doReload(server string) (slotInfos []*SlotInfo, err error) {
	var conn net.Conn
	conn, err = d.connPool.GetConn(server)
	if err != nil {
		log.Error(server, err)
		return
	}
	defer func() {
		if err != nil {
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()
	_, err = conn.Write(CLUSTER_SLOTS)
	if err != nil {
		log.Error(server, err)
		return
	}
	r := bufio.NewReader(conn)
	var data *resp.Data
	data, err = resp.ReadData(r)
	if err != nil {
		log.Error(server, err)
		return
	}
	var si *SlotInfo
	slotInfos = make([]*SlotInfo, 0, len(data.Array))
	for _, info := range data.Array {
		if si, err = NewSlotInfo(info); err != nil {
			return
		} else {
			slotInfos = append(slotInfos, si)
		}
	}
	return
}

// schedule a reload task
// this call is inherently throttled, so that multiple clients can call it at
// the same time and it will only actually occur once
func (d *Dispatcher) TriggerReloadSlots() {
	select {
	case d.slotReloadChan <- struct{}{}:
	default:
	}
}
func (d *Dispatcher) Exit() {
	close(d.reqCh)
}
