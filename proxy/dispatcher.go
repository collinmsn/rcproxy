package proxy

import (
	"net"
	"time"

	"bufio"

	"math/rand"
	"strings"

	"errors"

	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
	"gopkg.in/fatih/pool.v2"
)

const (
	// write commands always go to master
	// read from master
	READ_PREFER_MASTER = iota
	// read from slave if possible
	READ_PREFER_SLAVE
	// read from slave in the same idc if possible
	READ_PREFER_SLAVE_IDC
)

var (
	REDIS_CMD_CLUSTER_SLOTS  []byte
	REDIS_CMD_READ_ONLY      []byte
	errEmptyClusterSlots     = errors.New("empty cluster slots")
	slotNotCoveredRespObject = resp.NewObjectFromData(
		&resp.Data{
			T:      resp.T_Error,
			String: []byte("slot is not covered")})
)

func init() {
	cmd, _ := resp.NewCommand("CLUSTER", "SLOTS")
	REDIS_CMD_CLUSTER_SLOTS = cmd.Format()
	cmd, _ = resp.NewCommand("READONLY")
	REDIS_CMD_READ_ONLY = cmd.Format()
}

// Dispatcher routes requests from all clients to the right backend
// it also maintains the slot table
type Dispatcher interface {
	InitSlotTable() error
	Run()
	Schedule(req *Request)
	TriggerReloadSlots()
}

type DefaultDispatcher struct {
	startupNodes       []string
	slotReloadInterval time.Duration
	connPool           *ConnPool
	readPrefer         int

	slotTable   *SlotTable
	requestChan chan *Request
	backends    map[string]BackendSession
	// notify slots changed
	slotInfoChan   chan []*SlotInfo
	slotReloadChan chan struct{}
}

func NewDefaultDispatcher(startupNodes []string, slotReloadInterval time.Duration, connPool *ConnPool, readPrefer int) *DefaultDispatcher {
	d := &DefaultDispatcher{
		startupNodes:       startupNodes,
		slotTable:          NewSlotTable(),
		slotReloadInterval: slotReloadInterval,
		requestChan:        make(chan *Request, 10000),
		connPool:           connPool,
		backends:           make(map[string]BackendSession),
		slotInfoChan:       make(chan []*SlotInfo),
		slotReloadChan:     make(chan struct{}, 1),
		readPrefer:         readPrefer,
	}
	return d
}

func (d *DefaultDispatcher) InitSlotTable() error {
	if slotInfos, err := d.reloadTopology(); err != nil {
		return err
	} else {
		for _, si := range slotInfos {
			d.slotTable.SetSlotInfo(si)
		}
	}
	return nil
}

func (d *DefaultDispatcher) Run() {
	go d.slotsReloadLoop()
	for {
		select {
		case req, ok := <-d.requestChan:
			// dispatch req
			if !ok {
				log.Info("exit dispatch loop")
				return
			}
			var server string
			if req.readOnly {
				server = d.slotTable.ReadServer(req.slot)
			} else {
				server = d.slotTable.WriteServer(req.slot)
			}
			taskRunner, ok := d.backends[server]
			if !ok {
				if server == "" {
					// slot is not covered
					req.obj = slotNotCoveredRespObject
					req.Done()
					continue
				} else {
					log.Info("create task runner", server)
					taskRunner = NewDefaultBackendSession(server, d.connPool)
					taskRunner.Start()
					d.backends[server] = taskRunner
				}
			}
			taskRunner.Schedule(req)
		case info := <-d.slotInfoChan:
			d.handleSlotInfoChanged(info)
		}
	}
}

// remove unused task runner
func (d *DefaultDispatcher) handleSlotInfoChanged(slotInfos []*SlotInfo) {
	newServers := make(map[string]bool)
	for _, si := range slotInfos {
		d.slotTable.SetSlotInfo(si)
		newServers[si.write] = true
		for _, read := range si.read {
			newServers[read] = true
		}
	}

	for server, tr := range d.backends {
		if _, ok := newServers[server]; !ok {
			log.Info("exit unused task runner", server)
			tr.Exit()
			delete(d.backends, server)
		}
	}
}

func (d *DefaultDispatcher) Schedule(req *Request) {
	d.requestChan <- req
	log.Debug("add to reqCh")
}

// wait for the slot reload chan and reload cluster topology
// at most every slotReloadInterval
// it also reload topology at a relative long periodic interval
func (d *DefaultDispatcher) slotsReloadLoop() {
	periodicReloadInterval := 300 * time.Second
	for {
		select {
		case <-time.After(d.slotReloadInterval):
			select {
			case _, ok := <-d.slotReloadChan:
				if !ok {
					log.Infof("exit reload slot table loop")
					return
				}
				log.Infof("request reload triggered")
				if slotInfos, err := d.reloadTopology(); err != nil {
					log.Errorf("reload slot table failed")
				} else {
					d.slotInfoChan <- slotInfos
				}
			case <-time.After(periodicReloadInterval):
				log.Infof("periodic reload triggered")
				if slotInfos, err := d.reloadTopology(); err != nil {
					log.Errorf("reload slot table failed")
				} else {
					d.slotInfoChan <- slotInfos
				}
			}
		}
	}
}

// request "CLUSTER SLOTS" to retrieve the cluster topology
// try each start up nodes until the first success one
func (d *DefaultDispatcher) reloadTopology() (slotInfos []*SlotInfo, err error) {
	log.Info("reload slot table")
	indexes := rand.Perm(len(d.startupNodes))
	for _, index := range indexes {
		if slotInfos, err = d.doReload(d.startupNodes[index]); err == nil {
			break
		}
	}
	return
}

/**
获取cluster slots信息，并利用cluster nodes信息来将failed的slave过滤掉
*/
func (d *DefaultDispatcher) doReload(server string) (slotInfos []*SlotInfo, err error) {
	var conn net.Conn
	conn, err = d.connPool.GetConn(server)
	if err != nil {
		log.Error(server, err)
		return
	} else {
		log.Infof("query cluster slots from %s", server)
	}
	defer func() {
		if err != nil {
			conn.(*pool.PoolConn).MarkUnusable()
		}
		conn.Close()
	}()
	_, err = conn.Write(REDIS_CMD_CLUSTER_SLOTS)
	if err != nil {
		log.Errorf("write cluster slots error", server, err)
		return
	}
	r := bufio.NewReader(conn)
	var data *resp.Data
	data, err = resp.ReadData(r)
	if err != nil {
		log.Error(server, err)
		return
	}
	if data.T == resp.T_Error {
		err = errors.New(string(data.Format()))
		return
	}
	if len(data.Array) == 0 {
		err = errEmptyClusterSlots
		return
	}
	slotInfos = make([]*SlotInfo, 0, len(data.Array))
	for _, info := range data.Array {
		slotInfos = append(slotInfos, NewSlotInfo(info))
	}

	for _, si := range slotInfos {
		if d.readPrefer == READ_PREFER_MASTER {
			si.read = []string{si.write}
		} else if d.readPrefer == READ_PREFER_SLAVE || d.readPrefer == READ_PREFER_SLAVE_IDC {
			localIPPrefix := LocalIP()
			if len(localIPPrefix) > 0 {
				segments := strings.SplitN(localIPPrefix, ".", 3)
				localIPPrefix = strings.Join(segments[:2], ".")
				localIPPrefix += "."
			}
			var readNodes []string
			for _, node := range si.read {
				if d.readPrefer == READ_PREFER_SLAVE_IDC {
					// ips are regarded as in the same idc if they have the same first two segments, eg 10.4.x.x
					if !strings.HasPrefix(node, localIPPrefix) {
						log.Infof("filter %s by read prefer slave idc", node)
						continue
					}
				}
				readNodes = append(readNodes, node)
			}
			if len(readNodes) == 0 {
				readNodes = []string{si.write}
			}
			si.read = readNodes
		}
	}
	return
}

// schedule a reload task
// this call is inherently throttled, so that multiple clients can call it at
// the same time and it will only actually occur once
func (d *DefaultDispatcher) TriggerReloadSlots() {
	select {
	case d.slotReloadChan <- struct{}{}:
	default:
	}
}
func (d *DefaultDispatcher) Exit() {
	close(d.requestChan)
}
