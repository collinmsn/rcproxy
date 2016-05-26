package proxy

import (
	"time"

	"math/rand"
	"strings"

	"errors"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	log "github.com/ngaut/logging"
	redisclient "gopkg.in/redis.v3"
)

// dispatcher routes requests from all clients to the right backend
// it also maintains the slot table

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
	errEmptyClusterSlots     = errors.New("Empty cluster slots")
	slotNotCoveredRespObject = redis.NewError([]byte("slot not covered"))
)

type Dispatcher interface {
	TriggerReloadSlots()
	Schedule(req *PipelineRequest)
}

type RequestDispatcher struct {
	startupNodes       []string
	slotTable          *SlotTable
	slotReloadInterval time.Duration
	reqCh              chan *PipelineRequest
	connPool           *ConnPool
	backends           map[string]*Backend
	// notify slots changed
	slotInfoChan   chan []*SlotInfo
	slotReloadChan chan struct{}
	readPrefer     int
}

func NewRequestDispatcher(startupNodes []string, slotReloadInterval time.Duration, connPool *ConnPool, readPrefer int) *RequestDispatcher {
	d := &RequestDispatcher{
		startupNodes:       startupNodes,
		slotTable:          NewSlotTable(),
		slotReloadInterval: slotReloadInterval,
		reqCh:              make(chan *PipelineRequest, 10000),
		connPool:           connPool,
		backends:           make(map[string]*Backend),
		slotInfoChan:       make(chan []*SlotInfo),
		slotReloadChan:     make(chan struct{}, 1),
		readPrefer:         readPrefer,
	}
	return d
}

func (d *RequestDispatcher) InitSlotTable() error {
	if slotInfos, err := d.reloadTopology(); err != nil {
		return err
	} else {
		for _, si := range slotInfos {
			d.slotTable.SetSlotInfo(si)
		}
	}
	return nil
}

func (d *RequestDispatcher) Run() {
	go d.slotsReloadLoop()
	for {
		select {
		case req, ok := <-d.reqCh:
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
			backend, ok := d.backends[server]
			if !ok {
				if server == "" {
					// slot is not covered
					req.backQ <- &PipelineResponse{
						req: req,
						obj: slotNotCoveredRespObject,
					}
					continue
				} else {
					log.Info("create backend", server)
					backend = NewBackend(server, d.connPool, 1)
					backend.Start()
					d.backends[server] = backend
				}
			}
			backend.Schedule(req)
		case info := <-d.slotInfoChan:
			d.handleSlotInfoChanged(info)
		}
	}
}

// remove unused task runner
func (d *RequestDispatcher) handleSlotInfoChanged(slotInfos []*SlotInfo) {
	newServers := make(map[string]bool)
	for _, si := range slotInfos {
		d.slotTable.SetSlotInfo(si)
		newServers[si.write] = true
		for _, read := range si.read {
			newServers[read] = true
		}
	}

	for server, backend := range d.backends {
		if _, ok := newServers[server]; !ok {
			log.Info("exit unused backend", server)
			backend.Stop()
			delete(d.backends, server)
		}
	}
}

func (d *RequestDispatcher) Schedule(req *PipelineRequest) {
	select {
	case d.reqCh <- req:
	default:
		log.Error("dispatch overflow")
	}
}

// wait for the slot reload chan and reload cluster topology
// at most every slotReloadInterval
// it also reload topology at a relative long periodic interval
func (d *RequestDispatcher) slotsReloadLoop() {
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
func (d *RequestDispatcher) reloadTopology() (slotInfos []*SlotInfo, err error) {
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
func (d *RequestDispatcher) doReload(server string) (slotInfos []*SlotInfo, err error) {
	cli := redisclient.NewClusterClient(&redisclient.ClusterOptions{
		Addrs: []string{server},
	})
	cmd := cli.ClusterSlots()
	var clusterSlotInfos []redisclient.ClusterSlotInfo
	clusterSlotInfos, err = cmd.Result()
	if err != nil {
		return
	} else if len(clusterSlotInfos) == 0 {
		err = errEmptyClusterSlots
		return
	}

	for _, clusterSlotInfo := range clusterSlotInfos {
		si := &SlotInfo{
			start: clusterSlotInfo.Start,
			end:   clusterSlotInfo.End,
			write: clusterSlotInfo.Addrs[0],
			read:  clusterSlotInfo.Addrs[1:],
		}
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
		slotInfos = append(slotInfos, si)
	}
	return
}

// schedule a reload task
// this call is inherently throttled, so that multiple clients can call it at
// the same time and it will only actually occur once
func (d *RequestDispatcher) TriggerReloadSlots() {
	select {
	case d.slotReloadChan <- struct{}{}:
	default:
	}
}
func (d *RequestDispatcher) Exit() {
	close(d.reqCh)
}
