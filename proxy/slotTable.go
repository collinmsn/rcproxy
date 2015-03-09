package proxy

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/walu/resp"
	"sync"
	"time"
)

const (
	NumSlots = 16384
)

var errAllNodesFailed = errors.New("all startup nodes are failed to get cluster slots")
var errInvalidSlotInfo = errors.New("invalid slot info")

type SlotTable struct {
	slotServers  []string
	mu           sync.RWMutex
	startupNodes []string
	connPool     *ConnPool
	reloadChan   chan struct{}
}

type slotInfo struct {
	start, end int
	master     string
}

func NewSlotTable(startupNodes []string, connPool *ConnPool) *SlotTable {
	sm := &SlotTable{
		slotServers:  make([]string, NumSlots),
		startupNodes: startupNodes,
		connPool:     connPool,
		reloadChan:   make(chan struct{}, 1),
	}
	go sm.reloadLoop()
	return sm
}

func (st *SlotTable) Exit() {
	close(st.reloadChan)
}

func (st *SlotTable) reloadLoop() {
	for {
		select {
		case <-time.After(time.Second):
			if _, ok := <-st.reloadChan; !ok {
				log.Infof("exit reload slot table loop")
				return
			}
			log.Infof("reload slot table loop")
			if err := st.doReload(); err != nil {
				log.Fatalf("reload slot table failed")
			}
		}
	}
}

func (st *SlotTable) doReload() error {
	cmd, _ := resp.NewCommand("CLUSTER", "SLOTS")
	queryBytes := cmd.Format()
	for _, server := range st.startupNodes {
		cs, err := st.connPool.GetConn(server)
		if err != nil {
			log.Error(err)
			continue
		}
		defer cs.Close()
		_, err = cs.Write(queryBytes)
		if err != nil {
			log.Print(err)
			continue
		}
		data, err := resp.ReadData(cs)
		if err != nil {
			log.Print(err)
			continue
		}
		slotInfos := make([]*slotInfo, 0, len(data.Array))
		for _, info := range data.Array {
			si, err := st.convSlotInfo(info)
			if err != nil {
				// should never happen
				log.Fatalf("conv slot info failed")
			}
			slotInfos = append(slotInfos, si)
		}
		st.setBatch(slotInfos)
		return nil
	}
	return errAllNodesFailed
}

func (st *SlotTable) Get(slot int) string {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.slotServers[slot]
}

func (st *SlotTable) Set(slot int, server string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.slotServers[slot] = server
}

func (st *SlotTable) setBatch(slotInfos []*slotInfo) {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, si := range slotInfos {
		for i := si.start; i <= si.end; i++ {
			st.slotServers[i] = si.master
			log.Debugf("slot: %d server:%s", i, si.master)
		}
	}
}

func (st *SlotTable) Reload() {
	select {
	case st.reloadChan <- struct{}{}:
		log.Debugf("schedule reload")
	default:
		log.Debugf("drop reload req")
	}
}

func (st *SlotTable) Init() error {
	return st.doReload()
}

func (st *SlotTable) convSlotInfo(data *resp.Data) (*slotInfo, error) {
	if len(data.Array) != 3 || len(data.Array[2].Array) < 2 {
		log.Error(data.Array)
		return nil, errInvalidSlotInfo
	}
	host := string(data.Array[2].Array[0].String)
	if len(host) == 0 {
		host = "127.0.0.1"
	}
	si := &slotInfo{
		start:  int(data.Array[0].Integer),
		end:    int(data.Array[1].Integer),
		master: fmt.Sprintf("%s:%d", host, int(data.Array[2].Array[1].Integer)),
	}
	return si, nil
}
