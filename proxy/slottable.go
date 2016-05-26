package proxy

import "bytes"

const (
	NumSlots                   = 16384
	CLUSTER_SLOTS_START        = 0
	CLUSTER_SLOTS_END          = 1
	CLUSTER_SLOTS_SERVER_START = 2
)

// ServerGroup根据cluster slots和ReadPrefer得出
type ServerGroup struct {
	write string
	read  []string
}

type SlotTable struct {
	serverGroups []*ServerGroup
	// a cheap way to random select read backend
	counter uint32
}

func NewSlotTable() *SlotTable {
	st := &SlotTable{
		serverGroups: make([]*ServerGroup, NumSlots),
	}
	return st
}

func (st *SlotTable) WriteServer(slot int) string {
	return st.serverGroups[slot].write
}

func (st *SlotTable) ReadServer(slot int) string {
	st.counter += 1
	readServers := st.serverGroups[slot].read
	return readServers[st.counter%uint32(len(readServers))]
}

func (st *SlotTable) SetSlotInfo(si *SlotInfo) {
	for i := si.start; i <= si.end; i++ {
		st.serverGroups[i] = &ServerGroup{
			write: si.write,
			read:  si.read,
		}
	}
}

type SlotInfo struct {
	start int
	end   int
	write string
	read  []string
}

func Key2Slot(key []byte) int {
	if pos := bytes.IndexByte(key, '{'); pos != -1 {
		pos += 1
		if pos2 := bytes.IndexByte(key[pos:], '}'); pos2 > 0 {
			slot := CRC16(key[pos:pos+pos2]) % NumSlots
			return int(slot)
		}
	}
	slot := CRC16(key) % NumSlots
	return int(slot)
}
