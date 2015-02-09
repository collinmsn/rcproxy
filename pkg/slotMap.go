package pkg

import (
	"sync/atomic"
	"sync"
)

const (
	NumSlots = 16384
)

type Slot2ServerGroup map[int]*ServerGroup


type SlotMap struct {
	m  atomic.Value
	mu sync.Mutex // used only by writers
}

func NewSlotMap() *SlotMap {
	sm := &SlotMap{}
	sm.m.Store(make(Slot2ServerGroup))
	return sm
}

func (sm *SlotMap) ServerGroup(slot int) *ServerGroup {
	v := sm.m.Load().(Slot2ServerGroup)
	return v[slot]
}

func (sm *SlotMap) Update(slot2serverGroup Slot2ServerGroup) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m.Store(slot2serverGroup)
}

func (sm *SlotMap) Get() Slot2ServerGroup {
	v := sm.m.Load().(Slot2ServerGroup)
	return v
}


