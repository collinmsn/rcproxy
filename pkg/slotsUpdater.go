package pkg

import "time"

type SlotUpdater struct {
	startupNodes   []string
	updateInterval time.Duration
	slotMap *SlotMap
	exitChan       chan struct{}
}

func NewSlotUpdater(startupNodes []string, updateInterval time.Duration, slotMap *SlotMap, exitChan chan struct{}) *SlotUpdater {
	su := &SlotUpdater{
		startupNodes: startupNodes,
		updateInterval:updateInterval,
		slotMap: slotMap,
		exitChan: exitChan,
	}
	return su

}

func (su *SlotUpdater) Run() {
	for {
		select {
		case <-su.exitChan:
			return
		case <-time.Tick(su.updateInterval):
		}
	}
}
