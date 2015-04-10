package proxy

import (
	"sync"

	"github.com/collinmsn/resp"
)

type PipelineRequest struct {
	cmd   *resp.Command
	slot  int   // key slot
	seq   int64 // session wide request sequence number
	backQ chan *PipelineResponse
	wg    *sync.WaitGroup // session wide wait group
}

type PipelineResponse struct {
	rsp *resp.Object
	ctx *PipelineRequest
	err error
}

type PipelineResponseHeap []*PipelineResponse

func (h PipelineResponseHeap) Len() int {
	return len(h)
}
func (h PipelineResponseHeap) Less(i, j int) bool {
	return h[i].ctx.seq < h[j].ctx.seq
}
func (h PipelineResponseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *PipelineResponseHeap) Push(x interface{}) {
	*h = append(*h, x.(*PipelineResponse))
}
func (h *PipelineResponseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Peek will return the heap top element
func (h PipelineResponseHeap) Top() *PipelineResponse {
	if h.Len() == 0 {
		return nil
	}
	return h[0]
}
