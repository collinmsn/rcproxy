package proxy

import (
	"sync"

	"github.com/walu/resp"
)

type PipelineRequest struct {
	cmd   *resp.Command
	slot  int
	seq   int64
	backQ chan *PipelineResponse
	wg    *sync.WaitGroup
}

type PipelineResponse struct {
	rsp *resp.Data
	ctx *PipelineRequest
	err error
}
