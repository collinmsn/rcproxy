package proxy

import (
	"github.com/walu/resp"
	"sync"
)

type PipelineRequest struct {
	cmd   *resp.Command
	seq   int64
	backQ chan *PipelineResponse
	wg    *sync.WaitGroup
}

type PipelineResponse struct {
	rsp *resp.Data
	ctx *PipelineRequest
	err error
}
