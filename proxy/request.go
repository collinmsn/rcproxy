package proxy

import (
	"sync"

	"github.com/collinmsn/resp"
)

type Request struct {
	// if request has finished
	*sync.WaitGroup
	// request command
	cmd *resp.Command
	// response object
	obj *resp.Object
	// response error
	err error
	// if it is readOnly command
	readOnly bool
	// key slot
	slot int
	// for multi key command
	parentCmd *MultiRequest
	// sub sequence number for multi key command
	subSeq int
}
