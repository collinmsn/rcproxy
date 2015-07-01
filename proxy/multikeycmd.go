package proxy

import (
	"bufio"
	"bytes"
	"github.com/collinmsn/resp"
	log "github.com/ngaut/logging"
)

const (
	MGET = "MGET"
	MSET = "MSET"
	DEL  = "DEL"
)

var (
	OK_DATA *resp.Data
)

func init() {
	OK_DATA = &resp.Data{
		T:      resp.T_SimpleString,
		String: []byte("OK"),
	}
}

type MultiKeyCmd struct {
	cmd               *resp.Command
	numSubCmds        int
	numPendingSubCmds int
	subCmdRsps        []*PipelineResponse
}

func NewMultiKeyCmd(cmd *resp.Command, numSubCmds int) *MultiKeyCmd {
	mc := &MultiKeyCmd{
		cmd:               cmd,
		numSubCmds:        numSubCmds,
		numPendingSubCmds: numSubCmds,
	}
	mc.subCmdRsps = make([]*PipelineResponse, numSubCmds)
	return mc
}

func (mc *MultiKeyCmd) FinishSubCmd(rsp *PipelineResponse) {
	mc.subCmdRsps[rsp.ctx.subSeq] = rsp
	mc.numPendingSubCmds--
}

func (mc *MultiKeyCmd) Finished() bool {
	return mc.numPendingSubCmds == 0
}

func (mc *MultiKeyCmd) BuildRsp() *PipelineResponse {
	plRsp := &PipelineResponse{}
	switch mc.cmd.Name() {
	case MGET:
		rsp := &resp.Data{T: resp.T_Array, Array: make([]*resp.Data, mc.numSubCmds)}
		for i, subCmdRsp := range mc.subCmdRsps {
			if subCmdRsp.err != nil {
				rsp.Array[i] = &resp.Data{T: resp.T_BulkString, IsNil: true}
			} else {
				// TODO: optimize it
				reader := bufio.NewReader(bytes.NewReader(subCmdRsp.rsp.Raw()))
				if data, err := resp.ReadData(reader); err != nil {
					log.Error(err)
					rsp.Array[i] = &resp.Data{T: resp.T_BulkString, IsNil: true}
				} else {
					rsp.Array[i] = data
				}
			}
		}
		plRsp.rsp = resp.NewObjectFromData(rsp)
	case MSET:
		var err error
		for _, subCmdRsp := range mc.subCmdRsps {
			if subCmdRsp.err != nil {
				err = subCmdRsp.err
				break
			}
		}
		if err != nil {
			rsp := &resp.Data{T: resp.T_Error, String: []byte(err.Error())}
			plRsp.rsp = resp.NewObjectFromData(rsp)
		} else {
			plRsp.rsp = resp.NewObjectFromData(OK_DATA)
		}
	case DEL:
		rsp := &resp.Data{T: resp.T_Integer}
		for _, subCmdRsp := range mc.subCmdRsps {
			if subCmdRsp.err != nil {
				continue
			}
			reader := bufio.NewReader(bytes.NewReader(subCmdRsp.rsp.Raw()))
			data, err := resp.ReadData(reader)
			if err != nil {
				log.Error(err)
			} else {
				rsp.Integer += data.Integer
			}
		}
		plRsp.rsp = resp.NewObjectFromData(rsp)
	}
	return plRsp
}

func IsMultiCmd(cmd *resp.Command) (multiKey bool, numKeys int) {
	multiKey = true
	switch cmd.Name() {
	case MGET:
		numKeys = len(cmd.Args) - 1
	case MSET:
		numKeys = (len(cmd.Args) - 1) / 2
	case DEL:
		numKeys = len(cmd.Args) - 1
	default:
		multiKey = false
	}
	return
}
