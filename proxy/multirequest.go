package proxy

import (
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	resp "github.com/collinmsn/stvpresp"
)

const (
	MGET = iota
	MSET
	DEL
)

/*
multi key cmd被拆分成numKeys个子请求按普通的pipeline request发送，最后在写出response时进行合并
当最后一个子请求的response到来时，整个multi key cmd完成，拼接最终response并写出

只要有一个子请求失败，都认定整个请求失败
多个子请求共享一个request sequence number

请求的失败包含两种类型：1、网络失败，比如读取超时，2，请求错误，比如本来该在A机器上，请求到了B机器上，表现为response type为error
*/
type MultiRequest struct {
	cmd               resp.Command
	cmdType           int
	numSubCmds        int
	numPendingSubCmds int
	subCmdRsps        []*PipelineResponse
}

func NewMultiRequest(cmd resp.Command, op string, numSubCmds int) *MultiRequest {
	mc := &MultiRequest{
		cmd:               cmd,
		numSubCmds:        numSubCmds,
		numPendingSubCmds: numSubCmds,
	}
	switch op {
	case "MGET":
		mc.cmdType = MGET
	case "MSET":
		mc.cmdType = MSET
	case "DEL":
		mc.cmdType = DEL
	default:
		panic("not multi key command")
	}
	mc.subCmdRsps = make([]*PipelineResponse, numSubCmds)
	return mc
}

func (mc *MultiRequest) OnSubCmdFinished(rsp *PipelineResponse) {
	mc.subCmdRsps[rsp.req.subSeq] = rsp
	mc.numPendingSubCmds--
}

func (mc *MultiRequest) Finished() bool {
	return mc.numPendingSubCmds == 0
}

func (mc *MultiRequest) CoalesceRsp() *PipelineResponse {
	plRsp := &PipelineResponse{}
	/*
		var rsp *resp.Data
		switch mc.CmdType() {
		case MGET:
			rsp = &resp.Data{T: resp.T_Array, Array: make([]*resp.Data, mc.numSubCmds)}
		case MSET:
			rsp = OK_DATA
		case DEL:
			rsp = &resp.Data{T: resp.T_Integer}
		default:
			panic("invalid multi key cmd name")
		}
		for i, subCmdRsp := range mc.subCmdRsps {
			if subCmdRsp.err != nil {
				rsp = &resp.Data{T: resp.T_Error, String: []byte(subCmdRsp.err.Error())}
				break
			}
			reader := bufio.NewReader(bytes.NewReader(subCmdRsp.obj.Raw()))
			data, err := resp.ReadData(reader)
			if err != nil {
				log.Errorf("re-parse response err=%s", err)
				rsp = &resp.Data{T: resp.T_Error, String: []byte(err.Error())}
				break
			}
			if data.T == resp.T_Error {
				rsp = data
				break
			}
			switch mc.CmdType() {
			case MGET:
				rsp.Array[i] = data
			case MSET:
			case DEL:
				rsp.Integer += data.Integer
			default:
				panic("invalid multi key cmd name")
			}
		}
		plRsp.obj = resp.NewObjectFromData(rsp)
	*/
	return plRsp
}

func (mc *MultiRequest) CmdType() int {
	return mc.cmdType
}

func IsMultiCmd(op string, cmd *redis.Resp) (multiKey bool, numKeys int) {
	multiKey = true
	switch op {
	case "MGET":
		numKeys = len(cmd.Array) - 1
	case "MSET":
		numKeys = (len(cmd.Array) - 1) / 2
	case "DEL":
		numKeys = len(cmd.Array) - 1
	default:
		multiKey = false
	}
	return
}
