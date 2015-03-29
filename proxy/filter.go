package proxy

import (
	"github.com/collinmsn/resp"
)

var blackList = []string{
	"CLUSTER",
	"SELECT",
	"KEYS", "MOVE", "OBJECT", "RENAME", "RENAMENX", "SORT", "SCAN", "BITOP",
	"MSETNX", "SCAN",
	"BLPOP", "BRPOP", "BRPOPLPUSH", "PSUBSCRIBE，PUBLISH", "PUNSUBSCRIBE", "SUBSCRIBE", "RANDOMKEY",
	"UNSUBSCRIBE", "DISCARD", "EXEC", "MULTI", "UNWATCH", "WATCH", "SCRIPT EXISTS", "SCRIPT FLUSH", "SCRIPT KILL",
	"SCRIPT LOAD", "AUTH", "ECHO", "QUIT", "BGREWRITEAOF", "BGSAVE", "CLIENT KILL", "CLIENT LIST",
	"CONFIG GET", "CONFIG SET", "CONFIG RESETSTAT", "DBSIZE", "DEBUG OBJECT", "DEBUG SEGFAULT", "FLUSHALL", "FLUSHDB",
	"LASTSAVE", "MONITOR", "SAVE", "SHUTDOWN", "SLAVEOF", "SLOWLOG", "SYNC", "TIME", "SLOTSMGRTONE", "SLOTSMGRT",
	"SLOTSDEL",
}

var BlackListCmds = make(map[string]bool)

func init() {
	for _, cmd := range blackList {
		BlackListCmds[cmd] = true
	}
}

// filter return true if a
func IsBlackListCmd(cmd *resp.Command) bool {
	return BlackListCmds[cmd.Name()]
}

func IsMultiOpCmd(cmd *resp.Command) (multiKey bool, numKeys int) {
	multiKey = true
	switch cmd.Name() {
	case "MGET":
		numKeys = len(cmd.Args) - 1
	case "MSET":
		numKeys = (len(cmd.Args) - 1) / 2
	case "DEL":
		numKeys = len(cmd.Args) - 1
	default:
		multiKey = false
	}
	return
}
