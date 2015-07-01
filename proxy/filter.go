package proxy

import (
	"github.com/collinmsn/resp"
)

var blackList = []string{
	// cluster commands
	"CLUSTER",
	// keys commands
	"KEYS",
	"MIGRATE",
	"MOVE",
	"OBJECT",
	"RANDOMKEY",
	"RENAME",
	"RENAMENX",
	"SCAN",
	// strings commands
	"BITOP",
	"MSETNX",
	// hashes commands
	// lists commands
	"BLPOP",
	"BRPOP",
	"BRPOPLPUSH",
	// sets commands
	// sorted sets commands
	// hyperloglog commands
	// pub/sub commands
	"PSUBSCRIBE",
	"PUBLISH",
	"PUNSUBSCRIBE",
	"SUBSCRIBE",
	"UNSUBSCRIBE",
	// transactions commands
	"DISCARD",
	"EXEC",
	"MULTI",
	"UNWATCH",
	"WATCH",
	// scripting commands
	"SCRIPT",
	// connection commands
	"AUTH",
	"PING",
	"ECHO",
	"QUIT",
	"SELECT",
	// server commands
	"BGREWRITEAOF",
	"BGSAVE",
	"CLIENT",
	"CONFIG",
	"DBSIZE",
	"DEBUG",
	"FLUSHALL",
	"FLUSHDB",
	"INFO",
	"LASTSAVE",
	"MONITOR",
	"SAVE",
	"SHUTDOWN",
	"SLAVEOF",
	"SLOWLOG",
	"SYNC",
	"TIME",
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
