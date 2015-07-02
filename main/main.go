package main

import (
	"flag"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/artyom/autoflags"
	"github.com/collinmsn/rcproxy/proxy"
	log "github.com/ngaut/logging"
)

var config = struct {
	//flag:"flagName,usage string"
	Addr                   string        `flag:"addr, proxy serving addr"`
	DebugAddr              string        `flag:"debug-addr, proxy debug listen address for pprof and set log level, default not enabled"`
	StartupNodes           string        `flag:"startup-nodes, startup nodes used to query cluster topology"`
	ConnectTimeout         time.Duration `flag:"connect-timeout, connect to backend timeout"`
	SlotsReloadInterval    time.Duration `flag:"slots-reload-interval, slots reload interval"`
	LogLevel               string        `flag:"log-level, log level eg. debug, info, warn, error, fatal and panic"`
	LogFile                string        `flag:"log-file, log file path"`
	LogEveryN              int           `flag:"log-every-n, output an access log for every N commands"`
	BackendIdleConnections int           `flag:"backend-idle-connections, max number of idle connections for each backend server"`
}{
	Addr:                   "0.0.0.0:8088",
	DebugAddr:              "",
	StartupNodes:           "127.0.0.1:7001",
	ConnectTimeout:         250 * time.Millisecond,
	SlotsReloadInterval:    3 * time.Second,
	LogLevel:               "info",
	LogFile:                "rcproxy.log",
	LogEveryN:              100,
	BackendIdleConnections: 5,
}

func handleSetLogLevel(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	level := r.Form.Get("level")
	log.SetLevelByString(level)
	log.Info("set log level to ", level)
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte("OK"))
}

func main() {
	if err := autoflags.Define(&config); err != nil {
		log.Fatal(err)
	}
	flag.Parse()
	log.SetLevelByString(config.LogLevel)
	// to avoid pprof being optimized by gofmt
	log.Debug(pprof.Handler("profile"))
	if len(config.LogFile) != 0 {
		log.SetOutputByName(config.LogFile)
		log.SetRotateByDay()
	}
	if config.LogEveryN <= 0 {
		proxy.LogEveryN = 1
	} else {
		var logEveryN interface{}
		logEveryN = uint32(config.LogEveryN)
		if n, ok := logEveryN.(uint32); ok {
			proxy.LogEveryN = n
		} else {
			log.Fatalf("invalid value for param log-every-n", config.LogEveryN)
		}
	}
	log.Infof("%#v", config)
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	log.Infof("pid %d", os.Getpid())
	if len(config.DebugAddr) != 0 {
		http.HandleFunc("/setloglevel", handleSetLogLevel)
		go func() {
			log.Fatal(http.ListenAndServe(config.DebugAddr, nil))
		}()
		log.Infof("debug service listens on %s", config.DebugAddr)
	}

	// shuffle startup nodes
	startupNodes := strings.Split(config.StartupNodes, ",")
	indexes := rand.Perm(len(startupNodes))
	for i, startupNode := range startupNodes {
		startupNodes[i] = startupNodes[indexes[i]]
		startupNodes[indexes[i]] = startupNode
	}
	connPool := proxy.NewConnPool(config.BackendIdleConnections, config.ConnectTimeout)
	dispatcher := proxy.NewDispatcher(startupNodes, config.SlotsReloadInterval, connPool)
	if err := dispatcher.InitSlotTable(); err != nil {
		log.Fatal(err)
	}
	proxy := proxy.NewProxy(config.Addr, dispatcher, connPool)
	go proxy.Run()
	sig := <-sigChan
	log.Infof("terminated by %#v", sig)
	proxy.Exit()
}
