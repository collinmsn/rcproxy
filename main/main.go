package main

import (
	"flag"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
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
	LogEveryN              uint64        `flag:"log-every-n, output an access log for every N commands"`
	BackendIdleConnections int           `flag:"backend-idle-connections, max number of idle connections for each backend server"`
	ReadPrefer             int           `flag:"read-prefer, where read command to send to, valid options are: 0: READ_PREFER_MASTER, 1: READ_PREFER_SLAVE, 2: READ_PREFER_SLAVE_IDC"`
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
	ReadPrefer:             proxy.READ_PREFER_MASTER,
}

func handleSetLogLevel(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	level := r.Form.Get("level")
	log.SetLevelByString(level)
	log.Info("set log level to ", level)
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte("OK"))
}

func initRandSeed() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	initRandSeed()
	autoflags.Define(&config)
	flag.Parse()
	log.SetLevelByString(config.LogLevel)
	log.SetFlags(log.Ldate | log.Lshortfile | log.Lmicroseconds)
	if len(config.LogFile) != 0 {
		log.SetOutputByName(config.LogFile)
		log.SetHighlighting(false)
		log.SetRotateByDay()
	}
	if config.LogEveryN <= 0 {
		proxy.LogEveryN = 1
	} else {
		proxy.LogEveryN = config.LogEveryN
	}
	log.Info(config)
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
	for i := len(startupNodes) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		startupNodes[i], startupNodes[j] = startupNodes[j], startupNodes[i]
	}

	connPool := proxy.NewConnPool(config.BackendIdleConnections, config.ConnectTimeout, config.ReadPrefer != proxy.READ_PREFER_MASTER)
	dispatcher := proxy.NewDefaultDispatcher(startupNodes, config.SlotsReloadInterval, connPool, config.ReadPrefer)
	if err := dispatcher.InitSlotTable(); err != nil {
		log.Fatal(err)
	}
	proxy := proxy.NewProxy(config.Addr, dispatcher, connPool)
	go proxy.Run()
	sig := <-sigChan
	log.Infof("terminated by %#v", sig)
	proxy.Exit()
}
