package main

import (
	"flag"
	"fmt"
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
	Port                   int           `flag:"port, proxy serving port"`
	DebugPort              int           `flag:"debug-port, proxy debug port for pprof and set log level, default port+1000, set it to -1 to disable debug service"`
	StartupNodes           string        `flag:"startup-nodes, startup nodes used to query cluster topology"`
	ConnectTimeout         time.Duration `flag:"connect-timeout, connect to backend timeout"`
	ReadTimeout            time.Duration `flag:"read-timeout, read from backend timeout"`
	SlotsReloadInterval    time.Duration `flag:"slots-reload-interval, slots reload interval"`
	LogLevel               string        `flag:"log-level, log level eg. debug, info, warn, error, fatal and panic"`
	BackendIdleConnections int           `flag:"backend-idle-connections, max number of idle connections for each backend server"`
}{
	Port:                   8088,
	DebugPort:              0,
	StartupNodes:           "127.0.0.1:7001",
	ConnectTimeout:         1 * time.Second,
	ReadTimeout:            1 * time.Second,
	SlotsReloadInterval:    3 * time.Second,
	LogLevel:               "info",
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
	log.Infof("%#v", config)
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	log.Infof("pid %d", os.Getpid())
	if config.DebugPort != -1 {
		if config.DebugPort == 0 {
			config.DebugPort = config.Port + 1000
		}
		debugAddr := fmt.Sprintf(":%d", config.DebugPort)
		http.HandleFunc("/setloglevel", handleSetLogLevel)
		go func() {
			log.Fatal(http.ListenAndServe(debugAddr, nil))
		}()
		log.Infof("debug service listens on port %d", config.DebugPort)
	}

	startupNodes := strings.Split(config.StartupNodes, ",")
	connPool := proxy.NewConnPool(config.BackendIdleConnections, config.ConnectTimeout)
	dispatcher := proxy.NewDispatcher(startupNodes, config.SlotsReloadInterval, connPool)
	if err := dispatcher.InitSlotTable(); err != nil {
		log.Fatal(err)
	}
	proxy := proxy.NewProxy(config.Port, config.ReadTimeout, dispatcher, connPool)
	go proxy.Run()
	sig := <-sigChan
	log.Infof("terminated by %#v", sig)
	proxy.Exit()
}
