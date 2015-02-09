package main

import (
	"github.com/artyom/autoflags"
	"github.com/collinmsn/rcproxy/pkg"
	"time"
	"flag"
	"log"
	"strings"
	"os"
	"os/signal"
)

var config = struct {
		Port                int `flag:"port, listen port"`
		StartupNodes        string `flag:"startup-nodes,startup nodes"`
		ConnectTimeout      time.Duration `flag:"connect-timeout, connect to backend timeout"`
		ReadTimeout         time.Duration `flag:"read-timeout, backend read timeout"`
		SlotsUpdateInterval time.Duration `flag:"slots-update-interval, slots update interval"`
	}{
	Port: 8088,
}

func main() {
	if err := autoflags.Define(&config) ; err != nil {
		log.Fatal(err)
	}
	flag.Parse()
	log.Print(config)
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, os.Kill)

	startupNodes := strings.Split(config.StartupNodes, ",")
	exitChan := make(chan struct{})
	slotMap := pkg.NewSlotMap()
	proxy := pkg.NewProxy(config.Port, config.ConnectTimeout, config.ReadTimeout, slotMap, exitChan)
	go proxy.Run()
	slotUpdater := pkg.NewSlotUpdater(startupNodes, config.SlotsUpdateInterval, slotMap, exitChan)
	go slotUpdater.Run()
	<-sigChan
	close(exitChan)
}
