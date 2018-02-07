package main

import (
	"errors"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"

	httpfrontend "github.com/chihaya/chihaya/frontend/http"
	udpfrontend "github.com/chihaya/chihaya/frontend/udp"
	"github.com/chihaya/chihaya/middleware"
	"github.com/chihaya/chihaya/storage"
	"github.com/chihaya/chihaya/storage/memory"
	"github.com/chihaya/chihaya/storage/redis"
)

func rootCmdRun(cmd *cobra.Command, args []string) error {
	debugLog, _ := cmd.Flags().GetBool("debug")
	if debugLog {
		log.SetLevel(log.DebugLevel)
		log.Debugln("debug logging enabled")
	}
	cpuProfilePath, _ := cmd.Flags().GetString("cpuprofile")
	if cpuProfilePath != "" {
		log.Infoln("enabled CPU profiling to", cpuProfilePath)
		f, err := os.Create(cpuProfilePath)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	configFilePath, _ := cmd.Flags().GetString("config")
	log.Debugf("configFilePath: %s", configFilePath)
	configFile, err := ParseConfigFile(configFilePath)
	if err != nil {
		return errors.New("failed to read config: " + err.Error())
	}
	cfg := configFile.MainConfigBlock
	log.Debugf("Main config block: %v", cfg)
	go func() {
		promServer := http.Server{
			Addr:    cfg.PrometheusAddr,
			Handler: prometheus.Handler(),
		}
		log.Infoln("started serving prometheus stats on", cfg.PrometheusAddr)
		if err := promServer.ListenAndServe(); err != nil {
			log.Fatalln("failed to start prometheus server:", err.Error())
		}
	}()

	var peerStore storage.PeerStore
	// Force the compiler to enforce memory against the storage interface.
	if cfg.Storage.GarbageCollectionInterval > 0 {
		peerStore, err = memory.New(cfg.Storage)
		if err != nil {
			return errors.New("failed to create memory storage: " + err.Error())
		}
	} else if cfg.RedisStorage.GarbageCollectionInterval > 0 {
		peerStore, err = redis.New(cfg.RedisStorage)
		if err != nil {
			return errors.New("failed to create redis storage: " + err.Error())
		}
	} else {
		return errors.New("invalid storage configuration.")
	}

	preHooks, postHooks, err := configFile.CreateHooks()
	if err != nil {
		return errors.New("failed to create hooks: " + err.Error())
	}

	logic := middleware.NewLogic(cfg.Config, peerStore, preHooks, postHooks)
	if err != nil {
		return errors.New("failed to create TrackerLogic: " + err.Error())
	}

	shutdown := make(chan struct{})
	errChan := make(chan error)

	var httpFrontend *httpfrontend.Frontend
	var udpFrontend *udpfrontend.Frontend

	if cfg.HTTPConfig.Addr != "" {
		httpFrontend = httpfrontend.NewFrontend(logic, cfg.HTTPConfig)

		go func() {
			log.Infoln("started serving HTTP on", cfg.HTTPConfig.Addr)
			if err := httpFrontend.ListenAndServe(); err != nil {
				errChan <- errors.New("failed to cleanly shutdown HTTP frontend: " + err.Error())
			}
		}()
	}

	if cfg.UDPConfig.Addr != "" {
		udpFrontend = udpfrontend.NewFrontend(logic, cfg.UDPConfig)

		go func() {
			log.Infoln("started serving UDP on", cfg.UDPConfig.Addr)
			if err := udpFrontend.ListenAndServe(); err != nil {
				errChan <- errors.New("failed to cleanly shutdown UDP frontend: " + err.Error())
			}
		}()
	}

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigChan:
		case <-shutdown:
		}

		if udpFrontend != nil {
			udpFrontend.Stop()
		}

		if httpFrontend != nil {
			httpFrontend.Stop()
		}

		for err := range peerStore.Stop() {
			if err != nil {
				errChan <- err
			}
		}

		// Stop hooks.
		errs := logic.Stop()
		for _, err := range errs {
			errChan <- err
		}

		close(errChan)
	}()

	closed := false
	var bufErr error
	for err = range errChan {
		if err != nil {
			if !closed {
				close(shutdown)
				closed = true
			} else {
				log.Infoln(bufErr)
			}
			bufErr = err
		}
	}

	return bufErr
}

func main() {
	var rootCmd = &cobra.Command{
		Use:   "chihaya",
		Short: "BitTorrent Tracker",
		Long:  "A customizible, multi-protocol BitTorrent Tracker",
		Run: func(cmd *cobra.Command, args []string) {
			if err := rootCmdRun(cmd, args); err != nil {
				log.Fatal(err)
			}
		},
	}
	rootCmd.Flags().String("config", "/etc/chihaya.yaml", "location of configuration file")
	rootCmd.Flags().String("cpuprofile", "", "location to save a CPU profile")
	rootCmd.Flags().Bool("debug", false, "enable debug logging")

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
