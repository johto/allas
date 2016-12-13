package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

type PrometheusConfig struct {
	Enabled bool
	Listen ListenConfig

	registry *prometheus.Registry
}

func (c *PrometheusConfig) RegisterMetricsCollector(coll prometheus.Collector) error {
	if c.registry == nil {
		panic("registry not initialized")
	}
	return c.registry.Register(coll)
}

type elogWrapper struct {
}

func (w elogWrapper) Println(v ...interface{}) {
	elog.Warningf("Prometheus handler error: %s", fmt.Sprintln(v...))
}

var MetricNotificationsReceived prometheus.Counter

func (cfg *PrometheusConfig) InitializeMetrics(r *prometheus.Registry) error {
	MetricNotificationsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "allas",
		Name: "notifications_received_total",
		Help: "how many notifications have been received from the PostgreSQL server so far",
	})
	err := r.Register(MetricNotificationsReceived)
	if err != nil {
		return err
	}

	return nil
}

func (cfg *PrometheusConfig) Setup() error {
	elogWrapper := elogWrapper{}
	registry := prometheus.NewPedanticRegistry()
	err := cfg.InitializeMetrics(registry)
	if err != nil {
		return err
	}
	cfg.registry = registry

	metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog: elogWrapper,
	})
	muxer := http.NewServeMux()
	muxer.Handle("/metrics", metricsHandler)
	s := &http.Server{
		Handler: muxer,
	}
	l, err := cfg.Listen.Listen()
	if err != nil {
		return err
	}
	go func() {
		elog.Fatalf("Prometheus HTTP endpoint failed: %s", s.Serve(l))
	}()
	return nil
}
