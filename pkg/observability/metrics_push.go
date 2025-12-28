package observability

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"

	"github.com/justjake/pglink/pkg/config"
)

// MetricsPusher handles push-based metrics export via Prometheus remote write.
// It collects metrics from prometheus.DefaultGatherer and pushes them to a
// Prometheus remote write endpoint.
type MetricsPusher struct {
	endpoint     string
	pushInterval time.Duration
	client       *http.Client
	gatherer     prometheus.Gatherer
	logger       *slog.Logger
	extraLabels  map[string]string

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewMetricsPusher creates a new MetricsPusher that exports metrics via Prometheus remote write.
// The endpoint should be the Prometheus remote write endpoint (e.g., "localhost:19090").
func NewMetricsPusher(ctx context.Context, cfg *config.PrometheusConfig, logger *slog.Logger) (*MetricsPusher, error) {
	if cfg == nil || cfg.Push == nil {
		return nil, nil
	}

	endpoint := cfg.Push.Endpoint
	if endpoint == "" {
		return nil, fmt.Errorf("push endpoint is required")
	}

	// Build the remote write URL
	remoteWriteURL := fmt.Sprintf("http://%s/api/v1/write", endpoint)

	// Get push interval
	pushInterval := 10 * time.Second
	if cfg.Push.PushInterval > 0 {
		pushInterval = time.Duration(cfg.Push.PushInterval)
	}

	mp := &MetricsPusher{
		endpoint:     remoteWriteURL,
		pushInterval: pushInterval,
		client:       &http.Client{Timeout: 30 * time.Second},
		gatherer:     prometheus.DefaultGatherer,
		logger:       logger,
		extraLabels:  cfg.ExtraLabels,
		stopCh:       make(chan struct{}),
	}

	// Start the push loop
	mp.wg.Add(1)
	go mp.pushLoop(ctx)

	return mp, nil
}

// pushLoop periodically collects and pushes metrics.
func (mp *MetricsPusher) pushLoop(ctx context.Context) {
	defer mp.wg.Done()

	ticker := time.NewTicker(mp.pushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final push before exit
			mp.push(context.Background())
			return
		case <-mp.stopCh:
			// Final push before exit
			mp.push(context.Background())
			return
		case <-ticker.C:
			mp.push(ctx)
		}
	}
}

// push collects metrics and sends them to the remote write endpoint.
func (mp *MetricsPusher) push(ctx context.Context) {
	// Gather metrics from prometheus registry
	mfs, err := mp.gatherer.Gather()
	if err != nil {
		mp.logger.Warn("failed to gather metrics", "error", err)
		return
	}

	// Convert to remote write format
	timeSeries := mp.metricsToTimeSeries(mfs)
	if len(timeSeries) == 0 {
		return
	}

	// Create write request
	req := &prompb.WriteRequest{
		Timeseries: timeSeries,
	}

	// Serialize and compress
	data, err := proto.Marshal(req)
	if err != nil {
		mp.logger.Warn("failed to marshal write request", "error", err)
		return
	}

	compressed := snappy.Encode(nil, data)

	// Send to remote write endpoint
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, mp.endpoint, bytes.NewReader(compressed))
	if err != nil {
		mp.logger.Warn("failed to create request", "error", err)
		return
	}

	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := mp.client.Do(httpReq)
	if err != nil {
		mp.logger.Warn("failed to send metrics", "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		mp.logger.Warn("remote write failed",
			"status", resp.StatusCode,
			"body", string(body))
		return
	}

	mp.logger.Debug("pushed metrics",
		"timeseries", len(timeSeries),
		"bytes", len(compressed))
}

// metricsToTimeSeries converts prometheus metric families to remote write time series.
func (mp *MetricsPusher) metricsToTimeSeries(mfs []*dto.MetricFamily) []prompb.TimeSeries {
	var result []prompb.TimeSeries
	now := time.Now().UnixMilli()

	for _, mf := range mfs {
		name := mf.GetName()

		for _, m := range mf.GetMetric() {
			// Build labels
			labels := []prompb.Label{
				{Name: "__name__", Value: name},
			}

			// Add metric labels
			for _, lp := range m.GetLabel() {
				labels = append(labels, prompb.Label{
					Name:  lp.GetName(),
					Value: lp.GetValue(),
				})
			}

			// Add extra labels from config
			for k, v := range mp.extraLabels {
				labels = append(labels, prompb.Label{
					Name:  k,
					Value: v,
				})
			}

			// Convert based on metric type
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				if c := m.GetCounter(); c != nil {
					result = append(result, prompb.TimeSeries{
						Labels:  labels,
						Samples: []prompb.Sample{{Value: c.GetValue(), Timestamp: now}},
					})
				}
			case dto.MetricType_GAUGE:
				if g := m.GetGauge(); g != nil {
					result = append(result, prompb.TimeSeries{
						Labels:  labels,
						Samples: []prompb.Sample{{Value: g.GetValue(), Timestamp: now}},
					})
				}
			case dto.MetricType_SUMMARY:
				if s := m.GetSummary(); s != nil {
					// Sum
					sumLabels := append([]prompb.Label{{Name: "__name__", Value: name + "_sum"}}, labels[1:]...)
					result = append(result, prompb.TimeSeries{
						Labels:  sumLabels,
						Samples: []prompb.Sample{{Value: s.GetSampleSum(), Timestamp: now}},
					})
					// Count
					countLabels := append([]prompb.Label{{Name: "__name__", Value: name + "_count"}}, labels[1:]...)
					result = append(result, prompb.TimeSeries{
						Labels:  countLabels,
						Samples: []prompb.Sample{{Value: float64(s.GetSampleCount()), Timestamp: now}},
					})
					// Quantiles
					for _, q := range s.GetQuantile() {
						qLabels := make([]prompb.Label, len(labels))
						copy(qLabels, labels)
						qLabels = append(qLabels, prompb.Label{
							Name:  "quantile",
							Value: fmt.Sprintf("%g", q.GetQuantile()),
						})
						result = append(result, prompb.TimeSeries{
							Labels:  qLabels,
							Samples: []prompb.Sample{{Value: q.GetValue(), Timestamp: now}},
						})
					}
				}
			case dto.MetricType_HISTOGRAM:
				if h := m.GetHistogram(); h != nil {
					// Sum
					sumLabels := append([]prompb.Label{{Name: "__name__", Value: name + "_sum"}}, labels[1:]...)
					result = append(result, prompb.TimeSeries{
						Labels:  sumLabels,
						Samples: []prompb.Sample{{Value: h.GetSampleSum(), Timestamp: now}},
					})
					// Count
					countLabels := append([]prompb.Label{{Name: "__name__", Value: name + "_count"}}, labels[1:]...)
					result = append(result, prompb.TimeSeries{
						Labels:  countLabels,
						Samples: []prompb.Sample{{Value: float64(h.GetSampleCount()), Timestamp: now}},
					})
					// Buckets
					for _, b := range h.GetBucket() {
						bLabels := make([]prompb.Label, len(labels))
						copy(bLabels, labels)
						bLabels[0] = prompb.Label{Name: "__name__", Value: name + "_bucket"}
						bLabels = append(bLabels, prompb.Label{
							Name:  "le",
							Value: fmt.Sprintf("%g", b.GetUpperBound()),
						})
						result = append(result, prompb.TimeSeries{
							Labels:  bLabels,
							Samples: []prompb.Sample{{Value: float64(b.GetCumulativeCount()), Timestamp: now}},
						})
					}
				}
			case dto.MetricType_UNTYPED:
				if u := m.GetUntyped(); u != nil {
					result = append(result, prompb.TimeSeries{
						Labels:  labels,
						Samples: []prompb.Sample{{Value: u.GetValue(), Timestamp: now}},
					})
				}
			}
		}
	}

	return result
}

// Shutdown gracefully shuts down the metrics pusher, flushing any pending metrics.
func (mp *MetricsPusher) Shutdown(ctx context.Context) error {
	if mp == nil {
		return nil
	}

	close(mp.stopCh)

	// Wait for push loop to finish with timeout
	done := make(chan struct{})
	go func() {
		mp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Enabled returns true if metrics pushing is enabled.
func (mp *MetricsPusher) Enabled() bool {
	return mp != nil
}
