// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/relabel"
	"github.com/prometheus/tsdb"
	"golang.org/x/time/rate"
)

// String constants for instrumentation.
const (
	namespace = "prometheus"
	subsystem = "remote_storage"
	queue     = "queue"

	// We track samples in/out and how long pushes take using an Exponentially
	// Weighted Moving Average.
	ewmaWeight          = 0.2
	shardUpdateDuration = 10 * time.Second

	// Allow 30% too many shards before scaling down.
	shardToleranceFraction = 0.3

	// Limit to 1 log event every 10s
	logRateLimit = 0.1
	logBurst     = 10
)

var (
	succeededSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "succeeded_samples_total",
			Help:      "Total number of samples successfully sent to remote storage.",
		},
		[]string{queue},
	)
	failedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failed_samples_total",
			Help:      "Total number of samples which failed on send to remote storage.",
		},
		[]string{queue},
	)
	droppedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "dropped_samples_total",
			Help:      "Total number of samples which were dropped due to the queue being full.",
		},
		[]string{queue},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "sent_batch_duration_seconds",
			Help:      "Duration of sample batch send calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{queue},
	)
	queueLength = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_length",
			Help:      "The number of processed samples queued to be sent to the remote storage.",
		},
		[]string{queue},
	)
	queueSentTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_last_sent_timestamp",
			Help:      "Timestamp of the last successful send by this queue.",
		},
		[]string{queue},
	)
	shardCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shard_capacity",
			Help:      "The capacity of each shard of the queue used for parallel sending to the remote storage.",
		},
		[]string{queue},
	)
	numShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "shards",
			Help:      "The number of shards used for parallel sending to the remote storage.",
		},
		[]string{queue},
	)
)

func init() {
	prometheus.MustRegister(succeededSamplesTotal)
	prometheus.MustRegister(failedSamplesTotal)
	prometheus.MustRegister(droppedSamplesTotal)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(queueLength)
	prometheus.MustRegister(queueSentTimestamp)
	prometheus.MustRegister(shardCapacity)
	prometheus.MustRegister(numShards)
}

// StorageClient defines an interface for sending a batch of samples to an
// external timeseries database.
type StorageClient interface {
	// Store stores the given samples in the remote storage.
	Store(context.Context, []byte) error
	// Name identifies the remote storage implementation.
	Name() string
}

// QueueManager manages a queue of samples to be sent to the Storage
// indicated by the provided StorageClient. Implements Writer interface
// used by WAL Watcher.
type QueueManager struct {
	logger log.Logger

	flushDeadline  time.Duration
	cfg            config.QueueConfig
	externalLabels model.LabelSet
	relabelConfigs []*config.RelabelConfig
	client         StorageClient
	queueName      string
	logLimiter     *rate.Limiter
	watcher        *WALWatcher
	sentTimestamp  prometheus.Gauge

	seriesMtx sync.Mutex
	series    map[uint64][]*prompb.Label

	shardsMtx   sync.Mutex
	shards      *shards
	numShards   int
	reshardChan chan int
	quit        chan struct{}
	wg          sync.WaitGroup

	samplesIn, samplesOut, samplesOutDuration *ewmaRate
	integralAccumulator                       float64
}

// NewQueueManager builds a new QueueManager.
func NewQueueManager(logger log.Logger, walDir string, samplesIn *ewmaRate, cfg config.QueueConfig, externalLabels model.LabelSet, relabelConfigs []*config.RelabelConfig, client StorageClient, flushDeadline time.Duration) *QueueManager {
	if logger == nil {
		logger = log.NewNopLogger()
	} else {
		logger = log.With(logger, "queue", client.Name())
	}
	t := &QueueManager{
		logger:         logger,
		flushDeadline:  flushDeadline,
		cfg:            cfg,
		externalLabels: externalLabels,
		relabelConfigs: relabelConfigs,
		client:         client,
		queueName:      client.Name(),

		series: make(map[uint64][]*prompb.Label),

		logLimiter:  rate.NewLimiter(logRateLimit, logBurst),
		numShards:   1,
		reshardChan: make(chan int),
		quit:        make(chan struct{}),

		samplesIn:          samplesIn,
		samplesOut:         newEWMARate(ewmaWeight, shardUpdateDuration),
		samplesOutDuration: newEWMARate(ewmaWeight, shardUpdateDuration),
	}

	t.sentTimestamp = queueSentTimestamp.WithLabelValues(t.queueName)
	t.watcher = NewWALWatcher(logger, t, walDir)
	t.shards = t.newShards(t.numShards)

	numShards.WithLabelValues(t.queueName).Set(float64(t.numShards))
	shardCapacity.WithLabelValues(t.queueName).Set(float64(t.cfg.Capacity))

	// Initialise counter labels to zero.
	sentBatchDuration.WithLabelValues(t.queueName)
	succeededSamplesTotal.WithLabelValues(t.queueName)
	failedSamplesTotal.WithLabelValues(t.queueName)
	droppedSamplesTotal.WithLabelValues(t.queueName)

	return t
}

// Append queues a sample to be sent to the remote storage. It drops the
// sample on the floor if the queue is full.
// Always returns nil.
func (t *QueueManager) Append(s []tsdb.RefSample) bool {
	tempSamples := make([]tsdb.RefSample, 0, len(s))

	t.seriesMtx.Lock()
	for _, sample := range s {
		// If we have no labels for the series, due to relabelling or otherwise, don't send the sample.
		if _, ok := t.series[sample.Ref]; !ok {
			droppedSamplesTotal.WithLabelValues(t.queueName).Inc()
			continue
		}
		tempSamples = append(tempSamples, sample)
	}
	t.seriesMtx.Unlock()

	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	for _, sample := range tempSamples[:len(tempSamples)] {
		if err := t.shards.enqueue(sample); err != nil {
			level.Error(t.logger).Log("err", err)
			return false
		}
	}
	return true
}

// NeedsThrottling implements storage.SampleAppender. It will always return
// false as a remote storage drops samples on the floor if backlogging instead
// of asking for throttling.
func (*QueueManager) NeedsThrottling() bool {
	return false
}

// Start the queue manager sending samples to the remote storage.
// Does not block.
func (t *QueueManager) Start() {
	t.wg.Add(2)
	go t.updateShardsLoop()
	go t.reshardLoop()
	t.watcher.Start()

	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	t.shards.start()
}

// Stop stops sending samples to the remote storage and waits for pending
// sends to complete.
func (t *QueueManager) Stop() {
	level.Info(t.logger).Log("msg", "Stopping remote storage...")
	close(t.quit)
	t.watcher.Stop()
	t.wg.Wait()

	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	t.shards.stop(t.flushDeadline)

	level.Info(t.logger).Log("msg", "Remote storage stopped.")
}

// Keep track of which series we know about for lookups when sending samples to remote.
func (t *QueueManager) StoreSeries(series []tsdb.RefSeries) {
	temp := make(map[uint64][]*prompb.Label)
	for i := 0; i < len(series); i++ {
		var ls = make(model.LabelSet)
		for _, label := range series[i].Labels {
			ls[model.LabelName(label.Name)] = model.LabelValue(label.Value)
		}
		t.processExternalLabels(ls)
		relabel.Process(ls, t.relabelConfigs...)
		temp[series[i].Ref] = labelsToLabelsProto(labelsetToLabels(ls))
	}
	t.seriesMtx.Lock()
	defer t.seriesMtx.Unlock()
	for ref, labels := range temp {
		t.series[ref] = labels
	}
}

// Clear all series stored in series labels cache.
func (t *QueueManager) ClearSeries() {
	t.seriesMtx.Lock()
	defer t.seriesMtx.Unlock()
	t.series = make(map[uint64][]*prompb.Label)
}

func (t *QueueManager) processExternalLabels(ls model.LabelSet) {
	for ln, lv := range t.externalLabels {
		if _, ok := ls[ln]; !ok {
			ls[ln] = lv
		}
	}
}

func (t *QueueManager) updateShardsLoop() {
	defer t.wg.Done()

	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.calculateDesiredShards()
		case <-t.quit:
			return
		}
	}
}

func (t *QueueManager) calculateDesiredShards() {
	t.samplesIn.tick()
	t.samplesOut.tick()
	t.samplesOutDuration.tick()

	// We use the number of incoming samples as a prediction of how much work we
	// will need to do next iteration.  We add to this any pending samples
	// (received - send) so we can catch up with any backlog. We use the average
	// outgoing batch latency to work out how many shards we need.
	var (
		samplesIn          = t.samplesIn.rate()
		samplesOut         = t.samplesOut.rate()
		samplesPending     = samplesIn - samplesOut
		samplesOutDuration = t.samplesOutDuration.rate()
	)

	// We use an integral accumulator, like in a PID, to help dampen oscillation.
	t.integralAccumulator = t.integralAccumulator + (samplesPending * 0.1)

	if samplesOut <= 0 {
		return
	}

	var (
		timePerSample = samplesOutDuration / samplesOut
		desiredShards = (timePerSample * (samplesIn + samplesPending + t.integralAccumulator)) / float64(time.Second)
	)
	level.Debug(t.logger).Log("msg", "QueueManager.caclulateDesiredShards",
		"samplesIn", samplesIn, "samplesOut", samplesOut,
		"samplesPending", samplesPending, "desiredShards", desiredShards)

	// Changes in the number of shards must be greater than shardToleranceFraction.
	var (
		lowerBound = float64(t.numShards) * (1. - shardToleranceFraction)
		upperBound = float64(t.numShards) * (1. + shardToleranceFraction)
	)
	level.Debug(t.logger).Log("msg", "QueueManager.updateShardsLoop",
		"lowerBound", lowerBound, "desiredShards", desiredShards, "upperBound", upperBound)
	if lowerBound <= desiredShards && desiredShards <= upperBound {
		return
	}

	numShards := int(math.Ceil(desiredShards))
	if numShards > t.cfg.MaxShards {
		numShards = t.cfg.MaxShards
	} else if numShards < 1 {
		numShards = 1
	}
	if numShards == t.numShards {
		return
	}

	// Resharding can take some time, and we want this loop
	// to stay close to shardUpdateDuration.
	select {
	case t.reshardChan <- numShards:
		level.Info(t.logger).Log("msg", "Remote storage resharding", "from", t.numShards, "to", numShards)
		t.numShards = numShards
	default:
		level.Info(t.logger).Log("msg", "Currently resharding, skipping.")
	}
}

func (t *QueueManager) reshardLoop() {
	defer t.wg.Done()

	for {
		select {
		case numShards := <-t.reshardChan:
			t.reshard(numShards)
		case <-t.quit:
			return
		}
	}
}

func (t *QueueManager) reshard(n int) {
	numShards.WithLabelValues(t.queueName).Set(float64(n))

	t.shardsMtx.Lock()
	newShards := t.newShards(n)
	oldShards := t.shards
	t.shards = newShards
	t.shardsMtx.Unlock()

	oldShards.stop(t.flushDeadline)

	// We start the newShards after we have stopped (the therefore completely
	// flushed) the oldShards, to guarantee we only every deliver samples in
	// order.
	newShards.start()
}

type shards struct {
	qm      *QueueManager
	queues  []chan tsdb.RefSample
	done    chan struct{}
	running int32
	ctx     context.Context
	cancel  context.CancelFunc
}

func (t *QueueManager) newShards(numShards int) *shards {
	queues := make([]chan tsdb.RefSample, numShards)
	for i := 0; i < numShards; i++ {
		queues[i] = make(chan tsdb.RefSample, 10)
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &shards{
		qm:      t,
		queues:  queues,
		done:    make(chan struct{}),
		running: int32(numShards),
		ctx:     ctx,
		cancel:  cancel,
	}
	return s
}

func (t *QueueManager) buildWriteRequest(samples []tsdb.RefSample, series map[uint64][]*prompb.Label) ([]byte, error) {
	t.seriesMtx.Lock()
	defer t.seriesMtx.Unlock()
	req := &prompb.WriteRequest{
		Timeseries: make([]*prompb.TimeSeries, 0, len(samples)),
	}
	for _, s := range samples {
		ts := prompb.TimeSeries{
			Labels: series[s.Ref],
			Samples: []prompb.Sample{
				prompb.Sample{
					Value:     float64(s.V),
					Timestamp: int64(s.T),
				},
			},
		}
		req.Timeseries = append(req.Timeseries, &ts)
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	compressed := snappy.Encode(nil, data)

	return compressed, nil
}

func (s *shards) len() int {
	return len(s.queues)
}

func (s *shards) start() {
	for i := 0; i < len(s.queues); i++ {
		go s.runShard(i)
	}
}

func (s *shards) stop(deadline time.Duration) {
	// Attempt a clean shutdown.
	for _, shard := range s.queues {
		close(shard)
	}
	select {
	case <-s.done:
		return
	case <-time.After(deadline):
		level.Error(s.qm.logger).Log("msg", "Failed to flush all samples on shutdown")
	}

	// Force an unclean shutdown.
	s.cancel()
	// Why does this send block now...?
	// <-s.done
	return
}

func (s *shards) enqueue(sample tsdb.RefSample) error {

	shard := uint64(sample.Ref) % uint64(len(s.queues))
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case s.queues[shard] <- sample:
			s.qm.samplesIn.incr(1)
			return nil
		}

	}
}

func (s *shards) runShard(i int) {
	defer func() {
		if atomic.AddInt32(&s.running, -1) == 0 {
			close(s.done)
		}
	}()

	queue := s.queues[i]

	// Send batches of at most MaxSamplesPerSend samples to the remote storage.
	// If we have fewer samples than that, flush them out after a deadline
	// anyways.
	pendingSamples := []tsdb.RefSample{}

	timer := time.NewTimer(time.Duration(s.qm.cfg.BatchSendDeadline))
	stop := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	defer stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case sample, ok := <-queue:
			if !ok {
				if len(pendingSamples) > 0 {
					level.Debug(s.qm.logger).Log("msg", "Flushing samples to remote storage...", "count", len(pendingSamples))
					s.sendSamples(pendingSamples)
					level.Debug(s.qm.logger).Log("msg", "Done flushing.")
				}
				return
			}

			queueLength.WithLabelValues(s.qm.queueName).Dec()
			pendingSamples = append(pendingSamples, sample)

			if len(pendingSamples) >= 100 {
				s.sendSamples(pendingSamples[:100])
				pendingSamples = pendingSamples[100:]

				stop()
				timer.Reset(time.Duration(s.qm.cfg.BatchSendDeadline))
			}
		case <-timer.C:
			if len(pendingSamples) > 0 {
				s.sendSamples(pendingSamples)
				pendingSamples = pendingSamples[:0]
			}
			timer.Reset(time.Duration(s.qm.cfg.BatchSendDeadline))
		}
	}
}

func (s *shards) sendSamples(samples []tsdb.RefSample) {
	begin := time.Now()
	err := s.sendSamplesWithBackoff(samples)
	if err != nil {
		level.Error(s.qm.logger).Log("msg", "non-recoverable error", "count", len(samples), "err", err)
		return
	}

	// These counters are used to calculate the dynamic sharding, and as such
	// should be maintained irrespective of success or failure.
	s.qm.samplesOut.incr(int64(len(samples)))
	s.qm.samplesOutDuration.incr(int64(time.Since(begin)))
}

// sendSamples to the remote storage with backoff for recoverable errors.
func (s *shards) sendSamplesWithBackoff(samples []tsdb.RefSample) error {
	backoff := s.qm.cfg.MinBackoff
	req, err := s.qm.buildWriteRequest(samples, s.qm.series)
	// Failing to build the write request is non-recoverable, since it will
	// only error if marshaling the proto to bytes fails.
	if err != nil {
		return err
	}
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
			begin := time.Now()
			err := s.qm.client.Store(s.ctx, req)

			sentBatchDuration.WithLabelValues(s.qm.queueName).Observe(time.Since(begin).Seconds())
			if err == nil {
				succeededSamplesTotal.WithLabelValues(s.qm.queueName).Add(float64(len(samples)))
				s.qm.sentTimestamp.SetToCurrentTime()
				return nil
			}

			if _, ok := err.(recoverableError); !ok {
				return err
			}

			if s.qm.logLimiter.Allow() {
				level.Error(s.qm.logger).Log("err", err)
			}

			time.Sleep(time.Duration(backoff))
			backoff = backoff * 2
			if backoff > s.qm.cfg.MaxBackoff {
				backoff = s.qm.cfg.MaxBackoff
			}
		}
	}
}
