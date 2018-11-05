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
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

const defaultFlushDeadline = 1 * time.Minute

type TestStorageClient struct {
	receivedSamples map[string][]prompb.Sample
	expectedSamples map[string][]prompb.Sample
	wg              sync.WaitGroup
	mtx             sync.Mutex
}

func NewTestStorageClient() *TestStorageClient {
	return &TestStorageClient{
		receivedSamples: map[string][]prompb.Sample{},
		expectedSamples: map[string][]prompb.Sample{},
	}
}

// MetricToLabelProtos builds a []*prompb.Label from a model.Metric
func MetricToLabelProtos(metric model.Metric) []*prompb.Label {
	labels := make([]*prompb.Label, 0, len(metric))
	for k, v := range metric {
		labels = append(labels, &prompb.Label{
			Name:  string(k),
			Value: string(v),
		})
	}
	sort.Slice(labels, func(i int, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	return labels
}

func createTimeseries(n int) ([]tsdb.RefSample, []tsdb.RefSeries) {
	samples := make([]tsdb.RefSample, 0, n)
	series := make([]tsdb.RefSeries, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("test_metric_%d", i)
		samples = append(samples, tsdb.RefSample{
			Ref: uint64(i),
			T:   int64(i),
			V:   float64(i),
		})
		series = append(series, tsdb.RefSeries{
			Ref:    uint64(i),
			Labels: labels.Labels{labels.Label{Name: "__name__", Value: name}},
		})
	}
	return samples, series
}

func getSeriesNameFromRef(r tsdb.RefSeries) string {
	for _, l := range r.Labels {
		if l.Name == "__name__" {
			return l.Value
		}
	}
	return ""
}

func refSeriesToLabelsProto(series []tsdb.RefSeries) map[uint64][]*prompb.Label {
	result := make(map[uint64][]*prompb.Label)
	for _, s := range series {
		for _, l := range s.Labels {
			result[s.Ref] = append(result[s.Ref], &prompb.Label{
				Name:  l.Name,
				Value: l.Value,
			})
		}
	}
	return result
}

func (c *TestStorageClient) expectSamples(ss []tsdb.RefSample, series []tsdb.RefSeries) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.expectedSamples = map[string][]prompb.Sample{}
	c.receivedSamples = map[string][]prompb.Sample{}

	for _, s := range ss {
		seriesName := getSeriesNameFromRef(series[s.Ref])
		c.expectedSamples[seriesName] = append(c.expectedSamples[seriesName], prompb.Sample{
			Timestamp: s.T,
			Value:     s.V,
		})
	}
	c.wg.Add(len(ss))
}

func (c *TestStorageClient) waitForExpectedSamples(t *testing.T) {
	c.wg.Wait()
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for ts, expectedSamples := range c.expectedSamples {
		if !reflect.DeepEqual(expectedSamples, c.receivedSamples[ts]) {
			t.Fatalf("%s: Expected %v, got %v", ts, expectedSamples, c.receivedSamples[ts])
		}
	}
}

func (c *TestStorageClient) Store(_ context.Context, req []byte) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	reqBuf, err := snappy.Decode(nil, req)
	if err != nil {
		return err
	}

	var reqProto prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &reqProto); err != nil {
		return err
	}
	count := 0
	for _, ts := range reqProto.Timeseries {
		var seriesName string
		labels := labelProtosToLabels(ts.Labels)
		for _, label := range labels {
			if label.Name == "__name__" {
				seriesName = label.Value
			}
		}
		for _, sample := range ts.Samples {
			count++
			c.receivedSamples[seriesName] = append(c.receivedSamples[seriesName], sample)
		}
	}
	c.wg.Add(-count)
	return nil
}

func (c *TestStorageClient) Name() string {
	return "teststorageclient"
}

func TestSampleDelivery(t *testing.T) {
	// Let's create an even number of send batches so we don't run into the
	// batch timeout case.
	// n := config.DefaultQueueConfig.Capacity * 2
	samples, series := createTimeseries(20)

	c := NewTestStorageClient()
	c.expectSamples(samples[:len(samples)/2], series)

	cfg := config.DefaultQueueConfig
	cfg.BatchSendDeadline = model.Duration(100 * time.Millisecond)
	cfg.MaxShards = 1
	m := NewQueueManager(nil, "", newEWMARate(ewmaWeight, shardUpdateDuration), cfg, nil, nil, c, defaultFlushDeadline)
	m.series = refSeriesToLabelsProto(series)

	// These should be received by the client.
	m.Append(samples[:len(samples)/2])
	m.Start()
	defer m.Stop()

	c.waitForExpectedSamples(t)
	m.Append(samples[len(samples)/2:])
	c.expectSamples(samples[len(samples)/2:], series)
	c.waitForExpectedSamples(t)
}

func TestSampleDeliveryTimeout(t *testing.T) {
	// Let's send one less sample than batch size, and wait the timeout duration
	n := 9
	samples, series := createTimeseries(n)
	c := NewTestStorageClient()

	cfg := config.DefaultQueueConfig
	cfg.MaxShards = 1
	cfg.BatchSendDeadline = model.Duration(100 * time.Millisecond)
	m := NewQueueManager(nil, "", newEWMARate(ewmaWeight, shardUpdateDuration), cfg, nil, nil, c, defaultFlushDeadline)
	m.series = refSeriesToLabelsProto(series)
	m.Start()
	defer m.Stop()

	// Send the samples twice, waiting for the samples in the meantime.
	c.expectSamples(samples, series)
	m.Append(samples)
	c.waitForExpectedSamples(t)

	c.expectSamples(samples, series)
	m.Append(samples)
	c.waitForExpectedSamples(t)
}

func TestSampleDeliveryOrder(t *testing.T) {
	ts := 10
	n := config.DefaultQueueConfig.MaxSamplesPerSend * ts
	samples := make([]tsdb.RefSample, 0, n)
	series := make([]tsdb.RefSeries, 0, n)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("test_metric_%d", i%ts)
		samples = append(samples, tsdb.RefSample{
			Ref: uint64(i),
			T:   int64(i),
			V:   float64(i),
		})
		series = append(series, tsdb.RefSeries{
			Ref:    uint64(i),
			Labels: labels.Labels{labels.Label{Name: "__name__", Value: name}},
		})
	}

	c := NewTestStorageClient()
	c.expectSamples(samples, series)
	m := NewQueueManager(nil, "", newEWMARate(ewmaWeight, shardUpdateDuration), config.DefaultQueueConfig, nil, nil, c, defaultFlushDeadline)
	m.series = refSeriesToLabelsProto(series)

	m.Start()
	defer m.Stop()
	// These should be received by the client.
	m.Append(samples)
	c.waitForExpectedSamples(t)
}

// TestBlockingStorageClient is a queue_manager StorageClient which will block
// on any calls to Store(), until the `block` channel is closed, at which point
// the `numCalls` property will contain a count of how many times Store() was
// called.
type TestBlockingStorageClient struct {
	numCalls uint64
	block    chan bool
}

func NewTestBlockedStorageClient() *TestBlockingStorageClient {
	return &TestBlockingStorageClient{
		block:    make(chan bool),
		numCalls: 0,
	}
}

func (c *TestBlockingStorageClient) Store(ctx context.Context, _ []byte) error {
	atomic.AddUint64(&c.numCalls, 1)
	select {
	case <-c.block:
	case <-ctx.Done():
	}
	return nil
}

func (c *TestBlockingStorageClient) NumCalls() uint64 {
	return atomic.LoadUint64(&c.numCalls)
}

func (c *TestBlockingStorageClient) unlock() {
	close(c.block)
}

func (c *TestBlockingStorageClient) Name() string {
	return "testblockingstorageclient"
}

func (t *QueueManager) queueLen() int {
	t.shardsMtx.Lock()
	defer t.shardsMtx.Unlock()
	queueLength := 0
	for _, shard := range t.shards.queues {
		queueLength += len(shard)
	}
	return queueLength
}

func tsdbLabelsToLabelsProto(labels labels.Labels) []*prompb.Label {
	result := make([]*prompb.Label, 0, len(labels))
	for _, l := range labels {
		result = append(result, &prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return result
}

func TestShutdown(t *testing.T) {
	deadline := 5 * time.Second
	c := NewTestBlockedStorageClient()
	m := NewQueueManager(nil, "", newEWMARate(ewmaWeight, shardUpdateDuration), config.DefaultQueueConfig, nil, nil, c, deadline)
	samples, series := createTimeseries(config.DefaultQueueConfig.MaxSamplesPerSend)
	for _, s := range series {
		m.series[s.Ref] = tsdbLabelsToLabelsProto(s.Labels)
	}

	m.Start()
	m.Append(samples)

	start := time.Now()
	m.Stop()
	duration := time.Now().Sub(start)
	if duration > deadline+(deadline/10) {
		t.Errorf("Took too long to shutdown: %s > %s", duration, deadline)
	}
}
