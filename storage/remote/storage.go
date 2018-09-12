// Copyright 2017 The Prometheus Authors
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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
	tsdbLabels "github.com/prometheus/tsdb/labels"
	"github.com/prometheus/tsdb/wal"
	fsnotify "gopkg.in/fsnotify/fsnotify.v1"
)

// Callback func that return the oldest timestamp stored in a storage.
type startTimeCallback func() (int64, error)

// Storage represents all the remote read and write endpoints.  It implements
// storage.Storage.
type Storage struct {
	logger log.Logger
	mtx    sync.RWMutex

	// For writes
	walDir  string
	watcher *fsnotify.Watcher
	wal     *wal.WAL
	series  map[uint64]tsdbLabels.Labels
	clients []*Client
	// queues []*QueueManager

	// For reads
	queryables             []storage.Queryable
	localStartTimeCallback startTimeCallback
	flushDeadline          time.Duration
}

// NewStorage returns a remote.Storage.
func NewStorage(l log.Logger, stCallback startTimeCallback, flushDeadline time.Duration) *Storage {
	if l == nil {
		l = log.NewNopLogger()
	}
	return &Storage{
		logger:                 l,
		localStartTimeCallback: stCallback,
		flushDeadline:          flushDeadline,
	}
}

// ApplyConfig updates the state as the new config requires.
func (s *Storage) ApplyConfig(conf *config.Config) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// TODO: we should only stop & recreate queues which have changes,
	// as this can be quite disruptive.
	for i, rwConf := range conf.RemoteWriteConfigs {
		c, err := NewClient(i, &ClientConfig{
			URL:              rwConf.URL,
			Timeout:          rwConf.RemoteTimeout,
			HTTPClientConfig: rwConf.HTTPClientConfig,
		})
		if err != nil {
			return err
		}
		s.clients = append(s.clients, c)
	}

	// Update read clients

	s.queryables = make([]storage.Queryable, 0, len(conf.RemoteReadConfigs))
	for i, rrConf := range conf.RemoteReadConfigs {
		c, err := NewClient(i, &ClientConfig{
			URL:              rrConf.URL,
			Timeout:          rrConf.RemoteTimeout,
			HTTPClientConfig: rrConf.HTTPClientConfig,
		})
		if err != nil {
			return err
		}

		q := QueryableClient(c)
		q = ExternalLabelsHandler(q, conf.GlobalConfig.ExternalLabels)
		if len(rrConf.RequiredMatchers) > 0 {
			q = RequiredMatchersFilter(q, labelsToEqualityMatchers(rrConf.RequiredMatchers))
		}
		if !rrConf.ReadRecent {
			q = PreferLocalStorageFilter(q, s.localStartTimeCallback)
		}
		s.queryables = append(s.queryables, q)
	}

	return nil
}

func (s *Storage) SetWALDir(dir string) {
	s.logger.Log("msg", fmt.Sprintf("setting wal dir to %s", dir))
	s.walDir = dir
}

// keep track of which series we know about for lookups when sending samples to remote
func (s *Storage) storeSeries(series []tsdb.RefSeries) {
	for i := 0; i < len(series); i++ {
		_, ok := s.series[series[i].Ref]
		if !ok {
			s.series[series[i].Ref] = series[i].Labels
		}
	}
}

func (s *Storage) storeSamples(samples []tsdb.RefSample) {
	var sa []*model.Sample
	s.logger.Log("msg", "in store samples")
	for _, sample := range samples {
		s.logger.Log("sample", fmt.Sprintf("%+v", sample))
		_, ok := s.series[sample.Ref]
		if !ok {
			s.logger.Log("msg", "unknown series", "ref", sample.Ref)
			continue
		}
		s.logger.Log("msg", "send samples for series here", "ref", sample.Ref)
		// convert RefSample to model.Sample
		metric := make(model.Metric, len(s.series[sample.Ref]))
		for _, v := range s.series[sample.Ref] {
			metric[model.LabelName(v.Name)] = model.LabelValue(v.Value)
		}
		c := &model.Sample{
			Metric:    metric,
			Value:     model.SampleValue(sample.V),
			Timestamp: model.Time(sample.T),
		}
		sa = append(sa, c)
	}
	req := ToWriteRequest(sa)
	s.logger.Log("write request:", fmt.Sprintf("%+v", req))
	for _, c := range s.clients {
		s.logger.Log("msg", "should be sending here **********************************", "url", c.url.String())
		ctx := context.Background()
		// we just need to pass any ctx for now
		c.Store(ctx, req)
	}
}

func (s *Storage) decodeSegment(segment *wal.Segment) bool {
	// todo: callum, is there an easy way to detect if r.Next() has returned the last possible record in a segment.
	// For now just recover from the panic that is thrown if we call r.Next() when we shouldn't have
	defer func() {
		if r := recover(); r != nil {
			s.logger.Log("err", r)
			// level.Error(log.With(logger)).Log("err", r)
			segment.Close()
		}
	}()
	r := wal.NewReader(segment)
	var (
		dec      tsdb.RecordDecoder
		series   []tsdb.RefSeries
		samples  []tsdb.RefSample
		nSeries  = 0
		nSamples = 0
	)
	for {
		ok := r.Next()
		rec := r.Record()
		switch dec.Type(rec) {
		case tsdb.RecordSeries:
			s.logger.Log("msg", "new series!!!!")
			series, err := dec.Series(rec, series[:0])
			if err != nil {
				s.logger.Log("err", err)
				// level.Error(log.With(logger)).Log("err", err)
				return ok
			}
			s.storeSeries(series)
			nSeries += len(series)
		case tsdb.RecordSamples:
			s.logger.Log("msg", "samples!!!!")
			samples, err := dec.Samples(rec, samples[:0])
			if err != nil {
				s.logger.Log("err", err)
				// level.Error(log.With(logger)).Log("err", err)
				return ok
			}
			s.storeSamples(samples)
			nSamples += len(samples)
		}
		if !ok {
			return ok
		}
	}
}

func (s *Storage) StartWALWatcher() {
	if s.series == nil {
		s.series = make(map[uint64]tsdbLabels.Labels)
	}
	var err error
	s.wal, err = wal.New(nil, nil, s.walDir)
	if err != nil {
		s.logger.Log("err", err)

		// level.Error(log.With(logger)).Log("err", err)
	}
	_, n, err := s.wal.Segments()

	if err != nil {
		s.logger.Log("err", err)

		// level.Error(log.With(logger)).Log("err", err)
	}
	if n == -1 {
		s.logger.Log("err", "no segments found")

		// level.Error(log.With(logger)).Log("err", "no segments found")
		return
	}
	s.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		s.logger.Log("err", err)
		// level.Error(log.With(logger)).Log("err", err)
		return
	}

	// done := make(chan bool)
	go func() {
		s.logger.Log("msg", "started go routine")
		defer s.watcher.Close()
		// Open the current segment.
		// lock := &sync.Mutex{}
		var segment *wal.Segment
		segment, err = wal.OpenReadSegment(wal.SegmentName(s.walDir, n))
		defer segment.Close()

		for {
			select {
			case event, ok := <-s.watcher.Events:
				s.logger.Log("msg", "watcher event")
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					// level.Info(logger).Log("event", "write", "file", event.Name)
					s.logger.Log("event", "write", "file", event.Name)
					s.decodeSegment(segment)
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					// TODO: callum, we should check if the file name is a new segment and not a checkpoint or something else
					// level.Info(logger).Log("event", "create", "file", event.Name)
					s.logger.Log("event", "create", "file", event.Name)

					segment.Close()
					n++
					segment, err = wal.OpenReadSegment(wal.SegmentName(s.walDir, n))
				}
			case err, ok := <-s.watcher.Errors:
				if !ok {
					return
				}
				s.logger.Log("err", err)

				// level.Error(log.With(logger)).Log("err", err)
			}
		}
		s.logger.Log("msg", "ending go routine")
	}()

	err = s.watcher.Add(s.walDir)
	if err != nil {
		s.logger.Log("err", err)
		// level.Error(log.With(logger)).Log("err", err)
		return
	}
	// <-done
}

// StartTime implements the Storage interface.
func (s *Storage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

// Querier returns a storage.MergeQuerier combining the remote client queriers
// of each configured remote read endpoint.
func (s *Storage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	s.mtx.Lock()
	queryables := s.queryables
	s.mtx.Unlock()

	queriers := make([]storage.Querier, 0, len(queryables))
	for _, queryable := range queryables {
		q, err := queryable.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		queriers = append(queriers, q)
	}
	return storage.NewMergeQuerier(queriers), nil
}

// Close the background processing of the storage queues.
func (s *Storage) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// for _, q := range s.queues {
	// 	q.Stop()
	// }

	return nil
}

func labelsToEqualityMatchers(ls model.LabelSet) []*labels.Matcher {
	ms := make([]*labels.Matcher, 0, len(ls))
	for k, v := range ls {
		ms = append(ms, &labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  string(k),
			Value: string(v),
		})
	}
	return ms
}
