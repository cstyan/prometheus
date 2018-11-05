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
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/tsdb"
)

// Callback func that return the oldest timestamp stored in a storage.
type startTimeCallback func() (int64, error)

// Storage represents all the remote read and write endpoints.  It implements
// storage.Storage.
type Storage struct {
	logger log.Logger
	mtx    sync.RWMutex

	// For writes
	walDir          string
	queuesMtx       sync.Mutex
	queues          map[*QueueManager]struct{}
	failedQueues    map[*QueueManager]struct{}
	db              *tsdb.DB
	samplesIn       *ewmaRate
	samplesInMetric prometheus.Counter

	// For reads
	queryables             []storage.Queryable
	localStartTimeCallback startTimeCallback
	flushDeadline          time.Duration
}

// NewStorage returns a remote.Storage.
func NewStorage(l log.Logger, reg prometheus.Registerer, stCallback startTimeCallback, walDir string, db *tsdb.DB, flushDeadline time.Duration) *Storage {
	if l == nil {
		l = log.NewNopLogger()
	}
	shardUpdateDuration := 10 * time.Second
	s := &Storage{
		logger:                 l,
		localStartTimeCallback: stCallback,
		flushDeadline:          flushDeadline,
		walDir:                 walDir,
		db:                     db,
		queues:                 make(map[*QueueManager]struct{}),
		failedQueues:           make(map[*QueueManager]struct{}),
		samplesIn:              newEWMARate(ewmaWeight, shardUpdateDuration),
		samplesInMetric: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_remote_storage_samples_in_total",
			Help: "Samples in to remote storage, compare to samples out for queue managers.",
		}),
	}
	reg.MustRegister(s.samplesInMetric)
	return s
}

// ApplyConfig updates the state as the new config requires.
func (s *Storage) ApplyConfig(conf *config.Config) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	var a struct{}

	// Update write queues
	newQueues := make(map[*QueueManager]struct{})
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
		q := NewQueueManager(
			s.logger,
			s.walDir,
			s.samplesIn,
			rwConf.QueueConfig,
			conf.GlobalConfig.ExternalLabels,
			rwConf.WriteRelabelConfigs,
			c,
			s.flushDeadline)

		newQueues[q] = a
	}

	for q := range s.queues {
		q.Stop()
	}

	s.queues = newQueues
	for q := range s.queues {
		q.Start()
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

func (s *Storage) SetDB(db *tsdb.DB) {
	s.db = db
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

	for q := range s.queues {
		q.Stop()
	}

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
