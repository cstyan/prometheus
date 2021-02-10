// Copyright 2020 The Prometheus Authors
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

package tsdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type exemplarMetrics struct {
	outOfOrderExemplars prometheus.Counter
	duplicateExemplars  prometheus.Counter
}

func newExemplarMetrics(r prometheus.Registerer) *exemplarMetrics {
	m := &exemplarMetrics{
		outOfOrderExemplars: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_exemplar_out_of_order_exemplars_total",
			Help: "Total number of out of order samples ingestion failed attempts",
		}),
		duplicateExemplars: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_exemplar_duplicate_exemplars_total",
			Help: "Total number of series in the head block.",
		}),
	}
	if r != nil {
		r.MustRegister(
			m.outOfOrderExemplars,
			m.duplicateExemplars,
		)
	}
	return m
}

type CircularExemplarStorage struct {
	metrics   *exemplarMetrics
	lock      sync.RWMutex
	exemplars []*circularBufferEntry
	nextIndex int

	// Map of series labels as a string to index entry, which points to the first
	// and last exemplar for the series the exemplars circular buffer.
	index map[string]*indexEntry
}

type indexEntry struct {
	first int
	last  int
}

type circularBufferEntry struct {
	exemplar     exemplar.Exemplar
	seriesLabels labels.Labels
	next         int
}

// If we assume the average case 95 bytes per exemplar we can fit 5651272 exemplars in
// 1GB of extra memory, accounting for the fact that this is heap allocated space.
func NewCircularExemplarStorage(len int, reg prometheus.Registerer) (*CircularExemplarStorage, error) {
	if len < 1 {
		return nil, fmt.Errorf("must initialize exemplar storage with space for at least 1 exemplar, %d is invalid", len)
	}
	return &CircularExemplarStorage{
		exemplars: make([]*circularBufferEntry, len),
		index:     make(map[string]*indexEntry),
		metrics:   newExemplarMetrics(reg),
	}, nil
}

func (ce *CircularExemplarStorage) ExemplarAppender() storage.ExemplarAppender {
	return ce
}

func (ce *CircularExemplarStorage) ExemplarQuerier(_ context.Context) (storage.ExemplarQuerier, error) {
	return ce, nil
}

func (ce *CircularExemplarStorage) Querier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return ce, nil
}

// Select returns exemplars for a given set of series labels hash.
func (ce *CircularExemplarStorage) Select(start, end int64, l labels.Labels) ([]exemplar.Exemplar, error) {
	var (
		ret []exemplar.Exemplar
		e   *circularBufferEntry
		idx *indexEntry
		ok  bool
	)

	ce.lock.RLock()
	defer ce.lock.RUnlock()

	if idx, ok = ce.index[l.String()]; !ok {
		return nil, nil
	}

	e = ce.exemplars[idx.first]
	for {
		if e.exemplar.Ts < start {
			if e.next == -1 {
				break
			}
			e = ce.exemplars[e.next]
			continue
		}
		if e.exemplar.Ts > end {
			break
		}

		ret = append(ret, e.exemplar)
		if e.next == -1 {
			break
		}
		e = ce.exemplars[e.next]
	}
	return ret, nil
}

// indexGc takes the circularBufferEntry that will be overwritten and updates the
// storages index for that entries labelset if necessary.
func (ce *CircularExemplarStorage) indexGc(cbe *circularBufferEntry) {
	if cbe == nil {
		return
	}

	l := cbe.seriesLabels.String()
	i := cbe.next
	if i == -1 {
		delete(ce.index, l)
		return
	}

	ce.index[l] = &indexEntry{i, ce.index[l].last}
}

func (ce *CircularExemplarStorage) AddExemplar(l labels.Labels, e exemplar.Exemplar) error {
	seriesLabels := l.String()
	ce.lock.Lock()
	defer ce.lock.Unlock()

	idx, ok := ce.index[seriesLabels]
	if !ok {
		ce.indexGc(ce.exemplars[ce.nextIndex])
		// Default the next value to -1 (which we use to detect that we've iterated through all exemplars for a series in Select)
		// since this is the first exemplar stored for this series.
		ce.exemplars[ce.nextIndex] = &circularBufferEntry{
			exemplar:     e,
			seriesLabels: l,
			next:         -1}
		ce.index[seriesLabels] = &indexEntry{ce.nextIndex, ce.nextIndex}
		ce.nextIndex = (ce.nextIndex + 1) % len(ce.exemplars)
		return nil
	}

	// Check for duplicate vs last stored exemplar for this series.
	if ce.exemplars[idx.last].exemplar.Equals(e) {
		ce.metrics.duplicateExemplars.Inc()
		return storage.ErrDuplicateExemplar
	}

	if e.Ts <= ce.exemplars[idx.last].exemplar.Ts {
		ce.metrics.outOfOrderExemplars.Inc()
		return storage.ErrOutOfOrderExemplar
	}
	ce.indexGc(ce.exemplars[ce.nextIndex])
	ce.exemplars[ce.nextIndex] = &circularBufferEntry{
		exemplar:     e,
		seriesLabels: l,
		next:         -1,
	}

	ce.exemplars[ce.index[seriesLabels].last].next = ce.nextIndex
	ce.index[seriesLabels].last = ce.nextIndex
	ce.nextIndex = (ce.nextIndex + 1) % len(ce.exemplars)
	return nil
}

func (ce *CircularExemplarStorage) AddExemplarFast(ref uint64, e exemplar.Exemplar) error {
	// no op
	return nil
}
