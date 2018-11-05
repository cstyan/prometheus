// Copyright 2018 The Prometheus Authors
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
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/wal"
	fsnotify "gopkg.in/fsnotify/fsnotify.v1"
)

var (
	segmentRegex    = regexp.MustCompile(`\d{8}`)
	checkpointRegex = regexp.MustCompile(`checkpoint.\d{6}`)
)

type WriteTo interface {
	Append([]tsdb.RefSample) bool
	StoreSeries([]tsdb.RefSeries)
	ClearSeries()
}

// WALWatcher watches the TSDB WAL for a given WriteTo.
type WALWatcher struct {
	writer  WriteTo
	logger  log.Logger
	walDir  string
	watcher *fsnotify.Watcher

	currentSegment  int
	lastForwardedTs prometheus.Gauge

	ctx    context.Context
	cancel context.CancelFunc
	quit   chan struct{}
}

// NewWALWatcher creates a new WAL watcher for a given WriteTo.
func NewWALWatcher(logger log.Logger, writer WriteTo, walDir string) *WALWatcher {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &WALWatcher{
		logger: logger,
		writer: writer,
		walDir: path.Join(walDir, "wal"),
		ctx:    ctx,
		cancel: cancel,
		quit:   make(chan struct{}),
	}
	w.lastForwardedTs = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_remote_write_last_forwarded_timestamp",
		Help: "Timestamp of the last time the WAL watcher sent a sample to queue managers.",
	})
	prometheus.MustRegister(w.lastForwardedTs)
	return w
}

func (w *WALWatcher) Start() {
	go w.runWatcher()
}

func (w *WALWatcher) Stop() {
	close(w.quit)
	return
}

func (w *WALWatcher) readSeriesRecords(r *wal.Reader) {
	var (
		dec    tsdb.RecordDecoder
		series []tsdb.RefSeries
	)
	for r.Next() {
		rec := r.Record()
		if dec.Type(rec) != tsdb.RecordSeries {
			continue
		}

		series, err := dec.Series(rec, series[:0])
		if err != nil {
			level.Error(log.With(w.logger)).Log("err", err)
			break
		}

		w.writer.StoreSeries(series)
	}
}

// Read all the series records from a Checkpoint directory.
func (w *WALWatcher) readCheckpoint(checkpointDir string) error {
	sr, err := wal.NewSegmentsReader(checkpointDir)
	if err != nil {
		return errors.Wrap(err, "open checkpoint")
	}
	defer sr.Close()

	w.readSeriesRecords(wal.NewReader(sr))
	level.Debug(w.logger).Log("msg", "read series references from checkpoint", "checkpoint", checkpointDir)
	return nil
}

// When starting the WAL watcher, there is potentially an existing WAL. In that case, we
// should read to the end of the newest existing segment before listening for file events,
// storing data from series records along the way.
// Unfortunately this function is almost a duplicate of TSDB Head.Init().
func (w *WALWatcher) readToEnd(walDir string, lastSegment int) (*wal.Segment, error) {
	// Backfill from the checkpoint first if it exists.
	dir, startFrom, err := tsdb.LastCheckpoint(walDir)
	if err != nil && err != tsdb.ErrNotFound {
		return nil, errors.Wrap(err, "find last checkpoint")
	}
	if err == nil {
		err = w.readCheckpoint(path.Join(walDir, dir))
		if err != nil {
			return nil, err
		}
		startFrom++
	}

	// Backfill segments from the last checkpoint onwards if at least 2 segments exist.
	if lastSegment > 0 {
		sr, err := wal.NewSegmentsRangeReader(walDir, startFrom, lastSegment-1)
		if err != nil {
			return nil, err
		}
		w.readSeriesRecords(wal.NewReader(sr))
	}

	// We want to start the WAL Watcher from the end of the last segment on start,
	// so we make sure to return the wal.Segment pointer
	segment, err := wal.OpenReadSegment(wal.SegmentName(w.walDir, lastSegment))
	w.readSeriesRecords(wal.NewReader(segment))
	return segment, nil
}

func (w *WALWatcher) watch(segment *wal.Segment) bool {
	currentSegmentName := fmt.Sprintf("%08d", w.currentSegment)
	defer segment.Close()

	for {
		select {
		case <-w.quit:
			level.Info(w.logger).Log("quitting WAL watcher watch loop")
			return false
		// TODO: callum, handle maintaining the WAL pointer somehow across apply configs?
		case event, ok := <-w.watcher.Events:
			_, fileName := filepath.Split(event.Name)

			// TODO: callum, in what case do we get !ok?
			if !ok {
				level.Info(w.logger).Log("exiting WAL watcher watch loop")
				return false
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				if fileName != currentSegmentName {
					continue
				}
				w.decodeSegment(segment)
			}
			if event.Op&fsnotify.Create == fsnotify.Create {
				if ok := segmentRegex.MatchString(fileName); ok {
					// TODO: callum, ensure the create was for the segment n+1 and not n+2 or more
					// Read the current segment one last time in case we receive events out of order. Not sure if this is necessary.
					return true
				}
				if ok := checkpointRegex.MatchString(fileName); ok {
					// Head was truncated and WAL segments were checkpointed, so we should
					// read the Checkpoint to find out what series are still active.
					w.writer.ClearSeries()
					err := w.readCheckpoint(path.Join(w.walDir, fileName))
					if err != nil {
						level.Error(w.logger).Log("err", err)
						level.Info(w.logger).Log("exiting WAL watcher watch loop")
						return false
					}
				}
			}
		case err, ok := <-w.watcher.Errors:
			// TODO: callum, are these errors recoverable?
			level.Error(w.logger).Log("err", err)
			if !ok {
				level.Info(w.logger).Log("exiting WAL watcher watch loop")
				return false
			}
		}
	}
}

func (w *WALWatcher) runWatcher() {
	// The WAL dir may not exist when Prometheus first starts up.
	for {
		if _, err := os.Stat(w.walDir); os.IsNotExist(err) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	var err error
	nw, err := wal.New(nil, nil, w.walDir)
	if err != nil {
		level.Error(w.logger).Log("err", err)
		return
	}

	_, n, err := nw.Segments()
	if err != nil {
		level.Error(w.logger).Log("err", err)
		return
	}

	if n == -1 {
		level.Error(w.logger).Log("err", err)
		return
	}

	w.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		level.Error(w.logger).Log("err", err)
		return
	}

	defer w.watcher.Close()
	// Read series records in the current WAL and latest checkpoint, get the segment pointer back.
	segment, err := w.readToEnd(w.walDir, n)
	if err != nil {
		level.Error(w.logger).Log("err", err)
		return
	}

	w.currentSegment = n

	err = w.watcher.Add(w.walDir)
	if err != nil {
		level.Error(w.logger).Log("err", err)
		return
	}

	for {
		level.Debug(w.logger).Log("msg", "watching segment", "segment", w.currentSegment)
		// On start, after reading the existing WAL for series records, we have a pointer to what is the latest segment.
		// On subsequent calls to this function, currentSegment will have been incremented and we should open that segment.
		ok := w.watch(segment)
		if !ok {
			return
		}
		w.currentSegment++
		segment, err = wal.OpenReadSegment(wal.SegmentName(w.walDir, w.currentSegment))
	}
}

// Blocks  until the sample is sent to all remote write endpoints
func (w *WALWatcher) forwardSamples(s []tsdb.RefSample) error {
	for {
		select {
		case <-w.quit:
			return fmt.Errorf("exit when attempting to forward samples")
		default:
			ok := w.writer.Append(s)
			if ok {
				return nil
			}
		}
	}
}

func (w *WALWatcher) decodeSegment(segment *wal.Segment) {
	r := wal.NewReader(segment)
	var (
		dec      tsdb.RecordDecoder
		series   []tsdb.RefSeries
		samples  []tsdb.RefSample
		nSeries  = 0
		nSamples = 0
	)

	for r.Next() {
		rec := r.Record()
		switch dec.Type(rec) {
		case tsdb.RecordSeries:
			series, err := dec.Series(rec, series[:0])
			if err != nil {
				level.Error(log.With(w.logger)).Log("err", err)
				break
			}
			w.writer.StoreSeries(series)
			nSeries += len(series)
		case tsdb.RecordSamples:
			samples, err := dec.Samples(rec, samples[:0])
			if err != nil {
				level.Error(log.With(w.logger)).Log("err", err)
				break
			}
			err = w.forwardSamples(samples)
			if err != nil {
				return
			}

			nSamples += len(samples)
		case tsdb.RecordTombstones:
			continue
		case tsdb.RecordInvalid:
			continue
		default:
			level.Info(w.logger).Log("msg", "unknown TSDB record type in decodeSegment")
		}
	}
}
