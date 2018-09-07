package main

import (
	"os"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/wal"
	"gopkg.in/alecthomas/kingpin.v2"
	fsnotify "gopkg.in/fsnotify/fsnotify.v1"
)

var (
	a      = kingpin.New("WAL tail example", "Example program for tailing a Prometheus WAL.")
	walDir = a.Arg("wal.dir", "Full path to a Prometheus WAL directory.").Required().String()
	logger = log.With(log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr)), "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
)

func decode(segment *wal.Segment) bool {
	// todo: callum, is there an easy way to detect if r.Next() has returned the last possible record in a segment.
	// For now just recover from the panic that is thrown if we call r.Next() when we shouldn't have
	defer func() {
		if r := recover(); r != nil {
			level.Error(log.With(logger)).Log("err", r)
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
			series, err := dec.Series(rec, series[:0])
			if err != nil {
				level.Error(log.With(logger)).Log("err", err)
				return ok
			}
			nSeries += len(series)
		case tsdb.RecordSamples:
			samples, err := dec.Samples(rec, samples[:0])
			if err != nil {
				level.Error(log.With(logger)).Log("err", err)
				return ok
			}
			nSamples += len(samples)
		}
		if !ok {
			return ok
		}
	}
}

func main() {
	a.HelpFlag.Short('h')

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		level.Error(log.With(logger)).Log("err", err)
		return
	}
	// change this to your wal dir
	// dir := "/home/callum/go/src/github.com/prometheus/prometheus/data/wal/"
	w, err := wal.New(nil, nil, *walDir)
	if err != nil {
		level.Error(log.With(logger)).Log("err", err)
	}
	m, n, err := w.Segments()

	if err != nil {
		level.Error(log.With(logger)).Log("err", err)
	}
	if n == -1 {
		level.Error(log.With(logger)).Log("err", "no segments found")
		return
	}

	// Read the existing segments except the last one.
	var segment *wal.Segment
	for ; m < n; m++ {
		segment, err = wal.OpenReadSegment(wal.SegmentName(*walDir, m))
		if err != nil {
			level.Error(log.With(logger)).Log("err", err)
		}
		defer segment.Close()
		for {

			ok := decode(segment)

			if !ok {
				break
			}
		}
	}

	// Setup file watching.
	// We'll now essentially tail the WAL starting from the most recent segment.
	// From now on we use n to track which segment we should be reading.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		level.Error(log.With(logger)).Log("err", err)
		return
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		// Open the current segment.
		// lock := &sync.Mutex{}
		segment, err = wal.OpenReadSegment(wal.SegmentName(*walDir, n))
		defer segment.Close()

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					level.Info(logger).Log("event", "write", "file", event.Name)
					decode(segment)
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					// TODO: callum, we should check if the file name is a new segment and not a checkpoint or something else
					level.Info(logger).Log("event", "create", "file", event.Name)
					segment.Close()
					n++
					segment, err = wal.OpenReadSegment(wal.SegmentName(*walDir, n))
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				level.Error(log.With(logger)).Log("err", err)
			}
		}
	}()

	err = watcher.Add(*walDir)
	if err != nil {
		level.Error(log.With(logger)).Log("err", err)
		return
	}
	<-done
}
