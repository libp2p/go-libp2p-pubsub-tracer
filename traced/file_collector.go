package traced

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	ggio "github.com/gogo/protobuf/io"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// NewFileTraceCollector creates a new pubsub traces collector. A collector is a process
// that listens on a libp2p endpoint, accepts pubsub tracing streams from peers,
// and records the incoming data into rotating gzip files.
// If the json argument is not empty, then every time a new trace is generated, it will be written
// to this directory in json format for online processing.
func NewFileTraceCollector(host host.Host, dir, jsonTrace string) (*TraceCollector, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	c := &TraceCollector{
		host:          host,
		dir:           dir,
		jsonTrace:     jsonTrace,
		notifyWriteCh: make(chan struct{}, 1),
		flushFileCh:   make(chan struct{}, 1),
		exitCh:        make(chan struct{}, 1),
		doneCh:        make(chan struct{}, 1),
	}

	host.SetStreamHandler(pubsub.RemoteTracerProtoID, c.handleStream)

	go c.collectFileWorker()
	go c.monitorWorker()

	return c, nil
}

// writeFile records incoming traces into a gzipped file, and yields every time
// a flush is triggered.
func (tc *TraceCollector) writeFile(out io.WriteCloser) (result error) {
	var (
		flush bool

		// buf is an internal buffer that gets swapped with the collector's
		// buffer on write. This saves allocs, and the slices grow to
		// accommodate the volume that's being written.
		buf []*pb.TraceEvent

		gzipW = gzip.NewWriter(out)
		w     = ggio.NewDelimitedWriter(gzipW)
	)

	for !flush {
		select {
		case <-tc.notifyWriteCh:
		case <-tc.flushFileCh:
			flush = true
		}

		// Swap the buffers. We take the collector's buffer, and replace that
		// with our local buffer (trimmed to 0-length). On the next loop
		// iteration, the same will occur, so we're effectively swapping buffers
		// continuously.
		tc.mx.Lock()
		tmp := tc.buf
		tc.buf = buf[:0]
		buf = tmp
		tc.mx.Unlock()

		for i, evt := range buf {
			err := w.WriteMsg(evt)
			if err != nil {
				logger.Errorf("error writing event trace: %s", err)
				return err
			}

			buf[i] = nil
		}
	}

	logger.Debugf("closing trace log")

	err := gzipW.Close()
	if err != nil {
		logger.Errorf("error closing trace log: %s", err)
		result = multierror.Append(result, err)
	}

	err = out.Close()
	if err != nil {
		logger.Errorf("error closing trace log: %s", err)
		result = multierror.Append(result, err)
	}

	return result
}

// collectWorker is the main worker. It keeps recording traces into the
// `current` file and rotates the file when it's filled.
func (tc *TraceCollector) collectFileWorker() {
	defer close(tc.doneCh)

	current := fmt.Sprintf("%s/current", tc.dir)

	for {
		out, err := os.OpenFile(current, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		err = tc.writeFile(out)
		if err != nil {
			panic(err)
		}

		// Rotate the file.
		base := fmt.Sprintf("trace.%d", time.Now().UnixNano())
		next := fmt.Sprintf("%s/%s.pb.gz", tc.dir, base)
		logger.Debugf("move %s -> %s", current, next)
		err = os.Rename(current, next)
		if err != nil {
			panic(err)
		}

		// Generate the json output if so desired
		if tc.jsonTrace != "" {
			tc.writeJsonTrace(next, base)
		}

		// yield if we're done.
		select {
		case <-tc.exitCh:
			return
		default:
		}
	}
}

func (tc *TraceCollector) writeJsonTrace(trace, name string) {
	// open the trace, read it and transcode to json
	in, err := os.Open(trace)
	if err != nil {
		panic(err)
	}
	defer in.Close()

	gzipR, err := gzip.NewReader(in)
	if err != nil {
		panic(err)
	}
	defer gzipR.Close()

	tmpTrace := fmt.Sprintf("/tmp/%s.json", name)
	out, err := os.OpenFile(tmpTrace, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	var evt pb.TraceEvent
	pbr := ggio.NewDelimitedReader(gzipR, 1<<20)
	enc := json.NewEncoder(out)

loop:
	for {
		evt.Reset()

		switch err = pbr.ReadMsg(&evt); err {
		case nil:
			err = enc.Encode(&evt)
			if err != nil {
				panic(err)
			}
		case io.EOF:
			break loop
		default:
			panic(err)
		}
	}

	err = out.Close()
	if err != nil {
		panic(err)
	}

	jsonTrace := fmt.Sprintf("%s/%s.json", tc.jsonTrace, name)
	err = os.Rename(tmpTrace, jsonTrace)
	if err != nil {
		panic(err)
	}
}

// monitorWorker watches the current file and triggers closure+rotation based on
// time and size.
//
// TODO (#3): this needs to use select so we can rcv from exitCh and yield.
func (tc *TraceCollector) monitorWorker() {
	current := fmt.Sprintf("%s/current", tc.dir)

Outer:
	for {
		start := time.Now()

	Inner:
		for {
			time.Sleep(time.Minute)

			now := time.Now()
			if now.After(start.Add(MaxLogTime)) {
				select {
				case tc.flushFileCh <- struct{}{}:
				default:
				}
				continue Outer
			}

			finfo, err := os.Stat(current)
			if err != nil {
				logger.Warnf("error stating trace log file: %s", err)
				continue Inner
			}

			if finfo.Size() > int64(MaxLogSize) {
				select {
				case tc.flushFileCh <- struct{}{}:
				default:
				}

				continue Outer
			}
		}
	}
}
