package traced

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
	multierror "github.com/hashicorp/go-multierror"
	logging "github.com/ipfs/go-log"
)

var (
	// MaxLogSize is the size threshold at which the current trace file will be
	// rotated. The default value is 10Mb.
	MaxLogSize = 1024 * 1024 * 64

	// MaxLogTime is the maximum amount of time a file will remain open before
	// being rotated.
	MaxLogTime = time.Hour

	logger = logging.Logger("tracecollector")
)

type TraceCollector struct {
	host host.Host
	dir  string

	mx  sync.Mutex
	buf []*pb.TraceEvent

	notifyWriteCh chan struct{}
	flushFileCh   chan struct{}
	exitCh        chan struct{}
	doneCh        chan struct{}
}

// NewTraceCollector creates a new pubsub traces collector. A collector is a process
// that listens on a libp2p endpoint, accepts pubsub tracing streams from peers,
// and records the incoming data into rotating gzip files.
func NewTraceCollector(host host.Host, dir string) (*TraceCollector, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	c := &TraceCollector{
		host:          host,
		dir:           dir,
		notifyWriteCh: make(chan struct{}, 1),
		flushFileCh:   make(chan struct{}, 1),
		exitCh:        make(chan struct{}, 1),
		doneCh:        make(chan struct{}, 1),
	}

	host.SetStreamHandler(pubsub.RemoteTracerProtoID, c.handleStream)

	go c.collectWorker()
	go c.monitorWorker()

	return c, nil
}

// Stop stops the collector.
func (tc *TraceCollector) Stop() {
	close(tc.exitCh)
	tc.flushFileCh <- struct{}{}
	<-tc.doneCh
}

// Flush flushes and rotates the current file.
func (tc *TraceCollector) Flush() {
	tc.flushFileCh <- struct{}{}
}

// handleStream accepts an incoming tracing stream and drains it into the
// buffer, until the stream is closed or an error occurs.
func (tc *TraceCollector) handleStream(s network.Stream) {
	defer s.Close()

	logger.Debugf("new stream from %s", s.Conn().RemotePeer())

	gzipR, err := gzip.NewReader(s)
	if err != nil {
		logger.Debugf("error opening compressed stream from %s: %s", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	r := ggio.NewDelimitedReader(gzipR, 1<<20)
	var msg pb.TraceEventBatch

	for {
		msg.Reset()

		switch err = r.ReadMsg(&msg); err {
		case nil:
			tc.mx.Lock()
			tc.buf = append(tc.buf, msg.Batch...)
			tc.mx.Unlock()

			select {
			case tc.notifyWriteCh <- struct{}{}:
			default:
			}

		case io.EOF:
			return

		default:
			logger.Debugf("error reading batch from %s: %s", s.Conn().RemotePeer(), err)
			return
		}
	}
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
func (tc *TraceCollector) collectWorker() {
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
		next := fmt.Sprintf("%s/trace.%d.pb.gz", tc.dir, time.Now().UnixNano())
		logger.Debugf("move %s -> %s", current, next)
		err = os.Rename(current, next)
		if err != nil {
			panic(err)
		}

		// yield if we're done.
		select {
		case <-tc.exitCh:
			return
		default:
		}
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
				logger.Warningf("error stating trace log file: %s", err)
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
