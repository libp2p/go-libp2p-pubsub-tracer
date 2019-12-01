package main

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

	logger = logging.Logger("collectd")
)

type Collector struct {
	host host.Host
	dir  string

	mx  sync.Mutex
	buf []*pb.TraceEvent

	notifyWriteCh chan struct{}
	flushFileCh   chan struct{}
	exitCh        chan struct{}
	doneCh        chan struct{}
}

// NewCollector creates a new pubsub traces collector. A collector is a process
// that listens on a libp2p endpoint, accepts pubsub tracing streams from peers,
// and records the incoming data into rotating gzip files.
func NewCollector(host host.Host, dir string) (*Collector, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	c := &Collector{
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
func (c *Collector) Stop() {
	close(c.exitCh)
	c.flushFileCh <- struct{}{}
	<-c.doneCh
}

// Flush flushes and rotates the current file.
func (c *Collector) Flush() {
	c.flushFileCh <- struct{}{}
}

// handleStream accepts an incoming tracing stream and drains it into the
// buffer, until the stream is closed or an error occurs.
func (c *Collector) handleStream(s network.Stream) {
	defer s.Close()

	logger.Debugf("new stream from %s", s.Conn().RemotePeer())

	gzipR, err := gzip.NewReader(s)
	if err != nil {
		logger.Debugf("error opening compressed stream from %s: %s", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	r := ggio.NewDelimitedReader(gzipR, 1<<24)
	var msg pb.TraceEventBatch

	for {
		msg.Reset()

		switch err = r.ReadMsg(&msg); err {
		case nil:
			c.mx.Lock()
			c.buf = append(c.buf, msg.Batch...)
			c.mx.Unlock()

			select {
			case c.notifyWriteCh <- struct{}{}:
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
func (c *Collector) writeFile(out io.WriteCloser) (result error) {
	var (
		flush bool

		gzipW = gzip.NewWriter(out)
		w     = ggio.NewDelimitedWriter(gzipW)
	)

	for !flush {
		select {
		case <-c.notifyWriteCh:
		case <-c.flushFileCh:
			flush = true
		}

		c.mx.Lock()
		buf := make([]*pb.TraceEvent, 0, len(c.buf))
		copy(buf, c.buf)

		c.buf = c.buf[:0]
		c.mx.Unlock()

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
func (c *Collector) collectWorker() {
	defer close(c.doneCh)

	current := fmt.Sprintf("%s/current", c.dir)

	for {
		out, err := os.OpenFile(current, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		err = c.writeFile(out)
		if err != nil {
			panic(err)
		}

		// Rotate the file.
		next := fmt.Sprintf("%s/trace.%d.pb.gz", c.dir, time.Now().UnixNano())
		logger.Debugf("move %s -> %s", current, next)
		err = os.Rename(current, next)
		if err != nil {
			panic(err)
		}

		// yield if we're done.
		select {
		case <-c.exitCh:
			return
		default:
		}
	}
}

// monitorWorker watches the current file and triggers closure+rotation based on
// time and size.
//
// TODO (#3): this needs to use select so we can rcv from exitCh and yield.
func (c *Collector) monitorWorker() {
	current := fmt.Sprintf("%s/current", c.dir)

Outer:
	for {
		start := time.Now()

	Inner:
		for {
			time.Sleep(time.Minute)

			now := time.Now()
			if now.After(start.Add(MaxLogTime)) {
				select {
				case c.flushFileCh <- struct{}{}:
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
				case c.flushFileCh <- struct{}{}:
				default:
				}

				continue Outer
			}
		}
	}
}
