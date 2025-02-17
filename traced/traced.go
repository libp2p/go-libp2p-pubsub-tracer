package traced

import (
	"compress/gzip"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
	logging "github.com/ipfs/go-log"
)

var (
	// MaxLogSize is the size threshold at which the current trace file will be
	// rotated. The default value is 10Mb.
	MaxLogSize = 1024 * 1024 * 64

	// MaxLogTime is the maximum amount of time a file will remain open before
	// being rotated.
	MaxLogTime = time.Hour

	logger = logging.Logger("traced")
)

type TraceCollector struct {
	host      host.Host
	dir       string
	jsonTrace string

	mx  sync.Mutex
	buf []*pb.TraceEvent

	notifyWriteCh chan struct{}
	flushFileCh   chan struct{}
	exitCh        chan struct{}
	doneCh        chan struct{}
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

	r := ggio.NewDelimitedReader(gzipR, 1<<22)
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
