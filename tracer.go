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

var log = logging.Logger("traced")

var MaxLogSize = 1024 * 1024 * 64
var MaxLogTime = time.Hour

type Tracer struct {
	host host.Host
	dir  string

	mx  sync.Mutex
	buf []*pb.TraceEvent

	wr    chan struct{}
	flush chan struct{}
	done  chan struct{}
	exit  bool
}

func NewTracer(host host.Host, dir string) (*Tracer, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	t := &Tracer{
		host:  host,
		dir:   dir,
		wr:    make(chan struct{}, 1),
		flush: make(chan struct{}, 1),
		done:  make(chan struct{}, 1),
	}

	host.SetStreamHandler(pubsub.RemoteTracerProtoID, t.handleStream)
	go t.doWrite()
	go t.doMonitor()

	return t, nil
}

func (t *Tracer) Stop() {
	t.exit = true
	t.flush <- struct{}{}
	<-t.done
}

func (t *Tracer) handleStream(s network.Stream) {
	log.Debugf("New stream from %s", s.Conn().RemotePeer())

	gzipR, err := gzip.NewReader(s)
	if err != nil {
		log.Debugf("error opening compressed stream from %s: %s", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	r := ggio.NewDelimitedReader(gzipR, 1<<24)
	var msg pb.TraceEventBatch

	for {
		msg.Reset()
		err := r.ReadMsg(&msg)

		if err != nil {
			if err != io.EOF {
				log.Debugf("error reading batch from %s: %s", s.Conn().RemotePeer(), err)
				s.Reset()
			} else {
				s.Close()
			}
			return
		}

		t.mx.Lock()
		t.buf = append(t.buf, msg.Batch...)
		t.mx.Unlock()

		select {
		case t.wr <- struct{}{}:
		default:
		}
	}
}

func (t *Tracer) handleWrite(out io.WriteCloser) error {
	var buf []*pb.TraceEvent
	var flush bool

	gzipW := gzip.NewWriter(out)
	w := ggio.NewDelimitedWriter(gzipW)

	for {
		select {
		case <-t.wr:
		case <-t.flush:
			flush = true
		}

		t.mx.Lock()
		tmp := t.buf
		t.buf = buf[:0]
		buf = tmp
		t.mx.Unlock()

		for i, evt := range buf {
			err := w.WriteMsg(evt)
			if err != nil {
				log.Errorf("error writing event trace: %s", err)
				return err
			}
			buf[i] = nil
		}

		if flush {
			var result error

			log.Debugf("flushing trace log")

			err := gzipW.Close()
			if err != nil {
				log.Errorf("error flushing trace log: %s", err)
				result = multierror.Append(result, err)
			}

			err = out.Close()
			if err != nil {
				log.Errorf("error closing trace log: %s", err)
				result = multierror.Append(result, err)
			}

			return result
		}
	}
}

func (t *Tracer) doWrite() {
	current := fmt.Sprintf("%s/current", t.dir)
	for {
		out, err := os.OpenFile(current, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		err = t.handleWrite(out)
		if err != nil {
			panic(err)
		}

		next := fmt.Sprintf("%s/trace.%d.pb.gz", t.dir, time.Now().UnixNano())
		log.Debugf("move %s -> %s", current, next)
		err = os.Rename(current, next)
		if err != nil {
			panic(err)
		}

		if t.exit {
			t.done <- struct{}{}
			return
		}
	}
}

func (t *Tracer) doMonitor() {
	current := fmt.Sprintf("%s/current", t.dir)

outer:
	for {
		start := time.Now()

	inner:
		for {
			time.Sleep(time.Minute)

			now := time.Now()
			if now.After(start.Add(MaxLogTime)) {
				select {
				case t.flush <- struct{}{}:
				default:
				}
				continue outer
			}

			f, err := os.Open(current)
			if err != nil {
				log.Warningf("error opening trace log file: %s", err)
				continue inner
			}

			finfo, err := f.Stat()
			f.Close()

			if err != nil {
				log.Warningf("error stating trace log file: %s", err)
				continue inner
			}

			if finfo.Size() > int64(MaxLogSize) {
				select {
				case t.flush <- struct{}{}:
				default:
				}

				continue outer
			}
		}
	}
}
