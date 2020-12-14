package traced

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
	"github.com/hashicorp/go-multierror"
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

// sourceRange specifies which range of buf entries were reported by this remote maddr.
type sourceRange struct {
	start, end int // start and end are both inclusive.
	maddr      multiaddr.Multiaddr
}

type TraceCollector struct {
	host host.Host
	dir  string

	json    bool
	jsonDir string

	mx      sync.Mutex
	buf     []*pb.TraceEvent
	sources []sourceRange

	notifyWriteCh chan struct{}
	flushFileCh   chan struct{}
	exitCh        chan struct{}
	doneCh        chan struct{}
}

// NewTraceCollector creates a new pubsub traces collector. A collector is a process
// that listens on a libp2p endpoint, accepts pubsub tracing streams from peers,
// and records the incoming data into rotating gzip files.
// If the json argument is not empty, then every time a new trace is generated, it will be written
// to this directory in json format for online processing.
func NewTraceCollector(host host.Host, dir, jsonDir string) (*TraceCollector, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	if jsonDir != "" {
		err := os.MkdirAll(jsonDir, 0755)
		if err != nil {
			return nil, err
		}
	}

	c := &TraceCollector{
		host:          host,
		dir:           dir,
		json:          jsonDir != "",
		jsonDir:       jsonDir,
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

	r := ggio.NewDelimitedReader(gzipR, 1<<22)
	var msg pb.TraceEventBatch

	for {
		msg.Reset()

		switch err = r.ReadMsg(&msg); err {
		case nil:
			if len(msg.Batch) == 0 {
				continue
			}

			tc.mx.Lock()
			start := len(tc.buf)
			tc.buf = append(tc.buf, msg.Batch...)
			end := len(tc.buf)
			tc.sources = append(tc.sources, sourceRange{start: start, end: end, maddr: s.Conn().RemoteMultiaddr()})
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

// writeFiles records incoming traces into a gzipped file, and yields every time
// a flush is triggered.
func (tc *TraceCollector) writeFiles(out io.WriteCloser, jsonOut io.WriteCloser) (result error) {
	var (
		flush bool

		// buf is an internal buffer that gets swapped with the collector's
		// buffer on write. This saves allocs, and the slices grow to
		// accommodate the volume that's being written.
		buf []*pb.TraceEvent

		gzipW = gzip.NewWriter(out)
		w     = ggio.NewDelimitedWriter(gzipW)

		jsonEncoder *json.Encoder
	)

	if jsonOut != nil {
		jsonEncoder = json.NewEncoder(jsonOut)
		defer jsonOut.Close()
	}

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
		//
		// Since we expect the sources slice to be small, we don't need the same
		// dance. We just make the sources in state slice nil.
		tc.mx.Lock()
		// buffer dance.
		tmp := tc.buf
		tc.buf = buf[:0]
		buf = tmp
		// reset sources.
		sources := tc.sources
		tc.sources = nil
		tc.mx.Unlock()

		var (
			currSourceIdx int
			currSource    = &sources[0]
		)
		for i, evt := range buf {
			if i > currSource.end {
				// finished the range for this source, move to next.
				// no need to do any slice boundary check where because
				// consistency is guaranteed.
				currSourceIdx++
				currSource = &sources[currSourceIdx]
			}

			buf[i] = nil

			// write to the binary output.
			err := w.WriteMsg(evt)
			if err != nil {
				logger.Errorf("error writing event trace: %s", err)
				return err
			}

			if jsonEncoder == nil {
				continue
			}

			// convert the event to a map[string]interface{} for easier manipulation.
			m := structMap(evt)

			// transform the event.
			m = transformRec(m)

			// enrich with the IP address from the multiaddr.
			if ipaddr, err := manet.ToIP(currSource.maddr); err == nil {
				m["ip"] = ipaddr.String()
			}

			if err = jsonEncoder.Encode(m); err != nil {
				logger.Errorf("error writing JSON trace: %s", err)
				return err
			}
		}
	}

	logger.Debugf("closing trace logs")

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
// `current` files and rotates them when filled.
func (tc *TraceCollector) collectWorker() {
	defer close(tc.doneCh)

	var (
		currentBin  = filepath.Join(tc.dir, "current")
		currentJSON string
	)

	if tc.json {
		currentJSON = filepath.Join(tc.jsonDir, "current")
	}

	for {
		// (re-)create the binary file.
		binaryOut, err := os.OpenFile(currentBin, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}

		// (re-)create the json file, if emitting json.
		var jsonOut *os.File
		if tc.json {
			jsonOut, err = os.OpenFile(currentJSON, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
		}

		err = tc.writeFiles(binaryOut, jsonOut)
		if err != nil {
			panic(err)
		}

		base := fmt.Sprintf("trace.%d", time.Now().UnixNano())

		// Rotate the binary file.
		archiveBin := filepath.Join(tc.dir, base+".pb.gz")
		logger.Debugf("move %s -> %s", currentBin, archiveBin)
		err = os.Rename(currentBin, archiveBin)
		if err != nil {
			panic(err)
		}

		// Rotate the JSON file.
		if tc.json {
			archiveJSON := filepath.Join(tc.dir, base+".json")
			logger.Debugf("move %s -> %s", currentJSON, archiveJSON)
			err = os.Rename(currentJSON, archiveJSON)
			if err != nil {
				panic(err)
			}
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

func structMap(obj interface{}) map[string]interface{} {
	if obj == nil {
		return map[string]interface{}{}
	}

	var (
		ret = make(map[string]interface{})
		typ = reflect.TypeOf(obj)
		val = reflect.Indirect(reflect.ValueOf(obj))
	)

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json") // json tag.
		fieldValue := val.Field(i)
		if tag == "" || tag == "-" || fieldValue.IsZero() { // skip if no tag, or -, or field is zero.
			continue
		}
		tag = strings.Split(tag, ",")[0]
		if ftyp := typ.Field(i).Type; ftyp.Kind() == reflect.Struct ||
			(ftyp.Kind() == reflect.Ptr && ftyp.Elem().Kind() == reflect.Struct) {
			ret[tag] = structMap(fieldValue.Interface())
		} else {
			ret[tag] = fieldValue.Interface()
		}
	}
	return ret
}

func transformRec(m map[string]interface{}) map[string]interface{} {
	for k, v := range m {
		if innerMap, ok := v.(map[string]interface{}); ok {
			transformRec(innerMap)
			continue
		}
		switch k {
		case "type":
			// replace numeric enum with string
			m["type"] = v.(*pb.TraceEvent_Type).String()
		case "timestamp":
			// patch the timestamp to drop the microsecond and nanosecond portions.
			m["timestamp"] = *(v.(*int64)) / int64(time.Millisecond)
		case "peerID":
			// parse the peer ID.
			if id, err := peer.IDFromBytes(v.([]byte)); err == nil {
				m["peerID"] = id.String()
			}
		}
	}
	return m
}
