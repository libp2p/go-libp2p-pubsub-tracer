package traced

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// NewFileTraceCollector creates a new pubsub traces collector. A collector is a process
// that listens on a libp2p endpoint, accepts pubsub tracing streams from peers,
// and sends traces to elasticsearch instance
func NewElasticsearchTraceCollector(host host.Host, connectionString string, index string) (*TraceCollector, error) {
	conUrl, err := url.Parse(connectionString)

	username := conUrl.User.Username()
	password, _ := conUrl.User.Password()
	cfg := elasticsearch.Config{
		Addresses: []string{
			conUrl.Scheme + "://" + conUrl.Host,
		},
		Username: username,
		Password: password,
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	c := &TraceCollector{
		host:          host,
		notifyWriteCh: make(chan struct{}, 1),
		flushFileCh:   make(chan struct{}, 1),
		exitCh:        make(chan struct{}, 1),
		doneCh:        make(chan struct{}, 1),
	}
	host.SetStreamHandler(pubsub.RemoteTracerProtoID, c.handleStream)

	go c.collectElasticsearchWorker(es, index)

	return c, nil
}

// collectToElasticsearchWorker routes collected traces to elasticsearch instance
func (tc *TraceCollector) collectElasticsearchWorker(es *elasticsearch.Client, index string) {
	defer close(tc.doneCh)

	var buf []*pb.TraceEvent

	for {
		tc.mx.Lock()
		tmp := tc.buf
		tc.buf = buf[:0]
		buf := tmp
		tc.mx.Unlock()

		for i, evt := range buf {
			jsonEvt, err := json.Marshal(evt)
			if err != nil {
				buf[i] = nil
				fmt.Printf("Failed marshaling event: %s", err)
			}

			req := esapi.IndexRequest{
				Index:   index,
				Body:    strings.NewReader(string(jsonEvt)),
				Refresh: "true",
			}

			res, err := req.Do(context.Background(), es)
			if err != nil {
				buf[i] = nil
				fmt.Printf("Failed sending event to elasticsearch: %s", err)
				continue
			}

			res.Body.Close()
			buf[i] = nil
		}

		// yield if we're done
		select {
		case <-tc.exitCh:
			return
		default:
		}
	}
}
