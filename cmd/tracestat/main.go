package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
)

func main() {
	stat := &tracestat{peers: make(map[peer.ID]*msgstat), msgs: make(map[string][]int64)}
	for _, f := range os.Args[1:] {
		stat.load(f)
	}

	stat.compute()

	// this will just print some stuff to stdout
	// TODO: produce JSON output
	stat.printSummary()
}

type tracestat struct {
	// peer peer stats
	peers map[peer.ID]*msgstat

	// aggregate stats
	aggregate msgstat

	// message propagation trace: timestamps from published and delivered messages
	// we can compute propagation delay distributions from this
	msgs map[string][]int64

	// this is the computed propagation delay distribution
	delayCDF []sample
}

// message statistics
type msgstat struct {
	publish   int
	deliver   int
	duplicate int
	reject    int
	sendRPC   int
	dropRPC   int
}

type sample struct {
	delay int64
	count int
}

func (ts *tracestat) load(f string) {
	r, err := os.Open(f)
	if err != nil {
		log.Printf("error opening trace file %s: %s", f, err)
		return
	}
	defer r.Close()

	gzipR, err := gzip.NewReader(r)
	if err != nil {
		log.Printf("error opening gzip reader for %s: %s", f, err)
		return
	}
	defer gzipR.Close()

	var evt pb.TraceEvent
	pbr := ggio.NewDelimitedReader(gzipR, 1<<20)

	for {
		evt.Reset()

		err = pbr.ReadMsg(&evt)
		if err != nil {
			if err == io.EOF {
				return
			}

			log.Printf("error decoding trace event from %s: %s", f, err)
			return
		}

		ts.addEvent(&evt)
	}
}

func (ts *tracestat) addEvent(evt *pb.TraceEvent) {
	peer := peer.ID(evt.GetPeerID())
	ps, ok := ts.peers[peer]
	if !ok {
		ps = &msgstat{}
		ts.peers[peer] = ps
	}
	timestamp := evt.GetTimestamp()

	switch evt.GetType() {
	case pb.TraceEvent_PUBLISH_MESSAGE:
		ps.publish++
		ts.aggregate.publish++
		mid := string(evt.GetPublishMessage().GetMessageID())
		ts.msgs[mid] = append(ts.msgs[mid], timestamp)

	case pb.TraceEvent_REJECT_MESSAGE:
		ps.reject++
		ts.aggregate.reject++

	case pb.TraceEvent_DUPLICATE_MESSAGE:
		ps.duplicate++
		ts.aggregate.duplicate++

	case pb.TraceEvent_DELIVER_MESSAGE:
		ps.deliver++
		ts.aggregate.deliver++
		mid := string(evt.GetPublishMessage().GetMessageID())
		ts.msgs[mid] = append(ts.msgs[mid], timestamp)

	case pb.TraceEvent_SEND_RPC:
		ps.sendRPC++
		ts.aggregate.sendRPC++

	case pb.TraceEvent_DROP_RPC:
		ps.dropRPC++
		ts.aggregate.dropRPC++
	}
}

func (ts *tracestat) compute() {
	// TODO
}

func (ts *tracestat) printSummary() {
	fmt.Printf("=== Trace Summary ===\n")
	fmt.Printf("Peers: %d\n", len(ts.peers))
	fmt.Printf("Published Messages: %d\n", ts.aggregate.publish)
	fmt.Printf("Delivered Messages: %d\n", ts.aggregate.deliver)
	fmt.Printf("Duplicate Messages: %d\n", ts.aggregate.duplicate)
	fmt.Printf("Rejected Messages: %d\n", ts.aggregate.reject)
	fmt.Printf("Sent RPCs: %d\n", ts.aggregate.sendRPC)
	fmt.Printf("Dropped RPCS: %d\n", ts.aggregate.dropRPC)
}
