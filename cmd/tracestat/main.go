package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p-core/peer"

	ggio "github.com/gogo/protobuf/io"
)

func main() {
	summary := flag.Bool("summary", true, "print trace summary")
	cdf := flag.Bool("cdf", false, "print propagation delay CDF")
	flag.Parse()

	stat := &tracestat{
		peers:  make(map[peer.ID]*msgstat),
		msgs:   make(map[string][]int64),
		delays: make(map[string][]int64),
	}
	for _, f := range flag.Args() {
		stat.load(f)
	}

	stat.compute()

	// this will just print some stuff to stdout
	// TODO: produce JSON output
	if *summary {
		stat.printSummary()
	}
	if *cdf {
		stat.printCDF()
	}
}

type tracestat struct {
	// per peer stats
	peers map[peer.ID]*msgstat

	// aggregate stats
	aggregate msgstat

	// message propagation trace: timestamps from published and delivered messages
	msgs map[string][]int64

	// message propagation delays
	delays map[string][]int64

	// this is the computed propagation delay distribution across all messages
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
	delay int
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
		mid := string(evt.GetDeliverMessage().GetMessageID())
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
	// sort the message publish/delivery timestamps and transform to delays
	for mid, timestamps := range ts.msgs {
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		delays := make([]int64, len(timestamps)-1)
		t0 := timestamps[0]
		for i, t := range timestamps[1:] {
			delays[i] = t - t0
		}
		ts.delays[mid] = delays
	}

	// compute the CDF rounded to millisecond precision
	samples := make(map[int]int)
	for _, delays := range ts.delays {
		for _, dt := range delays {
			mdt := int((dt + 499999) / 1000000)
			samples[mdt]++
		}
	}

	xsamples := make([]sample, 0, len(samples))
	for dt, count := range samples {
		xsamples = append(xsamples, sample{dt, count})
	}
	sort.Slice(xsamples, func(i, j int) bool {
		return xsamples[i].delay < xsamples[j].delay
	})
	for i := 1; i < len(xsamples); i++ {
		xsamples[i].count += xsamples[i-1].count
	}
	ts.delayCDF = xsamples
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

func (ts *tracestat) printCDF() {
	fmt.Printf("=== Propagation Delay CDF ===\n")
	for _, sample := range ts.delayCDF {
		fmt.Printf("%d %d\n", sample.delay, sample.count)
	}
}
