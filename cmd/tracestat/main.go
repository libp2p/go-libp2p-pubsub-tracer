package main

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
)

// tracestat is a program that parses a pubsub tracer dump and calculates
// statistics. By default, stats are printed to stdout, but they can be
// optionally written to a JSON file for further processing.
func main() {
	var err error
	defer func() {
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	summary := flag.Bool("summary", true, "print trace summary")
	cdf := flag.Bool("cdf", false, "print propagation delay CDF")
	jsonOut := flag.String("json", "", "save analysis output to json file")
	topic := flag.String("topic", "", "analyze traces for a specific topic only")
	flag.Parse()

	stat := &tracestat{
		peers:       make(map[peer.ID]*msgstat),
		topics:      make(map[string]struct{}),
		msgsInTopic: make(map[string]struct{}),
		msgs:        make(map[string][]int64),
		delays:      make(map[string][]int64),
	}

	if *topic != "" {
		// do a first pass to get the msgIDs in the topic
		for _, f := range flag.Args() {
			err = load(f, func(evt *pb.TraceEvent) {
				stat.markEventForTopic(evt, *topic)
			})
			if err != nil {
				return
			}
		}

		// and now do a second pass adding events for the relevant topic only
		for _, f := range flag.Args() {
			err = load(f, stat.addEventForTopic)
			if err != nil {
				return
			}
		}

	} else {
		for _, f := range flag.Args() {
			err = load(f, stat.addEvent)
			if err != nil {
				return
			}
		}
	}

	if *cdf || *jsonOut != "" {
		stat.compute()
	}

	// this will just print some stuff to stdout
	// TODO: produce JSON output
	if *summary {
		stat.printSummary()
	}
	if *cdf {
		stat.printCDF()
	}
	if *jsonOut != "" {
		err = stat.dumpJSON(*jsonOut)
	}
}

// tracestat is the tree that's populated as we parse the dump.
type tracestat struct {
	// peers summarizes per-peer stats.
	peers map[peer.ID]*msgstat

	// topics is tha map of topics recorded in the trace
	topics map[string]struct{}

	// msgsInTopic contains a set of message IDs in the target topic
	msgsInTopic map[string]struct{}

	// aggregate stats.
	aggregate msgstat

	// msgs contains per-message propagation traces: timestamps from published
	// and delivered messages.
	msgs map[string][]int64

	// delays is the propagation delay per message, in millis.
	delays map[string][]int64

	// delayCDF is the computed propagation delay distribution across all
	// messages.
	delayCDF []sample
}

// msgstat holds message statistics.
type msgstat struct {
	publish   int
	deliver   int
	duplicate int
	reject    int
	sendRPC   int
	dropRPC   int
}

// sample represents a CDF bucket.
type sample struct {
	delay int
	count int
}

func load(f string, addEvent func(*pb.TraceEvent)) error {
	r, err := os.Open(f)
	if err != nil {
		return fmt.Errorf("error opening trace file %s: %w", f, err)
	}
	defer r.Close()

	gzipR, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("error opening gzip reader for %s: %w", f, err)
	}
	defer gzipR.Close()

	var evt pb.TraceEvent
	pbr := ggio.NewDelimitedReader(gzipR, 1<<20)

	for {
		evt.Reset()

		switch err = pbr.ReadMsg(&evt); err {
		case nil:
			addEvent(&evt)
		case io.EOF:
			return nil
		default:
			return fmt.Errorf("error decoding trace event from %s: %w", f, err)
		}
	}
}

func (ts *tracestat) markEventForTopic(evt *pb.TraceEvent, topic string) {
	switch evt.GetType() {
	case pb.TraceEvent_PUBLISH_MESSAGE:
		msgTopic := evt.GetPublishMessage().GetTopic()
		if msgTopic == topic {
			mid := string(evt.GetPublishMessage().GetMessageID())
			ts.msgsInTopic[mid] = struct{}{}
			break
		}
	}
}

func (ts *tracestat) addEventForTopic(evt *pb.TraceEvent) {
	addEventInTopic := func(evt *pb.TraceEvent, mid string) {
		_, ok := ts.msgsInTopic[mid]
		if ok {
			ts.addEvent(evt)
		}
	}

	switch evt.GetType() {
	case pb.TraceEvent_PUBLISH_MESSAGE:
		mid := string(evt.GetPublishMessage().GetMessageID())
		addEventInTopic(evt, mid)

	case pb.TraceEvent_REJECT_MESSAGE:
		mid := string(evt.GetRejectMessage().GetMessageID())
		addEventInTopic(evt, mid)

	case pb.TraceEvent_DUPLICATE_MESSAGE:
		mid := string(evt.GetDuplicateMessage().GetMessageID())
		addEventInTopic(evt, mid)

	case pb.TraceEvent_DELIVER_MESSAGE:
		mid := string(evt.GetDeliverMessage().GetMessageID())
		addEventInTopic(evt, mid)
	}
}

func (ts *tracestat) addEvent(evt *pb.TraceEvent) {
	var (
		peer      = peer.ID(evt.GetPeerID())
		timestamp = evt.GetTimestamp()
	)

	ps, ok := ts.peers[peer]
	if !ok {
		ps = &msgstat{}
		ts.peers[peer] = ps
	}

	switch evt.GetType() {
	case pb.TraceEvent_PUBLISH_MESSAGE:
		ps.publish++
		ts.aggregate.publish++
		mid := string(evt.GetPublishMessage().GetMessageID())
		ts.msgs[mid] = append(ts.msgs[mid], timestamp)
		topic := evt.GetPublishMessage().GetTopic()
		ts.topics[topic] = struct{}{}

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
	fmt.Printf("Topics: %s\n", topicList(ts.topics))
	fmt.Printf("Published Messages: %d\n", ts.aggregate.publish)
	fmt.Printf("Delivered Messages: %d\n", ts.aggregate.deliver)
	fmt.Printf("Duplicate Messages: %d\n", ts.aggregate.duplicate)
	fmt.Printf("Rejected Messages: %d\n", ts.aggregate.reject)
	fmt.Printf("Sent RPCs: %d\n", ts.aggregate.sendRPC)
	fmt.Printf("Dropped RPCs: %d\n", ts.aggregate.dropRPC)
}

func (ts *tracestat) printCDF() {
	fmt.Printf("=== Propagation Delay CDF (ms) ===\n")
	for _, sample := range ts.delayCDF {
		fmt.Printf("%d %d\n", sample.delay, sample.count)
	}
}

func (ts *tracestat) dumpJSON(f string) error {
	w, err := os.OpenFile(f, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer w.Close()

	enc := json.NewEncoder(w)

	// Counts breaks down counts per event type.
	type Counts struct {
		Publish, Deliver, Duplicate, Reject, SendRPC, DropRPC int
	}

	// Bucket represents a bucket in a cumulative distribution function.
	type Bucket struct {
		Millis int
		Count  int
	}

	// Dump represents the full JSON dump tree.
	var dump struct {
		Events struct {
			PerPeer map[string]Counts
			Totals  Counts
		}
		Delays struct {
			PerMessage map[string][]int
			CDF        []Bucket
		}
	}

	dump.Events.PerPeer = make(map[string]Counts)
	dump.Delays.PerMessage = make(map[string][]int)

	for p, st := range ts.peers {
		dump.Events.PerPeer[p.Pretty()] = Counts{
			Publish:   st.publish,
			Deliver:   st.deliver,
			Duplicate: st.duplicate,
			Reject:    st.reject,
			SendRPC:   st.sendRPC,
			DropRPC:   st.dropRPC,
		}
	}

	dump.Events.Totals = Counts{
		Publish:   ts.aggregate.publish,
		Deliver:   ts.aggregate.deliver,
		Duplicate: ts.aggregate.duplicate,
		Reject:    ts.aggregate.reject,
		SendRPC:   ts.aggregate.sendRPC,
		DropRPC:   ts.aggregate.dropRPC,
	}

	for mid, delays := range ts.delays {
		delayMillis := make([]int, len(delays))
		for i, dt := range delays {
			delayMillis[i] = int((dt + 499999) / 1000000) // round up.
		}
		midHex := hex.EncodeToString([]byte(mid))
		dump.Delays.PerMessage[midHex] = delayMillis
	}

	delayCDF := make([]Bucket, len(ts.delayCDF))
	for i, s := range ts.delayCDF {
		delayCDF[i] = Bucket{Millis: s.delay, Count: s.count}
	}
	dump.Delays.CDF = delayCDF

	enc.Encode(dump)
	return nil
}

func topicList(topics map[string]struct{}) string {
	var lst []string
	for topic := range topics {
		lst = append(lst, topic)
	}
	return strings.Join(lst, ",")
}
