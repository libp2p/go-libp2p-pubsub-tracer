package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
)

func main() {
	var err error
	defer func() {
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	topic := flag.String("topic", "", "topic for which to print the mesh")
	flag.Parse()

	if *topic == "" {
		fmt.Printf("Topic must be specified")
		os.Exit(1)
	}

	mesh := make(map[peer.ID]map[peer.ID]struct{})

	for _, f := range flag.Args() {
		err = load(f, mesh, *topic)
		if err != nil {
			return
		}
	}

	// print out the mesh
	for p, peers := range mesh {
		fmt.Printf("%s ", p)
		for pp := range peers {
			fmt.Printf("%s ", pp)
		}
		fmt.Printf("\n")
	}
}

func load(f string, mesh map[peer.ID]map[peer.ID]struct{}, topic string) error {
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
			addEvent(&evt, mesh, topic)
		case io.EOF:
			return nil
		default:
			return fmt.Errorf("error decoding trace event from %s: %w", f, err)
		}
	}
}

func addEvent(evt *pb.TraceEvent, mesh map[peer.ID]map[peer.ID]struct{}, topic string) {
	switch evt.GetType() {
	case pb.TraceEvent_GRAFT:
		p := peer.ID(evt.GetPeerID())
		graft := evt.GetGraft()

		et := graft.GetTopic()
		if et != topic {
			break
		}

		peers, ok := mesh[p]
		if !ok {
			peers = make(map[peer.ID]struct{})
			mesh[p] = peers
		}

		pp := peer.ID(graft.GetPeerID())
		peers[pp] = struct{}{}

	case pb.TraceEvent_PRUNE:
		p := peer.ID(evt.GetPeerID())
		prune := evt.GetPrune()

		et := prune.GetTopic()
		if et != topic {
			break
		}

		peers, ok := mesh[p]
		if !ok {
			break
		}

		pp := peer.ID(prune.GetPeerID())
		delete(peers, pp)
	}
}
