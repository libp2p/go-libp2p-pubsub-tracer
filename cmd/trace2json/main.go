package main

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"log"
	"os"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
)

func main() {
	for _, f := range os.Args[1:] {
		trace2json(f)
	}
}

func trace2json(f string) {
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
	enc := json.NewEncoder(os.Stdout)

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

		err = enc.Encode(&evt)
		if err != nil {
			log.Printf("error encoding trace event from %s: %s", f, err)
		}
	}
}
