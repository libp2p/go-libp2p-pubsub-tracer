package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	ggio "github.com/gogo/protobuf/io"
)

// The trace2json program converts the binary protobuf-encoded trace from file b
// into an ndjson stream, printing the result on stdout.
func main() {
	w := os.Stdout
	defer w.Close()

	for _, f := range os.Args[1:] {
		err := trace2json(f, w)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

func trace2json(f string, w io.WriteCloser) error {
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
	enc := json.NewEncoder(w)

	for {
		evt.Reset()

		switch err = pbr.ReadMsg(&evt); err {
		case nil:
			err = enc.Encode(&evt)
			if err != nil {
				return fmt.Errorf("error encoding trace event from %s: %w", f, err)
			}
		case io.EOF:
			return nil
		default:
			return fmt.Errorf("error decoding trace event from %s: %w", f, err)
		}
	}
}
