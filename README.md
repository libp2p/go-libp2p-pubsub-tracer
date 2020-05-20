# go-libp2p-pubsub-tracer

Tracing daemon and tools for pubsub tracing.

## Installation

```
$ git clone git@github.com:libp2p/go-libp2p-pubsub-tracer.git
$ cd go-libp2p-pubsub-tracer
$ go install ./...
```

## Tools

### traced

This is the tracing daemon for receiving and recording traces from remote pubsub peers.
Pubsub peers emitting traces should instantiate pubsub using the `WithEventTracer` option,
with an instance of `RemoteTracer` pointing to your `traced` instance. Traces are captured
as a series of compressed protobuf files, containing the aggregate traces reported from the
traced peers.

Usage:
```
traced [-d <directory>] [-p <port>] [-id <identity>]

  port: port where the traced should listen on; defaults to 4001
  directory: directory where traces should be written; defaults to ./traced.out
  identity: file containing the peer private key; defaults to identity
```


### trace2json

This is a simple tool to convert one or more trace files into json; the json output is dumped
to stdout.

Usage:
```
trace2json <trace-file> ...
```

### tracestat

This is a trace analysis tool, that can analyze one or more trace files.

Usage:
```
tracestat [-summary] [-cdf] [-json <file>] [-topic <topic>] <trace-file> ...

  -summary: Print an analysis summary to stdout; defaults to true
  -cdf: Print a CDF of message propagation delays to stdout; defaults to false
  -json <file>: Output the result of the analysis to a json file for further processing by other tools
  -topic <topic>: Analyze messages only for a specific topic
```
