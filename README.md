# go-libp2p-pubsub-tracer

Tracing daemon and tools for pubsub tracing.

## Installation

```
$ git clone git@github.com:libp2p/go-libp2p-pubsub-tracer.git
$ cd go-libp2p-pubsub-tracer
$ go install ./...
```

## Tools

### collectd

This is the tracing daemon for receiving and recording traces from remote pubsub peers.
Pubsub peers emitting traces should instantiate pubsub using the `WithEventTracer` option,
with an instance of `RemoteTracer` pointing to your `collectd` instance. Traces are captured
as a series of compressed protobuf files, containing the aggregate traces reported from the
collectd peers.

Usage:
```
collectd [-d <directory>] [-p <port>] [-id <identity>]

  port: port where the collectd should listen on; defaults to 4001
  directory: directory where traces should be written; defaults to ./collectd.out
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
tracestat [-summary] [-cdf] [-json <file>] <trace-file> ...

  -summary: Print an analysis summary to stdout; defaults to true
  -cdf: Print a CDF of message propagation delays to stdout; defaults to false
  -json <file>: Output the result of the analysis to a json file for further processing by other tools
```
