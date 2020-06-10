statsdaemon
==========

Port of Etsy's statsd (https://github.com/etsy/statsd), written in Go (originally based
on [amir/gographite](https://github.com/amir/gographite)).

Supports:

* Timing (with optional percentiles)
* Counters (positive and negative with optional sampling)
* Gauges (including relative operations)
* Sets

Initially only integers were supported for metric values,
but now double-precision floating-point is supported.

[![Build Status](https://secure.travis-ci.org/bitly/statsdaemon.png)](http://travis-ci.org/bitly/statsdaemon)

Installing
==========

### Binary Releases
Pre-built binaries for darwin and linux.

### Current Stable Release: `v0.7.2`
* [statsdaemon-0.7.2.darwin-amd64.go1.12.1.tar.gz](https://github.com/bitly/statsdaemon/releases/download/v0.7.1/statsdaemon-0.7.2.darwin-amd64.go1.12.1.tar.gz)
* [statsdaemon-0.7.2.linux-amd64.go1.12.1.tar.gz](https://github.com/bitly/statsdaemon/releases/download/v0.7.2/statsdaemon-0.7.2.linux-amd64.go1.12.1.tar.gz)
* [statsdaemon-0.7.2.freebsd-amd64.go1.12.1.tar.gz](https://github.com/bitly/statsdaemon/releases/download/v0.7.2/statsdaemon-0.7.2.freebsd-amd64.go1.12.1.tar.gz)

### Building from Source
```
go get https://github.com/bitly/statsdaemon
```


Command Line Options
====================

```
Usage of ./statsdaemon:
  -address=":8125": UDP service address
  -debug=false: print statistics sent to graphite
  -delete-gauges=true: don't send values to graphite for inactive gauges, as opposed to sending the previous value
  -flush-interval=10: Flush interval (seconds)
  -graphite="127.0.0.1:2003": Graphite service address (or - to disable)
  -max-udp-packet-size=1472: Maximum UDP packet size
  -percent-threshold=[]: percentile calculation for timers (0-100, may be given multiple times)
  -persist-count-keys=60: number of flush-intervals to persist count keys
  -postfix="": Postfix for all stats
  -prefix="": Prefix for all stats
  -receive-counter="": Metric name for total metrics received per interval
  -tcpaddr="": TCP service address, if set
  -version=false: print version string
  -heartbeat-file="": heartbeat file to update after a successful write to graphite
```
