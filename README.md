statsdaemon
==========

Port of Etsy's statsd (https://github.com/etsy/statsd), written in Go (originally based 
on [amir/gographite](https://github.com/amir/gographite)).

Supports

* Timing (with optional percentiles)
* Counters (with optional sampling)
* Gauges

[![Build Status](https://secure.travis-ci.org/bitly/statsdaemon.png)](http://travis-ci.org/bitly/statsdaemon)

Installing
==========

```bash
go get github.com/bitly/statsdaemon
```

Command Line Options
====================

```
Usage of ./statsdaemon:
  -address=":8125": UDP service address
  -debug=false: print statistics sent to graphite
  -flush-interval=10: Flush interval (seconds)
  -graphite="127.0.0.1:2003": Graphite service address (or - to disable)
  -percent-threshold=[]: Threshold percent (0-100, may be given multiple times)
  -persist-count-keys=60: number of flush-interval's to persist count keys
  -receive-counter="": Metric name for total metrics recevied per interval
  -version=false: print version string
```

Building a Debian package
=========================
In your terminal, execute:

    ./build-deb.sh <version> <386|amd64>

where ``<version>`` is something like `0.5.0-5`. To compile this on mac you
need to install ``dpkg`` using either Homebrew or Ports. You also need support
for cross compiling Go applications.
