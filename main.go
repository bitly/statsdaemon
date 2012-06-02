package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const (
	TCP = "tcp"
	UDP = "udp"
)

type Packet struct {
	Bucket   string
	Value    int
	Modifier string
	Sampling float32
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	graphiteAddress  = flag.String("graphite", "127.0.0.1:2003", "Graphite service address (or - to dissable)")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	percentThreshold = flag.Int("percent-threshold", 90, "Threshold percent")
)

var (
	In       = make(chan Packet, 10000)
	counters = make(map[string]int)
	gauges   = make(map[string]int)
	timers   = make(map[string][]int)
)

func monitor() {
	t := time.NewTicker(time.Duration(*flushInterval) * time.Second)
	for {
		select {
		case <-t.C:
			submit()
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t []int
					timers[s.Bucket] = t
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value)
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = int(s.Value)
			} else {
				_, ok := counters[s.Bucket]
				if !ok {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int(float32(s.Value) * (1 / s.Sampling))
			}
		}
	}
}

func submit() {
	client, err := net.Dial(TCP, *graphiteAddress)
	if err != nil {
		log.Printf("Error dialing", err.Error())
		return
	}
	defer client.Close()

	numStats := 0
	now := time.Now().Unix()
	buffer := bytes.NewBufferString("")
	for s, c := range counters {
		if c == 0 {
			continue
		}
		valuePerSecond := int64(c) / *flushInterval
		fmt.Fprintf(buffer, "stats.%s %d %d\n", s, valuePerSecond, now)
		fmt.Fprintf(buffer, "stats_counts.%s %d %d\n", s, c, now)
		counters[s] = 0
		numStats++
	}

	for g, c := range gauges {
		if c == 0 {
			continue
		}
		fmt.Fprintf(buffer, "stats.gauges.%s %d %d\n", g, c, now)
		gauges[g] = 0
		numStats++
	}

	for u, t := range timers {
		if len(t) > 0 {
			numStats++
			sort.Ints(t)
			min := t[0]
			max := t[len(t)-1]
			mean := min
			maxAtThreshold := max
			count := len(t)
			if len(t) > 1 {
				var thresholdIndex int
				thresholdIndex = ((100 - *percentThreshold) / 100) * count
				numInThreshold := count - thresholdIndex
				values := t[0:numInThreshold]

				sum := 0
				for i := 0; i < numInThreshold; i++ {
					sum += values[i]
				}
				mean = sum / numInThreshold
			}
			var z []int
			timers[u] = z

			fmt.Fprintf(buffer, "stats.timers.%s.mean %d %d\n", u, mean, now)
			fmt.Fprintf(buffer, "stats.timers.%s.upper %d %d\n", u, max, now)
			fmt.Fprintf(buffer, "stats.timers.%s.upper_%d %d %d\n", u, *percentThreshold, maxAtThreshold, now)
			fmt.Fprintf(buffer, "stats.timers.%s.lower %d %d\n", u, min, now)
			fmt.Fprintf(buffer, "stats.timers.%s.count %d %d\n", u, count, now)
		}
	}
	if numStats == 0 {
		return
	}
	log.Printf("got %d stats", numStats)
	fmt.Fprintf(buffer, "statsd.numStats %d %d\n", numStats, now)
	data := buffer.Bytes()
	client.Write(data)

}

func handleMessage(conn *net.UDPConn, remaddr net.Addr, buf *bytes.Buffer) {
	var packet Packet
	var sanitizeRegexp = regexp.MustCompile("[^a-zA-Z0-9\\-_\\.:\\|@]")
	var packetRegexp = regexp.MustCompile("([a-zA-Z0-9_]+):([0-9]+)\\|(g|c|ms)(\\|@([0-9\\.]+))?")
	log.Printf("got %s", buf.String())
	s := sanitizeRegexp.ReplaceAllString(buf.String(), "")

	for _, item := range packetRegexp.FindAllStringSubmatch(s, -1) {
		value, err := strconv.Atoi(item[2])
		if err != nil {
			// todo print out this error
			if item[3] == "ms" {
				value = 0
			} else {
				value = 1
			}
		}

		sampleRate, err := strconv.ParseFloat(item[5], 32)
		if err != nil {
			sampleRate = 1
		}

		packet.Bucket = item[1]
		packet.Value = value
		packet.Modifier = item[3]
		packet.Sampling = float32(sampleRate)
		In <- packet
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr(UDP, *serviceAddress)
	log.Printf("Listening on %s", address)
	listener, err := net.ListenUDP(UDP, address)
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}
	defer listener.Close()
	for {
		message := make([]byte, 512)
		n, remaddr, error := listener.ReadFrom(message)
		if error != nil {
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		go handleMessage(listener, remaddr, buf)
	}
}

func main() {
	flag.Parse()
	go udpListener()
	monitor()
}
