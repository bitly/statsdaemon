package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	VERSION                 = "0.5.2-alpha"
	MAX_UNPROCESSED_PACKETS = 1000
	MAX_UDP_PACKET_SIZE     = 512
)

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	Value    interface{}
	Modifier string
	Sampling float32
}

type Uint64Slice []uint64

func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }

type Percentiles []*Percentile
type Percentile struct {
	float float64
	str   string
}

func (a *Percentiles) Set(s string) error {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*a = append(*a, &Percentile{f, strings.Replace(s, ".", "_", -1)})
	return nil
}
func (p *Percentile) String() string {
	return p.str
}
func (a *Percentiles) String() string {
	return fmt.Sprintf("%v", *a)
}

var (
	serviceAddress   = flag.String("address", ":8125", "UDP service address")
	graphiteAddress  = flag.String("graphite", "127.0.0.1:2003", "Graphite service address (or - to disable)")
	flushInterval    = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	debug            = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion      = flag.Bool("version", false, "print version string")
	persistCountKeys = flag.Int64("persist-count-keys", 60, "number of flush-interval's to persist count keys")
	percentThreshold = Percentiles{}
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold", "Threshold percent (0-100, may be given multiple times)")
}

var (
	In       = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters = make(map[string]int64)
	gauges   = make(map[string]uint64)
	timers   = make(map[string]Uint64Slice)
)

func monitor() {
	ticker := time.NewTicker(time.Duration(*flushInterval) * time.Second)
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %d... shutting down\n", sig)
			submit()
			return
		case <-ticker.C:
			submit()
		case s := <-In:
			if s.Modifier == "ms" {
				_, ok := timers[s.Bucket]
				if !ok {
					var t Uint64Slice
					timers[s.Bucket] = t
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = s.Value.(uint64)
			} else {
				v, ok := counters[s.Bucket]
				if !ok || v < 0 {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int64(float64(s.Value.(int64)) * float64(1/s.Sampling))
			}
		}
	}
}

func submit() {
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()

	client, err := net.Dial("tcp", *graphiteAddress)
	if err != nil {
		log.Printf("ERROR: dialing %s - %s", *graphiteAddress, err)
		if *debug {
			log.Printf("WARNING: resetting counters when in debug mode")
			processCounters(&buffer, now)
			processGauges(&buffer, now)
			processTimers(&buffer, now, percentThreshold)
		}
		return
	}
	defer client.Close()

	num += processCounters(&buffer, now)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	if num == 0 {
		return
	}

	if *debug {
		for _, line := range bytes.Split(buffer.Bytes(), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			log.Printf("DEBUG: %s", line)
		}
	}

	_, err = client.Write(buffer.Bytes())
	if err != nil {
		log.Printf("ERROR: failed to write stats - %s", err)
		return
	}

	log.Printf("sent %d stats to %s", num, *graphiteAddress)
}

func processCounters(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time
	// even if we have no new data. for more context see https://github.com/bitly/statsdaemon/pull/8
	for s, c := range counters {
		switch {
		case c <= *persistCountKeys:
			// consider this purgable
			delete(counters, s)
			continue
		case c < 0:
			counters[s] -= 1
			fmt.Fprintf(buffer, "%s %d %d\n", s, 0, now)
		case c >= 0:
			counters[s] = -1
			fmt.Fprintf(buffer, "%s %d %d\n", s, c, now)
		}
		num++
	}
	return num
}

func processGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	for g, c := range gauges {
		if c == math.MaxUint64 {
			continue
		}
		fmt.Fprintf(buffer, "%s %d %d\n", g, c, now)
		gauges[g] = math.MaxUint64
		num++
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for u, t := range timers {
		if len(t) > 0 {
			num++

			sort.Sort(t)
			min := t[0]
			max := t[len(t)-1]
			maxAtThreshold := max
			count := len(t)

			sum := uint64(0)
			for _, value := range t {
				sum += value
			}
			mean := float64(sum) / float64(len(t))

			for _, pct := range pctls {

				if len(t) > 1 {
					var abs float64
					if pct.float >= 0 {
						abs = pct.float
					} else {
						abs = 100 + pct.float
					}
					// poor man's math.Round(x):
					// math.Floor(x + 0.5)
					indexOfPerc := int(math.Floor(((abs / 100.0) * float64(count)) + 0.5))
					if pct.float >= 0 {
						indexOfPerc -= 1  // index offset=0
					}
					maxAtThreshold = t[indexOfPerc]
				}

				var tmpl string
				var pctstr string
				if pct.float >= 0 {
					tmpl = "%s.upper_%s %d %d\n"
					pctstr = pct.str
				} else {
					tmpl = "%s.lower_%s %d %d\n"
					pctstr = pct.str[1:]
				}
				fmt.Fprintf(buffer, tmpl, u, pctstr, maxAtThreshold, now)
			}

			var z Uint64Slice
			timers[u] = z

			fmt.Fprintf(buffer, "%s.mean %f %d\n", u, mean, now)
			fmt.Fprintf(buffer, "%s.upper %d %d\n", u, max, now)
			fmt.Fprintf(buffer, "%s.lower %d %d\n", u, min, now)
			fmt.Fprintf(buffer, "%s.count %d %d\n", u, count, now)
		}
	}
	return num
}

var packetRegexp = regexp.MustCompile("^([^:]+):(-?[0-9]+)\\|(g|c|ms)(\\|@([0-9\\.]+))?\n?$")

func parseMessage(data []byte) []*Packet {
	var output []*Packet
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}

		item := packetRegexp.FindSubmatch(line)
		if len(item) == 0 {
			continue
		}

		var err error
		var value interface{}
		modifier := string(item[3])
		switch modifier {
		case "c":
			value, err = strconv.ParseInt(string(item[2]), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseInt %s - %s", item[2], err)
				continue
			}
		default:
			value, err = strconv.ParseUint(string(item[2]), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseUint %s - %s", item[2], err)
				continue
			}
		}

		sampleRate, err := strconv.ParseFloat(string(item[5]), 32)
		if err != nil {
			sampleRate = 1
		}

		packet := &Packet{
			Bucket:   string(item[1]),
			Value:    value,
			Modifier: modifier,
			Sampling: float32(sampleRate),
		}
		output = append(output, packet)
	}
	return output
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}
	defer listener.Close()

	message := make([]byte, MAX_UDP_PACKET_SIZE)
	for {
		n, remaddr, err := listener.ReadFromUDP(message)
		if err != nil {
			log.Printf("ERROR: reading UDP packet from %+v - %s", remaddr, err)
			continue
		}

		for _, p := range parseMessage(message[:n]) {
			In <- p
		}
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)
	*persistCountKeys = -1 * (*persistCountKeys)

	go udpListener()
	monitor()
}
