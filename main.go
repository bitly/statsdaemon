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
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const VERSION = "0.4.4"

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	Value    interface{}
	Modifier string
	Sampling float32
}

type Uint64Slice []uint64
func (s Uint64Slice) Len() int        { return len(s) }
func (s Uint64Slice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
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
	flag.Var(&percentThreshold, "percent-threshold", "Threshold percent (may be given multiple times)")
}

var (
	In       = make(chan *Packet, 1000)
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
				gauges[s.Bucket] = uint64(s.Value.(int64))
			} else {
				v, ok := counters[s.Bucket]
				if !ok || v < 0 {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int64(float64(s.Value.(int64)) * float64(1 / s.Sampling))
			}
		}
	}
}

func submit() {
	client, err := net.Dial("tcp", *graphiteAddress)
	if err != nil {
		log.Printf("Error dialing %s %s", *graphiteAddress, err.Error())
		if *debug == false {
			return
		} else {
			log.Printf("WARNING: in debug mode. resetting counters even though connection to graphite failed")
		}
	} else {
		defer client.Close()
	}

	numStats := 0
	now := time.Now().Unix()
	buffer := bytes.NewBuffer([]byte{})

	// continue sending zeros for counters for a short period of time
	// even if we have no new data. for more context see https://github.com/bitly/gographite/pull/8
	for s, c := range counters {
		switch {
		case c <= *persistCountKeys:
			continue
		case c < 0:
			counters[s] -= 1
			fmt.Fprintf(buffer, "%s %d %d\n", s, 0, now)
		case c >= 0:
			counters[s] = -1
			fmt.Fprintf(buffer, "%s %d %d\n", s, c, now)
		}
		numStats++
	}

	for g, c := range gauges {
		if c == math.MaxUint64 {
			continue
		}
		fmt.Fprintf(buffer, "%s %d %d\n", g, c, now)
		gauges[g] = math.MaxUint64
		numStats++
	}

	for u, t := range timers {
		if len(t) > 0 {
			numStats++
			sort.Sort(t)
			min := t[0]
			max := t[len(t)-1]
			mean := t[len(t)/2]
			maxAtThreshold := max
			count := len(t)

			for _, pct := range percentThreshold {

				if len(t) > 1 {
					indexOfPerc := int(math.Ceil(((pct.float / 100.0) * float64(count)) + 0.5))
					if indexOfPerc >= count {
						indexOfPerc = count - 1
					}
					maxAtThreshold = t[indexOfPerc]
				}

				fmt.Fprintf(buffer, "%s.upper_%s %d %d\n", u, pct.str, maxAtThreshold, now)
			}

			var z Uint64Slice
			timers[u] = z

			fmt.Fprintf(buffer, "%s.mean %d %d\n", u, mean, now)
			fmt.Fprintf(buffer, "%s.upper %d %d\n", u, max, now)
			fmt.Fprintf(buffer, "%s.lower %d %d\n", u, min, now)
			fmt.Fprintf(buffer, "%s.count %d %d\n", u, count, now)
		}
	}
	if numStats == 0 {
		return
	}
	data := buffer.Bytes()
	if client != nil {
		log.Printf("sent %d stats to %s", numStats, *graphiteAddress)
		client.Write(data)
	}
	if *debug {
		lines := bytes.NewBuffer(data)
		for {
			line, err := lines.ReadString([]byte("\n")[0])
			if line == "" || err != nil {
				break
			}
			log.Printf("debug: %s", line)
		}
	}
}

func parseMessage(buf *bytes.Buffer) []*Packet {
	var packetRegexp = regexp.MustCompile("^([^:]+):([0-9]+)\\|(g|c|ms)(\\|@([0-9\\.]+))?\n?$")

	var output []*Packet
	var err error
	var line string
	for {
		if err != nil {
			break
		}
		line, err = buf.ReadString('\n')
		if line != "" {
			item := packetRegexp.FindStringSubmatch(line)
			if len(item) == 0 {
				continue
			}

			var value interface{}
			modifier := item[3]
			switch modifier {
			case "c":
				value, err = strconv.ParseInt(item[2], 10, 64)
				if err != nil {
					log.Printf("ERROR: failed to ParseInt %s - %s", item[2], err.Error())
				}
			default:
				value, err = strconv.ParseUint(item[2], 10, 64)
				if err != nil {
					log.Printf("ERROR: failed to ParseUint %s - %s", item[2], err.Error())
				}
			}
			if err != nil {
				if modifier == "ms" {
					value = 0
				} else {
					value = 1
				}
			}

			sampleRate, err := strconv.ParseFloat(item[5], 32)
			if err != nil {
				sampleRate = 1
			}

			packet := &Packet{
				Bucket:   item[1],
				Value:    value,
				Modifier: modifier,
				Sampling: float32(sampleRate),
			}
			output = append(output, packet)
		}
	}
	return output
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("Listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ListenAndServe: %s", err.Error())
	}
	defer listener.Close()
	message := make([]byte, 512)
	for {
		n, remaddr, err := listener.ReadFrom(message)
		if err != nil {
			log.Printf("error reading from %v %s", remaddr, err.Error())
			continue
		}
		buf := bytes.NewBuffer(message[0:n])
		packets := parseMessage(buf)
		for _, p := range packets {
			In <- p
		}
	}
}

func main() {
	flag.Parse()
	if *showVersion {
		fmt.Printf("gographite v%s\n", VERSION)
		return
	}
	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM)
	*persistCountKeys = -1 * (*persistCountKeys)

	go udpListener()
	monitor()
}
