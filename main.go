package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"syscall"
	"time"
)

const VERSION = "0.4.2"

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	Value    int
	Modifier string
	Sampling float32
}

type Percentiles []int

func (a *Percentiles) Set(s string) error {
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*a = append(*a, i)
	return nil
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
	percentThreshold = Percentiles{}
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold", "Threshold percent (may be given multiple times)")
}

var (
	In       = make(chan *Packet, 1000)
	counters = make(map[string]int)
	gauges   = make(map[string]int)
	timers   = make(map[string][]int)
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
					var t []int
					timers[s.Bucket] = t
				}
				timers[s.Bucket] = append(timers[s.Bucket], s.Value)
			} else if s.Modifier == "g" {
				gauges[s.Bucket] = int(s.Value)
			} else {
				v, ok := counters[s.Bucket]
				if !ok || v == -1 {
					counters[s.Bucket] = 0
				}
				counters[s.Bucket] += int(float32(s.Value) * (1 / s.Sampling))
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
	for s, c := range counters {
		if c == -1 {
			continue
		}
		fmt.Fprintf(buffer, "%s %d %d\n", s, c, now)
		counters[s] = -1
		numStats++
	}

	for g, c := range gauges {
		if c == -1 {
			continue
		}
		fmt.Fprintf(buffer, "%s %d %d\n", g, c, now)
		gauges[g] = -1
		numStats++
	}

	for u, t := range timers {
		if len(t) > 0 {
			numStats++
			sort.Ints(t)
			min := t[0]
			max := t[len(t)-1]
			mean := t[len(t)/2]
			maxAtThreshold := max
			count := len(t)

			for _, pct := range percentThreshold {

				if len(t) > 1 {
					var thresholdIndex int
					thresholdIndex = ((100 - pct) / 100) * count
					numInThreshold := count - thresholdIndex
					maxAtThreshold = t[numInThreshold-1]
				}

				fmt.Fprintf(buffer, "%s.upper_%d %d %d\n", u, pct, maxAtThreshold, now)
			}

			var z []int
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

			packet := &Packet{
				Bucket:   item[1],
				Value:    value,
				Modifier: item[3],
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

	go udpListener()
	monitor()
}
