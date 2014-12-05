package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
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

type GaugeData struct {
	Relative bool
	Negative bool
	Value    uint64
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
	persistCountKeys = flag.Int64("persist-count-keys", 60, "number of flush-intervals to persist count keys")
	receiveCounter   = flag.String("receive-counter", "", "Metric name for total metrics received per interval")
	percentThreshold = Percentiles{}
	prefix           = flag.String("prefix", "", "Prefix for all stats")
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold",
		"percentile calculation for timers (0-100, may be given multiple times)")
}

var (
	In              = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters        = make(map[string]int64)
	gauges          = make(map[string]uint64)
	trackedGauges   = make(map[string]uint64)
	timers          = make(map[string]Uint64Slice)
	countInactivity = make(map[string]int64)
)

func monitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %d... shutting down\n", sig)
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
			return
		case <-ticker.C:
			if err := submit(time.Now().Add(period)); err != nil {
				log.Printf("ERROR: %s", err)
			}
		case s := <-In:
			packetHandler(s)
		}
	}
}

func packetHandler(s *Packet) {
	if *receiveCounter != "" {
		v, ok := counters[*receiveCounter]
		if !ok || v < 0 {
			counters[*receiveCounter] = 0
		}
		counters[*receiveCounter] += 1
	}

	if s.Modifier == "ms" {
		_, ok := timers[s.Bucket]
		if !ok {
			var t Uint64Slice
			timers[s.Bucket] = t
		}
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
	} else if s.Modifier == "g" {
		gaugeValue, _ := gauges[s.Bucket]

		gaugeData := s.Value.(GaugeData)
		if gaugeData.Relative {
			if gaugeData.Negative {
				// subtract checking for -ve numbers
				if gaugeData.Value > gaugeValue {
					gaugeValue = 0
				} else {
					gaugeValue -= gaugeData.Value
				}
			} else {
				// watch out for overflows
				if gaugeData.Value > (math.MaxUint64 - gaugeValue) {
					gaugeValue = math.MaxUint64
				} else {
					gaugeValue += gaugeData.Value
				}
			}
		} else {
			gaugeValue = gaugeData.Value
		}

		gauges[s.Bucket] = gaugeValue

	} else if s.Modifier == "c" {
		_, ok := counters[s.Bucket]
		if !ok {
			counters[s.Bucket] = 0
		}
		counters[s.Bucket] += int64(float64(s.Value.(int64)) * float64(1/s.Sampling))
	}
}

func submit(deadline time.Time) error {
	var buffer bytes.Buffer
	var num int64

	now := time.Now().Unix()

	if *graphiteAddress == "-" {
		return nil
	}

	client, err := net.Dial("tcp", *graphiteAddress)
	if err != nil {
		if *debug {
			log.Printf("WARNING: resetting counters when in debug mode")
			processCounters(&buffer, now)
			processGauges(&buffer, now)
			processTimers(&buffer, now, percentThreshold)
		}
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphiteAddress, err)
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		errmsg := fmt.Sprintf("could not set deadline:", err)
		return errors.New(errmsg)
	}

	num += processCounters(&buffer, now)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	if num == 0 {
		return nil
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
		errmsg := fmt.Sprintf("failed to write stats - %s", err)
		return errors.New(errmsg)
	}

	log.Printf("sent %d stats to %s", num, *graphiteAddress)

	return nil
}

func processCounters(buffer *bytes.Buffer, now int64) int64 {
	var num int64
	// continue sending zeros for counters for a short period of time even if we have no new data
	for bucket, value := range counters {
		fmt.Fprintf(buffer, "%s %d %d\n", bucket, value, now)
		delete(counters, bucket)
		countInactivity[bucket] = 0
		num++
	}
	for bucket, purgeCount := range countInactivity {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s %d %d\n", bucket, 0, now)
			num++
		}
		countInactivity[bucket] += 1
		if countInactivity[bucket] > *persistCountKeys {
			delete(countInactivity, bucket)
		}
	}
	return num
}

func processGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64

	for g, c := range gauges {
		lastValue, ok := trackedGauges[g]

		if ok && c == lastValue {
			continue
		}
		fmt.Fprintf(buffer, "%s %d %d\n", g, c, now)
		trackedGauges[g] = c
		num++
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	for u, t := range timers {
		if len(t) == 0 {
			continue
		}

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
					indexOfPerc -= 1 // index offset=0
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
	return num
}

func parseMessage(data []byte) []*Packet {
	var (
		output []*Packet
		input  []byte
	)

	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		input = line

		index := bytes.IndexByte(input, ':')
		if index < 0 {
			if *debug {
				log.Printf("ERROR: failed to parse line: %s\n", string(line))
			}
			continue
		}

		name := input[:index]

		index++
		input = input[index:]

		index = bytes.IndexByte(input, '|')
		if index < 0 {
			if *debug {
				log.Printf("ERROR: failed to parse line: %s\n", string(line))
			}
			continue
		}

		val := input[:index]
		index++

		var mtypeStr string

		if input[index] == 'm' {
			index++
			if index >= len(input) || input[index] != 's' {
				if *debug {
					log.Printf("ERROR: failed to parse line: %s\n", string(line))
				}
				continue
			}
			mtypeStr = "ms"
		} else {
			mtypeStr = string(input[index])
		}

		index++
		input = input[index:]

		var (
			value interface{}
			err   error
		)

		if mtypeStr[0] == 'c' {
			value, err = strconv.ParseInt(string(val), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseInt %s - %s", string(val), err)
				continue
			}
		} else if mtypeStr[0] == 'g' {
			var relative, negative bool
			var stringToParse string

			switch val[0] {
			case '+', '-':
				relative = true
				negative = val[0] == '-'
				stringToParse = string(val[1:])
			default:
				relative = false
				negative = false
				stringToParse = string(val)
			}

			gaugeValue, err := strconv.ParseUint(stringToParse, 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseUint %s - %s", string(val), err)
				continue
			}

			value = GaugeData{relative, negative, gaugeValue}
		} else {
			value, err = strconv.ParseUint(string(val), 10, 64)
			if err != nil {
				log.Printf("ERROR: failed to ParseUint %s - %s", string(val), err)
				continue
			}
		}

		var sampleRate float32 = 1

		if len(input) > 0 && bytes.HasPrefix(input, []byte("|@")) {
			input = input[2:]
			rate, err := strconv.ParseFloat(string(input), 32)
			if err == nil {
				sampleRate = float32(rate)
			}
		}

		packet := &Packet{
			Bucket:   *prefix + string(name),
			Value:    value,
			Modifier: mtypeStr,
			Sampling: sampleRate,
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

	go udpListener()
	monitor()
}
