package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
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
	TCP_READ_SIZE           = 4096
)

var signalchan chan os.Signal

type Packet struct {
	Bucket   string
	ValFlt   float64
	ValStr   string
	Modifier string
	Sampling float32
}

type Float64Slice []float64

func (s Float64Slice) Len() int           { return len(s) }
func (s Float64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Float64Slice) Less(i, j int) bool { return s[i] < s[j] }

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

func sanitizeBucket(bucket []byte) string {
	var bl int

	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case c >= byte('a') && c <= byte('z'):
			fallthrough
		case c >= byte('A') && c <= byte('Z'):
			fallthrough
		case c >= byte('0') && c <= byte('9'):
			fallthrough
		case c == byte('-') || c == byte('.') || c == byte('_'):
			bucket[bl] = c
			bl++
		case c == byte(' '):
			bucket[bl] = byte('_')
			bl++
		case c == byte('/'):
			bucket[bl] = byte('-')
			bl++
		}
	}
	return string(bucket[:bl])
}

var (
	serviceAddress    = flag.String("address", ":8125", "UDP service address")
	tcpServiceAddress = flag.String("tcpaddr", "", "TCP service address, if set")
	maxUdpPacketSize  = flag.Int("max-udp-packet-size", 1472, "Maximum UDP packet size")
	graphiteAddress   = flag.String("graphite", "127.0.0.1:2003", "Graphite service address (or - to disable)")
	flushInterval     = flag.Int64("flush-interval", 10, "Flush interval (seconds)")
	debug             = flag.Bool("debug", false, "print statistics sent to graphite")
	showVersion       = flag.Bool("version", false, "print version string")
	deleteGauges      = flag.Bool("delete-gauges", true, "don't send values to graphite for inactive gauges, as opposed to sending the previous value")
	persistCountKeys  = flag.Uint("persist-count-keys", 60, "number of flush-intervals to persist count keys (at zero)")
	persistTimerKeys  = flag.Uint("persist-timer-counts", 0, "number of flush-intervals to persist timer count keys (at zero)")
	receiveCounter    = flag.String("receive-counter", "", "Metric name for total metrics received per interval")
	percentThreshold  = Percentiles{}
	prefix            = flag.String("prefix", "", "Prefix for all stats")
	postfix           = flag.String("postfix", "", "Postfix for all stats")
)

func init() {
	flag.Var(&percentThreshold, "percent-threshold",
		"percentile calculation for timers (0-100, may be given multiple times)")
}

var (
	receiveCount    uint64
	In              = make(chan *Packet, MAX_UNPROCESSED_PACKETS)
	counters        = make(map[string]float64)
	gauges          = make(map[string]float64)
	timers          = make(map[string]Float64Slice)
	sets            = make(map[string][]string)
	inactivCounters = make(map[string]uint)
	inactivTimers   = make(map[string]uint)
)

func monitor() {
	period := time.Duration(*flushInterval) * time.Second
	ticker := time.NewTicker(period)
	for {
		select {
		case sig := <-signalchan:
			fmt.Printf("!! Caught signal %v... shutting down\n", sig)
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
	receiveCount++

	switch s.Modifier {
	case "ms":
		vals := timers[s.Bucket]
		if vals == nil {
			vals = make([]float64, 1, 5)
			vals[0] = 0.0
		}
		// first slot is sampled count, following are times
		vals[0] += float64(1 / s.Sampling)
		timers[s.Bucket] = append(vals, s.ValFlt)
	case "g":
		var gaugeValue float64
		if s.ValStr == "" {
			gaugeValue = s.ValFlt
		} else if s.ValStr == "+" {
			gaugeValue = gauges[s.Bucket] + s.ValFlt
		} else if s.ValStr == "-" {
			gaugeValue = gauges[s.Bucket] - s.ValFlt
		}
		gauges[s.Bucket] = gaugeValue
	case "c":
		counters[s.Bucket] += s.ValFlt / float64(s.Sampling)
	case "s":
		sets[s.Bucket] = append(sets[s.Bucket], s.ValStr)
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
			processSets(&buffer, now)
		}
		errmsg := fmt.Sprintf("dialing %s failed - %s", *graphiteAddress, err)
		return errors.New(errmsg)
	}
	defer client.Close()

	err = client.SetDeadline(deadline)
	if err != nil {
		return err
	}

	num += processCounters(&buffer, now)
	num += processGauges(&buffer, now)
	num += processTimers(&buffer, now, percentThreshold)
	num += processSets(&buffer, now)
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
	persist := *persistCountKeys

	// avoid adding prefix/postfix to receiveCounter
	if *receiveCounter != "" && receiveCount > 0 {
		fmt.Fprintf(buffer, "%s %d %d\n", *receiveCounter, receiveCount, now)
		if persist > 0 {
			inactivCounters[*receiveCounter] = 0
		}
		num++
	}
	receiveCount = 0

	for bucket, value := range counters {
		fullbucket := *prefix + bucket + *postfix
		fmt.Fprintf(buffer, "%s %s %d\n", fullbucket, strconv.FormatFloat(value, 'f', -1, 64), now)
		delete(counters, bucket)
		if persist > 0 {
			inactivCounters[fullbucket] = 0
		}
		num++
	}

	// continue sending zeros for no-longer-active counters for configured flush-intervals
	for bucket, purgeCount := range inactivCounters {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s 0 %d\n", bucket, now)
			num++
		}
		if purgeCount >= persist {
			delete(inactivCounters, bucket)
		} else {
			inactivCounters[bucket] = purgeCount + 1
		}
	}
	return num
}

func processGauges(buffer *bytes.Buffer, now int64) int64 {
	var num int64

	for bucket, currentValue := range gauges {
		valstr := strconv.FormatFloat(currentValue, 'f', -1, 64)
		fmt.Fprintf(buffer, "%s%s%s %s %d\n", *prefix, bucket, *postfix, valstr, now)
		num++
		if *deleteGauges {
			delete(gauges, bucket)
		}
	}
	return num
}

func processSets(buffer *bytes.Buffer, now int64) int64 {
	num := int64(len(sets))
	for bucket, set := range sets {

		uniqueSet := map[string]bool{}
		for _, str := range set {
			uniqueSet[str] = true
		}

		fmt.Fprintf(buffer, "%s%s%s %d %d\n", *prefix, bucket, *postfix, len(uniqueSet), now)
		delete(sets, bucket)
	}
	return num
}

func processTimers(buffer *bytes.Buffer, now int64, pctls Percentiles) int64 {
	var num int64
	persist := *persistTimerKeys

	for bucket, timer := range timers {
		num++
		sampled := timer[0]
		timer = timer[1:]

		sort.Sort(timer)
		min := timer[0]
		max := timer[len(timer)-1]
		maxAtThreshold := max
		count := len(timer)

		sum := float64(0)
		for _, value := range timer {
			sum += value
		}
		mean := sum / float64(len(timer))

		for _, pct := range pctls {
			if len(timer) > 1 {
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
				maxAtThreshold = timer[indexOfPerc]
			}

			ptype := "upper"
			pctstr := pct.str
			if pct.float < 0 {
				ptype = "lower"
				pctstr = pct.str[1:]
			}
			threshold_s := strconv.FormatFloat(maxAtThreshold, 'f', -1, 64)
			fmt.Fprintf(buffer, "%s%s.%s_%s%s %s %d\n",
				*prefix, bucket, ptype, pctstr, *postfix, threshold_s, now)
		}

		mean_s := strconv.FormatFloat(mean, 'f', -1, 64)
		max_s := strconv.FormatFloat(max, 'f', -1, 64)
		min_s := strconv.FormatFloat(min, 'f', -1, 64)
		count_s := strconv.FormatFloat(sampled, 'f', -1, 64)

		fmt.Fprintf(buffer, "%s%s.mean%s %s %d\n", *prefix, bucket, *postfix, mean_s, now)
		fmt.Fprintf(buffer, "%s%s.upper%s %s %d\n", *prefix, bucket, *postfix, max_s, now)
		fmt.Fprintf(buffer, "%s%s.lower%s %s %d\n", *prefix, bucket, *postfix, min_s, now)
		fmt.Fprintf(buffer, "%s%s.count%s %s %d\n", *prefix, bucket, *postfix, count_s, now)

		delete(timers, bucket)
		if persist > 0 {
			countKey := fmt.Sprintf("%s%s.count%s", *prefix, bucket, *postfix)
			inactivTimers[countKey] = 0
		}
	}

	// continue sending zeros for no-longer-active timer counts for configured flush-intervals
	for bucket, purgeCount := range inactivTimers {
		if purgeCount > 0 {
			fmt.Fprintf(buffer, "%s 0 %d\n", bucket, now)
			num++
		}
		if purgeCount >= persist {
			delete(inactivTimers, bucket)
		} else {
			inactivTimers[bucket] = purgeCount + 1
		}
	}
	return num
}

type MsgParser struct {
	reader       io.Reader
	newbuf       []byte
	buffer       []byte
	partialReads bool
	done         bool
}

func NewParser(reader io.Reader, partialReads bool) *MsgParser {
	bufsz := *maxUdpPacketSize
	if partialReads {
		bufsz = TCP_READ_SIZE
	}
	newbuf := make([]byte, bufsz)
	return &MsgParser{reader, newbuf, newbuf[:0], partialReads, false}
}

func (mp *MsgParser) Next() (*Packet, bool) {
	buf := mp.buffer

	for {
		line, rest := mp.lineFrom(buf)

		if line != nil {
			mp.buffer = rest
			return parseLine(line), true
		}

		if mp.done {
			if len(rest) > 0 {
				return parseLine(rest), false
			}
			return nil, false
		}

		// for udp, each message independent
		// for tcp, copy to front and append
		// unless no '\n' in entire TCP_READ_SIZE
		idx := 0
		if mp.partialReads && len(buf) < TCP_READ_SIZE {
			idx = len(buf)
			copy(mp.newbuf, buf)
		}
		buf = mp.newbuf

		n, err := mp.reader.Read(buf[idx:])
		buf = buf[:idx+n]
		if err != nil {
			if err != io.EOF {
				log.Printf("ERROR: %s", err)
			}
			mp.done = true
		}
	}
}

func (mp *MsgParser) lineFrom(input []byte) ([]byte, []byte) {
	split := bytes.SplitN(input, []byte("\n"), 2)
	if len(split) == 2 {
		return split[0], split[1]
	}

	if !mp.partialReads {
		if len(input) == 0 {
			input = nil
		}
		return input, []byte{}
	}

	// if input ended in '\n' then len(split) == 2 and returned above
	return nil, input
}

func parseLine(line []byte) *Packet {
	split := bytes.SplitN(line, []byte{'|'}, 3)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}

	keyval := split[0]
	typeCode := string(split[1])

	sampling := float32(1)
	if typeCode == "c" || typeCode == "ms" {
		if len(split) == 3 && len(split[2]) > 0 && split[2][0] == '@' {
			f64, err := strconv.ParseFloat(string(split[2][1:]), 32)
			if err != nil {
				log.Printf(
					"ERROR: failed to ParseFloat %s - %s",
					string(split[2][1:]),
					err,
				)
				return nil
			}
			sampling = float32(f64)
		}
	}

	split = bytes.SplitN(keyval, []byte{':'}, 2)
	if len(split) < 2 {
		logParseFail(line)
		return nil
	}
	name := split[0]
	val := split[1]
	if len(val) == 0 {
		logParseFail(line)
		return nil
	}

	var (
		err      error
		floatval float64
		strval   string
	)

	switch typeCode {
	case "c":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "g":
		var s string

		if val[0] == '+' || val[0] == '-' {
			strval = string(val[0])
			s = string(val[1:])
		} else {
			s = string(val)
		}
		floatval, err = strconv.ParseFloat(s, 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	case "s":
		strval = string(val)
	case "ms":
		floatval, err = strconv.ParseFloat(string(val), 64)
		if err != nil {
			log.Printf("ERROR: failed to ParseFloat %s - %s", string(val), err)
			return nil
		}
	default:
		log.Printf("ERROR: unrecognized type code %q for metric %q", typeCode, name)
		return nil
	}

	return &Packet{
		Bucket:   sanitizeBucket(name),
		ValFlt:   floatval,
		ValStr:   strval,
		Modifier: typeCode,
		Sampling: sampling,
	}
}

func logParseFail(line []byte) {
	if *debug {
		log.Printf("ERROR: failed to parse line: %q\n", string(line))
	}
}

func parseTo(conn io.ReadCloser, partialReads bool, out chan<- *Packet) {
	defer conn.Close()

	parser := NewParser(conn, partialReads)
	for {
		p, more := parser.Next()
		if p != nil {
			out <- p
		}

		if !more {
			break
		}
	}
}

func udpListener() {
	address, _ := net.ResolveUDPAddr("udp", *serviceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenUDP - %s", err)
	}

	parseTo(listener, false, In)
}

func tcpListener() {
	address, _ := net.ResolveTCPAddr("tcp", *tcpServiceAddress)
	log.Printf("listening on %s", address)
	listener, err := net.ListenTCP("tcp", address)
	if err != nil {
		log.Fatalf("ERROR: ListenTCP - %s", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Fatalf("ERROR: AcceptTCP - %s", err)
		}
		go parseTo(conn, true, In)
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("statsdaemon v%s (built w/%s)\n", VERSION, runtime.Version())
		return
	}
	flag.Set("prefix", sanitizeBucket([]byte(*prefix)))
	flag.Set("postfix", sanitizeBucket([]byte(*postfix)))

	signalchan = make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGTERM, syscall.SIGINT)

	go udpListener()
	if *tcpServiceAddress != "" {
		go tcpListener()
	}
	monitor()
}
