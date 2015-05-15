package main

import (
	"bytes"
	"flag"
	"math"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

var commonPercentiles = Percentiles{
	&Percentile{
		99,
		"99",
	},
}

func TestParseLineGauge(t *testing.T) {
	d := []byte("gaugor:333|g")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 333}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:-10|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, true, 10}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:+4|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, false, 4}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// >max(int64) && <max(uint64)
	d = []byte("gaugor:18446744073709551606|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 18446744073709551606}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineCount(t *testing.T) {
	d := []byte("gorets:2|c|@0.1")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(2), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineTimer(t *testing.T) {
	d := []byte("glork:320|ms")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, uint64(320), packet.Value.(uint64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms|@0.1")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, uint64(320), packet.Value.(uint64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)
}

func TestParseLineSet(t *testing.T) {
	d := []byte("uniques:765|s")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "uniques", packet.Bucket)
	assert.Equal(t, "765", packet.Value)
	assert.Equal(t, "s", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineMisc(t *testing.T) {
	d := []byte("a.key.with-0.dash:4|c")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with 0.space:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.space", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with/0.slash:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with-0.slash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with@#*&%$^_0.garbage:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.garbage", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	flag.Set("prefix", "test.")
	d = []byte("prefix:4|c")
	packet = parseLine(d)
	assert.Equal(t, "test.prefix", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	flag.Set("prefix", "")

	flag.Set("postfix", ".test")
	d = []byte("postfix:4|c")
	packet = parseLine(d)
	assert.Equal(t, "postfix.test", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	flag.Set("postfix", "")

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	parser := NewParser(bytes.NewBuffer(d), true)
	packet, more := parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 3}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("a.key.with-0.dash:4")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5m")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|mg")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:5|ms|@")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gorets:xxx|c")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|g")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("gaugor:xxx|z")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("deploys.test.myservice4:100|t")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-colon:")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}

	d = []byte("up-to-pipe:1|")
	packet = parseLine(d)
	if packet != nil {
		t.Fail()
	}
}

func TestMultiLine(t *testing.T) {
	b := bytes.NewBuffer([]byte("a.key.with-0.dash:4|c\ngauge:3|g"))
	parser := NewParser(b, true)

	packet, more := parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 3}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestPacketHandlerReceiveCounter(t *testing.T) {
	counters = make(map[string]int64)
	*receiveCounter = "countme"

	p := &Packet{
		Bucket:   "gorets",
		Value:    int64(100),
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, counters["countme"], int64(1))

	packetHandler(p)
	assert.Equal(t, counters["countme"], int64(2))
}

func TestPacketHandlerCount(t *testing.T) {
	counters = make(map[string]int64)

	p := &Packet{
		Bucket:   "gorets",
		Value:    int64(100),
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(100))

	p.Value = int64(3)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(103))

	p.Value = int64(-4)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(99))

	p.Value = int64(-100)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], int64(-1))
}

func TestPacketHandlerGauge(t *testing.T) {
	gauges = make(map[string]uint64)

	p := &Packet{
		Bucket:   "gaugor",
		Value:    GaugeData{false, false, 333},
		Modifier: "g",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], uint64(333))

	// -10
	p.Value = GaugeData{true, true, 10}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], uint64(323))

	// +4
	p.Value = GaugeData{true, false, 4}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], uint64(327))

	// <0 overflow
	p.Value = GaugeData{false, false, 10}
	packetHandler(p)
	p.Value = GaugeData{true, true, 20}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], uint64(0))

	// >2^64 overflow
	p.Value = GaugeData{false, false, uint64(math.MaxUint64 - 10)}
	packetHandler(p)
	p.Value = GaugeData{true, false, 20}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], uint64(math.MaxUint64))
}

func TestPacketHandlerTimer(t *testing.T) {
	timers = make(map[string]Uint64Slice)

	p := &Packet{
		Bucket:   "glork",
		Value:    uint64(320),
		Modifier: "ms",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 1)
	assert.Equal(t, timers["glork"][0], uint64(320))

	p.Value = uint64(100)
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 2)
	assert.Equal(t, timers["glork"][1], uint64(100))
}

func TestPacketHandlerSet(t *testing.T) {
	sets = make(map[string][]string)

	p := &Packet{
		Bucket:   "uniques",
		Value:    "765",
		Modifier: "s",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 1)
	assert.Equal(t, sets["uniques"][0], "765")

	p.Value = "567"
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 2)
	assert.Equal(t, sets["uniques"][1], "567")
}

func TestProcessCounters(t *testing.T) {

	*persistCountKeys = int64(10)
	counters = make(map[string]int64)
	var buffer bytes.Buffer
	now := int64(1418052649)

	counters["gorets"] = int64(123)

	num := processCounters(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gorets 123 1418052649\n")

	// run processCounters() enough times to make sure it purges items
	for i := 0; i < int(*persistCountKeys)+10; i++ {
		num = processCounters(&buffer, now)
	}
	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	// expect two more lines - the good one and an empty one at the end
	assert.Equal(t, len(lines), int(*persistCountKeys+2))
	assert.Equal(t, string(lines[0]), "gorets 123 1418052649")
	assert.Equal(t, string(lines[*persistCountKeys]), "gorets 0 1418052649")
}

func TestProcessTimers(t *testing.T) {
	// Some data with expected mean of 20
	timers = make(map[string]Uint64Slice)
	timers["response_time"] = []uint64{0, 30, 30}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{})

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.mean 20.000000 1418052649")
	assert.Equal(t, string(lines[1]), "response_time.upper 30 1418052649")
	assert.Equal(t, string(lines[2]), "response_time.lower 0 1418052649")
	assert.Equal(t, string(lines[3]), "response_time.count 3 1418052649")

	num = processTimers(&buffer, now, Percentiles{})
	assert.Equal(t, num, int64(0))
}

func TestProcessGauges(t *testing.T) {
	// Some data with expected mean of 20
	flag.Set("delete-gauges", "false")
	gauges = make(map[string]uint64)
	gauges["gaugor"] = math.MaxUint64

	now := int64(1418052649)

	var buffer bytes.Buffer

	num := processGauges(&buffer, now)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	gauges["gaugor"] = 12345
	num = processGauges(&buffer, now)
	assert.Equal(t, num, int64(1))

	gauges["gaugor"] = math.MaxUint64
	num = processGauges(&buffer, now)
	assert.Equal(t, buffer.String(), "gaugor 12345 1418052649\ngaugor 12345 1418052649\n")
	assert.Equal(t, num, int64(1))
}

func TestProcessDeleteGauges(t *testing.T) {
	// Some data with expected mean of 20
	flag.Set("delete-gauges", "true")
	gauges = make(map[string]uint64)
	gauges["gaugordelete"] = math.MaxUint64

	now := int64(1418052649)

	var buffer bytes.Buffer

	num := processGauges(&buffer, now)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	gauges["gaugordelete"] = 12345
	num = processGauges(&buffer, now)
	assert.Equal(t, num, int64(1))

	gauges["gaugordelete"] = math.MaxUint64
	num = processGauges(&buffer, now)
	assert.Equal(t, buffer.String(), "gaugordelete 12345 1418052649\n")
	assert.Equal(t, num, int64(0))
}

func TestProcessSets(t *testing.T) {
	sets = make(map[string][]string)

	now := int64(1418052649)

	var buffer bytes.Buffer

	// three unique values
	sets["uniques"] = []string{"123", "234", "345"}
	num := processSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 3 1418052649\n")

	// one value is repeated
	buffer.Reset()
	sets["uniques"] = []string{"123", "234", "234"}
	num = processSets(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "uniques 2 1418052649\n")

	// make sure sets are purged
	num = processSets(&buffer, now)
	assert.Equal(t, num, int64(0))
}

func TestProcessTimersUpperPercentile(t *testing.T) {
	// Some data with expected 75% of 2
	timers = make(map[string]Uint64Slice)
	timers["response_time"] = []uint64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{
		&Percentile{
			75,
			"75",
		},
	})

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.upper_75 2 1418052649")
}

func TestProcessTimersUpperPercentilePostfix(t *testing.T) {
	flag.Set("postfix", ".test")
	// Some data with expected 75% of 2
	timers = make(map[string]Uint64Slice)
	timers["postfix_response_time.test"] = []uint64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{
		&Percentile{
			75,
			"75",
		},
	})

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "postfix_response_time.upper_75.test 2 1418052649")
	flag.Set("postfix", "")
}

func TestProcessTimesLowerPercentile(t *testing.T) {
	timers = make(map[string]Uint64Slice)
	timers["time"] = []uint64{0, 1, 2, 3}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{
		&Percentile{
			-75,
			"-75",
		},
	})

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "time.lower_75 1 1418052649")
}

func TestMultipleUDPSends(t *testing.T) {
	addr := "127.0.0.1:8126"

	address, _ := net.ResolveUDPAddr("udp", addr)
	listener, err := net.ListenUDP("udp", address)
	assert.Equal(t, nil, err)

	ch := make(chan *Packet, MAX_UNPROCESSED_PACKETS)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		parseTo(listener, false, ch)
		wg.Done()
	}()

	conn, err := net.DialTimeout("udp", addr, 50*time.Millisecond)
	assert.Equal(t, nil, err)

	n, err := conn.Write([]byte("deploys.test.myservice:2|c"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice:2|c"), n)

	n, err = conn.Write([]byte("deploys.test.my:service:2|c"))

	n, err = conn.Write([]byte("deploys.test.myservice:1|c"))
	assert.Equal(t, nil, err)
	assert.Equal(t, len("deploys.test.myservice:1|c"), n)

	select {
	case pack := <-ch:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, int64(2), pack.Value.(int64))
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	select {
	case pack := <-ch:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, int64(1), pack.Value.(int64))
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	listener.Close()
	wg.Wait()
}

func BenchmarkManyDifferentSensors(t *testing.B) {
	r := rand.New(rand.NewSource(438))
	for i := 0; i < 1000; i++ {
		bucket := "response_time" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := uint64(r.Uint32() % 1000)
			timers[bucket] = append(timers[bucket], a)
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "count" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := int64(r.Uint32() % 1000)
			counters[bucket] = a
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "gauge" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := uint64(r.Uint32() % 1000)
			gauges[bucket] = a
		}
	}

	var buff bytes.Buffer
	now := time.Now().Unix()
	t.ResetTimer()
	processTimers(&buff, now, commonPercentiles)
	processCounters(&buff, now)
	processGauges(&buff, now)
}

func BenchmarkOneBigTimer(t *testing.B) {
	r := rand.New(rand.NewSource(438))
	bucket := "response_time"
	for i := 0; i < 10000000; i++ {
		a := uint64(r.Uint32() % 1000)
		timers[bucket] = append(timers[bucket], a)
	}

	var buff bytes.Buffer
	t.ResetTimer()
	processTimers(&buff, time.Now().Unix(), commonPercentiles)
}

func BenchmarkLotsOfTimers(t *testing.B) {
	r := rand.New(rand.NewSource(438))
	for i := 0; i < 1000; i++ {
		bucket := "response_time" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := uint64(r.Uint32() % 1000)
			timers[bucket] = append(timers[bucket], a)
		}
	}

	var buff bytes.Buffer
	t.ResetTimer()
	processTimers(&buff, time.Now().Unix(), commonPercentiles)
}

func BenchmarkParseLine(b *testing.B) {
	d := []byte("a.key.with-0.dash:4|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseLine(d)
	}
}
