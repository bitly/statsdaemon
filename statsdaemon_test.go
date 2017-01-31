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
	assert.Equal(t, float64(333), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:-10|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(10), packet.ValFlt)
	assert.Equal(t, "-", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:+4|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "+", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// >max(int64) && <max(uint64)
	d = []byte("gaugor:18446744073709551606|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(18446744073709551606), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// float values
	d = []byte("gaugor:3.3333|g")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, float64(3.3333), packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineCount(t *testing.T) {
	d := []byte("gorets:2|c|@0.1")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(2), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, float64(-4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:1.25|c")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, 1.25, packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineTimer(t *testing.T) {
	d := []byte("glork:320|ms")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms|@0.1")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(320), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("glork:3.7211|ms")
	packet = parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, float64(3.7211), packet.ValFlt)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineSet(t *testing.T) {
	d := []byte("uniques:765|s")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "uniques", packet.Bucket)
	assert.Equal(t, "765", packet.ValStr)
	assert.Equal(t, "s", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestParseLineMisc(t *testing.T) {
	d := []byte("a.key.with-0.dash:4|c")
	packet := parseLine(d)
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with 0.space:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.space", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with/0.slash:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with-0.slash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with@#*&%$^_0.garbage:4|c")
	packet = parseLine(d)
	assert.Equal(t, "a.key.with_0.garbage", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	flag.Set("prefix", "test.")
	d = []byte("prefix:4|c")
	packet = parseLine(d)
	assert.Equal(t, "test.prefix", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	flag.Set("prefix", "")

	flag.Set("postfix", ".test")
	d = []byte("postfix:4|c")
	packet = parseLine(d)
	assert.Equal(t, "postfix.test", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
	flag.Set("postfix", "")

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	parser := NewParser(bytes.NewBuffer(d), true)
	packet, more := parser.Next()
	assert.Equal(t, more, true)
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, 3.0, packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
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
	assert.Equal(t, float64(4), packet.ValFlt)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet, more = parser.Next()
	assert.NotEqual(t, packet, nil)
	assert.Equal(t, more, false)
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, 3.0, packet.ValFlt)
	assert.Equal(t, "", packet.ValStr)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)
}

func TestPacketHandlerReceiveCounter(t *testing.T) {
	counters = make(map[string]float64)
	*receiveCounter = "countme"

	p := &Packet{
		Bucket:   "gorets",
		ValFlt:   100,
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, counters["countme"], float64(1))

	packetHandler(p)
	assert.Equal(t, counters["countme"], float64(2))
}

func TestPacketHandlerCount(t *testing.T) {
	counters = make(map[string]float64)

	p := &Packet{
		Bucket:   "gorets",
		ValFlt:   100,
		Modifier: "c",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, counters["gorets"], float64(100))

	p.ValFlt = float64(3)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], float64(103))

	p.ValFlt = float64(-4)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], float64(99))

	p.ValFlt = float64(-100)
	packetHandler(p)
	assert.Equal(t, counters["gorets"], float64(-1))
}

func TestPacketHandlerGauge(t *testing.T) {
	gauges = make(map[string]float64)

	p := &Packet{
		Bucket:   "gaugor",
		ValFlt:   333,
		ValStr:   "",
		Modifier: "g",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(333))

	// -10
	p.ValFlt = 10
	p.ValStr = "-"
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(323))

	// +4
	p.ValFlt = 4
	p.ValStr = "+"
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(327))

	// <0 overflow
	p.ValFlt = 10
	p.ValStr = ""
	packetHandler(p)
	p.ValFlt = 20
	p.ValStr = "-"
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(0))

	// >MaxFloat64 overflow
	p.ValFlt = float64(math.MaxFloat64 - 10)
	p.ValStr = ""
	packetHandler(p)
	p.ValFlt = 20
	p.ValStr = "+"
	packetHandler(p)
	assert.Equal(t, gauges["gaugor"], float64(math.MaxFloat64))
}

func TestPacketHandlerTimer(t *testing.T) {
	timers = make(map[string]Float64Slice)

	p := &Packet{
		Bucket:   "glork",
		ValFlt:   float64(320),
		Modifier: "ms",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 1)
	assert.Equal(t, timers["glork"][0], float64(320))

	p.ValFlt = float64(100)
	packetHandler(p)
	assert.Equal(t, len(timers["glork"]), 2)
	assert.Equal(t, timers["glork"][1], float64(100))
}

func TestPacketHandlerSet(t *testing.T) {
	sets = make(map[string][]string)

	p := &Packet{
		Bucket:   "uniques",
		ValStr:   "765",
		Modifier: "s",
		Sampling: float32(1),
	}
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 1)
	assert.Equal(t, sets["uniques"][0], "765")

	p.ValStr = "567"
	packetHandler(p)
	assert.Equal(t, len(sets["uniques"]), 2)
	assert.Equal(t, sets["uniques"][1], "567")
}

func TestProcessCounters(t *testing.T) {

	*persistCountKeys = int64(10)
	counters = make(map[string]float64)
	var buffer bytes.Buffer
	now := int64(1418052649)

	counters["gorets"] = float64(123)

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
	timers = make(map[string]Float64Slice)
	timers["response_time"] = []float64{0, 30, 30}

	now := int64(1418052649)

	var buffer bytes.Buffer
	num := processTimers(&buffer, now, Percentiles{})

	lines := bytes.Split(buffer.Bytes(), []byte("\n"))

	assert.Equal(t, num, int64(1))
	assert.Equal(t, string(lines[0]), "response_time.mean 20 1418052649")
	assert.Equal(t, string(lines[1]), "response_time.upper 30 1418052649")
	assert.Equal(t, string(lines[2]), "response_time.lower 0 1418052649")
	assert.Equal(t, string(lines[3]), "response_time.count 3 1418052649")

	num = processTimers(&buffer, now, Percentiles{})
	assert.Equal(t, num, int64(0))
}

func TestProcessGauges(t *testing.T) {
	flag.Set("delete-gauges", "false")
	gauges = make(map[string]float64)
	var buffer bytes.Buffer

	now := int64(1418052649)

	num := processGauges(&buffer, now)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "")

	p := &Packet{
		Bucket:   "gaugor",
		ValFlt:   12345,
		ValStr:   "",
		Modifier: "g",
		Sampling: 1.0,
	}
	packetHandler(p)
	num = processGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	num = processGauges(&buffer, now+20)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugor 12345 1418052649\ngaugor 12345 1418052669\n")

	buffer = bytes.Buffer{}
	p.ValFlt = 12346.75
	packetHandler(p)
	p.ValFlt = 12347.25
	packetHandler(p)
	num = processGauges(&buffer, now+40)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugor 12347.25 1418052689\n")
}

func TestProcessDeleteGauges(t *testing.T) {
	flag.Set("delete-gauges", "true")
	gauges = make(map[string]float64)
	var buffer bytes.Buffer

	now := int64(1418052649)

	p := &Packet{
		Bucket:   "gaugordelete",
		ValFlt:   12345,
		ValStr:   "",
		Modifier: "g",
		Sampling: 1.0,
	}

	packetHandler(p)
	num := processGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugordelete 12345 1418052649\n")

	num = processGauges(&buffer, now+20)
	assert.Equal(t, num, int64(0))
	assert.Equal(t, buffer.String(), "gaugordelete 12345 1418052649\n")
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
	timers = make(map[string]Float64Slice)
	timers["response_time"] = []float64{0, 1, 2, 3}

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
	timers = make(map[string]Float64Slice)
	timers["postfix_response_time.test"] = []float64{0, 1, 2, 3}

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
	timers = make(map[string]Float64Slice)
	timers["time"] = []float64{0, 1, 2, 3}

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
		assert.Equal(t, float64(2), pack.ValFlt)
		assert.Equal(t, "c", pack.Modifier)
		assert.Equal(t, float32(1), pack.Sampling)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("packet receive timeout")
	}

	select {
	case pack := <-ch:
		assert.Equal(t, "deploys.test.myservice", pack.Bucket)
		assert.Equal(t, float64(1), pack.ValFlt)
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
			a := float64(r.Uint32() % 1000)
			timers[bucket] = append(timers[bucket], a)
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "count" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
			counters[bucket] = a
		}
	}

	for i := 0; i < 1000; i++ {
		bucket := "gauge" + strconv.Itoa(i)
		for i := 0; i < 10000; i++ {
			a := float64(r.Uint32() % 1000)
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
		a := float64(r.Uint32() % 1000)
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
			a := float64(r.Uint32() % 1000)
			timers[bucket] = append(timers[bucket], a)
		}
	}

	var buff bytes.Buffer
	t.ResetTimer()
	processTimers(&buff, time.Now().Unix(), commonPercentiles)
}

func BenchmarkParseLineCounter(b *testing.B) {
	d1 := []byte("a.key.with-0.dash:4|c|@0.5")
	d2 := []byte("normal.key.space:1|c")

	for i := 0; i < b.N; i++ {
		parseLine(d1)
		parseLine(d2)
	}
}
func BenchmarkParseLineGauge(b *testing.B) {
	d1 := []byte("gaugor.whatever:333.4|g")
	d2 := []byte("gaugor.whatever:-5|g")

	for i := 0; i < b.N; i++ {
		parseLine(d1)
		parseLine(d2)
	}
}
func BenchmarkParseLineTimer(b *testing.B) {
	d1 := []byte("glork.some.keyspace:3.7211|ms")
	d2 := []byte("glork.some.keyspace:11223|ms")

	for i := 0; i < b.N; i++ {
		parseLine(d1)
		parseLine(d2)
	}
}
func BenchmarkParseLineSet(b *testing.B) {
	d1 := []byte("setof.some.keyspace:hiya|s")
	d2 := []byte("setof.some.keyspace:411|s")

	for i := 0; i < b.N; i++ {
		parseLine(d1)
		parseLine(d2)
	}
}
func BenchmarkPacketHandlerCounter(b *testing.B) {
	d1 := parseLine([]byte("a.key.with-0.dash:4|c|@0.5"))
	d2 := parseLine([]byte("normal.key.space:1|c"))
	counters = make(map[string]float64)

	for i := 0; i < b.N; i++ {
		packetHandler(d1)
		packetHandler(d2)
	}
}
func BenchmarkPacketHandlerGauge(b *testing.B) {
	d1 := parseLine([]byte("gaugor.whatever:333.4|g"))
	d2 := parseLine([]byte("gaugor.whatever:-5|g"))
	gauges = make(map[string]float64)

	for i := 0; i < b.N; i++ {
		packetHandler(d1)
		packetHandler(d2)
	}
}
func BenchmarkPacketHandlerTimer(b *testing.B) {
	d1 := parseLine([]byte("glork.some.keyspace:3.7211|ms"))
	d2 := parseLine([]byte("glork.some.keyspace:11223|ms"))
	timers = make(map[string]Float64Slice)

	for i := 0; i < b.N; i++ {
		packetHandler(d1)
		packetHandler(d2)
	}
}
func BenchmarkPacketHandlerSet(b *testing.B) {
	d1 := parseLine([]byte("setof.some.keyspace:hiya|s"))
	d2 := parseLine([]byte("setof.some.keyspace:411|s"))
	sets = make(map[string][]string)

	for i := 0; i < b.N; i++ {
		if i&0xff == 0xff {
			sets = make(map[string][]string)
		}
		packetHandler(d1)
		packetHandler(d2)
	}
}
