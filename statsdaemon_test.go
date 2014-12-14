package main

import (
	"bytes"
	"github.com/bmizerany/assert"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var commonPercentiles = Percentiles{
	&Percentile{
		99,
		"99",
	},
}

func TestPacketParse(t *testing.T) {
	d := []byte("gaugor:333|g")
	packets := parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet := packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 333}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:-10|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, true, 10}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gaugor:+4|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{true, false, 4}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	// >max(int64) && <max(uint64)
	d = []byte("gaugor:18446744073709551606|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 18446744073709551606}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:2|c|@0.1")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(2), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, uint64(320), packet.Value.(uint64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 2)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet = packets[1]
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, GaugeData{false, false, 3}, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("a.key.with-0.dash:4")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets:5m")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets:")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets:5|mg")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets:5|ms|@")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 1)

	d = []byte("")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gorets:xxx|c")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gaugor:xxx|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("gaugor:xxx|z")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)
}

func TestMalformedDataHandling(t *testing.T) {

	// reported as issue #45
	d := []byte("deploys.test.myservice4:100|t")
	packets := parseMessage(d)
	packetHandler(packets[0])

}

func TestReceiveCounterPacketHandling(t *testing.T) {
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

func TestCountPacketHandling(t *testing.T) {
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

func TestGaugePacketHandling(t *testing.T) {
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

func TestTimerPacketHandling(t *testing.T) {
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
	gauges = make(map[string]uint64)
	gauges["gaugor"] = 12345

	now := int64(1418052649)

	var buffer bytes.Buffer

	num := processGauges(&buffer, now)
	assert.Equal(t, num, int64(1))
	assert.Equal(t, buffer.String(), "gaugor 12345 1418052649\n")

	gauges["gaugor"] = 12345
	num = processGauges(&buffer, now)
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

func BenchmarkParseMessage(b *testing.B) {
	d := []byte("a.key.with-0.dash:4|c|@0.5")
	for i := 0; i < b.N; i++ {
		parseMessage(d)
	}
}
