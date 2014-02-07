package main

import (
	"bytes"
	"github.com/bmizerany/assert"
	"math/rand"
	"regexp"
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
	assert.Equal(t, uint64(333), packet.Value.(uint64))
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
	assert.Equal(t, uint64(3), packet.Value.(uint64))
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)

	d = []byte("a.key.with-0.dash:4")
	packets = parseMessage(d)
	assert.Equal(t, len(packets), 0)
}

func TestMean(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("response_time:0|ms\nresponse_time:30|ms\nresponse_time:30|ms")
	packets := parseMessage(d)

	for _, s := range packets {
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
	}

	var buff bytes.Buffer
	var num int64
	num += processTimers(&buff, time.Now().Unix(), Percentiles{})
	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()
	pattern := `response_time\.mean 20\.[0-9]+ `
	meanRegexp := regexp.MustCompile(pattern)

	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}

func TestUpperPercentile(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := parseMessage(d)

	for _, s := range packets {
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
	}

	var buff bytes.Buffer
	var num int64
	num += processTimers(&buff, time.Now().Unix(), Percentiles{
		&Percentile{
			75,
			"75",
		},
	})
	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()

	meanRegexp := regexp.MustCompile(`time\.upper_75 2 `)
	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}

func TestLowerPercentile(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("time:0|ms\ntime:1|ms\ntime:2|ms\ntime:3|ms")
	packets := parseMessage(d)

	for _, s := range packets {
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
	}

	var buff bytes.Buffer
	var num int64
	num += processTimers(&buff, time.Now().Unix(), Percentiles{
		&Percentile{
			-75,
			"-75",
		},
	})
	assert.Equal(t, num, int64(1))
	dataForGraphite := buff.String()

	meanRegexp := regexp.MustCompile(`time\.upper_75 1 `)
	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, false)

	meanRegexp = regexp.MustCompile(`time\.lower_75 1 `)
	matched = meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
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
