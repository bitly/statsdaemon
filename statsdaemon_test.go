package main

import (
	"bytes"
	"github.com/bmizerany/assert"
	"regexp"
	"testing"
	"time"
)

func TestPacketParse(t *testing.T) {

	d := []byte("gaugor:333|g")
	packets := parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet := packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, uint64(333), packet.Value.(uint64))
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:2|c|@0.1")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(2), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:-4|c")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, int64(-4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, uint64(320), packet.Value.(uint64))
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, int64(4), packet.Value.(int64))
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	packets = parseMessage(bytes.NewBuffer(d))
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
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 0)

	d = []byte("a.key.with-0.dash:4")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 0)
}

func TestMean(t *testing.T) {
	// Some data with expected mean of 20
	d := []byte("response_time:0|ms\nresponse_time:30|ms\nresponse_time:30|ms")
	packets := parseMessage(bytes.NewBuffer(d))

	for _, s := range packets {
		timers[s.Bucket] = append(timers[s.Bucket], s.Value.(uint64))
	}

	buff := bytes.NewBuffer([]byte{})
	numStats := 0
	processTimers(buff, &numStats, time.Now().Unix())
	assert.Equal(t, numStats, 1)
	dataForGraphite := buff.String()
	meanRegexp := regexp.MustCompile("response_time.mean.*20")

	matched := meanRegexp.MatchString(dataForGraphite)
	assert.Equal(t, matched, true)
}
