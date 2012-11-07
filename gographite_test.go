package main

import (
	"bytes"
	"github.com/bmizerany/assert"
	"testing"
)

func TestPacketParse(t *testing.T) {

	d := []byte("gaugor:333|g")
	packets := parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet := packets[0]
	assert.Equal(t, "gaugor", packet.Bucket)
	assert.Equal(t, 333, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("gorets:2|c|@0.1")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, 2, packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(0.1), packet.Sampling)

	d = []byte("gorets:4|c")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "gorets", packet.Bucket)
	assert.Equal(t, 4, packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("glork:320|ms")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "glork", packet.Bucket)
	assert.Equal(t, 320, packet.Value)
	assert.Equal(t, "ms", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 1)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, 4, packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4|c\ngauge:3|g")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 2)
	packet = packets[0]
	assert.Equal(t, "a.key.with-0.dash", packet.Bucket)
	assert.Equal(t, 4, packet.Value)
	assert.Equal(t, "c", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	packet = packets[1]
	assert.Equal(t, "gauge", packet.Bucket)
	assert.Equal(t, 3, packet.Value)
	assert.Equal(t, "g", packet.Modifier)
	assert.Equal(t, float32(1), packet.Sampling)

	d = []byte("a.key.with-0.dash:4\ngauge3|g")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 0)

	d = []byte("a.key.with-0.dash:4")
	packets = parseMessage(bytes.NewBuffer(d))
	assert.Equal(t, len(packets), 0)
}
