// queue_test.go

package taskq

import (
	"context"
	"testing"
)

type testPayload struct {
	Name string
	N    int
}

func TestQueueEnqueue_And_Decode_Roundtrip(t *testing.T) {
	tests := []struct {
		name    string
		payload testPayload
	}{
		{name: "zero value", payload: testPayload{}},
		{name: "populated", payload: testPayload{Name: "job-1", N: 42}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &captureBroker{}
			q := NewQueue[testPayload](b, "jobs")

			if err := q.Enqueue(context.Background(), tt.payload); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			task, err := decode[testPayload](b.last)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if task.Payload != tt.payload {
				t.Errorf("got payload %+v, want %+v", task.Payload, tt.payload)
			}
			if task.Queue != "jobs" {
				t.Errorf("got queue %q, want %q", task.Queue, "jobs")
			}
		})
	}
}

// captureBroker is a minimal Broker stub recording the last enqueued
// message — enough to test Queue.Enqueue and decode without pulling in
// membroker, which would make this a cross-package test.
type captureBroker struct {
	last Message
}

func (c *captureBroker) Enqueue(ctx context.Context, msg Message) error {
	c.last = msg
	return nil
}

func (c *captureBroker) Dequeue(ctx context.Context, queue string) (*Message, error) {
	return &Message{}, nil
}

func (c *captureBroker) Ack(ctx context.Context, msg Message) error { return nil }

func (c *captureBroker) Nack(ctx context.Context, msg Message, cause error) error {
	return nil
}
