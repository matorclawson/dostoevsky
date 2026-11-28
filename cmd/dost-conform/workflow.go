package main

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	d "github.com/rodriveracom/dostoevsky"
	"github.com/rodriveracom/dostoevsky/workflow"
)

type wfStep struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Attempts  int    `json:"attempts"`
	BackoffMS int64  `json:"backoff_ms"`
}

// wfEvent is one history event. Time is milliseconds since the Unix epoch, as a string
// so that no reader rounds it. Data is bytes: it need not be UTF-8.
type wfEvent struct {
	Sequence int64  `json:"sequence"`
	Time     int64  `json:"time,string"`
	Kind     string `json:"kind"`
	Step     int    `json:"step"`
	Data     []byte `json:"data"`
}

var errNoStore = errors.New("dost-conform: no workflow store is open")

// wf performs one wf_ request. main.go sends every operation with that prefix here.
func (s *driver) wf(q request) (any, error) {
	if q.Op == "wf_open" {
		if s.store != nil || s.node != nil {
			return nil, errors.New("dost-conform: a node or store is already open")
		}
		store, err := workflow.Open(q.Dir)
		if err != nil {
			return nil, err
		}
		s.store = store
		return nil, nil
	}
	w := s.store
	if w == nil {
		return nil, errNoStore
	}
	switch q.Op {
	case "wf_close":
		s.store = nil
		return nil, w.Close()
	case "wf_register":
		steps := make([]workflow.Step, len(q.Steps))
		for i, v := range q.Steps {
			steps[i] = workflow.Step{Kind: v.Kind, Name: v.Name, Attempts: v.Attempts, BackoffMillis: v.BackoffMS}
		}
		return nil, w.Register(q.Name, q.Version, steps)
	case "wf_start_once":
		created, err := w.StartOnce(q.Run, q.Name, q.Version, q.Queue, json.RawMessage(q.Input), q.Attrs)
		if err != nil {
			return nil, err
		}
		return map[string]any{"created": created}, nil
	case "wf_poll":
		if limit := int64(math.MaxInt64 / time.Millisecond); q.LeaseMS > limit || q.LeaseMS < -limit {
			return nil, d.ErrInvalid
		}
		task, err := w.Poll(q.Queue, time.Duration(q.LeaseMS)*time.Millisecond)
		if err != nil {
			return nil, err
		}
		if task == nil {
			return map[string]any{"task": nil}, nil
		}
		return map[string]any{"task": map[string]any{"run": task.Run, "activity": task.Activity, "token": task.Token,
			"step": task.Step, "attempt": task.Attempt, "input": []byte(task.Input)}}, nil
	case "wf_finish":
		return nil, w.Finish(q.Run, q.Token, json.RawMessage(q.Value), string(q.Failure))
	case "wf_signal":
		return nil, w.Signal(q.Run, q.Step, q.Name, json.RawMessage(q.Value))
	case "wf_history":
		events, err := w.History(q.Run, q.After, q.Limit)
		if err != nil {
			return nil, err
		}
		// Kinds and data are listed apart because events carry times, which differ between runs.
		kinds := make([]string, len(events))
		data := make([][]byte, len(events))
		out := make([]wfEvent, len(events))
		for i, e := range events {
			kinds[i] = e.Kind
			data[i] = []byte(e.Data)
			out[i] = wfEvent{e.Sequence, e.TimeMillis, e.Kind, e.Step, data[i]}
		}
		return map[string]any{"kinds": kinds, "data": data, "events": out}, nil
	}
	return nil, errors.New("dost-conform: unknown op " + strconv.Quote(q.Op))
}
