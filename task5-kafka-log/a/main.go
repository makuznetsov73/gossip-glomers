package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n              *maelstrom.Node
	counter        int
	mu             sync.Mutex
	data           map[string]map[int]any
	dataMaxOffsets map[string]int
	dataMinOffsets map[string]int

	committedOffsets   map[string]int
	muCommittedOffsets sync.Mutex

	kv *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	s := Server{n: n, data: make(map[string]map[int]any), dataMaxOffsets: make(map[string]int),
		dataMinOffsets: make(map[string]int), committedOffsets: make(map[string]int), kv: kv}
	return &s
}

type SendMsg struct {
	Key string `tag:"json:key"`
	Msg any    `tag:"json:msg"`
}

type PollMsg struct {
	Offsets map[string]int `tag:"json:offsets"`
}

type ListOffsets struct {
	Keys []string `tag:"json:keys"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewServer(n, kv)

	n.Handle("init", s.init)
	n.Handle("send", s.send)
	n.Handle("poll", s.poll)
	n.Handle("commit_offsets", s.commitOffsets)
	n.Handle("list_committed_offsets", s.listCommitedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) init(msg maelstrom.Message) error {
	return nil
}

func (s *Server) send(msg maelstrom.Message) error {
	var body SendMsg
	json.Unmarshal(msg.Body, &body)

	var respC int
	var v map[int]any
	var ok bool

	s.mu.Lock()
	if v, ok = s.data[body.Key]; !ok {
		v = make(map[int]any)
	}

	v[s.counter] = body.Msg
	s.dataMaxOffsets[body.Key] = s.counter
	if _, okMin := s.dataMinOffsets[body.Key]; !okMin {
		s.dataMinOffsets[body.Key] = s.counter
	}
	respC = s.counter

	if !ok {
		s.data[body.Key] = v
	}
	s.counter++
	s.mu.Unlock()

	resp := map[string]any{"type": "send_ok"}
	resp["offset"] = respC
	return s.n.Reply(msg, resp)
}

func (s *Server) poll(msg maelstrom.Message) error {
	var body PollMsg
	json.Unmarshal(msg.Body, &body)

	resp := map[string]any{"type": "poll_ok"}

	msgs := make(map[string][][2]any)

	var min, max int
	for k, v := range body.Offsets {
		s.mu.Lock()
		min = s.dataMinOffsets[k]
		max = s.dataMaxOffsets[k]
		if min < v {
			min = v
		}

		respK := make([][2]any, 0)

		data := s.data[k]
		for i := min; i <= max; i++ {
			if va, ok := data[i]; ok {
				respK = append(respK, [2]any{i, va})
			}
		}
		s.mu.Unlock()

		msgs[k] = respK
	}
	resp["msgs"] = msgs
	return s.n.Reply(msg, resp)
}

func (s *Server) commitOffsets(msg maelstrom.Message) error {
	var body PollMsg
	json.Unmarshal(msg.Body, &body)

	s.muCommittedOffsets.Lock()
	for k, v := range body.Offsets {
		s.committedOffsets[k] = v
	}
	s.muCommittedOffsets.Unlock()

	resp := map[string]any{"type": "commit_offsets_ok"}
	return s.n.Reply(msg, resp)
}

func (s *Server) listCommitedOffsets(msg maelstrom.Message) error {
	var body ListOffsets
	json.Unmarshal(msg.Body, &body)

	offsets := make(map[string]int)
	s.muCommittedOffsets.Lock()
	for _, k := range body.Keys {
		offsets[k] = s.committedOffsets[k]
	}
	s.muCommittedOffsets.Unlock()

	resp := map[string]any{"type": "list_committed_offsets_ok"}
	resp["offsets"] = offsets
	return s.n.Reply(msg, resp)
}
