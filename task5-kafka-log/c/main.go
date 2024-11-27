package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KeySlice struct {
	mu   sync.Mutex
	data [][2]any
}

type Server struct {
	n       *maelstrom.Node
	counter int
	mu      sync.Mutex
	data    map[string]*KeySlice

	committedOffsets   map[string]int
	muCommittedOffsets sync.Mutex

	kv *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	s := Server{n: n, data: make(map[string]*KeySlice), committedOffsets: make(map[string]int), kv: kv}
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

func (s *Server) isCoreNode() (bool, string) {
	ids := s.n.NodeIDs()
	return ids[0] == s.n.ID(), ids[0]
}

func (s *Server) init(msg maelstrom.Message) error {
	return nil
}

func (s *Server) syncRPCReply(msg *maelstrom.Message, coreId string) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
		msgCore, err := s.n.SyncRPC(ctx, coreId, msg.Body)
		cancel()
		if err != nil {
			continue
		}
		return s.n.Reply(*msg, msgCore.Body)
	}
}

func (s *Server) send(msg maelstrom.Message) error {
	if ok, coreId := s.isCoreNode(); !ok {
		return s.syncRPCReply(&msg, coreId)
	}
	var body SendMsg
	json.Unmarshal(msg.Body, &body)

	var respC int
	var keySlice *KeySlice
	var ok bool
	s.mu.Lock()
	if keySlice, ok = s.data[body.Key]; !ok {
		keySlice = &KeySlice{data: [][2]any{{0, body.Msg}}}
		s.data[body.Key] = keySlice
		s.mu.Unlock()
		respC = 0
	} else {
		s.mu.Unlock()
		keySlice.mu.Lock()
		keySlice.data = append(keySlice.data, [2]any{len(keySlice.data), body.Msg})
		respC = len(keySlice.data) - 1
		keySlice.mu.Unlock()
	}

	resp := map[string]any{"type": "send_ok"}
	resp["offset"] = respC
	return s.n.Reply(msg, resp)
}

func (s *Server) poll(msg maelstrom.Message) error {
	if ok, coreId := s.isCoreNode(); !ok {
		return s.syncRPCReply(&msg, coreId)
	}
	var body PollMsg
	json.Unmarshal(msg.Body, &body)

	resp := map[string]any{"type": "poll_ok"}

	msgs := make(map[string][][2]any)
	local := make(map[string]*KeySlice)
	for k := range body.Offsets {
		s.mu.Lock()
		keySlice, ok := s.data[k]
		s.mu.Unlock()
		if !ok {
			continue
		}
		local[k] = keySlice
	}

	for k, v := range body.Offsets {
		keySlice, ok := local[k]
		if !ok {
			continue
		}
		var res [][2]any
		keySlice.mu.Lock()
		for i := range keySlice.data {
			if int(keySlice.data[i][0].(int)) >= v {
				res = append(res, keySlice.data[i:]...)
				break
			}
		}
		keySlice.mu.Unlock()
		if len(res) != 0 {
			msgs[k] = res
		}
	}
	resp["msgs"] = msgs
	return s.n.Reply(msg, resp)
}

func (s *Server) commitOffsets(msg maelstrom.Message) error {
	if ok, coreId := s.isCoreNode(); !ok {
		return s.syncRPCReply(&msg, coreId)
	}
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
	if ok, coreId := s.isCoreNode(); !ok {
		return s.syncRPCReply(&msg, coreId)
	}
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
