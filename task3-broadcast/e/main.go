package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

type BroadcastMsg struct {
	Type     string `json:"type"`
	Message  int    `json:"message"`
	Messages []int  `json:"messages"`
}

type ReadMsg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type Server struct {
	n             *maelstrom.Node
	m             sync.Mutex
	values        map[int]struct{}
	topologyMutex sync.Mutex
	topologyValue map[string][]string

	batch      []int
	batchMutex sync.Mutex
	batchCh    chan bool
	batchSize  int

	stopCh chan struct{}
}

func NewServer(n *maelstrom.Node) *Server {
	s := Server{n: n, values: make(map[int]struct{}),
		topologyValue: make(map[string][]string), batchCh: make(chan bool),
		batchSize: 20, stopCh: make(chan struct{})}
	return &s
}

func main() {
	n := maelstrom.NewNode()

	server := NewServer(n)

	n.Handle("init", server.initHandler)
	n.Handle("broadcast", server.broadcast)
	n.Handle("read", server.read)
	n.Handle("topology", server.topology)

	go server.sendBatch()

	if err := n.Run(); err != nil {
		close(server.stopCh)
		log.Fatal(err)
	}
	close(server.stopCh)
}

func (s *Server) sendBatch() {
	sendFn := func() {
		s.batchMutex.Lock()
		defer s.batchMutex.Unlock()
		if len(s.batch) == 0 {
			return
		}
		var msg BroadcastMsg
		msg.Type = "broadcast"
		log.Info(s.batch, " batch before sending")
		msg.Messages = append(msg.Messages, s.batch...)
		log.Info(msg.Messages, " batch inside message before sending")
		for _, id := range s.n.NodeIDs() {
			go s.propagate(&msg, id)
		}
		s.batch = nil
	}

	log.Info("start batching worker")
	for {
		select {
		case <-time.After(time.Millisecond * 700):
			log.Info("start batching by timer")
			sendFn()
		case <-s.batchCh:
			sendFn()
		case <-s.stopCh:
			log.Info("stop batching")
			return
		}
	}
}

func (s *Server) initHandler(_ maelstrom.Message) error {
	usr, _ := user.Current()
	dir := usr.HomeDir
	f, err := os.OpenFile(fmt.Sprintf("%s/maelstorm-logs/%s.log", dir, s.n.ID()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	return nil
}

func (s *Server) propagate(msg *BroadcastMsg, dest string) {
	if dest == s.n.ID() {
		return
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*300)
		_, err := s.n.SyncRPC(ctx, dest, msg)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *Server) broadcast(msg maelstrom.Message) error {
	var body BroadcastMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	needToSend := false
	s.m.Lock()
	if len(body.Messages) == 0 {
		if _, ok := s.values[body.Message]; !ok {
			s.values[body.Message] = struct{}{}
			needToSend = true
			log.Info(body.Message, " got and saved value")
		}
	} else {
		for _, v := range body.Messages {
			s.values[v] = struct{}{}
		}
	}
	s.m.Unlock()

	if needToSend {
		log.Info(body.Message, " need to send")
		s.batchMutex.Lock()
		s.batch = append(s.batch, body.Message)
		log.Info(s.batch, " batch after new value")
		if len(s.batch) == s.batchSize {
			s.batchCh <- true
		}
		s.batchMutex.Unlock()
	}

	log.Info(body, "responding")
	response := map[string]any{"type": "broadcast_ok"}
	return s.n.Reply(msg, response)
}

func (s *Server) read(msg maelstrom.Message) error {
	s.m.Lock()
	response := map[string]any{"type": "read_ok"}
	valuesC := make([]int, 0, len(s.values))
	for k := range s.values {
		valuesC = append(valuesC, k)
	}
	s.m.Unlock()
	response["messages"] = valuesC
	return s.n.Reply(msg, response)
}

func (s *Server) topology(msg maelstrom.Message) error {
	var body TopologyMsg

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.topologyMutex.Lock()
	defer s.topologyMutex.Unlock()
	s.topologyValue = body.Topology

	response := map[string]any{"type": "topology_ok"}
	return s.n.Reply(msg, response)
}
