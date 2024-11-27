package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	m             sync.Mutex
	values        map[int]struct{} = make(map[int]struct{})
	topologyMutex sync.Mutex
	topologyValue map[string][]string
)

type ReadMsg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

type TopologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return broadcast(body, &msg, n)
	})
	n.Handle("read", func(msg maelstrom.Message) error {
		return read(&msg, n)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMsg

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return topology(body, &msg, n)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func propagate(body map[string]any, req *maelstrom.Message, n *maelstrom.Node, dest string) {
	if dest == req.Src {
		return
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*110)
		_, err := n.SyncRPC(ctx, dest, body)
		cancel()
		if err == nil {
			break
		}
	}
}

func broadcast(body map[string]any, req *maelstrom.Message, n *maelstrom.Node) error {
	v, ok := body["message"].(float64)
	if !ok {
		return errors.New(fmt.Sprint("message value is not an float64 (?) but ", reflect.ValueOf(body["message"]).Type().String()))
	}
	needToSend := false
	m.Lock()
	if _, ok := values[int(v)]; !ok {
		values[int(v)] = struct{}{}
		needToSend = true
	}
	for _, id := range n.NodeIDs() {
		if req.Src == id {
			needToSend = false
		}
	}
	m.Unlock()
	if needToSend {
		ids := make([]string, 0)
		topologyMutex.Lock()
		ids = append(ids, topologyValue[n.ID()]...)
		topologyMutex.Unlock()
		for _, id := range n.NodeIDs() {
			go propagate(body, req, n, id)
		}
	}
	response := map[string]any{"type": "broadcast_ok"}
	return n.Reply(*req, response)
}

func read(req *maelstrom.Message, n *maelstrom.Node) error {
	m.Lock()
	response := map[string]any{"type": "read_ok"}
	valuesC := make([]int, 0, len(values))
	for k := range values {
		valuesC = append(valuesC, k)
	}
	m.Unlock()
	response["messages"] = valuesC
	return n.Reply(*req, response)
}

func topology(body TopologyMsg, req *maelstrom.Message, n *maelstrom.Node) error {
	topologyMutex.Lock()
	defer topologyMutex.Unlock()
	topologyValue = body.Topology

	response := map[string]any{"type": "topology_ok"}
	return n.Reply(*req, response)
}
