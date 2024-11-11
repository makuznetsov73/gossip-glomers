package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	m             sync.Mutex
	values        map[int]struct{} = make(map[int]struct{})
	topologyMutex sync.Mutex
	topologyValue map[string][]string
)

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
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return read(&msg, n)
	})
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return topology(body, &msg, n)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
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
	m.Unlock()
	if needToSend {
		topologyMutex.Lock()
		for _, id := range topologyValue[n.ID()] {
			if id == req.Src {
				continue
			}
			err := n.Send(id, body)
			if err != nil {
				return err
			}
		}
		topologyMutex.Unlock()
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

func topology(body map[string]any, req *maelstrom.Message, n *maelstrom.Node) error {
	topologyMutex.Lock()
	defer topologyMutex.Unlock()
	mapAny, ok := body["topology"].(map[string]any)
	if !ok {
		return errors.New(fmt.Sprint("topology value is not map[string]string (?) but ", reflect.ValueOf(body["topology"]).Type().String()))
	}
	topologyValue = make(map[string][]string)
	for k, mv := range mapAny {
		switch v := mv.(type) {
		case []string:
			topologyValue[k] = v
		case []any:
			entry := make([]string, 0, len(v))
			for _, va := range v {
				vstr, ok := va.(string)
				if !ok {
					return errors.New(fmt.Sprint("some node id in topology entry value is not string (?) but ", reflect.ValueOf(va).Type().String()))
				}
				entry = append(entry, vstr)
			}
			topologyValue[k] = entry
		default:
			return errors.New(fmt.Sprint("topology entry value is not []string (?) but ", reflect.ValueOf(mv).Type().String()))
		}
	}

	response := map[string]any{"type": "topology_ok"}
	return n.Reply(*req, response)
}
