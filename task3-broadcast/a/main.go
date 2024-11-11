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
	m      sync.Mutex
	values []int
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
		return read(body, &msg, n)
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
	m.Lock()
	values = append(values, int(v))
	m.Unlock()
	response := map[string]any{"type": "broadcast_ok"}
	return n.Reply(*req, response)
}

func read(body map[string]any, req *maelstrom.Message, n *maelstrom.Node) error {
	m.Lock()
	response := map[string]any{"type": "read_ok"}
	valuesC := make([]int, 0, len(values))
	valuesC = append(valuesC, values...)
	m.Unlock()
	response["messages"] = valuesC
	return n.Reply(*req, response)
}

func topology(body map[string]any, req *maelstrom.Message, n *maelstrom.Node) error {
	response := map[string]any{"type": "topology_ok"}
	return n.Reply(*req, response)
}
