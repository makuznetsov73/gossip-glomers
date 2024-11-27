package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n    *maelstrom.Node
	data map[any]any
	mu   sync.Mutex
}

func NewServer(n *maelstrom.Node) *Server {
	return &Server{n: n, data: make(map[any]any)}
}

func main() {

	n := maelstrom.NewNode()
	s := NewServer(n)

	n.Handle("init", s.init)
	n.Handle("txn", s.txn)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) init(msg maelstrom.Message) error {
	return nil
}

func (s *Server) txn(msg maelstrom.Message) error {
	body := make(map[string]any)
	json.Unmarshal(msg.Body, &body)
	txRaw := body["txn"]
	tx := txRaw.([]any)
	txnResp := make([][3]any, 0, len(tx))
	s.mu.Lock()
	for i := range tx {
		opRaw := tx[i].([]any)
		t := opRaw[0].(string)
		key := opRaw[1]
		newValue := opRaw[2]
		switch t {
		case "r":
			if v, ok := s.data[key]; ok {
				txnResp = append(txnResp, [3]any{"r", key, v})
			} else {
				txnResp = append(txnResp, [3]any{"r", key, nil})
			}
		case "w":
			if newValue == nil {
				delete(s.data, key)
			} else {
				s.data[key] = newValue
			}
			txnResp = append(txnResp, [3]any{"w", key, newValue})
		}
	}
	s.mu.Unlock()
	resp := map[string]any{"type": "txn_ok"}
	resp["txn"] = txnResp
	return s.n.Reply(msg, resp)
}
