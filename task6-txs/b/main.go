package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n  *maelstrom.Node
	mu sync.Mutex
	kv *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	return &Server{n: n, kv: kv}
}

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	s := NewServer(n, kv)

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
		keyRaw := opRaw[1]
		key := strconv.Itoa(int(keyRaw.(float64)))
		newValue := opRaw[2]
		switch t {
		case "r":
			if v, err := s.kv.Read(context.Background(), key+"_key"); err == nil {
				txnResp = append(txnResp, [3]any{"r", keyRaw, v})
			} else {
				txnResp = append(txnResp, [3]any{"r", keyRaw, nil})
			}
		case "w":
			s.kv.Write(context.Background(), key+"_key", newValue)
			txnResp = append(txnResp, [3]any{"w", keyRaw, newValue})
		}
	}
	s.mu.Unlock()
	resp := map[string]any{"type": "txn_ok"}
	resp["txn"] = txnResp
	return s.n.Reply(msg, resp)
}
