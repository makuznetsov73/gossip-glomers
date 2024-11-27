package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	s := Server{n: n, kv: kv}
	return &s
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	s := NewServer(n, kv)

	n.Handle("init", s.init)
	n.Handle("add", s.add)
	n.Handle("read", s.read)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *Server) init(msg maelstrom.Message) error {
	return s.kv.Write(context.Background(), "counter", 0)
}

func (s *Server) add(msg maelstrom.Message) error {
	body := make(map[string]any)
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		return err
	}
	v, err := s.kv.ReadInt(context.Background(), "counter")
	if err != nil {
		return err
	}
	delta := int(body["delta"].(float64))
	for {
		err = s.kv.CompareAndSwap(context.Background(), "counter", v, v+delta, false)
		if err != nil {
			switch errCast := err.(type) {
			case *maelstrom.RPCError:
				if errCast.Code == maelstrom.PreconditionFailed {
					v, err = s.kv.ReadInt(context.Background(), "counter")
					if err != nil {
						return err
					}
					continue
				} else {
					return err
				}
			default:
				return err
			}
		}
		break
	}

	resp := map[string]any{"type": "add_ok"}
	return s.n.Reply(msg, resp)
}

func (s *Server) read(msg maelstrom.Message) error {
	resp := map[string]any{"type": "read_ok"}
	var err error
	resp["value"], err = s.kv.ReadInt(context.Background(), "counter")
	if err != nil {
		return err
	}
	return s.n.Reply(msg, resp)
}
