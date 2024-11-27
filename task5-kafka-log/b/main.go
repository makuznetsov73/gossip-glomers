package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func NewServer(n *maelstrom.Node, kv *maelstrom.KV) *Server {
	s := Server{n: n, kv: kv}
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

type MsgsSlice struct {
	Msgs [][2]any `tags:"json:msgs"`
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
	usr, _ := user.Current()
	dir := usr.HomeDir
	f, err := os.OpenFile(fmt.Sprintf("%s/maelstorm-logs/%s.log", dir, s.n.ID()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	return nil
}

func (s *Server) send(msg maelstrom.Message) error {
	var body SendMsg
	json.Unmarshal(msg.Body, &body)

	var respC int

	var msgsRead MsgsSlice
	doesExist := true
mainLoop:
	for {
		for {
			err := s.kv.ReadInto(context.Background(), body.Key, &msgsRead)
			if err != nil {
				if errV, ok := err.(*maelstrom.RPCError); ok {
					if errV.Code == maelstrom.KeyDoesNotExist {
						doesExist = false
						break
					}
				}
				continue
			}
			break
		}
		if doesExist {
			var msgsWrite MsgsSlice
			msgsWrite.Msgs = append(msgsWrite.Msgs, msgsRead.Msgs...)
			msgsWrite.Msgs = append(msgsWrite.Msgs, [2]any{len(msgsRead.Msgs), body.Msg})
			respC = len(msgsRead.Msgs)
			for {
				err := s.kv.CompareAndSwap(context.Background(), body.Key, msgsRead, msgsWrite, false)
				if err != nil {
					if errV, ok := err.(*maelstrom.RPCError); ok {
						if errV.Code == maelstrom.PreconditionFailed {
							continue mainLoop
						}
					}
					continue
				}
				break mainLoop
			}
		} else {
			var msgsWrite MsgsSlice
			msgsWrite.Msgs = [][2]any{{0, body.Msg}}
			respC = 0
			for {
				err := s.kv.CompareAndSwap(context.Background(), body.Key, nil, msgsWrite, true)
				if err != nil {
					if errV, ok := err.(*maelstrom.RPCError); ok {
						if errV.Code == maelstrom.PreconditionFailed || errV.Code == maelstrom.KeyDoesNotExist {
							continue mainLoop
						}
					}
					continue
				}
				break mainLoop
			}
		}
	}

	resp := map[string]any{"type": "send_ok"}
	resp["offset"] = respC
	return s.n.Reply(msg, resp)
}

func (s *Server) poll(msg maelstrom.Message) error {
	var body PollMsg
	json.Unmarshal(msg.Body, &body)

	resp := map[string]any{"type": "poll_ok"}

	msgs := make(map[string][][2]any)

mainLoop:
	for k, ofs := range body.Offsets {
		var msgsRead, msgsResp MsgsSlice
		for {
			err := s.kv.ReadInto(context.Background(), k, &msgsRead)
			if err != nil {
				if errV, ok := err.(*maelstrom.RPCError); ok {
					if errV.Code == maelstrom.KeyDoesNotExist {
						continue mainLoop
					}
				}
				continue
			}
			break
		}
		log.Info("read msgs ", msgsRead.Msgs, ofs)
		for i := range msgsRead.Msgs {
			if int(msgsRead.Msgs[i][0].(float64)) >= ofs {
				msgsResp.Msgs = msgsRead.Msgs[i:]
				break
			}
		}

		log.Info("read resp msgs ", msgsResp.Msgs, ofs)
		if len(msgsResp.Msgs) != 0 {
			msgs[k] = msgsResp.Msgs
		}
	}
	resp["msgs"] = msgs
	return s.n.Reply(msg, resp)
}

func (s *Server) commitOffsets(msg maelstrom.Message) error {
	var body PollMsg
	json.Unmarshal(msg.Body, &body)

	for k, v := range body.Offsets {
		err := s.kv.Write(context.Background(), k+"_commit", v)
		for err != nil {
			err = s.kv.Write(context.Background(), k+"_commit", v)
		}
	}
	resp := map[string]any{"type": "commit_offsets_ok"}
	return s.n.Reply(msg, resp)
}

func (s *Server) listCommitedOffsets(msg maelstrom.Message) error {
	var body ListOffsets
	json.Unmarshal(msg.Body, &body)

	offsets := make(map[string]int)
	for _, k := range body.Keys {
		for {
			v, err := s.kv.ReadInt(context.Background(), k+"_commit")
			if err != nil {
				if errV, ok := err.(*maelstrom.RPCError); ok {
					if errV.Code == maelstrom.KeyDoesNotExist {
						break
					}
				}
				continue
			}
			offsets[k] = v
			break
		}
	}

	resp := map[string]any{"type": "list_committed_offsets_ok"}
	resp["offsets"] = offsets
	return s.n.Reply(msg, resp)
}
