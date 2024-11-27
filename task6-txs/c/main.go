package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

var (
	writeLockSuffix = "_write_lock"
)

type Server struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

type Tx struct {
	function string
	key      string
	value    any
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
	usr, _ := user.Current()
	dir := usr.HomeDir
	f, err := os.OpenFile(fmt.Sprintf("%s/maelstorm-logs/%s-task6c.log", dir, s.n.ID()), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
	return nil
}

func (s *Server) lockWriteKeys(writeKeys map[string]string) {
	remainingWriteKeys := make([]string, 0)
	for k := range writeKeys {
		remainingWriteKeys = append(remainingWriteKeys, k)
	}
	sort.Strings(remainingWriteKeys)
	for _, k := range remainingWriteKeys {
		for {
			err := s.kv.CompareAndSwap(context.Background(), k+writeLockSuffix, nil, 1, true)
			if err == nil {
				break
			}
		}
	}
}

func (s *Server) unlockWriteKeys(writeKeys map[string]string) {
	for k := range writeKeys {
		for {
			err := s.kv.CompareAndSwap(context.Background(), k+writeLockSuffix, 1, nil, false)
			if err == nil {
				break
			}
		}
	}
}

func (s *Server) txn(msg maelstrom.Message) error {
	body := make(map[string]any)
	json.Unmarshal(msg.Body, &body)
	txnRaw := body["txn"]
	txsRaw := txnRaw.([]any)
	txnResp := make([][3]any, 0, len(txsRaw))

	writeKeys := make(map[string]string, 0)
	txs := make([]Tx, 0, len(txsRaw))

	for i := range txsRaw {
		opRaw := txsRaw[i].([]any)
		tx := Tx{function: opRaw[0].(string), key: strconv.Itoa(int(opRaw[1].(float64))),
			value: opRaw[2]}
		txs = append(txs, tx)
		writeKeys[tx.key] = tx.function
	}

	log.Info(writeKeys, " start locking keys")
	s.lockWriteKeys(writeKeys)
	log.Info(writeKeys, " locked keys")

	for i := range txs {
		switch txs[i].function {
		case "r":
			if v, err := s.kv.Read(context.Background(), txs[i].key+"_key"); err == nil {
				txnResp = append(txnResp, [3]any{"r", txsRaw[i].([]any)[1], v})
			} else {
				txnResp = append(txnResp, [3]any{"r", txsRaw[i].([]any)[1], nil})
			}
		case "w":
			s.kv.Write(context.Background(), txs[i].key+"_key", txs[i].value)
			txnResp = append(txnResp, [3]any{"w", txsRaw[i].([]any)[1], txs[i].value})
		}
	}

	s.unlockWriteKeys(writeKeys)
	log.Info(writeKeys, " unlocked keys")
	resp := map[string]any{"type": "txn_ok"}
	resp["txn"] = txnResp
	return s.n.Reply(msg, resp)
}
