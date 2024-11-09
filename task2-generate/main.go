package main

import (
	"crypto/rand"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	ids map[[32]byte]struct{}
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		switch body["type"] {
		case "generate":
			body["type"] = "generate_ok"
			body["id"] = generateID()
		default:
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func generateID() []byte {
	id := make([]byte, 32)
	for {
		_, err := rand.Read(id)
		if err != nil {
			panic(err)
		}
		if _, ok := ids[[32]byte(id)]; ok {
			continue
		} else {
			break
		}
	}
	return id
}
