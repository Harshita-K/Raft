package kvstore

import (
	"bytes"
	"encoding/gob"
)

type OpType uint8

const (
	OpPut OpType = iota
	OpDelete
)

// Command is what gets serialized into a raft log entry for every write.
type Command struct {
	Op    OpType
	Key   string
	Value string
}

func encodeCommand(cmd Command) []byte {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(cmd); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&cmd)
	return cmd, err
}
