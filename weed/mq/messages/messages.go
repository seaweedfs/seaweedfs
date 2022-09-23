package messages

import "time"

type Message struct {
	Key        []byte
	Content    []byte
	Properties map[string]string
	Ts         time.Time
}
