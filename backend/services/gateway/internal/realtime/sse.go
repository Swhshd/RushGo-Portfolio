package realtime

import (
	"encoding/json"
	"fmt"
)

func ReadyFrame(eventID string) []byte {
	data, _ := json.Marshal(struct {
		EventID string `json:"event_id"`
	}{EventID: eventID})
	return []byte(fmt.Sprintf("event: ready\ndata: %s\n\n", data))
}

func SeatUpdateFrame(messageID string, payload []byte) []byte {
	return []byte(fmt.Sprintf("event: seat-update\nid: %s\ndata: %s\n\n", messageID, payload))
}

const HeartbeatFrame = ": keep-alive\n\n"
