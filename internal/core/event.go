package core

import "time"

type Event struct {
	ID        int64     `json:"ID"`
	Timestamp time.Time `json:"Timestamp"`
	Source    string    `json:"Source"`
	Type      string    `json:"Type"`
	Severity  string    `json:"Severity"`
	Message   string    `json:"Message"`
}
