package tailgater

import "time"

type DatabaseConfig struct {
	DbHost     string
	DbDatabase string
	DbUser     string
	DbPassword string
	DbPort     string
}

type TailMessage struct {
	ID            uint64
	Message       []byte
	Exchange      string
	RouterKey     string
	CorrelationID string
	ReplyTo       string
	CreatedAt     time.Time
	Sent          bool
}
