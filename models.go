package tailgater

import "time"

type TailgaterConfig struct {
	Host           string
	Name           string
	User           string
	Password       string
	SSLMode        string
	Port           string
	UpdateInterval time.Duration
	DaysBefore     int
}

type TailMessage struct {
	ID            uint64         `json:"id"`
	Message       map[string]any `json:"message"`
	VHost         string         `json:"v_host"`
	Exchange      string         `json:"exchange"`
	RouterKey     string         `json:"router_key"`
	CorrelationID string         `json:"correlation_id"`
	ReplyTo       string         `json:"reply_to"`
	CreatedAt     time.Time      `json:"created_at"`
	Sent          bool           `json:"sent"`
}
