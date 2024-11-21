package tailgater

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	liberlogger "github.com/libercapital/liber-logger-go"
)

type Tailgater interface {
	Tail(ctx context.Context, message TailMessage) error
}

func StartFollowing(dbConfig DatabaseConfig, outbox Tailgater, updateInterval time.Duration, daysBefore int) error {
	ctx := context.Background()

	databaseService := NewDatabaseService(dbConfig, outbox)

	err := databaseService.Connect()

	if err != nil {
		return fmt.Errorf("failed to connect to database with error: %w", err)
	}

	go func() {
		ticker := time.NewTicker(updateInterval)

		for range ticker.C {
			databaseService.HandleNotSentMessages(daysBefore)
		}
	}()

	notificationChannel, err := databaseService.Subscribe(ctx, dbConfig.OutboxChannel)

	if err != nil {
		return fmt.Errorf("failed to subscribe to channel with error: %w", err)
	}

	go func() {
		for notification := range notificationChannel {
			var payload TailMessage

			err := json.Unmarshal([]byte(notification.Payload), &payload)

			if err != nil {
				liberlogger.Error(ctx, err).Msg("failed to unmarshal notification payload")
				continue
			}

			err = outbox.Tail(ctx, payload)

			if err != nil {
				liberlogger.Error(ctx, err).Msg("failed to tail message")
				continue
			}
		}

	}()

	return nil
}
