package tailgater

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/libercapital/tailgater-go/v2/internal/pgoutput"
)

type Tailgater interface {
	Tail(message TailMessage) error
}

func startSubscription(ctx context.Context, sub *pgoutput.Subscription, dbService DatabaseService, handler pgoutput.Handler) error {
	liberlogger.Info(ctx).Msgf("%v: tailgater subscriber connected successfully", sub.Name)

	err := sub.Start(ctx, dbService.GetReplicationConnection(), handler)
	if err != nil {
		if err.Error() == "EOF" {
			if err := dbService.Connect(); err != nil {
				liberlogger.Error(ctx, err).Msgf("error in reconecct with database")
				return err
			}
			return startSubscription(ctx, sub, dbService, handler)
		}
	}

	liberlogger.Error(ctx, err).Msgf("%v: subscription error", sub.Name)
	return err
}

func StartFollowing(dbConfig DatabaseConfig, outbox Tailgater, updateInterval time.Duration, daysBefore int) error {
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())

	databaseService := NewDatabaseService(dbConfig, outbox)

	if err := databaseService.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database with error: %w", err)
	}

	databaseService.DropInactiveReplicationSlots()

	set := pgoutput.NewRelationSet()

	publishCaller := publish(ctx, set, outbox, databaseService)

	handlerCaller := handler(set, publishCaller)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			databaseService.InsertHeartbeat()
		}

	}()

	go func() {
		dropTicker := time.NewTicker(30 * time.Second)
		for range dropTicker.C {
			databaseService.DropInactiveReplicationSlots()
		}
	}()

	go func() {
		ticker := time.NewTicker(updateInterval)
		for range ticker.C {
			databaseService.HandleNotSentMessages(daysBefore)
		}
	}()

	subscriberName := namesgenerator.GetRandomName(1)

	sub := pgoutput.NewSubscription(subscriberName, outboxPublication)

	startSubscription(ctx, sub, databaseService, handlerCaller)

	return nil
}
