package tailgater

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/v2/internal/pgoutput"
)

type Tailgater interface {
	Tail(message TailMessage)
}

func startSubscription(ctx context.Context, sub *pgoutput.Subscription, dbService DatabaseService, handler pgoutput.Handler) error {
	bavalogs.Info(ctx).Msgf("%v: tailgater subscriber connected successfully", sub.Name)

	err := sub.Start(ctx, dbService.GetReplicationConnection(), handler)
	if err != nil {
		if err.Error() == "EOF" {
			if err := dbService.Connect(); err != nil {
				bavalogs.Error(ctx, err).Msgf("error in reconecct with database")
				return err
			}
			return startSubscription(ctx, sub, dbService, handler)
		}
	}
	return err
}

func StartFollowing(dbConfig DatabaseConfig, outbox Tailgater) error {
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())

	databaseService := NewDatabaseService(dbConfig)

	if err := databaseService.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database with error: %w", err)
	}

	databaseService.DropInactiveReplicationSlots()

	set := pgoutput.NewRelationSet()

	publishCaller := publish(ctx, set, outbox, databaseService)

	handlerCaller := handler(set, publishCaller)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		dropTicker := time.NewTicker(30 * time.Minute)

		for range ticker.C {
			databaseService.InsertHeartbeat()
		}

		for range dropTicker.C {
			databaseService.DropInactiveReplicationSlots()
		}
	}()

	subscriberName := namesgenerator.GetRandomName(1)

	sub := pgoutput.NewSubscription(subscriberName, outboxPublication)

	startSubscription(ctx, sub, databaseService, handlerCaller)

	return nil
}
