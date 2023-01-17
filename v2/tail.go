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

func StartFollowing(dbConfig DatabaseConfig, outbox Tailgater) error {
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())

	databaseService := NewDatabaseService(dbConfig)

	if err := databaseService.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database with error: %w", err)
	}

	databaseService.DropInactiveReplicationSlots()

	repConn := databaseService.GetReplicationConnection()

	set := pgoutput.NewRelationSet()

	publishCaller := publish(ctx, set, outbox, databaseService)

	handlerCaller := handler(set, publishCaller)

	go func() {
		ticker := time.NewTicker(10 * time.Minute)
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

	bavalogs.Info(ctx).Msgf("%v: tailgater subscriber connected successfully", subscriberName)

	if err := sub.Start(ctx, repConn, handlerCaller); err != nil {
		return fmt.Errorf("error handling tail message: %w", err)
	}

	return nil
}
