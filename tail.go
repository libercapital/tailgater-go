package tailgater

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/internal/amqp"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/internal/database"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/internal/pgoutput"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

func StartFollowing(dbConfig tg_models.DatabaseConfig, amqpConfig tg_models.AmqpConfig) error {
	ctx := context.Background()
	conn, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database with error: %w", err)
	}

	amqpClient := amqp.NewClient(amqpConfig)

	set := pgoutput.NewRelationSet()

	publish := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %w", err)
		}
		payload, err := json.Marshal(values["message"].Get())
		if err != nil {
			return fmt.Errorf("error marshalling message: %w", err)
		}

		exchange := values["exchange"].Get()
		if exchange == nil {
			exchange = ""
		}

		routerKey := values["router_key"].Get()
		if routerKey == nil {
			routerKey = ""
		}

		correlationId := values["correlation_id"].Get()
		if correlationId == nil {
			correlationId = ""
		}

		err = amqpClient.Publish(ctx, exchange.(string), routerKey.(string), correlationId.(string), string(payload))
		if err != nil {
			return fmt.Errorf("error publishing message: %w", err)
		}
		return nil
	}

	handler := func(m pgoutput.Message) error {
		switch v := m.(type) {
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
			return publish(v.RelationID, v.Row)
		}
		return nil
	}

	sub := pgoutput.NewSubscription("outbox_subscription", "outbox_publication")
	log.Info().
		Msg("tailgater subscriber connected successfully")
	if err := sub.Start(ctx, &conn, handler); err != nil {
		return fmt.Errorf("error handling tail message: %w", err)
	}

	return nil
}
