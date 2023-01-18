package tailgater

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/v2/internal/pgoutput"
)

func publish(ctx context.Context, set *pgoutput.RelationSet, outbox Tailgater, databaseService DatabaseService) func(relation uint32, row []pgoutput.Tuple) error {
	return func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %w", err)
		}

		id := values["id"].Get()
		if id == nil {
			id = 0
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

		replyTo := values["reply_to"].Get()
		if replyTo == nil {
			replyTo = ""
		}

		createdAt := values["created_at"].Get()

		if replyTo == nil {
			replyTo = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
		}

		var messageBytes []byte
		message := values["message"].Get()

		if message != nil {
			if messageBytes, err = json.Marshal(message); err != nil {
				return nil
			}

		}

		outbox.Tail(TailMessage{
			ID:            uint64(id.(int64)),
			Message:       messageBytes,
			Exchange:      exchange.(string),
			RouterKey:     routerKey.(string),
			CorrelationID: correlationId.(string),
			ReplyTo:       replyTo.(string),
			CreatedAt:     createdAt.(time.Time),
			Sent:          true,
		})

		databaseService.SetOutboxMessageAsSent(id.(int64))

		return nil
	}

}
