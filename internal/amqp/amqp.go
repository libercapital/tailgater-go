package amqp

import (
	"context"

	"github.com/rs/zerolog/log"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/client"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/rabbitmq-client.git/app/models"
)

type Config struct {
	Host     string
	User     string
	Password string
	Protocol string
	VHost    string
}

type AMQPService interface {
	Publish(ctx context.Context, exchange string, routerKey string, corrID string, payload string) error
}

type amqpServiceImpl struct {
	client    client.Client
	publisher client.Publisher
}

func NewClient(config Config) AMQPService {
	credential := models.Credential{
		Host:     config.Host,
		User:     config.User,
		Password: config.Password,
		Protocol: config.Protocol,
		Vhost:    &config.VHost,
	}

	client, err := client.New(credential, 1)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("error connecting to rabbitmq")
	}

	publisher, err := client.NewPublisher(nil, nil)
	if err != nil {
		log.Fatal().Stack().Err(err).Msg("error creating a new publisher")
	}

	return &amqpServiceImpl{
		client:    client,
		publisher: publisher,
	}
}

func (amqp amqpServiceImpl) Publish(ctx context.Context, exchange string, routerKey string, corrID string, payload string) error {

	message := models.PublishingMessage{
		Body: []byte(payload),
	}

	if corrID != "" {
		message.CorrelationId = corrID
	}

	err := amqp.publisher.SendMessage(exchange, routerKey, false, false, message)
	if err != nil {
		return err
	}

	log.Info().
		Interface("correlation_id", message.CorrelationId).
		Interface("router_key", routerKey).
		Interface("payload", payload).
		Msg("Published AMQP Message")

	return nil
}
