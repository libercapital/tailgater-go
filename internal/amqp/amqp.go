package amqp

import (
	"context"

	"github.com/libercapital/liber-logger-go/tracing"
	"github.com/libercapital/rabbitmq-client-go/v3"
	tg_model "github.com/libercapital/tailgater-go/models"
	"github.com/rs/zerolog/log"
)

type AMQPService interface {
	Publish(ctx context.Context, exchange string, routerKey string, corrID string, payload string) error
	Close()
}

type amqpServiceImpl struct {
	client    rabbitmq.Client
	publisher rabbitmq.Publisher
}

func NewClient(config tg_model.AmqpConfig) AMQPService {
	credential := rabbitmq.Credential{
		Host:     config.Host,
		User:     config.User,
		Password: config.Password,
		Protocol: config.Protocol,
		Vhost:    &config.VHost,
	}

	client, err := rabbitmq.New(credential, rabbitmq.ClientOptions{ReconnectionDelay: 5})
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

	message := rabbitmq.PublishingMessage{
		Body: []byte(payload),
	}

	if corrID != "" {
		message.CorrelationId = corrID
	}

	err := amqp.publisher.SendMessage(ctx, exchange, routerKey, false, false, message, tracing.SpanConfig{})
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

func (amqp amqpServiceImpl) Close() {
	err := amqp.client.Close()
	if err != nil {
		log.Err(err).Stack().Msg("failed to close amqp connection at tailgater")
	}
}
