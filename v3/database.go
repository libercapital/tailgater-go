package tailgater

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	liberlogger "github.com/libercapital/liber-logger-go"
)

const ()

type DatabaseService interface {
	Connect() error
	Subscribe(ctx context.Context, channel string) (<-chan *pgconn.Notification, error)
	SetOutboxMessageAsSent(id int64)
	HandleNotSentMessages(daysBefore int)
}

type databaseServiceImpl struct {
	config        DatabaseConfig
	dbConn        *pgx.Conn
	hasSentColumn bool
	outbox        Tailgater
}

func (db databaseServiceImpl) Subscribe(ctx context.Context, channel string) (<-chan *pgconn.Notification, error) {
	notificationChannel := make(chan *pgconn.Notification)

	_, err := db.dbConn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))

	if err != nil {
		liberlogger.Error(ctx, err).Msg("failed to listen to channel")
		return nil, err
	}

	go func() {
		for {
			notification, err := db.dbConn.WaitForNotification(ctx)

			if err != nil {
				liberlogger.Error(ctx, err).Msg("failed to wait for notification")
				continue
			}

			if notification != nil {
				notificationChannel <- notification
			}
		}
	}()

	return notificationChannel, nil
}

func NewDatabaseService(dbConfig DatabaseConfig, outbox Tailgater) DatabaseService {
	return &databaseServiceImpl{
		config: dbConfig,
		outbox: outbox,
	}
}

func (db databaseServiceImpl) HandleNotSentMessages(daysBefore int) {
	ctx := context.Background()

	query := fmt.Sprintf(`
	select id, message, exchange, v_host, router_key, correlation_id, reply_to, created_at, sent from %s where sent = false
	and created_at <= (current_timestamp - 5 * interval '1 second')
	and created_at >= (current_timestamp - interval '%d day')`, outboxTableName, daysBefore)

	rows, err := db.dbConn.Query(ctx, query)

	if err != nil {
		liberlogger.Error(ctx, err).Msg("failed to query for not sent messages")
		return
	}

	for rows.Next() {
		var id uint64
		var message map[string]any
		var exchange, vHost, routerKey, correlationID, replyTo string
		var createdAt time.Time
		var sent bool

		if err := rows.Scan(&id, &message, &exchange, &vHost, &routerKey, &correlationID, &replyTo, &createdAt, &sent); err != nil {
			liberlogger.Error(ctx, err).Msg("failed to scan not sent message")
			continue
		}

		tailMessage := TailMessage{
			ID:            id,
			CorrelationID: correlationID,
			RouterKey:     routerKey,
			VHost:         vHost,
			Exchange:      exchange,
			Sent:          sent,
			ReplyTo:       replyTo,
			CreatedAt:     createdAt,
			Message:       message,
		}

		err = db.outbox.Tail(ctx, tailMessage)

		if err != nil {
			liberlogger.Error(ctx, err).Msg("error to tail not sent message")
			continue
		}

		db.SetOutboxMessageAsSent(int64(id))
	}
}

func (db *databaseServiceImpl) Connect() error {
	ctx := context.Background()

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", db.config.DbUser, db.config.DbPassword, db.config.DbHost, db.config.DbPort, db.config.DbDatabase)

	dbConn, err := pgx.Connect(ctx, connString)

	if err != nil {
		return err
	}

	db.dbConn = dbConn

	db.verifySentColumnExists()

	return nil
}

func (db *databaseServiceImpl) SetOutboxMessageAsSent(id int64) {
	ctx := context.Background()
	query := fmt.Sprintf("update %s set sent = true where id=$1", outboxTableName)

	if db.hasSentColumn {
		if _, err := db.dbConn.Exec(ctx, query, id); err != nil {
			liberlogger.Error(ctx, err).Interface("id", id).Msgf("failed to update outbox message with id %v", id)
		}
	}
}

func (db *databaseServiceImpl) verifySentColumnExists() {
	ctx := context.Background()
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='%s' AND column_name='sent');", outboxTableName)

	if err := db.dbConn.QueryRow(ctx, query).Scan(&db.hasSentColumn); err != nil {
		liberlogger.Error(ctx, err).Msg("failed to verify if sent column exists")
	}
}
