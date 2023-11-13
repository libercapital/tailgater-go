package tailgater

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"gitlab.com/bavatech/architecture/software/libs/go-modules/bavalogs.git"
)

const (
	errCodeDuplicateSlot = "42710"
	outboxPublication    = "outbox_publication"
	outboxTableName      = "outbox"
	heartBeatTableName   = "_heartbeat"
)

type DatabaseService interface {
	Connect() error
	InsertHeartbeat()
	DropInactiveReplicationSlots()
	GetReplicationConnection() *pgx.ReplicationConn
	SetOutboxMessageAsSent(id int64)
	HandleNotSentMessages(daysBefore int)
}

type databaseServiceImpl struct {
	config        DatabaseConfig
	repConn       *pgx.ReplicationConn
	dbConn        *pgxpool.Pool
	hasSentColumn bool
	outbox        Tailgater
}

func NewDatabaseService(dbConfig DatabaseConfig, outbox Tailgater) DatabaseService {
	return &databaseServiceImpl{
		config: dbConfig,
		outbox: outbox,
	}
}

func createHeartbeatTable(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.TODO(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS	%s (id bigint NOT NULL, last_heartbeat timestamptz NOT NULL);", heartBeatTableName))
	return err
}

func (db databaseServiceImpl) HandleNotSentMessages(daysBefore int) {
	ctx := context.Background()

	query := fmt.Sprintf(`
	select id, message, exchange, v_host, router_key, correlation_id, reply_to, created_at, sent from %s where sent = false
	and created_at <= (current_timestamp - 5 * interval '1 second') 
	and created_at >= (current_timestamp - interval '%d day')`, outboxTableName, daysBefore)

	rows, err := db.dbConn.Query(ctx, query)

	if err != nil {
		bavalogs.Error(ctx, err).Msg("failed to query for not sent messages")
		return
	}

	for rows.Next() {
		var id uint64
		var message map[string]any
		var exchange, vHost, routerKey, correlationID, replyTo string
		var createdAt time.Time
		var sent bool

		if err := rows.Scan(&id, &message, &exchange, &vHost, &routerKey, &correlationID, &replyTo, &createdAt, &sent); err != nil {
			bavalogs.Error(ctx, err).Msg("failed to scan not sent message")
			continue
		}

		messageBytes, err := json.Marshal(message)

		if err != nil {
			bavalogs.Error(ctx, err).Msg("failed to marshal not sent message")
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
			Message:       messageBytes,
		}

		err = db.outbox.Tail(tailMessage)

		if err != nil {
			bavalogs.Error(ctx, err).Msg("error to tail not sent message")
			continue
		}

		db.SetOutboxMessageAsSent(int64(id))
	}
}

func (db databaseServiceImpl) GetReplicationConnection() *pgx.ReplicationConn {
	return db.repConn
}

func (db *databaseServiceImpl) InsertHeartbeat() {
	ctx := context.Background()
	query := fmt.Sprintf(
		`
			WITH upsert AS (UPDATE %[1]s SET last_heartbeat=current_timestamp WHERE id=1 RETURNING *)
			INSERT INTO %[1]s (id, last_heartbeat) SELECT 1, current_timestamp WHERE NOT EXISTS (SELECT * FROM upsert);
		`,
		heartBeatTableName,
	)

	if _, err := db.dbConn.Exec(ctx, query); err != nil {
		bavalogs.Error(ctx, err).Msg("failed to insert heartbeat row")
	}
}

func (db *databaseServiceImpl) DropInactiveReplicationSlots() {
	ctx := context.Background()

	if _, err := db.dbConn.Exec(context.TODO(), `select pg_drop_replication_slot(slot_name) from pg_replication_slots where active = 'f'`); err != nil {
		bavalogs.Error(ctx, err).Msg("failed to query for inative replication slots")
	}
}

func (db *databaseServiceImpl) Connect() error {
	ctx := context.Background()

	port, err := strconv.ParseUint(db.config.DbPort, 10, 16)
	if err != nil {
		return err
	}

	repConfig := pgx.ConnConfig{Database: db.config.DbDatabase, User: db.config.DbUser, Password: db.config.DbPassword, Host: db.config.DbHost, Port: uint16(port)}

	repConn, err := pgx.ReplicationConnect(repConfig)
	if err != nil {
		return err
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", db.config.DbUser, db.config.DbPassword, db.config.DbHost, db.config.DbPort, db.config.DbDatabase)

	dbConn, err := pgxpool.Connect(ctx, connString)

	if err != nil {
		return err
	}

	if _, err := repConn.Exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", outboxPublication, outboxTableName)); err != nil {
		if err.(pgx.PgError).Code != errCodeDuplicateSlot {
			return err
		}
	}

	if err := createHeartbeatTable(dbConn); err != nil {
		return err
	}

	db.dbConn = dbConn
	db.repConn = repConn

	db.verifySentColumnExists()

	return nil
}

func (db *databaseServiceImpl) SetOutboxMessageAsSent(id int64) {
	ctx := context.Background()
	query := fmt.Sprintf("update %s set sent = true where id=$1", outboxTableName)

	if db.hasSentColumn {
		if _, err := db.dbConn.Exec(ctx, query, id); err != nil {
			bavalogs.Error(ctx, err).Interface("id", id).Msgf("failed to update outbox message with id %v", id)
		}
	}
}

func (db *databaseServiceImpl) verifySentColumnExists() {
	ctx := context.Background()
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='%s' AND column_name='sent');", outboxTableName)

	if err := db.dbConn.QueryRow(ctx, query).Scan(&db.hasSentColumn); err != nil {
		bavalogs.Error(ctx, err).Msg("failed to verify if sent column exists")
	}
}
