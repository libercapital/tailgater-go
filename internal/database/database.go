package database

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/zerolog/log"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

const ERRCODE_DUPLICATE_OBJECT string = "42710"

type DatabaseService interface {
	Connect() error
	InsertHeartbeat()
	DropInactiveReplicationSlots()
	GetReplicationConnection() *pgx.ReplicationConn
	SetOutboxMessageAsSent(id int64)
}

type databaseServiceImpl struct {
	config        tg_models.DatabaseConfig
	repConn       *pgx.ReplicationConn
	dbConn        *pgxpool.Pool
	hasSentColumn bool
}

func NewDatabaseService(dbConfig tg_models.DatabaseConfig) DatabaseService {
	return &databaseServiceImpl{
		config: dbConfig,
	}
}

func createHeartbeatTable(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.TODO(), `CREATE TABLE IF NOT EXISTS	_heartbeat (id bigint NOT NULL, last_heartbeat timestamptz NOT NULL);`)
	return err
}

func (db databaseServiceImpl) GetReplicationConnection() *pgx.ReplicationConn {
	return db.repConn
}

func (db *databaseServiceImpl) InsertHeartbeat() {
	_, err := db.dbConn.Exec(context.TODO(), `WITH upsert AS
	(UPDATE _heartbeat SET last_heartbeat=current_timestamp WHERE id=1 RETURNING *)
	INSERT INTO _heartbeat (id, last_heartbeat) SELECT 1, current_timestamp
	WHERE NOT EXISTS (SELECT * FROM upsert);`)
	if err != nil {
		log.Warn().Stack().Err(err).Msg("failed to insert heartbeat row")
	}
}

func (db *databaseServiceImpl) DropInactiveReplicationSlots() {
	type ReplicationSlot struct {
		SlotName string
	}

	rows, err := db.dbConn.Query(context.TODO(), `select slot_name from pg_replication_slots where active = 'f'`)
	if err != nil {
		log.Warn().Stack().Err(err).Msg("failed to query for inative replication slots")
	}
	defer rows.Close()

	var inactiveSlots []ReplicationSlot
	for rows.Next() {
		var r ReplicationSlot
		err := rows.Scan(&r.SlotName)
		if err != nil {
			log.Warn().Stack().Err(err).Msg("failed to scan for inative replication slots")
		}
		inactiveSlots = append(inactiveSlots, r)
	}
	if err := rows.Err(); err != nil {
		log.Warn().Stack().Err(err).Msg("failed to iterate for inative replication slots")
	}

	for _, items := range inactiveSlots {
		log.Info().Msgf("dropping inactive replication slot: %s", items.SlotName)
		_, err := db.dbConn.Exec(context.TODO(), "select pg_drop_replication_slot($1)", items.SlotName)
		if err != nil {
			log.Warn().Stack().Err(err).Msg("failed to drop inactive replication slot")
		}
	}
}

func (db *databaseServiceImpl) Connect() error {
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
	dbConn, err := pgxpool.Connect(context.TODO(), connString)
	if err != nil {
		return err
	}

	if _, err := repConn.Exec("CREATE PUBLICATION outbox_publication FOR TABLE outbox;"); err != nil {
		log.Info().Err(err)
		if err.(pgx.PgError).Code != ERRCODE_DUPLICATE_OBJECT {
			return err
		}
	}

	if err := createHeartbeatTable(dbConn); err != nil {
		log.Warn().Stack().Err(err).Msg("error to create heartbeat table")
	}

	db.dbConn = dbConn
	db.repConn = repConn

	db.verifySentColumnExists()
	return nil
}

func (db *databaseServiceImpl) SetOutboxMessageAsSent(id int64) {
	if db.hasSentColumn {
		_, err := db.dbConn.Exec(context.TODO(), `update outbox set sent = true where id=$1`, id)
		if err != nil {
			log.Err(err).Interface("id", id).Stack().Msgf(`failed to update outbox message with id %v`, id)
		}
	}
}

func (db *databaseServiceImpl) verifySentColumnExists() {
	var sentColumnExists bool
	err := db.dbConn.QueryRow(context.TODO(), `SELECT EXISTS (SELECT 1
		FROM information_schema.columns
		WHERE table_schema='public' AND table_name='outbox' AND column_name='sent');`).Scan(&sentColumnExists)
	if err != nil {
		log.Warn().Err(err).Stack().Msg("failed to verify if sent column exists")
	} else {
		db.hasSentColumn = sentColumnExists
	}
}
