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

func CreateHeartbeatTable(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.TODO(), `CREATE TABLE IF NOT EXISTS	_heartbeat (id bigint NOT NULL, last_heartbeat timestamptz NOT NULL);`)
	return err
}

func InsertHeartbeat(conn *pgxpool.Pool) {
	_, err := conn.Exec(context.TODO(), `WITH upsert AS
	(UPDATE _heartbeat SET last_heartbeat=current_timestamp WHERE id=1 RETURNING *)
	INSERT INTO _heartbeat (id, last_heartbeat) SELECT 1, current_timestamp
	WHERE NOT EXISTS (SELECT * FROM upsert);`)
	if err != nil {
		log.Warn().Stack().Err(err).Msg("failed to insert heartbeat row")
	}
}

func DropInactiveReplicationSlots(conn *pgxpool.Pool, repConn *pgx.ReplicationConn) {
	type ReplicationSlot struct {
		SlotName string
	}

	rows, err := conn.Query(context.TODO(), `select slot_name from pg_replication_slots where active = 'f'`)
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
		_, err := conn.Exec(context.TODO(), "select pg_drop_replication_slot($1)", items.SlotName)
		if err != nil {
			log.Warn().Stack().Err(err).Msg("failed to drop inactive replication slot")
		}
	}
}

func Connect(config tg_models.DatabaseConfig) (*pgx.ReplicationConn, *pgxpool.Pool, error) {
	port, err := strconv.ParseUint(config.DbPort, 10, 16)
	if err != nil {
		return nil, nil, err
	}
	repConfig := pgx.ConnConfig{Database: config.DbDatabase, User: config.DbUser, Password: config.DbPassword, Host: config.DbHost, Port: uint16(port)}
	repConn, err := pgx.ReplicationConnect(repConfig)
	if err != nil {
		return nil, nil, err
	}

	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", config.DbUser, config.DbPassword, config.DbHost, config.DbPort, config.DbDatabase)
	dbConn, err := pgxpool.Connect(context.TODO(), connString)
	if err != nil {
		return nil, nil, err
	}

	if _, err := repConn.Exec("CREATE PUBLICATION outbox_publication FOR TABLE outbox;"); err != nil {
		if err.(pgx.PgError).Code == ERRCODE_DUPLICATE_OBJECT {
			log.Warn().Stack().Err(err).Msg("failed to create publication")
		} else {
			return nil, nil, err
		}
	}

	if err := CreateHeartbeatTable(dbConn); err != nil {
		log.Warn().Stack().Err(err).Msg("error to create heartbeat table")
	}

	return repConn, dbConn, nil

}
