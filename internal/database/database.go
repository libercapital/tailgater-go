package database

import (
	"strconv"

	"github.com/jackc/pgx"
	"github.com/rs/zerolog/log"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

const ERRCODE_DUPLICATE_OBJECT string = "42710"

func CreateHeartbeatTable(conn *pgx.Conn) error {
	_, err := conn.Exec(`CREATE TABLE IF NOT EXISTS	_heartbeat (id bigint NOT NULL, last_heartbeat timestamptz NOT NULL);`)
	return err
}

func InsertHeartbeat(conn *pgx.Conn) {
	_, err := conn.Exec(`WITH upsert AS
	(UPDATE _heartbeat SET last_heartbeat=current_timestamp WHERE id=1 RETURNING *)
	INSERT INTO _heartbeat (id, last_heartbeat) SELECT 1, current_timestamp
	WHERE NOT EXISTS (SELECT * FROM upsert);`)
	if err != nil {
		log.Warn().Stack().Err(err).Msg("failed to insert heartbeat row")
	}
}

func Connect(config tg_models.DatabaseConfig) (*pgx.ReplicationConn, *pgx.Conn, error) {
	port, err := strconv.ParseUint(config.DbPort, 10, 16)
	if err != nil {
		return nil, nil, err
	}
	dbConfig := pgx.ConnConfig{Database: config.DbDatabase, User: config.DbUser, Password: config.DbPassword, Host: config.DbHost, Port: uint16(port)}
	repConn, err := pgx.ReplicationConnect(dbConfig)
	if err != nil {
		return nil, nil, err
	}

	dbConn, err := pgx.Connect(dbConfig)
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
