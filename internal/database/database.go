package database

import (
	"fmt"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/rs/zerolog/log"
)

type DatabaseConfig struct {
	DbHost     string
	DbDatabase string
	DbUser     string
	DbPassword string
	DbPort     string
}

const ERRCODE_DUPLICATE_OBJECT string = "42710"

func Connect(config DatabaseConfig) (error, pgx.ReplicationConn) {
	port, err := strconv.ParseUint(config.DbPort, 10, 16)
	if err != nil {
		return err, pgx.ReplicationConn{}
	}
	dbConfig := pgx.ConnConfig{Database: config.DbDatabase, User: config.DbUser, Password: config.DbPassword, Host: config.DbHost, Port: uint16(port)}
	conn, err := pgx.ReplicationConnect(dbConfig)
	if err != nil {
		return err, pgx.ReplicationConn{}
	}

	if err := conn.CreateReplicationSlot("outbox_subscription", "pgoutput"); err != nil {
		fmt.Printf("CODE MESSAGE %v", err.(pgx.PgError).Code)
		if err.(pgx.PgError).Code == ERRCODE_DUPLICATE_OBJECT {
			log.Warn().Stack().Err(err).Msg("failed to create replication slot")
		} else {
			return err, pgx.ReplicationConn{}
		}
	}

	if _, err := conn.Exec("CREATE PUBLICATION outbox_publication FOR TABLE outbox;"); err != nil {
		if err.(pgx.PgError).Code == ERRCODE_DUPLICATE_OBJECT {
			log.Warn().Stack().Err(err).Msg("failed to create publication")
		} else {
			return err, pgx.ReplicationConn{}
		}
	}

	return nil, *conn

}
