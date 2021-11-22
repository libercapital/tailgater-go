package database

import (
	"fmt"
	"strconv"

	"github.com/jackc/pgx"
	"github.com/rs/zerolog/log"
	tg_models "gitlab.com/bavatech/architecture/software/libs/go-modules/tailgater.git/models"
)

const ERRCODE_DUPLICATE_OBJECT string = "42710"

func Connect(config tg_models.DatabaseConfig) (pgx.ReplicationConn, error) {
	port, err := strconv.ParseUint(config.DbPort, 10, 16)
	if err != nil {
		return pgx.ReplicationConn{}, err
	}
	dbConfig := pgx.ConnConfig{Database: config.DbDatabase, User: config.DbUser, Password: config.DbPassword, Host: config.DbHost, Port: uint16(port)}
	conn, err := pgx.ReplicationConnect(dbConfig)
	if err != nil {
		return pgx.ReplicationConn{}, err
	}

	if err := conn.CreateReplicationSlot("outbox_subscription", "pgoutput"); err != nil {
		fmt.Printf("CODE MESSAGE %v", err.(pgx.PgError).Code)
		if err.(pgx.PgError).Code == ERRCODE_DUPLICATE_OBJECT {
			log.Warn().Stack().Err(err).Msg("failed to create replication slot")
		} else {
			return pgx.ReplicationConn{}, err
		}
	}

	if _, err := conn.Exec("CREATE PUBLICATION outbox_publication FOR TABLE outbox;"); err != nil {
		if err.(pgx.PgError).Code == ERRCODE_DUPLICATE_OBJECT {
			log.Warn().Stack().Err(err).Msg("failed to create publication")
		} else {
			return pgx.ReplicationConn{}, err
		}
	}

	return *conn, nil

}
