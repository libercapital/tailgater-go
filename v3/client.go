package tailgater

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	liberlogger "github.com/libercapital/liber-logger-go"
	"github.com/pkg/errors"
)

const (
	errCodeDuplicateSlot = "42710"
	outboxPublication    = "outbox_publication"
	outboxTableName      = "outbox"
	heartBeatTableName   = "_heartbeat"
)

type client struct {
	config   TailgaterConfig
	conn     *pgxpool.Pool
	replConn *pgconn.PgConn
}

type Client interface {
	Subscribe(context.Context, Tailgater) error
}

func New(config TailgaterConfig) Client {
	return &client{
		config: config,
	}
}

func (c *client) connect(ctx context.Context) error {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.config.User,
		c.config.Password,
		c.config.Host,
		c.config.Port,
		c.config.Name,
		c.config.SSLMode,
	)

	dbConn, err := pgxpool.New(ctx, connStr)

	if err != nil {
		return err
	}

	c.conn = dbConn

	replConnStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s&replication=database",
		c.config.User,
		c.config.Password,
		c.config.Host,
		c.config.Port,
		c.config.Name,
		c.config.SSLMode,
	)

	replConn, err := pgconn.Connect(ctx, replConnStr)

	if err != nil {
		return err
	}

	c.replConn = replConn

	_, err = c.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", outboxPublication, outboxTableName))

	if err != nil {
		if err.(*pgconn.PgError).Code != errCodeDuplicateSlot {
			return err
		}
	}

	return nil
}

// Subscribe implements Client.
func (c *client) Subscribe(ctx context.Context, publisher Tailgater) error {
	err := c.connect(ctx)

	if err != nil {
		return err
	}

	slotName := namesgenerator.GetRandomName(1)

	_, err = pglogrepl.CreateReplicationSlot(ctx, c.replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary: true,
	})

	if err != nil {
		return errors.Wrap(err, "failed to create replication slot")
	}

	sysIdent, err := pglogrepl.IdentifySystem(ctx, c.replConn)

	if err != nil {
		return errors.Wrap(err, "failed to identify system")
	}

	clientXLogPos := sysIdent.XLogPos

	err = pglogrepl.StartReplication(ctx, c.replConn, slotName, sysIdent.XLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", outboxPublication),
		},
	})

	if err != nil {
		return errors.Wrap(err, "failed to start replication")
	}

	liberlogger.Debug(ctx).Msgf("subscribed to replication %s on publication %s", slotName, outboxPublication)

	ticker := time.NewTicker(10 * time.Second)

	defer ticker.Stop()

	relations := map[uint32]*pglogrepl.RelationMessage{}

	for {
		select {
		case <-ticker.C:
			err := pglogrepl.SendStandbyStatusUpdate(context.Background(), c.replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
				WALFlushPosition: clientXLogPos,
				WALApplyPosition: clientXLogPos,
				ClientTime:       time.Now(),
			})

			if err != nil {
				return errors.Wrap(err, "failed to send standby status update")
			}

		default:
			rawMsg, err := c.replConn.ReceiveMessage(ctx)

			if err != nil {
				return errors.Wrap(err, "failed to receive message")
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)

			if !ok {
				liberlogger.Warn(ctx).Msgf("unknown message type %T", msg)
				continue
			}

			if msg.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])

				if err != nil {
					liberlogger.Error(ctx, errors.Wrap(err, "error to parse primary keep live message")).Send()
					continue
				}

				if pkm.ServerWALEnd > clientXLogPos {
					clientXLogPos = pkm.ServerWALEnd
				}
			}

			if msg.Data[0] == pglogrepl.XLogDataByteID {
				xLogData, err := pglogrepl.ParseXLogData(msg.Data[1:])

				if err != nil {
					liberlogger.Error(ctx, errors.Wrap(err, "error to parse x log data msg")).Send()
					continue
				}

				if xLogData.WALStart > clientXLogPos {
					clientXLogPos = xLogData.WALStart
				}

				err = c.processMessage(ctx, xLogData, publisher, relations)

				if err != nil {
					liberlogger.Error(ctx, errors.Wrap(err, "error to process message")).Send()
					continue
				}
			}

		}
	}
}

func (c *client) processMessage(ctx context.Context, xLogData pglogrepl.XLogData, publisher Tailgater, relations map[uint32]*pglogrepl.RelationMessage) error {
	logicalMsg, err := pglogrepl.Parse(xLogData.WALData)

	if err != nil {
		return errors.Wrap(err, "failed to parse logical message")
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[msg.RelationID] = msg

	case *pglogrepl.InsertMessage:
		values, err := c.extractValues(msg, relations)

		if err != nil {
			return errors.Wrap(err, "failed to extract values")
		}

		encoded, err := json.Marshal(values)

		if err != nil {
			return errors.Wrap(err, "failed to marshal values")
		}

		var message TailMessage

		err = json.Unmarshal(encoded, &message)

		if err != nil {
			return errors.Wrap(err, "failed to unmarshal values")
		}

		err = publisher.Tail(ctx, message)

		if err != nil {
			return errors.Wrap(err, "failed to publish message")
		}

		return nil
	}

	return nil
}

func (c *client) parseColumnData(data []byte, dataType uint32) (any, error) {
	value := string(data)

	switch dataType {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
		return strconv.Atoi(value)

	case pgtype.Float4OID, pgtype.Float8OID:
		return strconv.ParseFloat(value, 64)

	case pgtype.BoolOID:
		return value == "t", nil

	case pgtype.JSONOID, pgtype.JSONBOID:
		var jsonData map[string]any

		err := json.Unmarshal(data, &jsonData)

		if err != nil {
			return nil, errors.Wrap(err, "failed to parse JSON")
		}

		return jsonData, nil

	case pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID:
		return value, nil

	case pgtype.DateOID, pgtype.TimestampOID, pgtype.TimestamptzOID:
		return time.Parse("2006-01-02 15:04:05.999999Z07", value)

	default:
		return value, nil
	}
}

func (c *client) extractValues(insertMsg *pglogrepl.InsertMessage, relations map[uint32]*pglogrepl.RelationMessage) (map[string]any, error) {
	values := map[string]any{}

	relation, ok := relations[insertMsg.RelationID]

	if !ok {
		return nil, errors.Errorf("relation not found for RelationID %d", insertMsg.RelationID)
	}

	for i, col := range insertMsg.Tuple.Columns {
		columnName := relation.Columns[i].Name
		dataType := relation.Columns[i].DataType

		switch col.DataType {
		case 'n':
			values[columnName] = nil

		case 'u':
			values[columnName] = "[unchanged TOAST]"

		case 't':
			parsedValue, err := c.parseColumnData(col.Data, dataType)

			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse column %s", columnName)
			}

			values[columnName] = parsedValue

		default:
			return nil, errors.Errorf("unknown column data type: %c", col.DataType)
		}
	}

	return values, nil
}
