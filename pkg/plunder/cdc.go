package plunder

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

//const CONN = "postgres://postgres:postgres@localhost/psql-streamer?replication=database"
const SLOT_NAME = "replication_slot"
const OUTPUT_PLUGIN = "pgoutput"
const INSERT_TEMPLATE = "create table t (id int, name text);"

var Event = struct {
	Relation string
	Columns  []string
}{}

func Cdc(url string) {
	url += "&replication=database"
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	conn, err := pgconn.Connect(ctx, url)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	// 0. SET Wal level to logical
	//!!! postgre server needs to be restart after
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET wal_level = logical").ReadAll(); err != nil {
		fmt.Printf("failed to set wal level: %v", err)
	}

	// 	wal_level = logical
	// max_replication_slots = 5
	// max_wal_senders = 10

	//Setting wal_level to logical allows the WAL to record information needed for logical decoding.
	//Ensure that your max_replication_slots value is equal to or higher than the number of PostgreSQL connectors that use WAL plus the number of other replication slots your database uses.
	//Ensure that the max_wal_senders parameter, which specifies the maximum number of concurrent connections to the WAL, is at least twice the number of logical replication slots. For example, if your database uses 5 replication slots in total, the max_wal_senders value must be 10 or greater.

	// 1. Create table
	if _, err := conn.Exec(ctx, INSERT_TEMPLATE).ReadAll(); err != nil {
		fmt.Printf("failed to create table: %v", err)
	}

	// 2. ensure publication exists
	if _, err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS pub;").ReadAll(); err != nil {
		fmt.Printf("failed to drop publication: %v", err)
	}

	if _, err := conn.Exec(ctx, "CREATE PUBLICATION pub FOR ALL TABLES;").ReadAll(); err != nil {
		fmt.Printf("failed to create publication: %v", err)
	}

	// 3. create temproary replication slot server
	if _, err = pglogrepl.CreateReplicationSlot(ctx, conn, SLOT_NAME, OUTPUT_PLUGIN, pglogrepl.CreateReplicationSlotOptions{Temporary: true}); err != nil {
		fmt.Printf("failed to create a replication slot: %v", err)
	}

	var msgPointer pglogrepl.LSN
	pluginArguments := []string{"proto_version '1'", "publication_names 'pub'"}

	// 4. establish connection
	err = pglogrepl.StartReplication(ctx, conn, SLOT_NAME, msgPointer, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		fmt.Printf("failed to establish start replication: %v", err)
	}

	var pingTime time.Time
	for ctx.Err() != context.Canceled {
		if time.Now().After(pingTime) {
			if err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: msgPointer}); err != nil {
				fmt.Printf("failed to send standby update: %v", err)
			}
			pingTime = time.Now().Add(10 * time.Second)
			//fmt.Println("client: please standby")
		}

		ctx, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		msg, err := conn.ReceiveMessage(ctx)
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			fmt.Printf("something went wrong while listening for message: %v", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
			//	fmt.Println("server: confirmed standby")

			case pglogrepl.XLogDataByteID:
				walLog, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					fmt.Printf("failed to parse logical WAL log: %v", err)
				}

				var msg pglogrepl.Message
				if msg, err = pglogrepl.Parse(walLog.WALData); err != nil {
					fmt.Printf("failed to parse logical replication message: %v", err)
				}
				switch m := msg.(type) {
				case *pglogrepl.RelationMessage:
					Event.Columns = []string{}
					for _, col := range m.Columns {
						Event.Columns = append(Event.Columns, col.Name)
					}
					Event.Relation = m.RelationName
				case *pglogrepl.InsertMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("INSERT %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("%s: %s ", Event.Columns[i], string(m.Tuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
				case *pglogrepl.UpdateMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("UPDATE %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("%s: %s ", Event.Columns[i], string(m.NewTuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
				case *pglogrepl.DeleteMessage:
					var sb strings.Builder
					sb.WriteString(fmt.Sprintf("DELETE %s(", Event.Relation))
					for i := 0; i < len(Event.Columns); i++ {
						sb.WriteString(fmt.Sprintf("%s: %s ", Event.Columns[i], string(m.OldTuple.Columns[i].Data)))
					}
					sb.WriteString(")")
					fmt.Println(sb.String())
				case *pglogrepl.TruncateMessage:
					fmt.Println("ALL GONE (TRUNCATE)")
				}
			}
		default:
			fmt.Printf("received unexpected message: %T", msg)
		}
	}
}
