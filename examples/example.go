package main

import (
	"log"
	"time"

	cs "github.com/TerrexTech/go-cassandrautils/cassandra"
	"github.com/gocql/gocql"
)

var tableDef = map[string]cs.TableColumn{
	"data": cs.TableColumn{
		Name:     "data",
		DataType: "text",
	},
	"action": cs.TableColumn{
		Name:     "action",
		DataType: "text",
	},
	"uuid": cs.TableColumn{
		Name:            "uuid",
		DataType:        "uuid",
		PrimaryKeyIndex: "2",
	},
	"timestamp": cs.TableColumn{
		Name:            "timestamp",
		DataType:        "timestamp",
		PrimaryKeyIndex: "1",
		PrimaryKeyOrder: "DESC",
	},
	"userID": cs.TableColumn{
		Name:     "user_id",
		DataType: "int",
	},
	"yearBucket": cs.TableColumn{
		Name:            "year_bucket",
		DataType:        "smallint",
		PrimaryKeyIndex: "0",
	},
}

func main() {
	log.Println("Started Program")
	keyspaceName := "test"

	// ====================> Set Session Configuration
	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.ConnectTimeout = time.Millisecond * 1000
	cluster.Timeout = time.Millisecond * 1000
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
	}

	// You can use the same session throughout the application
	session, err := cs.GetSession(cluster)
	if err != nil {
		log.Fatalln(err)
		return
	}

	// ====================> Don't forget this!
	defer func() {
		session.Close()
		log.Println("Session Closed")
	}()

	log.Println("Session Created")

	// ====================> Create Keyspace
	keyspaceConfig := cs.KeyspaceConfig{
		Name:                keyspaceName,
		ReplicationStrategy: "NetworkTopologyStrategy",
		ReplicationStrategyArgs: map[string]int{
			"datacenter1": 1,
		},
	}

	keyspace, err := cs.NewKeyspace(session, keyspaceConfig)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Created Keyspace")

	tc := &cs.TableConfig{
		Keyspace: keyspace,
		Name:     "test_table",
	}

	// ====================> Create Table
	t, err := cs.NewTable(session, tc, &tableDef)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Created Table")

	uuid, _ := gocql.RandomUUID()
	dataTimestamp := time.Now()
	// ====================> Insert some Data
	type dataStruct struct {
		Action     string
		Data       string
		Timestamp  time.Time
		UserID     int
		UUID       gocql.UUID
		YearBucket uint16
	}

	d := &dataStruct{
		Action:     "asd",
		Data:       "sdfdf",
		Timestamp:  dataTimestamp,
		UserID:     1,
		UUID:       uuid,
		YearBucket: 2018,
	}
	err = <-t.AsyncInsert(d)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Inserted Data")

	// ====================> Get the data
	yearBucketCol, _ := t.Column("yearBucket")
	timestampCol, _ := t.Column("timestamp")

	colValues := []cs.ColumnComparator{
		cs.ColumnComparator{
			Name:  yearBucketCol,
			Value: 2018,
		}.Eq(),
		cs.ColumnComparator{
			Name:  timestampCol,
			Value: dataTimestamp,
		}.Eq(),
	}

	bind := []dataStruct{}
	sp := cs.SelectParams{
		ColumnValues:  colValues,
		PageSize:      10,
		SelectColumns: []string{yearBucketCol, timestampCol},
		ResultsBind:   &bind,
	}
	fetched, err := t.Select(sp)

	if err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Printing Fetched Data:")
		log.Println(fetched)
	}
	log.Println("Fetched Data")

	if bind[0].Timestamp.Unix() == dataTimestamp.Unix() {
		log.Println("Fetched-data matches with inserted data!")
	} else {
		// This should not be happening ;_;
		log.Panicln("Error: Fetched-data DOES NOT match with inserted data!")
	}
}
