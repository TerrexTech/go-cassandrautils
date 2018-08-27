package cassandra

import (
	"regexp"
	"strings"
	"time"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	"github.com/TerrexTech/go-cassandrautils/mocks"
	"github.com/TerrexTech/go-commonutils/utils"
	cql "github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {
	Context("data is inserted into table", func() {
		type datastruct struct {
			Action     string
			Data       string
			Timestamp  time.Time
			UserID     int
			UUID       cql.UUID
			YearBucket uint16
		}

		var (
			definition *map[string]TableColumn
			table      *Table
			data       *datastruct
		)

		BeforeEach(func() {
			definition = &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
				"text2": TableColumn{
					Name:     "textcol2",
					DataType: "text",
				},
				"uuid": TableColumn{
					Name:            "uuid",
					DataType:        "uuid",
					PrimaryKeyIndex: "2",
				},
				"timestamp": TableColumn{
					Name:            "timestamp",
					DataType:        "timestamp",
					PrimaryKeyIndex: "1",
					// We also test that invalid case is handled properly
					PrimaryKeyOrder: "DESC",
				},
				"monthBucket": TableColumn{
					Name:            "month_bucket",
					DataType:        "smallint",
					PrimaryKeyIndex: "0",
				},
			}

			keyspaceConfig := KeyspaceConfig{
				Name:                "test",
				ReplicationStrategy: "NetworkTopologyStrategy",
				ReplicationStrategyArgs: map[string]int{
					"datacenter1": 1,
				},
			}
			session := &mocks.Session{}
			keyspace, err := NewKeyspace(session, keyspaceConfig)
			Expect(err).ToNot(HaveOccurred())

			tableCfg := &TableConfig{
				Keyspace: keyspace,
				Name:     "test_table",
			}
			table, _ = NewTable(session, tableCfg, definition)

			uuid, _ := cql.RandomUUID()
			data = &datastruct{
				Action:     "asd",
				Data:       "sdfdf",
				Timestamp:  time.Now(),
				UserID:     1,
				UUID:       uuid,
				YearBucket: 2018,
			}
		})

		It("should create correct prepared statement", func() {
			queryx := &mocks.Queryx{}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			err := <-table.AsyncInsert(data)
			Expect(err).ToNot(HaveOccurred())

			stmt := queryx.Statement()
			rgx := regexp.MustCompile(
				`INSERT INTO test.test_table \(([a-z,_0-9]+)\) VALUES \(\?,\?,\?,\?,\?\)`,
			)
			Expect(
				rgx.Match([]byte(stmt)),
			).To(BeTrue())

			expectedColumns := []string{
				"uuid",
				"timestamp",
				"month_bucket",
				"textcol1",
				"textcol2",
			}
			i1 := strings.Index(stmt, "(") + 1
			i2 := strings.Index(stmt, ")")
			insertColumns := strings.Split(stmt[i1:i2], ",")
			Expect(
				utils.AreElementsInSliceStrict(insertColumns, expectedColumns),
			).To(BeTrue())
		})

		It("should pass correct columns as prepared-statement arguments", func() {
			queryx := &mocks.Queryx{}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			err := <-table.AsyncInsert(data)
			Expect(err).ToNot(HaveOccurred())

			expectedColumns := []string{
				"uuid",
				"timestamp",
				"month_bucket",
				"textcol1",
				"textcol2",
			}
			Expect(
				utils.AreElementsInSliceStrict(queryx.ColumnNames, expectedColumns),
			).To(BeTrue())
		})

		It("should bind data-struct to queryx", func() {
			isBindStructCalled := false
			queryx := &mocks.Queryx{
				MockBindStruct: func() {
					isBindStructCalled = true
				},
			}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			err := <-table.AsyncInsert(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(isBindStructCalled).To(BeTrue())
		})

		It("should execute the query", func() {
			isExecReleaseCalled := false
			queryx := &mocks.Queryx{
				MockExecRelease: func() {
					isExecReleaseCalled = true
				},
			}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			err := <-table.AsyncInsert(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(isExecReleaseCalled).To(BeTrue())
		})

		It("should return any errors that occured", func() {
			queryx := &mocks.Queryx{
				ExecError: "some-error",
			}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			err := <-table.AsyncInsert(data)
			Expect(err).To(HaveOccurred())
		})
	})
})
