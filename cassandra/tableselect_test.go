package cassandra

import (
	"errors"
	"reflect"
	"time"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	"github.com/TerrexTech/go-cassandrautils/mocks"
	cql "github.com/gocql/gocql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {
	Context("data is requested from table", func() {
		type datastruct struct {
			Action      string
			Data        string
			Timestamp   time.Time
			UserID      int
			UUID        cql.UUID
			MonthBucket uint8
		}

		var (
			definition *map[string]TableColumn
			table      *Table
			sp         SelectParams
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

			monthBucketCol, _ := table.Column("monthBucket")
			timestampCol, _ := table.Column("timestamp")

			colValues := []ColumnComparator{
				ColumnComparator{
					Name:  monthBucketCol,
					Value: 9,
				}.Eq(),
			}

			sp = SelectParams{
				ColumnValues:  colValues,
				PageSize:      10,
				SelectColumns: []string{monthBucketCol, timestampCol},
				ResultsBind:   &[]datastruct{},
			}
		})

		It("should generate correct prepared statement", func() {
			queryx := &mocks.Queryx{}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery: q,
				}
			}
			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())
			Expect(
				queryx.Statement(),
			).To(Equal(
				"SELECT month_bucket,timestamp FROM test.test_table WHERE month_bucket=? ",
			))
		})

		It("should bind values correctly using BindMap", func() {
			isBindMapCalled := false
			mapValue := 0
			mockBindMap := func(arg map[string]interface{}) {
				isBindMapCalled = true
				mapValue = arg["month_bucket"].(int)
			}

			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
					MockBindMap: mockBindMap,
				}
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery: q,
				}
			}
			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())
			Expect(isBindMapCalled).To(BeTrue())
			Expect(mapValue).To(Equal(9))
		})

		It("should set the limit if specified", func() {
			queryx := &mocks.Queryx{}
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				queryx.CqlQuery = q
				queryx.ColumnNames = names
				return queryx
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery: q,
				}
			}

			sp.Limit = 6
			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())
			Expect(
				queryx.Statement(),
			).To(Equal(
				"SELECT month_bucket,timestamp FROM test.test_table WHERE month_bucket=? LIMIT 6 ",
			))
		})

		It("should set the page-size if specified", func() {
			var query driver.QueryI
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				query = q
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
				}
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery: q,
				}
			}
			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())
			Expect(query.GetPageSize()).To(Equal(sp.PageSize))
		})

		It("should return default page-size if not specified", func() {
			// Equivalent to un-setting page-size
			sp.PageSize = 0

			var query driver.QueryI
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				query = q
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
				}
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery: q,
				}
			}

			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())

			expectedPageSize := uint(5000)
			Expect(query.GetPageSize()).To(Equal(expectedPageSize))
		})

		It("should passes correct ResultBind to #Select function", func() {
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
				}
			}

			var resultType string
			mockSelect := func(dest interface{}) error {
				resultType = reflect.ValueOf(dest).Type().String()
				return nil
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery:   q,
					MockSelect: mockSelect,
				}
			}
			_, err := table.Select(sp)
			Expect(err).ToNot(HaveOccurred())
			Expect(resultType).To(Equal("*[]cassandra.datastruct"))
		})

		It("should return any error that occurs when selecting from iter", func() {
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
				}
			}

			mockSelect := func(dest interface{}) error {
				return errors.New("some-error")
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery:   q,
					MockSelect: mockSelect,
				}
			}
			_, err := table.Select(sp)
			Expect(err).To(HaveOccurred())
		})

		It("should return any error that occurs when closing iter", func() {
			table.initQueryx = func(q driver.QueryI, names []string) driver.QueryxI {
				return &mocks.Queryx{
					CqlQuery:    q,
					ColumnNames: names,
				}
			}

			mockClose := func() error {
				return errors.New("some-error")
			}
			table.initIterx = func(q driver.QueryI) driver.IterxI {
				return &mocks.Iterx{
					CqlQuery:  q,
					MockClose: mockClose,
				}
			}
			_, err := table.Select(sp)
			Expect(err).To(HaveOccurred())
		})
	})
})
