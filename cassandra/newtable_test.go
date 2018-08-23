package cassandra

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/TerrexTech/go-cassandrautils/mocks"
	"github.com/TerrexTech/go-commonutils/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {
	Context("new table is requested", func() {
		var (
			definition *map[string]TableColumn
			tableCfg   *TableConfig
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

			tableCfg = &TableConfig{
				Keyspace: "test",
				Name:     "test_table",
			}
		})

		It("should return error if table-definition is not provided", func() {
			_, err := NewTable(nil, tableCfg, nil)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if table-name is not set", func() {
			tc := &TableConfig{
				Keyspace: "test",
			}
			_, err := NewTable(nil, tc, nil)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if duplicate primary-key index is found", func() {
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
					PrimaryKeyIndex: "2",
					// We also test that invalid case is handled properly
					PrimaryKeyOrder: "DESC",
				},
				"monthBucket": TableColumn{
					Name:            "month_bucket",
					DataType:        "smallint",
					PrimaryKeyIndex: "0",
				},
			}

			session := &mocks.Session{}
			_, err := NewTable(session, tableCfg, definition)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if table-definition contains invalid PrimaryKeyOrder", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:            "textcol1",
					DataType:        "text",
					PrimaryKeyOrder: "invalidOrder",
					PrimaryKeyIndex: "1",
				},
			}

			session := &mocks.Session{}
			_, err := NewTable(session, tableCfg, definition)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if PrimaryKeyOrder is specified without specifying PrimaryKeyIndex", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:            "textcol1",
					DataType:        "text",
					PrimaryKeyOrder: "DESC",
				},
			}

			session := &mocks.Session{}
			_, err := NewTable(session, tableCfg, definition)
			Expect(err).To(HaveOccurred())
		})

		It("should create table by just using name if keyspace is not specified", func() {
			var outputStr string
			session := &mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
			}
			tc := &TableConfig{
				Name: "test-table",
			}
			_, err := NewTable(session, tc, definition)

			outputStr = utils.StandardizeSpaces(outputStr)
			i := strings.IndexByte(outputStr, '(')
			Expect(
				strings.TrimSuffix(outputStr[:i], " "),
			).To(
				BeEquivalentTo("CREATE TABLE IF NOT EXISTS " + tc.Name),
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should generate correct query-declaration", func() {
			var outputStr string
			session := &mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
			}
			_, err := NewTable(session, tableCfg, definition)
			Expect(err).ToNot(HaveOccurred())
			tableName := fmt.Sprintf("%s.%s", tableCfg.Keyspace, tableCfg.Name)
			outputStr = utils.StandardizeSpaces(outputStr)

			i := strings.IndexByte(outputStr, '(')
			Expect(
				strings.TrimSuffix(outputStr[:i], " "),
			).To(
				BeEquivalentTo("CREATE TABLE IF NOT EXISTS " + tableName),
			)
		})

		It("generates query with correct clustering-keys declaration", func() {
			var outputStr string
			session := &mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
			}
			_, err := NewTable(session, tableCfg, definition)
			outputStr = utils.StandardizeSpaces(outputStr)

			rgx := regexp.MustCompile(`\)[A-Z ]+\(`)
			Expect(
				rgx.FindString(outputStr),
			).To(
				BeEquivalentTo(") WITH CLUSTERING ORDER BY ("),
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("generates query with correct primary and clustering keys sequence", func() {
			var outputStr string
			session := &mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
			}
			_, err := NewTable(session, tableCfg, definition)
			outputStr = utils.StandardizeSpaces(outputStr)

			// Input:
			//   CREATE TABLE IF NOT EXISTS test-table (PRIMARY KEY (month_bucket,
			//   timestamp, uuid), textcol2 text, uuid uuid, timestamp timestamp,
			//   month_bucket smallint, textcol1 text) WITH CLUSTERING ORDER BY
			//   (timestamp DESC, uuid ASC)
			// Output (primary-keys, and clustering-keys order):
			//   - (month_bucket, timestamp, uuid)
			//   - (timestamp DESC, uuid ASC)
			rgx := regexp.MustCompile(`\([a-zA-Z0-9_ ,]+\)`)
			braceExpressions := rgx.FindAllString(outputStr, -1)

			primaryKeys := strings.TrimPrefix(braceExpressions[0], "(")
			primaryKeys = strings.TrimSuffix(primaryKeys, ")")
			expectedPrimaryKeySeq := "month_bucket, timestamp, uuid"
			Expect(primaryKeys).To(Equal(expectedPrimaryKeySeq))

			clusteringKeys := strings.TrimPrefix(braceExpressions[1], "(")
			clusteringKeys = strings.TrimSuffix(clusteringKeys, ")")
			expectedClusteringKeySeq := "timestamp DESC, uuid ASC"
			Expect(clusteringKeys).To(Equal(expectedClusteringKeySeq))
			Expect(err).ToNot(HaveOccurred())
		})

		It("generates query with correct columns and data-types sequence", func() {
			var outputStr string
			session := &mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
			}
			_, err := NewTable(session, tableCfg, definition)
			outputStr = utils.StandardizeSpaces(outputStr)

			i1 := strings.Index(outputStr, "(") + 1
			i2 := strings.Index(outputStr, ") WITH CLUSTERING")
			k := outputStr[i1:i2] + "," // "," allows for easier regex matching

			// Input:
			//   PRIMARY KEY (month_bucket, timestamp, uuid), textcol2 text,
			//   uuid uuid, timestamp timestamp, month_bucket smallint, textcol1 text,
			// Output:
			//   textcol2 text, uuid uuid, timestamp timestamp, month_bucket smallint,
			//   textcol1 text,
			rgx := regexp.MustCompile(`([a-zA-Z0-9_]+[ ][a-zA-Z]+([,]|$))`)
			tableColumns := rgx.FindAllString(k, -1)
			expectedColumns := []string{
				"textcol1 text,",
				"textcol2 text,",
				"uuid uuid,",
				"timestamp timestamp,",
				"month_bucket smallint,",
			}

			Expect(
				utils.AreElementsInSliceStrict(
					tableColumns,
					expectedColumns,
				),
			).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return table struct with required values", func() {
			session := &mocks.Session{}
			t, err := NewTable(session, tableCfg, definition)

			Expect(t).To(BeAssignableToTypeOf(&Table{}))
			Expect(t.Definition()).To(Equal(definition))
			Expect(t.Keyspace()).To(Equal(tableCfg.Keyspace))
			Expect(t.Name()).To(Equal(tableCfg.Name))
			Expect(err).ToNot(HaveOccurred())
		})

		It("should execute the generated query", func() {
			isQueryExecuted := false
			session := &mocks.Session{
				MockQueryExec: func() {
					isQueryExecuted = true
				},
			}
			_, err := NewTable(session, tableCfg, definition)
			Expect(isQueryExecuted).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return any errors occured", func() {
			session := &mocks.Session{
				MockQueryExecError: "some-query-error",
			}
			_, err := NewTable(session, tableCfg, definition)
			Expect(err).To(HaveOccurred())
		})
	})
})
