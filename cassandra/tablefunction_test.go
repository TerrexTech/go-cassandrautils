package cassandra

import (
	"fmt"
	"reflect"

	"github.com/TerrexTech/go-cassandrautils/mocks"
	"github.com/TerrexTech/go-commonutils/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Table", func() {
	Context("table-name is requested", func() {
		It("should return the correct table-name", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
			}

			tableCfg := &TableConfig{
				Keyspace: "test",
				Name:     "test_table",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			Expect(t.Name()).To(Equal(tableCfg.Name))
		})
	})

	Context("full table-name is requested", func() {
		It("should return <keyspace>.<name> if keyspace is set", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
			}

			tableCfg := &TableConfig{
				Keyspace: "test",
				Name:     "test_table",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			expectedName := fmt.Sprintf("%s.%s", tableCfg.Keyspace, tableCfg.Name)
			Expect(t.FullName()).To(Equal(expectedName))
		})

		It("should return <name> if keyspace is not set", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
			}

			tableCfg := &TableConfig{
				Name: "test_table",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			Expect(t.FullName()).To(Equal(tableCfg.Name))
		})
	})

	Context("table-session is requested", func() {
		It("should return the wrapper database-session", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
			}

			tableCfg := &TableConfig{
				Keyspace: "test",
				Name:     "test_table",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			s := t.Schema()
			Expect(s).ToNot(BeNil())
		})
	})

	Context("table-keyspace is requested", func() {
		It("should return the correct table-keyspace", func() {
			definition := &map[string]TableColumn{
				"text1": TableColumn{
					Name:     "textcol1",
					DataType: "text",
				},
			}

			tableCfg := &TableConfig{
				Keyspace: "test-keyspace",
				Name:     "test_table",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			Expect(t.Keyspace()).To(Equal(tableCfg.Keyspace))
		})
	})

	Context("table-definition is requested", func() {
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

		It("should return the correct table-defintion", func() {
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)
			Expect(
				reflect.DeepEqual(t.Definition(), definition),
			).To(BeTrue())
		})
	})

	Context("table-schema is requested", func() {
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

		It("should return the correct table schema", func() {
			expectedSchema := &map[string]string{
				"textcol1":                 "text",
				"textcol2":                 "text",
				"uuid":                     "uuid",
				"timestamp":                "timestamp",
				"month_bucket":             "smallint",
				"PRIMARY KEY":              "(month_bucket, timestamp, uuid)",
				"WITH CLUSTERING ORDER BY": "(timestamp DESC, uuid ASC)",
			}
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			Expect(reflect.DeepEqual(t.Schema(), expectedSchema)).To(BeTrue())
		})
	})

	Context("table-columns are requested", func() {
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

		It("should return the correct table-columns", func() {
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			expectedColumns := []string{
				"month_bucket",
				"textcol1",
				"textcol2",
				"uuid",
				"timestamp",
			}
			Expect(
				utils.AreElementsInSlice(*t.Columns(), expectedColumns),
			).To(BeTrue())
		})
	})

	Context("table-columns with data-types are requested", func() {
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

		It("should return the correct table-columns with data-types", func() {
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			expectedColumns := [][]string{
				[]string{"uuid", "uuid"},
				[]string{"timestamp", "timestamp"},
				[]string{"month_bucket", "smallint"},
				[]string{"textcol1", "text"},
				[]string{"textcol2", "text"},
			}

			for _, v := range *t.ColumnsWithDataType() {
				isEq := false
				for _, ve := range expectedColumns {
					if v[0] == ve[0] && v[1] == ve[1] {
						isEq = true
						break
					}
				}
				Expect(isEq).To(BeTrue())
			}
		})
	})

	Context("A single column is requested", func() {
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

		It("should return error if the column doesn't exist", func() {
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)
			_, err := t.Column("invalidCol")
			Expect(err).To(HaveOccurred())
		})

		It("should return the actual column-name from common column-name if it exists", func() {
			session := &mocks.Session{}
			t, _ := NewTable(session, tableCfg, definition)

			c, err := t.Column("text2")
			Expect(c).To(Equal("textcol2"))
			Expect(err).ToNot(HaveOccurred())

			c, err = t.Column("uuid")
			Expect(c).To(Equal("uuid"))
			Expect(err).ToNot(HaveOccurred())

			c, err = t.Column("monthBucket")
			Expect(c).To(Equal("month_bucket"))
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
