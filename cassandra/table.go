package cassandra

import (
	"fmt"

	"github.com/scylladb/gocqlx/qb"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// TableConfig defines configuration for Table.
type TableConfig struct {
	Keyspace *Keyspace
	Name     string
}

// TableColumn represents column-definition for database.
type TableColumn struct {
	Name            string
	DataType        string
	PrimaryKeyIndex string
	PrimaryKeyOrder string
}

// ColumnComparator Creates comparator for select queries
type ColumnComparator struct {
	Name    string
	Value   interface{}
	cmpType qb.Cmp
}

// Comparator is a convenience function to create a new ColumnComparator
func Comparator(col string, value interface{}) ColumnComparator {
	return ColumnComparator{
		Name:  col,
		Value: value,
	}
}

// Eq creates an Equality (=) operator
func (cc ColumnComparator) Eq() ColumnComparator {
	cc.cmpType = qb.Eq(cc.Name)
	return cc
}

// Gt creates a Greater-Than (>) operator
func (cc ColumnComparator) Gt() ColumnComparator {
	cc.cmpType = qb.Gt(cc.Name)
	return cc
}

// GtOrEq creates a Greater-Than-Or-Equals-To (>=) operator
func (cc ColumnComparator) GtOrEq() ColumnComparator {
	cc.cmpType = qb.GtOrEq(cc.Name)
	return cc
}

// In creates a Value-In-Array operator. The provided value must be an array.
func (cc ColumnComparator) In() ColumnComparator {
	cc.cmpType = qb.In(cc.Name)
	return cc
}

// Lt creates a Less-Than (<) operator
func (cc ColumnComparator) Lt() ColumnComparator {
	cc.cmpType = qb.Lt(cc.Name)
	return cc
}

// LtOrEq creates a Less-Than-Or-Equals-To (<=) operator
func (cc ColumnComparator) LtOrEq() ColumnComparator {
	cc.cmpType = qb.LtOrEq(cc.Name)
	return cc
}

// SelectParams defines parameters for a SELECT query.
type SelectParams struct {
	ColumnValues []ColumnComparator
	// Add LIMIT parameter to the query
	Limit    uint
	PageSize uint
	// This should be the pointer to a struct slice representing the
	// returned data. The results are loaded into provided slice.
	ResultsBind interface{}
	// The columns to add in SELECT statement
	SelectColumns []string
}

// Table contains functions to help interact with table,
// which was created using the provided definition.
type Table struct {
	columns             []string
	columnsWithDataType [][]string
	definition          *map[string]TableColumn
	keyspace            *Keyspace
	name                string
	// This facilitates mocking by allowing overwriting these
	initIterx  func(q driver.QueryI) driver.IterxI
	initQueryx func(q driver.QueryI, names []string) driver.QueryxI
	schema     *map[string]string
	session    driver.SessionI
}

// Definition is the table detailed-structure, used to create the
// table and form its schema.
func (t *Table) Definition() *map[string]TableColumn {
	return t.definition
}

// Schema returns table-schema exactly as represented in database.
// This can be used to create the table in database.
func (t *Table) Schema() *map[string]string {
	// Schema gets generated at table-creation
	return t.schema
}

// AsyncInsert asynchronously inserts the specified data into table.
func (t *Table) AsyncInsert(dataStruct interface{}) <-chan error {
	errChan := make(chan error)
	go func() {
		stmt, columns := qb.Insert(t.FullName()).
			Columns(t.Columns()...).
			ToCql()

		q := t.Session().Query(stmt)
		err := t.initQueryx(q, columns).
			BindStruct(dataStruct).
			ExecRelease()
		errChan <- err
	}()
	return (<-chan error)(errChan)
}

// Select gets data from table. This returns a slice of struct
// containing the returned data. This slice is same as specified
// in SelectParams.ResultsBind
func (t *Table) Select(p SelectParams) (interface{}, error) {
	var cmp []qb.Cmp
	values := []interface{}{}
	for _, v := range p.ColumnValues {
		cmp = append(cmp, v.cmpType)
		values = append(values, v.Value)
	}

	sb := qb.Select(t.FullName()).
		Columns(p.SelectColumns...).
		Where(cmp...)
	if p.Limit != 0 {
		sb.Limit(p.Limit)
	}

	stmt, _ := sb.ToCql()
	q := t.Session().Query(stmt, values...)
	if p.PageSize != 0 {
		q.SetPageSize(p.PageSize)
	}

	i := t.initIterx(q)
	err := i.Select(p.ResultsBind)
	if err != nil {
		i.Close()
		return nil, err
	}
	err = i.Close()
	return p.ResultsBind, err
}

// Keyspace returns the table keyspace as specified when creating new table.
// This can only be set when #NewTable function is called.
func (t *Table) Keyspace() *Keyspace {
	return t.keyspace
}

// Name returns the table name as specified when creating new table.
func (t *Table) Name() string {
	return t.name
}

// FullName returns the table-name in keyspace.table format.
func (t *Table) FullName() string {
	return fmt.Sprintf("%s.%s", t.Keyspace().Name(), t.Name())
}

// Session returns the database-session used to create the table instance.
func (t *Table) Session() driver.SessionI {
	return t.session
}

// Columns returns all table-columns.
func (t *Table) Columns() []string {
	if t.columns == nil {
		var columns []string
		for key := range *t.Schema() {
			// We only want column-names,
			// "PRIMARY KEY" and such are not column-names.
			if key != "PRIMARY KEY" && key != "WITH CLUSTERING ORDER BY" {
				columns = append(columns, key)
			}
		}
		t.columns = columns
	}
	return t.columns
}

// ColumnsWithDataType returns a two-dimensional slice containing
// column-name and data-type pairs.
func (t *Table) ColumnsWithDataType() [][]string {
	if t.columnsWithDataType == nil {
		var columns [][]string
		for key, value := range *t.Schema() {
			// We only want column-names,
			// "PRIMARY KEY" and such are not column-names.
			if key != "PRIMARY KEY" && key != "WITH CLUSTERING ORDER BY" {
				columns = append(columns, []string{key, value})
			}
		}
		t.columnsWithDataType = columns
	}
	return t.columnsWithDataType
}

// Column returns the column-name (as used in database) from
// specified common-name of column.
// Use this for referencing columns in table.
// Returns error if no column matching the provided common-name is found.
func (t *Table) Column(columnName string) (string, error) {
	definition := *t.Definition()
	dbColumn := definition[columnName].Name
	if dbColumn == "" {
		return "", fmt.Errorf("No column matching %s was found", columnName)
	}
	return dbColumn, nil
}
