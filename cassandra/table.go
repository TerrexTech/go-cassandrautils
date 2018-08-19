package cassandra

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/scylladb/gocqlx/qb"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// TableConfig defines configuration for Table.
type TableConfig struct {
	Keyspace string
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
	keyspace            string
	name                string
	// This facilitates mocking by allowing overwriting these
	initIterx  func(q driver.QueryI) driver.IterxI
	initQueryx func(q driver.QueryI, names []string) driver.QueryxI
	schema     *map[string]string
	session    driver.SessionI
}

// NewTable creates a new table in database (if a table doesn't exist).
// This always returns table-structures as per provided definition,
// even if the table already existed in database (in which case
// the provided-definition is still not cross-checked/synced
// with the actual table from database).
func NewTable(
	session driver.SessionI,
	tc *TableConfig,
	definition *map[string]TableColumn,
) (*Table, error) {
	if definition == nil || len(*definition) == 0 {
		return nil, errors.New("Table Definition not set")
	}
	if tc.Name == "" {
		return nil, errors.New("Table name is required")
	}

	var name string
	if tc.Keyspace != "" {
		name = tc.Keyspace + "."
	}
	name += tc.Name

	t := &Table{
		definition: definition,
		keyspace:   tc.Keyspace,
		name:       tc.Name,
		initQueryx: driver.NewQueryx,
		initIterx:  driver.NewIterx,
		session:    session,
	}

	var err error
	tableColumns := ""
	clusteringOrder := ""
	schema, err := t.schemaFromDefinition(definition)
	if err != nil {
		return nil, err
	}
	for key, value := range *schema {
		if key != "WITH CLUSTERING ORDER BY" {
			tableColumns += fmt.Sprintf("%s %s, ", key, value)
		} else {
			clusteringOrder += fmt.Sprintf("%s %s", key, value)
		}
	}
	tableColumns = strings.TrimSuffix(tableColumns, ", ")

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (%s) %s`,
		name,
		tableColumns,
		clusteringOrder,
	)

	err = session.Query(query).Exec()
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Definition is the table detailed-structure, used to create the
// table and form its schema.
func (t *Table) Definition() *map[string]TableColumn {
	return t.definition
}

// schemaFromDefinition creates table-schema (can be used for creating table)
// from provided definition. The resulting map has column-names as keys and
// data-types as values, and also includes "PRIMARY KEY" and "CLUSTERING ORDER"
// keys/values.
func (t *Table) schemaFromDefinition(
	tableDefinition *map[string]TableColumn,
) (*map[string]string, error) {
	schema := make(map[string]string)
	// Sample layout:
	// {
	//   0: {
	//	   column: primaryKeyColumnName,
	//     order: DESC
	//   }
	// }
	primaryKeyDefinition := make(map[int]map[string]string)

	for _, value := range *tableDefinition {
		columnName := value.Name
		schema[columnName] = value.DataType
		// Convert string key-index to integer, and build primary-key-schema
		primaryKeyIndexStr := value.PrimaryKeyIndex
		primaryKeyOrder := value.PrimaryKeyOrder
		var err error

		if primaryKeyOrder != "" && primaryKeyIndexStr == "" {
			return nil, errors.New(
				"PrimaryKeyOrder cannot be specified without specifying PrimaryKeyIndex",
			)
		}

		if primaryKeyIndexStr != "" {
			primaryKeyIndex, _ := strconv.Atoi(primaryKeyIndexStr)
			primaryKeySchema, schemaErr := t.buildPrimaryKeySchema(
				columnName,
				primaryKeyIndexStr,
				primaryKeyOrder,
			)

			if schemaErr == nil {
				primaryKeyDefinition[primaryKeyIndex] = *primaryKeySchema
			}
			err = schemaErr
		}
		if err != nil {
			return nil, err
		}
	}

	primaryKeyStr, clusteringKeyOrderStr := t.primaryKeySchemaToQueryString(&primaryKeyDefinition)
	schema["PRIMARY KEY"] = fmt.Sprintf("(%s)", primaryKeyStr)
	schema["WITH CLUSTERING ORDER BY"] = fmt.Sprintf("(%s)", clusteringKeyOrderStr)

	return &schema, nil
}

// buildPrimaryKeySchema returns map with Primary-Key schema using
// the provided values. The primaryKeyIndexStr must have a valid value
// (an integer in string format, example: "1"), else a blank map is returned.
// Sample layout of the map returned:
// {
//   column: primaryKeyColumnName,
//   order: DESC
// }
func (t *Table) buildPrimaryKeySchema(
	columnName string,
	primaryKeyIndexStr string,
	primaryKeyOrder string,
) (*map[string]string, error) {
	if primaryKeyIndexStr == "" {
		return nil, errors.New(
			"PrimaryKeyIndex is required, but none was specified",
		)
	}
	if primaryKeyOrder != "" && primaryKeyIndexStr == "0" {
		return nil, errors.New(
			"PrimaryKeyOrder cannot be specified if PrimaryKeyIndex is 0",
		)
	}

	primaryKey := make(map[string]string)
	primaryKeyIndex, err := strconv.Atoi(primaryKeyIndexStr)
	if err != nil {
		return nil, err
	}
	primaryKey["column"] = columnName

	// Partitioning-key has no order
	if primaryKeyIndex > 0 {
		switch strings.ToUpper(primaryKeyOrder) {
		case "DESC":
			primaryKey["order"] = "DESC"
		case "ASC":
			primaryKey["order"] = "ASC"
		case "":
			primaryKey["order"] = "ASC"
		default:
			err := fmt.Errorf(
				"Invalid PrimaryKeyOrder specified: \"%s\". Valid values are: \"DESC\" or \"ASC\"",
				primaryKeyOrder,
			)
			return nil, err
		}
	}
	return &primaryKey, nil
}

// primaryKeySchemaToQueryString transforms primary-key-schema into strings
// that can be directly used for table-declaration (or creation) in database.
// Returns two strings: primary-key and clustering-key-order respectively.
func (t *Table) primaryKeySchemaToQueryString(
	primaryKeyDefinition *map[int]map[string]string,
) (string, string) {
	primaryKeys := make([][]string, len(*primaryKeyDefinition))
	for key, value := range *primaryKeyDefinition {
		primaryKeys[key] = []string{
			value["column"],
			value["order"],
		}
	}

	primaryKeyStr := ""
	clusteringKeyOrderStr := ""
	for index, value := range primaryKeys {
		primaryKeyStr += fmt.Sprintf("%s, ", value[0])
		if index > 0 {
			clusteringKeyOrderStr += fmt.Sprintf("%s %s, ", value[0], value[1])
		}
	}

	primaryKeyStr = strings.TrimSuffix(primaryKeyStr, ", ")
	clusteringKeyOrderStr = strings.TrimSuffix(clusteringKeyOrderStr, ", ")
	return primaryKeyStr, clusteringKeyOrderStr
}

// Schema returns table-schema exactly as represented in database.
// This can be used to create the table in database.
func (t *Table) Schema() *map[string]string {
	if t.schema == nil {
		definition := t.Definition()
		// Here we can ignore the error-check because its already checked
		// when calling the #NewTable function. Reaching here means
		// the schema generation was success.
		t.schema, _ = t.schemaFromDefinition(definition)
	}
	return t.schema
}

// AsyncInsert asynchronously inserts the specified data into table.
func (t *Table) AsyncInsert(dataStruct interface{}) <-chan error {
	errChan := make(chan error)
	go func() {
		stmt, columns := qb.Insert(t.FullName()).
			Columns(*t.Columns()...).
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
	valuesMap := make(map[string]interface{})
	for _, v := range p.ColumnValues {
		cmp = append(cmp, v.cmpType)
		valuesMap[v.Name] = v.Value
	}

	sb := qb.Select(t.FullName()).
		Columns(p.SelectColumns...).
		Where(cmp...)
	if p.Limit != 0 {
		sb.Limit(p.Limit)
	}

	stmt, columns := sb.ToCql()
	q := t.Session().Query(stmt)
	q = t.initQueryx(q, columns).BindMap(valuesMap).Query()
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
func (t *Table) Keyspace() string {
	return t.keyspace
}

// Name returns the table name as specified when creating new table.
func (t *Table) Name() string {
	return t.name
}

// FullName returns the table in format keyspace.table if keyspace is set.
// Without keyspace, the function acts same as #Name function, and just
// returns table-name without "<keyspace>.".
func (t *Table) FullName() string {
	var name string
	if t.Keyspace() != "" {
		name = t.Keyspace() + "."
	}
	return name + t.Name()
}

// Session returns the database-session used to create the table instance.
func (t *Table) Session() driver.SessionI {
	return t.session
}

// Columns returns all table-columns.
func (t *Table) Columns() *[]string {
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
	return &t.columns
}

// ColumnsWithDataType returns a two-dimensional slice containing
// column-name and data-type pairs.
func (t *Table) ColumnsWithDataType() *[][]string {
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
	return &t.columnsWithDataType
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
