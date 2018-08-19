package mocks

import (
	"errors"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// Queryx mocks the gocqlx Queryx
type Queryx struct {
	ColumnNames []string
	ExecError   string
	CqlQuery    driver.QueryI
	// Mock functions, called when respective implementations are executed
	MockBindMap     func(arg map[string]interface{})
	MockBindStruct  func()
	MockExecRelease func()
}

// BindMap mocks the map-binding for QueryxI.
// This binds query named parameters using Map.
func (q *Queryx) BindMap(arg map[string]interface{}) driver.QueryxI {
	if q.MockBindMap != nil {
		q.MockBindMap(arg)
	}
	return q
}

// BindStruct mocks the struct-binding for QueryxI.
// This binds query named parameters to values from arg using mapper.
// If value cannot be found an error is reported.
func (q *Queryx) BindStruct(arg interface{}) driver.QueryxI {
	if q.MockBindStruct != nil {
		q.MockBindStruct()
	}
	return q
}

// ExecRelease mocks the query execution and release for QueryxI
func (q *Queryx) ExecRelease() error {
	if q.ExecError != "" {
		return errors.New(q.ExecError)
	}
	if q.MockExecRelease != nil {
		q.MockExecRelease()
	}
	return nil
}

// Query returns the embedded gocql.Query.
func (q *Queryx) Query() driver.QueryI {
	return q.CqlQuery
}

// Statement returns the statement that was used to generate this query.
func (q *Queryx) Statement() string {
	return q.CqlQuery.Statement()
}
