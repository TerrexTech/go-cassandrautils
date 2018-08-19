package mocks

import (
	"errors"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	cql "github.com/gocql/gocql"
)

// Query mocks the implementation for session-query.
type Query struct {
	// If defined, the #Exec function will throw this error.
	ExecError       string
	MockExec        func()
	MockGetPageSize func() uint
	MockSetPageSize func(size uint)
	MockRelease     func()
	pageSize        uint
	statement       string
	WrappedQuery    *cql.Query
}

// DBQuery mocks the getter for wrapped GoCQL-Query.
// The WrappedQuery member of Query must be explicitely set, else this will
// return a gocql.Query with just statement set.
func (q *Query) DBQuery() *cql.Query {
	if q.WrappedQuery != nil {
		return q.WrappedQuery
	}
	s := cql.Session{}
	return s.Query(q.statement)
}

// Exec mocks the query-execution
func (q *Query) Exec() error {
	if q.ExecError != "" {
		return errors.New(q.ExecError)
	}
	if q.MockExec != nil {
		q.MockExec()
	}
	return nil
}

// Statement returns the statement used to create query.
func (q *Query) Statement() string {
	return q.statement
}

// GetPageSize mocks fetching the current page-size.
// Being a mock, this doesn't actually store/track current page style.
// Use the function #SetPageSize along with this to completely mock page-size.
func (q *Query) GetPageSize() uint {
	if q.MockGetPageSize != nil {
		return q.MockGetPageSize()
	}
	if q.pageSize == 0 {
		return 5000
	}
	return q.pageSize
}

// SetPageSize mocks the #SetPageSize function of driver.Query.
// SetPageSize tells the iterator to fetch the result in pages of size n.
// This is useful for iterating over large result sets.
func (q *Query) SetPageSize(n uint) driver.QueryI {
	q.pageSize = n
	if q.MockSetPageSize != nil {
		q.MockSetPageSize(n)
	}
	return q
}

// Release mocks the #Release function driver.Query.
// It releases the query. Released queries cannot be reused.
func (q *Query) Release() {
	if q.MockRelease != nil {
		q.MockRelease()
	}
}
