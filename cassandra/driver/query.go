package driver

import cql "github.com/gocql/gocql"

// QueryI is the query-handler for database-session.
type QueryI interface {
	DBQuery() *cql.Query
	Exec() error
	GetPageSize() uint
	SetPageSize(n uint) QueryI
	Release()
	Statement() string
}

// Query is the query-handler implementation for database-session.
type Query struct {
	pageSize uint
	query    *cql.Query
}

// DBQuery returns the embedded GoCQL query.
func (q *Query) DBQuery() *cql.Query {
	return q.query
}

// Exec executes the query.
func (q *Query) Exec() error {
	err := q.query.Exec()
	return err
}

// GetPageSize returns the current page-size
func (q *Query) GetPageSize() uint {
	if q.pageSize == 0 {
		// Just return the default page size for gocql
		return 5000
	}
	return q.pageSize
}

// SetPageSize tells the iterator to fetch the result in pages of size n.
// This is useful for iterating over large result sets, but setting the
// page size too low might decrease the performance. This feature is only
// available in Cassandra 2 and onwards.
func (q *Query) SetPageSize(n uint) QueryI {
	q.query.PageSize(int(n))
	return q
}

// Release releases the query. Released queries cannot be reused.
func (q *Query) Release() {
	q.query.Release()
}

// Statement returns the statement that was used to generate this query.
func (q *Query) Statement() string {
	return q.query.Statement()
}
