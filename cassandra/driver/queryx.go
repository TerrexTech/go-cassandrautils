package driver

import (
	cqlx "github.com/scylladb/gocqlx"
)

// QueryxI is a wrapper around gocql.Query which adds
// and gocqlx.Queryx struct binding capabilities.
type QueryxI interface {
	BindMap(map[string]interface{}) QueryxI
	BindStruct(interface{}) QueryxI
	ExecRelease() error
	Query() QueryI
	Statement() string
}

// Queryx is the implementation for QueryxI.
type Queryx struct {
	ColumnNames []string
	query       QueryI
}

// NewQueryx returns a new Queryx instance
func NewQueryx(q QueryI, names []string) QueryxI {
	return &Queryx{
		query:       q,
		ColumnNames: names,
	}
}

// BindMap binds query named parameters using Map.
func (q *Queryx) BindMap(arg map[string]interface{}) QueryxI {
	cqx := cqlx.Query(q.query.GoCqlQuery(), q.ColumnNames).
		BindMap(arg).
		Query
	q.query = &Query{
		query: cqx,
	}
	return q
}

// BindStruct binds query named parameters to values from arg using mapper.
// If value cannot be found an error is reported.
func (q *Queryx) BindStruct(arg interface{}) QueryxI {
	cqx := cqlx.Query(q.query.GoCqlQuery(), q.ColumnNames).
		BindStruct(arg).
		Query
	q.query = &Query{
		query: cqx,
	}
	return q
}

// ExecRelease executes and releases the query, a released query cannot be reused.
func (q *Queryx) ExecRelease() error {
	err := q.query.Exec()
	q.query.Release()
	return err
}

// Query returns the embedded gocql.Query.
func (q *Queryx) Query() QueryI {
	return q.query
}

// Statement returns the statement that was used to generate this query.
func (q *Queryx) Statement() string {
	return q.query.Statement()
}
