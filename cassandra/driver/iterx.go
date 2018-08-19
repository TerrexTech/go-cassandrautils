package driver

import (
	cqlx "github.com/scylladb/gocqlx"
)

// IterxI allows iterating over the results from SELECT query.
// This can also be used for paging the results.
type IterxI interface {
	Close() error
	Select(dest interface{}) error
}

// Iterx is the implementation for IterxI.
// This allows iterating over the results from SELECT query.
type Iterx struct {
	iterx *cqlx.Iterx
	query QueryI
}

// NewIterx returns a new Iterx Instance
func NewIterx(q QueryI) IterxI {
	return &Iterx{
		iterx: cqlx.Iter(q.DBQuery()),
		query: q,
	}
}

// Close closes the iterator and returns any errors that happened during the query or the iteration.
func (i *Iterx) Close() error {
	return i.iterx.Close()
}

// Select returns the statement that was used to generate this query.
func (i *Iterx) Select(dest interface{}) error {
	return i.iterx.Select(dest)
}
