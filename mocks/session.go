package mocks

import (
	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	cql "github.com/gocql/gocql"
)

// Session mocks the implementation for database connection-session.
type Session struct {
	MockQuery          func(stmt string, values ...interface{})
	MockQueryExec      func()
	MockQueryExecError string
}

// DBSession is a no-op
func (s *Session) DBSession() *cql.Session {
	return nil
}

// Query is mock for session's query-preparation function.
// This function internally executes #MockQuery function.
func (s *Session) Query(stmt string, values ...interface{}) driver.QueryI {
	if s.MockQuery != nil {
		s.MockQuery(stmt, values)
	}
	return &Query{
		ExecError: s.MockQueryExecError,
		MockExec:  s.MockQueryExec,
		statement: stmt,
	}
}
