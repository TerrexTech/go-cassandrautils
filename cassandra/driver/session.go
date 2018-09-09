package driver

import cql "github.com/gocql/gocql"

// SessionI is the database connection-session.
type SessionI interface {
	Query(stmt string, values ...interface{}) QueryI
	GoCqlSession() *cql.Session
}

// Session is the database-session implementation.
type Session struct {
	session *cql.Session
}

// NewSession creates a new database-session.
func NewSession(session *cql.Session) *Session {
	return &Session{
		session: session,
	}
}

// Query prepates the specified prepared-statement with given column-name values.
func (s *Session) Query(stmt string, values ...interface{}) QueryI {
	return &Query{
		query: s.session.Query(stmt, values...),
	}
}

// Close closes the database-session.
func (s *Session) Close() {
	s.session.Close()
}

// GoCqlSession returns the original wrapped GoCql-Session object.
func (s *Session) GoCqlSession() *cql.Session {
	return s.session
}

// SetPageSize sets the default page size for this session. A value <= 0 will disable paging.
// This setting can also be changed on a per-query basis.
func (s *Session) SetPageSize(n uint) {
	s.session.SetPageSize(int(n))
}
