package cassandra

import (
	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
	cql "github.com/gocql/gocql"
)

// ClusterDriver is the implementation for Cassandra-driver.
type ClusterDriver interface {
	CreateSession() (*cql.Session, error)
}

var session *driver.Session

// GetSession creates new GoCql connection if required,
// and returns the existing or newly creating session.
// The returned Session is a Singleton.
func GetSession(cluster ClusterDriver) (*driver.Session, error) {
	if session == nil || session.GoCqlSession().Closed() {
		var s *cql.Session
		s, err := cluster.CreateSession()
		if err != nil {
			return nil, err
		}
		session = driver.NewSession(s)
	}

	return session, nil
}
