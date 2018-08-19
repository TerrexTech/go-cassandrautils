package mocks

import (
	"errors"

	cql "github.com/gocql/gocql"
)

// ClusterDriver creates a mock gocql.ClusterConfig.
type ClusterDriver struct {
	// If defined, the #CreateSession function will throw this error.
	CreateSessionError string
	// This function gets executed by #CreateSession function
	// and can be used for mocking tests.
	// This function is always executed by #CreateSession if defined.
	MockCreateSession func()
}

// CreateSession mocks the session-creation function for CassandraDriver.
// This function internally executes #MockCreationSession function.
func (cd *ClusterDriver) CreateSession() (*cql.Session, error) {
	if cd.CreateSessionError != "" {
		return nil, errors.New(cd.CreateSessionError)
	}
	if cd.MockCreateSession != nil {
		cd.MockCreateSession()
	}
	return nil, nil
}
