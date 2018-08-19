package mocks

import (
	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// Iterx mocks the gocqlx Iterx
type Iterx struct {
	CqlQuery   driver.QueryI
	MockClose  func() error
	MockSelect func(dest interface{}) error
}

// Close mocks the Iterx#Close function.
func (i *Iterx) Close() error {
	if i.MockClose == nil {
		return nil
	}
	return i.MockClose()
}

// Select mocks the Iterx#Select function.
func (i *Iterx) Select(dest interface{}) error {
	if i.MockSelect == nil {
		return nil
	}
	return i.MockSelect(dest)
}
