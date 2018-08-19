package cassandra

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"
)

// KeyspaceConfig defines configuration for Keyspace.
type KeyspaceConfig struct {
	Name                    string
	ReplicationStrategy     string
	ReplicationStrategyArgs map[string]int
}

// Keyspace acts as utility-entity corresponding to Cassandra Keyspace.
type Keyspace struct {
	name                    string
	replicationStrategy     string
	replicationStrategyArgs map[string]int
}

// NewKeyspace creates a new Keyspace-entity instance, and also creates the
// Keyspace in database if it doesn't exist.
func NewKeyspace(session driver.SessionI, kc KeyspaceConfig) (*Keyspace, error) {
	k := &Keyspace{
		name:                    kc.Name,
		replicationStrategy:     kc.ReplicationStrategy,
		replicationStrategyArgs: kc.ReplicationStrategyArgs,
	}

	queryInitial := "CREATE KEYSPACE IF NOT EXISTS"
	err := k.manipulationQuery(session, kc, queryInitial)
	if err != nil {
		return nil, err
	}
	return k, nil
}

// Name returns the name of Keyspace.
func (k *Keyspace) Name() string {
	return k.name
}

// ReplicationStrategy returns the replicationStrategy of Keyspace.
func (k *Keyspace) ReplicationStrategy() string {
	return k.replicationStrategy
}

// ReplicationStrategyArgs returns the ReplicationStrategyArgs of Keyspace.
func (k *Keyspace) ReplicationStrategyArgs() map[string]int {
	return k.replicationStrategyArgs
}

// Parses replicationStrategyArgs and suffixes the provided query with result.
func (k *Keyspace) manipulationQuery(
	session driver.SessionI,
	kc KeyspaceConfig,
	queryInitial string,
) error {
	replicationStrategyArgs := ""
	for key, value := range k.replicationStrategyArgs {
		replicationStrategyArgs += fmt.Sprintf("'%s': %s, ", key, strconv.Itoa(value))
	}
	replicationStrategyArgs = strings.TrimSuffix(replicationStrategyArgs, ", ")

	err := session.Query(
		fmt.Sprintf(`
			%s %s
    		WITH replication = {
        	'class' : '%s',
        	%s
				}`,
			queryInitial,
			kc.Name,
			kc.ReplicationStrategy,
			replicationStrategyArgs,
		)).
		Exec()

	return err
}

// Alter allows changing replicationStrategy and replicationFactor of Keyspace.
func (k *Keyspace) Alter(session driver.SessionI, kc KeyspaceConfig) (*Keyspace, error) {
	k.name = kc.Name
	k.replicationStrategy = kc.ReplicationStrategy
	k.replicationStrategyArgs = kc.ReplicationStrategyArgs

	queryInitial := "ALTER KEYSPACE"
	err := k.manipulationQuery(session, kc, queryInitial)
	if err != nil {
		return nil, err
	}
	return k, nil
}
