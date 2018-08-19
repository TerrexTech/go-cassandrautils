### Go Cassandra-Utils
---

This package helps in bootstrap Cassandra, aka tasks such as creating database-sessions, keyspaces, and tables.

**[Go Doc][0]**  
Check example usage here: [examples/example.go][1]  
More examples can be found in [test-files][2].

#### Developer Notes
---

Since gocql doesn't provide interfaces to mock, this library internally works by creating wrapper-interfaces
around gocql and gocqlx to allow better testing.

Here, we mostly test if things are being passed to gocql correctly, assuming that the gocql works as intended.
The final integration_test ensures that our library and gocql together are working as intended.

Better suggestions with proper reasoning are welcomed.

  [0]: https://godoc.org/github.com/TerrexTech/go-cassandrautils/cassandra
  [1]: https://github.com/TerrexTech/go-cassandrautils/blob/master/examples/example.go
  [2]: https://github.com/TerrexTech/go-cassandrautils/tree/master/cassandra
