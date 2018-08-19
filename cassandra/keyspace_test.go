package cassandra

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/TerrexTech/go-cassandrautils/mocks"
	"github.com/TerrexTech/go-commonutils/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Keyspace", func() {
	Context("new keyspace is requested", func() {
		var (
			err             error
			keyspaceConfig  KeyspaceConfig
			isQueryExecuted bool
			outputStr       string
		)

		BeforeEach(func() {
			keyspaceConfig = KeyspaceConfig{
				Name:                "test",
				ReplicationStrategy: "NetworkTopologyStrategy",
				ReplicationStrategyArgs: map[string]int{
					"datacenter1": 1,
					"datacenter2": 2,
				},
			}

			isQueryExecuted = false
			session := mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
				MockQueryExec: func() {
					isQueryExecuted = true
				},
			}

			_, err = NewKeyspace(&session, keyspaceConfig)
			outputStr = utils.StandardizeSpaces(outputStr)
		})

		It("should create correct query for creating keyspace", func() {
			rgx := regexp.MustCompile(
				`CREATE KEYSPACE IF NOT EXISTS test WITH replication = {([a-zA-Z0-9' :,]+)}`,
			)
			Expect(
				rgx.MatchString(outputStr),
			).To(BeTrue())
		})

		It("should include correct keys for creating keyspace", func() {
			rgx := regexp.MustCompile(`([a-zA-Z0-9: ']+)([,|}])`)
			columns := rgx.FindAllString(outputStr, -1)
			trimmedColumns := []string{}
			for _, v := range columns {
				// Remove spaces
				v = strings.Replace(v, " ", "", -1)
				// Remove commas
				v = strings.TrimSuffix(v, ",")
				// Regex doesn't remove the last curly brace
				v = strings.TrimSuffix(v, "}")
				trimmedColumns = append(trimmedColumns, v)
			}
			expectedColumns := []string{
				"'class':'NetworkTopologyStrategy'",
				"'datacenter1':1",
				"'datacenter2':2",
			}
			Expect(
				utils.AreElementsInSlice(trimmedColumns, expectedColumns),
			).To(BeTrue())

			Expect(err).ToNot(HaveOccurred())
		})

		It("should execute the query", func() {
			Expect(isQueryExecuted).To(BeTrue())
		})

		It("should return the Keyspace struct with required values", func() {
			session := mocks.Session{}
			ks, err := NewKeyspace(&session, keyspaceConfig)

			eq := reflect.DeepEqual(
				ks.ReplicationStrategyArgs(),
				keyspaceConfig.ReplicationStrategyArgs,
			)

			Expect(ks.Name()).To(Equal(keyspaceConfig.Name))
			Expect(ks.ReplicationStrategy()).To(Equal(keyspaceConfig.ReplicationStrategy))
			Expect(err).ToNot(HaveOccurred())
			Expect(eq).To(BeTrue())
		})

		It("should return any errors that occur", func() {
			keyspaceConfig := KeyspaceConfig{
				Name:                "test",
				ReplicationStrategy: "SomethingInvalidStrategy",
				ReplicationStrategyArgs: map[string]int{
					"bad": 1,
				},
			}

			session := mocks.Session{
				MockQueryExecError: "some-error",
			}

			_, err := NewKeyspace(&session, keyspaceConfig)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("keyspace alteration is requested", func() {
		var (
			keyspaceConfig  KeyspaceConfig
			isQueryExecuted bool
			outputStr       string
			session         mocks.Session
		)

		BeforeEach(func() {
			keyspaceConfig = KeyspaceConfig{
				Name:                "test",
				ReplicationStrategy: "NetworkTopologyStrategy",
				ReplicationStrategyArgs: map[string]int{
					"datacenter1": 1,
					"datacenter2": 2,
				},
			}

			isQueryExecuted = false
			session = mocks.Session{
				MockQuery: func(stmt string, values ...interface{}) {
					outputStr = stmt
				},
				MockQueryExec: func() {
					isQueryExecuted = true
				},
			}
		})

		It("should create correct query for altering keyspace", func() {
			ks := Keyspace{}
			_, err := ks.Alter(&session, keyspaceConfig)
			rgx := regexp.MustCompile(
				`ALTER KEYSPACE test WITH replication = {([a-zA-Z0-9' :,]+)}`,
			)
			outputStr = utils.StandardizeSpaces(outputStr)
			Expect(
				rgx.MatchString(outputStr),
			).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should include correct keys for creating keyspace", func() {
			ks := Keyspace{}
			_, err := ks.Alter(&session, keyspaceConfig)
			outputStr = utils.StandardizeSpaces(outputStr)

			rgx := regexp.MustCompile(`([a-zA-Z0-9: ']+)([,|}])`)
			columns := rgx.FindAllString(outputStr, -1)
			trimmedColumns := []string{}
			for _, v := range columns {
				// Remove spaces
				v = strings.Replace(v, " ", "", -1)
				// Remove commas
				v = strings.TrimSuffix(v, ",")
				// Regex doesn't remove the last curly brace
				v = strings.TrimSuffix(v, "}")
				trimmedColumns = append(trimmedColumns, v)
			}
			expectedColumns := []string{
				"'class':'NetworkTopologyStrategy'",
				"'datacenter1':1",
				"'datacenter2':2",
			}
			Expect(
				utils.AreElementsInSlice(trimmedColumns, expectedColumns),
			).To(BeTrue())

			Expect(err).ToNot(HaveOccurred())
		})

		It("should execute the query", func() {
			ks := Keyspace{}
			_, err := ks.Alter(&session, keyspaceConfig)
			Expect(isQueryExecuted).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return the Keyspace struct with new values set", func() {
			ks := Keyspace{}
			keyspace, err := ks.Alter(&session, keyspaceConfig)

			eq := reflect.DeepEqual(
				keyspace.ReplicationStrategyArgs(),
				keyspaceConfig.ReplicationStrategyArgs,
			)

			Expect(keyspace.Name()).To(Equal(keyspaceConfig.Name))
			Expect(keyspace.ReplicationStrategy()).To(Equal(keyspaceConfig.ReplicationStrategy))
			Expect(err).ToNot(HaveOccurred())
			Expect(eq).To(BeTrue())
		})

		It("should return any errors that occur", func() {
			keyspaceConfig := KeyspaceConfig{
				Name:                "test",
				ReplicationStrategy: "SomethingInvalidStrategy",
				ReplicationStrategyArgs: map[string]int{
					"bad": 1,
				},
			}

			session := mocks.Session{
				MockQueryExecError: "some-error",
			}

			ks := Keyspace{}
			_, err := ks.Alter(&session, keyspaceConfig)
			Expect(err).To(HaveOccurred())
		})
	})
})
