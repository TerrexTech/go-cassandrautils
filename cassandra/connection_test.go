package cassandra

import (
	cql "github.com/gocql/gocql"

	"github.com/TerrexTech/go-cassandrautils/cassandra/driver"

	"github.com/TerrexTech/go-cassandrautils/mocks"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Connection", func() {
	Context("new session is requested", func() {
		var (
			isCreateSessionCalled bool
		)

		BeforeEach(func() {
			session = nil
			isCreateSessionCalled = false
		})

		It("should return existing session if a session exists", func() {
			session = driver.NewSession(&cql.Session{})
			driver := &mocks.ClusterDriver{
				MockCreateSession: func() {
					isCreateSessionCalled = true
				},
			}

			_, err := GetSession(driver)
			Expect(isCreateSessionCalled).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should create a new session if none exists", func() {
			driver := &mocks.ClusterDriver{
				MockCreateSession: func() {
					isCreateSessionCalled = true
				},
			}

			_, err := GetSession(driver)
			Expect(isCreateSessionCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should create a new session if session is closed", func() {
			session = driver.NewSession(&cql.Session{})
			session.Close()

			driver := &mocks.ClusterDriver{
				MockCreateSession: func() {
					isCreateSessionCalled = true
				},
			}

			_, err := GetSession(driver)
			Expect(isCreateSessionCalled).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return any errors that occur", func() {
			driver := &mocks.ClusterDriver{
				CreateSessionError: "some-error occured.",
			}

			_, err := GetSession(driver)
			Expect(err).To(HaveOccurred())
		})
	})
})
