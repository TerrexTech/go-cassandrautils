package cassandra

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCassandra(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cassandra Suite")
}
