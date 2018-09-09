package cassandra

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/scylladb/gocqlx/qb"
)

var _ = Describe("ColumnComparator", func() {
	Context("Comparator function is called", func() {
		It("should return a ColumnComparator instance with Name and Value set", func() {
			cc := Comparator("test", 1)
			Expect(cc.Name).To(Equal("test"))
			Expect(cc.Value).To(Equal(1))
		})
	})

	Context("Comparator operation is applied", func() {
		var cc ColumnComparator
		BeforeEach(func() {
			cc = Comparator("test", 1)
			Expect(cc.Name).To(Equal("test"))
			Expect(cc.Value).To(Equal(1))
		})

		Specify("Eq on Eq operation", func() {
			cc = cc.Eq()
			Expect(cc.cmpType).To(Equal(qb.Eq(cc.Name)))
		})

		Specify("Gt on Gt operation", func() {
			cc = cc.Gt()
			Expect(cc.cmpType).To(Equal(qb.Gt(cc.Name)))
		})

		Specify("GtOrEq on GtOrEq operation", func() {
			cc = cc.GtOrEq()
			Expect(cc.cmpType).To(Equal(qb.GtOrEq(cc.Name)))
		})

		Specify("Lt on Lt operation", func() {
			cc = cc.Lt()
			Expect(cc.cmpType).To(Equal(qb.Lt(cc.Name)))
		})

		Specify("LtOrEq on LtOrEq operation", func() {
			cc = cc.LtOrEq()
			Expect(cc.cmpType).To(Equal(qb.LtOrEq(cc.Name)))
		})
	})
})
