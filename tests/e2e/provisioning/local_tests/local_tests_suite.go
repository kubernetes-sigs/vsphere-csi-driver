package localtests_test

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("TableBasicTest", func() {
	var (
		name string
	)

	ginkgo.DescribeTableSubtree("TableTests",
		func(author string, isValid bool, firstName string, lastName string) {
			ginkgo.BeforeEach(func() {
				name = "Go : "
			})
			ginkgo.AfterEach(func() {
				name = name + firstName
				fmt.Println("In AfterEach :", name)
			})
			ginkgo.It("should return the expected status code", func() {
				name = name + " --> " + author
				fmt.Println("In It : ", name)

				gomega.Expect(isValid).To(gomega.Equal(true))
			})
		},
		// ginkgo.Entry("1", "Victor Hugo", true, "Victor", "Hugo"),
		// ginkgo.Entry("2", "Hugo", false, "", "Hugo"),
		ginkgo.Entry("3", "Victor Marie Hugo", true, "Victor", "Hugo"),
		// ginkgo.Entry("4", "", false, "", ""),
	)
})
