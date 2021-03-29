package gofsutil

/*
RemoveDuplicates removes duplicate and empty values from the provided
slice of strings and maintains the original order of the list.

This function defers to RemoveDuplicatesExponentialOrdered, a variation
of RemoveDuplicatesExponentialUnordered as described in the article
"Learning Golang By Benchmarking Set Implementations" by Karl Seguin at
https://goo.gl/NTU36K. The variation preserves the slice's original order
and has minimal to no impact on performance.

The function's O(n^2) complexity could be altered to O(n) (linear) by
using a map[string]struct{} to track unique elements with zero memory
allocation. However, as long as the input data is small, the current
implementation is more performant both with respect to both CPU and
memory. Given that this function is used to remove duplicate mount
options, there should never be a sufficiently large enough dataset
that a linear version of this function would be more performant the
current implementation.

The function RemoveDuplicatesLinearOrdered is the linear version of this
function. If the situation should ever change such that mount options
number in the thousands then this function should defer to
RemoveDuplicatesLinearOrdered instead of RemoveDuplicatesExponentialOrdered.

        $ go test -run Bench -bench BenchmarkRemoveDuplicates -benchmem -v
        goos: darwin
        goarch: amd64
        pkg: github.com/thecodeteam/gofsutil
        BenchmarkRemoveDuplicates_Exponential_Ordered___SmallData-8   	20000000	       121 ns/op	       0 B/op	       0 allocs/op
        BenchmarkRemoveDuplicates_Exponential_Unordered_SmallData-8   	20000000	        99.0 ns/op	       0 B/op	       0 allocs/op
        BenchmarkRemoveDuplicates_Linear______Ordered___SmallData-8   	 2000000	       715 ns/op	     288 B/op	       1 allocs/op
        BenchmarkRemoveDuplicates_Exponential_Ordered___BigData-8     	   20000	     84731 ns/op	       0 B/op	       0 allocs/op
        BenchmarkRemoveDuplicates_Exponential_Unordered_BigData-8     	   10000	    156660 ns/op	       0 B/op	       0 allocs/op
        BenchmarkRemoveDuplicates_Linear______Ordered___BigData-8     	   50000	     36971 ns/op	   20512 B/op	       2 allocs/op
        PASS
        ok  	github.com/thecodeteam/gofsutil	22.085s
*/
func RemoveDuplicates(a []string) []string {
	return RemoveDuplicatesExponentialOrdered(a)
}

// RemoveDuplicatesExponentialOrdered removes duplicate and empty values
// from the provided slice of strings using an exponentially complex
// algorithm and maintains the original order of the list.
func RemoveDuplicatesExponentialOrdered(a []string) []string {
	// This trick (https://goo.gl/XF62YZ) uses filtering without allocating
	// since b shares the same backing array and capacity as a.
	b := a[:0]

	// d is used to skip duplicate options by checking if a value
	// from a is already in b
	d := func(b []string, s string) bool {
		for _, t := range b {
			if s == t {
				return true
			}
		}
		return false
	}

	for _, s := range a {
		// Skip empty options.
		if s == "" {
			continue
		}
		// Skip duplicate options.
		if d(b, s) {
			continue
		}
		// Keep the option.
		b = append(b, s)
	}

	return b
}

// RemoveDuplicatesExponentialUnordered removes duplicate and empty values
// from the provided slice of strings using an exponentially complex
// algorithm and does not maintain the original order of the list.
func RemoveDuplicatesExponentialUnordered(a []string) []string {
	l := len(a) - 1
	for i := 0; i < l+1; i++ {
		// Skip empty options.
		if a[i] == "" {
			a[i] = a[l]
			a = a[0:l]
			l--
			continue
		}
		for j := i + 1; j <= l; j++ {
			// Skip duplicates.
			if a[i] == a[j] {
				a[j] = a[l]
				a = a[0:l]
				l--
				j--
			}
		}
	}
	return a
}

// RemoveDuplicatesLinearOrdered removes duplicate and empty values
// from the provided slice of strings using a linearly complex
// algorithm and maintains the original order of the list.
func RemoveDuplicatesLinearOrdered(a []string) []string {
	var (
		i    int
		seen = make(map[string]struct{}, len(a))
	)
	for _, s := range a {
		// Skip empty options.
		if s == "" {
			continue
		}
		// Skip duplicates.
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		a[i] = s
		i++
	}
	return a[:i]
}
