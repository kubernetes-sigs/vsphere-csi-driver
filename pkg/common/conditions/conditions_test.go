/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package conditions

import (
	"testing"

	. "github.com/onsi/gomega"
)

func TestDelete(t *testing.T) {
	testCases := []struct {
		name string
		in   Conditions
		out  Conditions
	}{
		{
			name: "nil input",
			in:   nil,
			out:  nil,
		},
		{
			name: "type to delete does not exist",
			in: Conditions{
				{
					Type: "Hello",
				},
			},
			out: Conditions{
				{
					Type: "Hello",
				},
			},
		},
		{
			name: "type to delete does exist",
			in: Conditions{
				{
					Type: "Hello",
				},
				{
					Type: ReadyConditionType,
				},
			},
			out: Conditions{
				{
					Type: "Hello",
				},
			},
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			g.Expect(tc.in.Delete(ReadyConditionType)).To(Equal(tc.out))
		})
	}
}
