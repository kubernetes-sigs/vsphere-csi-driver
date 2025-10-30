/*
Copyright 2020-2024 The Kubernetes Authors.

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

//nolint:scopelint
package conditions

import (
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewConditionsGroup(t *testing.T) {
	testCases := []struct {
		name   string
		setter Setter
	}{
		{
			name:   "Kube object",
			setter: &kubeObj{},
		},
		{
			name:   "Non-kube object",
			setter: &nonKubeObj{},
		},
	}

	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			got := getConditionGroups(conditionsWithSource(
				tc.setter,
				[]*metav1.Condition{
					nil1,
					true1,
					true1,
					falseInfo1,
					falseWarning1,
					falseWarning1,
					falseError1,
					unknown1,
				}...))

			g.Expect(got).ToNot(BeNil())
			g.Expect(got).To(HaveLen(3))

			// The top group should be false and it should have four conditions
			g.Expect(got.TopGroup().status).To(Equal(metav1.ConditionFalse))
			g.Expect(got.TopGroup().conditions).To(HaveLen(4))

			// The true group should be true and it should have two conditions
			g.Expect(got.TrueGroup().status).To(Equal(metav1.ConditionTrue))
			g.Expect(got.TrueGroup().conditions).To(HaveLen(2))

			// The error group should be false and it should have one condition
			g.Expect(got.FalseGroup().status).To(Equal(metav1.ConditionFalse))
			g.Expect(got.FalseGroup().conditions).To(HaveLen(4))

			// got[0] should be False and it should have four conditions
			g.Expect(got[0].status).To(Equal(metav1.ConditionFalse))
			g.Expect(got[0].conditions).To(HaveLen(4))

			// got[1] should be True and it should have two conditions
			g.Expect(got[1].status).To(Equal(metav1.ConditionTrue))
			g.Expect(got[1].conditions).To(HaveLen(2))

			// got[4] should be Unknown and it should have one condition
			g.Expect(got[2].status).To(Equal(metav1.ConditionUnknown))
			g.Expect(got[2].conditions).To(HaveLen(1))
		})
	}
}

func TestMergeRespectPriority(t *testing.T) {
	tests := []struct {
		name       string
		conditions []*metav1.Condition
		want       *metav1.Condition
	}{
		{
			name:       "aggregate nil list return nil",
			conditions: nil,
			want:       nil,
		},
		{
			name:       "aggregate empty list return nil",
			conditions: []*metav1.Condition{},
			want:       nil,
		},
		{
			name:       "When there is false/error it returns false/error",
			conditions: []*metav1.Condition{falseError1, falseWarning1, falseInfo1, unknown1, true1},
			want:       FalseCondition("foo", "reason falseError1", "message falseError1"),
		},
		{
			name:       "When there is false/info and no false/error or false/warning, it returns false/info",
			conditions: []*metav1.Condition{falseInfo1, unknown1, true1},
			want:       FalseCondition("foo", "reason falseInfo1", "message falseInfo1"),
		},
		{
			name:       "When there is true and no false/*, it returns info",
			conditions: []*metav1.Condition{unknown1, true1},
			want:       TrueCondition("foo"),
		},
		{
			name:       "When there is unknown and no true or false/*, it returns unknown",
			conditions: []*metav1.Condition{unknown1},
			want:       UnknownCondition("foo", "reason unknown1", "message unknown1"),
		},
		{
			name:       "nil conditions are ignored",
			conditions: []*metav1.Condition{nil1, nil1, nil1},
			want:       nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			got := merge(conditionsWithSource(&kubeObj{}, tt.conditions...), "foo", &mergeOptions{})

			if tt.want == nil {
				g.Expect(got).To(BeNil())
				return
			}
			g.Expect(got).To(haveSameStateOf(tt.want))
		})
	}
}

func conditionsWithSource(obj Setter, conditions ...*metav1.Condition) []localizedCondition {
	obj.SetConditions(conditionList(conditions...))

	ret := make([]localizedCondition, 0, len(conditions))
	for i := range conditions {
		ret = append(ret, localizedCondition{
			Condition: conditions[i],
			Getter:    obj,
		})
	}

	return ret
}
