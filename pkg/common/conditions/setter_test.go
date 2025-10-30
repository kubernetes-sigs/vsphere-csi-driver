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
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHasSameState(t *testing.T) {
	g := NewWithT(t)

	// same condition
	falseInfo2 := falseInfo1.DeepCopy()
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeTrue())

	// different LastTransitionTime does not impact state
	falseInfo2 = falseInfo1.DeepCopy()
	falseInfo2.LastTransitionTime = metav1.NewTime(time.Date(1900, time.November, 10, 23, 0, 0, 0, time.UTC))
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeTrue())

	// different Type, Status, Reason, Severity and Message determine different state
	falseInfo2 = falseInfo1.DeepCopy()
	falseInfo2.Type = "another type"
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeFalse())

	falseInfo2 = falseInfo1.DeepCopy()
	falseInfo2.Status = metav1.ConditionTrue
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeFalse())

	falseInfo2 = falseInfo1.DeepCopy()
	falseInfo2.Reason = "another severity"
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeFalse())

	falseInfo2 = falseInfo1.DeepCopy()
	falseInfo2.Message = "another message"
	g.Expect(hasSameState(falseInfo1, falseInfo2)).To(BeFalse())
}

func TestLexicographicLess(t *testing.T) {
	g := NewWithT(t)

	// alphabetical order of Type is respected
	a := TrueCondition("A")
	b := TrueCondition("B")
	g.Expect(lexicographicLess(a, b)).To(BeTrue())

	a = TrueCondition("B")
	b = TrueCondition("A")
	g.Expect(lexicographicLess(a, b)).To(BeFalse())

	// Ready condition is treated as an exception and always goes first
	a = TrueCondition(ReadyConditionType)
	b = TrueCondition("A")
	g.Expect(lexicographicLess(a, b)).To(BeTrue())

	a = TrueCondition("A")
	b = TrueCondition(ReadyConditionType)
	g.Expect(lexicographicLess(a, b)).To(BeFalse())
}

func TestSet(t *testing.T) {
	a := TrueCondition("a")
	b := TrueCondition("b")
	ready := TrueCondition(ReadyConditionType)

	tests := []struct {
		name      string
		to        Setter
		condition *metav1.Condition
		want      []metav1.Condition
	}{
		{
			name:      "Set specifies nil condition",
			to:        setterWithConditions(a),
			condition: nil,
			want:      conditionList(a),
		},
		{
			name:      "Set adds a condition",
			to:        setterWithConditions(),
			condition: a,
			want:      conditionList(a),
		},
		{
			name:      "Set adds more conditions",
			to:        setterWithConditions(a),
			condition: b,
			want:      conditionList(a, b),
		},
		{
			name:      "Set does not duplicate existing conditions",
			to:        setterWithConditions(a, b),
			condition: a,
			want:      conditionList(a, b),
		},
		{
			name:      "Set sorts conditions in lexicographic order",
			to:        setterWithConditions(b, a),
			condition: ready,
			want:      conditionList(ready, a, b),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			Set(tt.to, tt.condition)

			g.Expect(tt.to.GetConditions()).To(haveSameConditionsOf(tt.want))
		})
	}
}

func TestSetLastTransitionTime(t *testing.T) {
	x := metav1.Date(2012, time.January, 1, 12, 15, 30, 5e8, time.UTC)

	foo := FalseCondition("foo", "reason foo", "message foo")
	fooWithLastTransitionTime := FalseCondition("foo", "reason foo", "message foo")
	fooWithLastTransitionTime.LastTransitionTime = x
	fooWithAnotherState := TrueCondition("foo")

	tests := []struct {
		name                    string
		to                      Setter
		new                     *metav1.Condition
		LastTransitionTimeCheck func(*WithT, metav1.Time)
	}{
		{
			name: "Set a condition that does not exists should set the last transition time if not defined",
			to:   setterWithConditions(),
			new:  foo,
			LastTransitionTimeCheck: func(g *WithT, lastTransitionTime metav1.Time) {
				g.Expect(lastTransitionTime).ToNot(BeZero())
			},
		},
		{
			name: "Set a condition that does not exists should preserve the last transition time if defined",
			to:   setterWithConditions(),
			new:  fooWithLastTransitionTime,
			LastTransitionTimeCheck: func(g *WithT, lastTransitionTime metav1.Time) {
				g.Expect(lastTransitionTime).To(Equal(x))
			},
		},
		{
			name: "Set a condition that already exists with the same state should preserves the last transition time",
			to:   setterWithConditions(fooWithLastTransitionTime),
			new:  foo,
			LastTransitionTimeCheck: func(g *WithT, lastTransitionTime metav1.Time) {
				g.Expect(lastTransitionTime).To(Equal(x))
			},
		},
		{
			name: "Set a condition that already exists but with different state should changes the last transition time",
			to:   setterWithConditions(fooWithLastTransitionTime),
			new:  fooWithAnotherState,
			LastTransitionTimeCheck: func(g *WithT, lastTransitionTime metav1.Time) {
				g.Expect(lastTransitionTime).ToNot(Equal(x))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			Set(tt.to, tt.new)

			tt.LastTransitionTimeCheck(g, Get(tt.to, "foo").LastTransitionTime)
		})
	}
}

func TestMarkMethods(t *testing.T) {
	g := NewWithT(t)

	var obj nonKubeObj

	// test MarkTrue
	MarkTrue(&obj, "conditionFoo")
	g.Expect(Get(obj, "conditionFoo")).To(haveSameStateOf(&metav1.Condition{
		Type:   "conditionFoo",
		Status: metav1.ConditionTrue,
		Reason: "True",
	}))

	// test MarkFalse
	MarkFalse(&obj, "conditionBar", "reasonBar", "messageBar")
	g.Expect(Get(obj, "conditionBar")).To(haveSameStateOf(&metav1.Condition{
		Type:    "conditionBar",
		Status:  metav1.ConditionFalse,
		Reason:  "reasonBar",
		Message: "messageBar",
	}))

	// test MarkError
	tErr := fmt.Errorf("errorBar")
	MarkError(&obj, "conditionBar", "reasonBar", tErr)
	g.Expect(Get(obj, "conditionBar")).To(haveSameStateOf(&metav1.Condition{
		Type:    "conditionBar",
		Status:  metav1.ConditionFalse,
		Reason:  "reasonBar",
		Message: "errorBar",
	}))

	// test MarkUnknown
	MarkUnknown(&obj, "conditionBaz", "reasonBaz", "messageBaz")
	g.Expect(Get(obj, "conditionBaz")).To(haveSameStateOf(&metav1.Condition{
		Type:    "conditionBaz",
		Status:  metav1.ConditionUnknown,
		Reason:  "reasonBaz",
		Message: "messageBaz",
	}))
}

func TestSetSummary(t *testing.T) {
	g := NewWithT(t)
	target := setterWithConditions(TrueCondition("foo"))

	SetSummary(target)

	g.Expect(Has(target, ReadyConditionType)).To(BeTrue())
}

func TestSetMirror(t *testing.T) {
	g := NewWithT(t)
	source := getterWithConditions(TrueCondition(ReadyConditionType))
	target := setterWithConditions()

	SetMirror(target, "foo", source)

	g.Expect(Has(target, "foo")).To(BeTrue())
}

func TestSetAggregate(t *testing.T) {
	g := NewWithT(t)
	source1 := getterWithConditions(TrueCondition(ReadyConditionType))
	source2 := getterWithConditions(TrueCondition(ReadyConditionType))
	target := setterWithConditions()

	SetAggregate(target, "foo", []Getter{source1, source2})

	g.Expect(Has(target, "foo")).To(BeTrue())
}

func setterWithConditions(conditions ...*metav1.Condition) Setter {
	var obj nonKubeObj
	obj.SetConditions(conditionList(conditions...))
	return &obj
}

func haveSameConditionsOf(expected []metav1.Condition) types.GomegaMatcher {
	return &ConditionsMatcher{
		Expected: expected,
	}
}

type ConditionsMatcher struct {
	Expected []metav1.Condition
}

func (matcher *ConditionsMatcher) Match(actual interface{}) (success bool, err error) {
	actualConditions, ok := actual.([]metav1.Condition)
	if !ok {
		return false, errors.New("Value should be a conditions list")
	}

	if len(actualConditions) != len(matcher.Expected) {
		return false, nil
	}

	for i := range actualConditions {
		if !hasSameState(&actualConditions[i], &matcher.Expected[i]) {
			return false, nil
		}
	}
	return true, nil
}

func (matcher *ConditionsMatcher) FailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "to have the same conditions of", matcher.Expected)
}
func (matcher *ConditionsMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "not to have the same conditions of", matcher.Expected)
}
