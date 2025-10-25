/*
Copyright 2020 The Kubernetes Authors.

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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Setter interface defines methods that a Cluster API object should implement in order to
// use the conditions package for setting conditions.
type Setter interface {
	Getter
	SetConditions([]metav1.Condition)
}

// Set sets the given condition.
// If a condition with the same type already exists, its LastTransitionTime is
// only updated if a change is detected in one of the following fields:
// Status, Reason, or Message.
func Set(to Setter, condition *metav1.Condition) {
	to.SetConditions(c4g(to).Set(condition))
}

// TrueCondition returns a condition with Status=True and the given type.
func TrueCondition(t string) *metav1.Condition {
	return &metav1.Condition{
		Type:   t,
		Status: metav1.ConditionTrue,
		// This is a non-empty field in metav1.Conditions, when it was not in our v1a1 Conditions. This
		// really doesn't work with how we've defined our conditions so do something to make things
		// work for now.
		Reason: string(metav1.ConditionTrue),
	}
}

// FalseCondition returns a condition with Status=False and the given type.
func FalseCondition(t string, reason string, messageFormat string, messageArgs ...interface{}) *metav1.Condition {
	if reason == "" {
		reason = string(metav1.ConditionFalse)
	}

	return &metav1.Condition{
		Type:    t,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: fmt.Sprintf(messageFormat, messageArgs...),
	}
}

// UnknownCondition returns a condition with Status=Unknown and the given type.
func UnknownCondition(t string, reason string, messageFormat string, messageArgs ...interface{}) *metav1.Condition {
	if reason == "" {
		reason = string(metav1.ConditionUnknown)
	}

	return &metav1.Condition{
		Type:    t,
		Status:  metav1.ConditionUnknown,
		Reason:  reason,
		Message: fmt.Sprintf(messageFormat, messageArgs...),
	}
}

// MarkTrue sets Status=True for the condition with the given type.
func MarkTrue(to Setter, t string) {
	to.SetConditions(c4g(to).MarkTrue(t))
}

// MarkUnknown sets Status=Unknown for the condition with the given type.
func MarkUnknown(to Setter, t string, reason, messageFormat string, messageArgs ...interface{}) {
	to.SetConditions(c4g(to).MarkUnknown(t, reason, messageFormat, messageArgs...))
}

// MarkFalse sets Status=False for the condition with the given type.
func MarkFalse(to Setter, t string, reason string, messageFormat string, messageArgs ...interface{}) {
	to.SetConditions(c4g(to).MarkFalse(t, reason, messageFormat, messageArgs...))
}

// MarkError sets Status=False and the error message for the condition with the given type.
func MarkError(to Setter, t string, reason string, err error) {
	to.SetConditions(c4g(to).MarkFalse(t, reason, "%s", err.Error()))
}

// SetSummary sets a Ready condition with a summary of all the existing
// conditions. If there are no existing conditions, no summary condition is
// generated.
func SetSummary(to Setter, options ...MergeOption) {
	to.SetConditions(c4g(to).SetSummary(options...))
}

// SetMirror creates a new condition by mirroring the Ready condition from a
// source object. If the source object does not have a Ready condition, the
// target is not modified.
func SetMirror(to Setter, targetCondition string, from Getter, options ...MirrorOptions) {
	to.SetConditions(c4g(to).SetMirror(targetCondition, from, options...))
}

// SetAggregate creates a new condition by aggregating all of the Ready
// conditions from a list of source objects. If a source object is missing the
// Ready condition, that object is excluded from aggregation. If none of the
// source objects have a Ready condition, the target is not modified.
func SetAggregate(to Setter, targetCondition string, from []Getter, options ...MergeOption) {
	to.SetConditions(c4g(to).SetAggregate(targetCondition, from, options...))
}

// Delete deletes the condition with the given type.
func Delete(to Setter, t string) {
	to.SetConditions(c4g(to).Delete(t))
}
