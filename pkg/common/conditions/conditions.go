// © Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
// SPDX-License-Identifier: Apache-2.0

package conditions

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// ReadyConditionType indicates a resource is ready.
	ReadyConditionType = "Ready"
)

// Conditions is an alias for a slice of metav1.Condition objects and provides
// helpful functions.
type Conditions []metav1.Condition

// c4g returns a Conditions type from a Getter. This is used throughout this
// package to reduce the need to write "Conditions(obj.GetConditions())".
func c4g(g Getter) Conditions {
	return g.GetConditions()
}

// GetConditions allows the Conditions type to implement the Getter interface.
func (l Conditions) GetConditions() []metav1.Condition {
	return l
}

// Get returns the condition with the given type, otherwise nil is returned.
func (l Conditions) Get(t string) *metav1.Condition {
	for _, c := range l {
		if c.Type == t {
			return &c
		}
	}
	return nil
}

// Has returns true if a condition with the given type exists.
func (l Conditions) Has(t string) bool {
	return l.Get(t) != nil
}

// IsTrue returns true if the condition with the given type exists and is
// True, otherwise false is returned.
func (l Conditions) IsTrue(t string) bool {
	if c := l.Get(t); c != nil {
		return c.Status == metav1.ConditionTrue
	}
	return false
}

// IsFalse returns true if the condition with the given type exists and is
// False, otherwise false is returned.
func (l Conditions) IsFalse(t string) bool {
	if c := l.Get(t); c != nil {
		return c.Status == metav1.ConditionFalse
	}
	return false
}

// IsUnknown returns true if the condition with the given type exists and is
// Unknown, otherwise false is returned.
func (l Conditions) IsUnknown(t string) bool {
	if c := l.Get(t); c != nil {
		return c.Status == metav1.ConditionUnknown
	}
	return true
}

// GetReason returns the condition's reason or an empty string if the condition
// does not exist.
func (l Conditions) GetReason(t string) string {
	if c := l.Get(t); c != nil {
		return c.Reason
	}
	return ""
}

// GetMessage returns the condition's message or an empty string if the
// condition does not exist.
func (l Conditions) GetMessage(t string) string {
	if c := l.Get(t); c != nil {
		return c.Message
	}
	return ""
}

// GetLastTransitionTime returns the condition's last transition time or a nil
// value if the condition does not exist.
func (l Conditions) GetLastTransitionTime(t string) *metav1.Time {
	if c := l.Get(t); c != nil {
		return &c.LastTransitionTime
	}
	return nil
}

// Set sets the given condition.
// If a condition with the same type already exists, its LastTransitionTime is
// only updated if a change is detected in one of the following fields:
// Status, Reason, or Message.
func (l Conditions) Set(c *metav1.Condition) Conditions {
	if c == nil {
		return l
	}

	// Check if the new conditions already exists, and change it only if there
	// is a status transition (otherwise preserve the current last transition
	// time).
	exists := false
	for i := range l {
		existingCondition := l[i]
		if existingCondition.Type == c.Type {
			exists = true
			if !hasSameState(&existingCondition, c) {
				c.LastTransitionTime = metav1.NewTime(
					time.Now().UTC().Truncate(time.Second))
				l[i] = *c
				break
			}
			c.LastTransitionTime = existingCondition.LastTransitionTime
			break
		}
	}

	// If the condition does not exist, add it, setting the transition time only
	// if it is not already set
	if !exists {
		if c.LastTransitionTime.IsZero() {
			c.LastTransitionTime = metav1.NewTime(
				time.Now().UTC().Truncate(time.Second))
		}
		l = append(l, *c)
	}

	// Sorts conditions for convenience of the consumer, i.e. kubectl.
	sort.Slice(l, func(i, j int) bool {
		return lexicographicLess(&l[i], &l[j])
	})

	return l
}

// MarkTrue sets Status=True for the condition with the given type.
func (l Conditions) MarkTrue(t string) Conditions {
	return l.Set(TrueCondition(t))
}

// MarkUnknown sets Status=Unknown for the condition with the given type.
func (l Conditions) MarkUnknown(
	t, reason, messageFormat string, messageArgs ...any) Conditions {

	return l.Set(UnknownCondition(t, reason, messageFormat, messageArgs...))
}

// MarkFalse sets Status=False for the condition with the given type.
func (l Conditions) MarkFalse(
	t, reason, messageFormat string, messageArgs ...any) Conditions {

	return l.Set(FalseCondition(t, reason, messageFormat, messageArgs...))
}

// MarkError sets Status=False and the error message for the condition with the given type.
func (l Conditions) MarkError(
	t, reason string, err error) Conditions {

	return l.Set(FalseCondition(t, reason, "%s", err.Error()))
}

// SetSummary sets a Ready condition with a summary of all the existing
// conditions. If there are no existing conditions, no summary condition is
// generated.
func (l Conditions) SetSummary(options ...MergeOption) Conditions {

	return l.Set(summary(l, options...))
}

// SetMirror creates a new condition by mirroring the Ready condition from a
// source object. If the source object does not have a Ready condition, the
// target is not modified.
func (l Conditions) SetMirror(
	targetCondition string,
	from Getter,
	options ...MirrorOptions) Conditions {

	return l.Set(mirror(from, targetCondition, options...))
}

// SetAggregate creates a new condition by aggregating all of the Ready
// conditions from a list of source objects. If a source object is missing the
// Ready condition, that object is excluded from aggregation. If none of the
// source objects have a Ready condition, the target is not modified.
func (l Conditions) SetAggregate(
	targetCondition string,
	from []Getter,
	options ...MergeOption) Conditions {

	return l.Set(aggregate(from, targetCondition, options...))
}

// Delete removes the condition with the given type.
func (l Conditions) Delete(t string) Conditions {
	if len(l) == 0 {
		return l
	}
	newConditions := make([]metav1.Condition, 0, len(l))
	for _, c := range l {
		if c.Type != t {
			newConditions = append(newConditions, c)
		}
	}
	return newConditions
}

// hasSameState returns true if a condition has the same state of another; state
// is defined by the union of following fields: Type, Status, Reason, and
// Message. The field LastTransitionTime is excluded.
func hasSameState(a, b *metav1.Condition) bool {
	return a.Type == b.Type &&
		a.Status == b.Status &&
		a.Reason == b.Reason &&
		a.Message == b.Message
}

// lexicographicLess returns true if a condition is less than another with
// regards to the to order of conditions designed for convenience of the
// consumer, i.e. kubectl. According to this order the Ready condition always
// goes first, followed by all the other conditions sorted by Type.
func lexicographicLess(a, b *metav1.Condition) bool {
	return (a.Type == ReadyConditionType || a.Type < b.Type) &&
		b.Type != ReadyConditionType
}
