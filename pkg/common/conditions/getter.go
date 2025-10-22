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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Getter interface defines methods that an object should implement in order to
// use the conditions package for getting conditions.
type Getter interface {
	// GetConditions returns the list of conditions for a cluster API object.
	GetConditions() []metav1.Condition
}

// Get returns the condition with the given type, otherwise nil is returned.
func Get(from Getter, t string) *metav1.Condition {
	return c4g(from).Get(t)
}

// Has returns true if a condition with the given type exists.
func Has(from Getter, t string) bool {
	return c4g(from).Has(t)
}

// IsTrue returns true if the condition with the given type exists and is
// True, otherwise false is returned.
func IsTrue(from Getter, t string) bool {
	return c4g(from).IsTrue(t)
}

// IsFalse returns true if the condition with the given type exists and is
// False, otherwise false is returned.
func IsFalse(from Getter, t string) bool {
	return c4g(from).IsFalse(t)
}

// IsUnknown returns true if the condition with the given type exists and is
// Unknown, otherwise false is returned.
func IsUnknown(from Getter, t string) bool {
	return c4g(from).IsUnknown(t)
}

// GetReason returns the condition's reason or an empty string if the condition
// does not exist.
func GetReason(from Getter, t string) string {
	return c4g(from).GetReason(t)
}

// GetMessage returns the condition's message or an empty string if the
// condition does not exist.
func GetMessage(from Getter, t string) string {
	return c4g(from).GetMessage(t)
}

// GetLastTransitionTime returns the condition's last transition time or a nil
// value if the condition does not exist.
func GetLastTransitionTime(from Getter, t string) *metav1.Time {
	return c4g(from).GetLastTransitionTime(t)
}

// summary returns a Ready condition with the summary of all the conditions existing
// on an object. If the object does not have other conditions, no summary condition is generated.
func summary(from Getter, options ...MergeOption) *metav1.Condition {
	conditions := from.GetConditions()

	mergeOpt := &mergeOptions{}
	for _, o := range options {
		o(mergeOpt)
	}

	// Identifies the conditions in scope for the Summary by taking all the existing conditions except Ready,
	// or, if a list of conditions types is specified, only the conditions the condition in that list.
	conditionsInScope := make([]localizedCondition, 0, len(conditions))
	for i := range conditions {
		c := conditions[i]
		if c.Type == ReadyConditionType {
			continue
		}

		if mergeOpt.conditionTypes != nil {
			found := false
			for _, t := range mergeOpt.conditionTypes {
				if c.Type == t {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		conditionsInScope = append(conditionsInScope, localizedCondition{
			Condition: &c,
			Getter:    from,
		})
	}

	// If it is required to add a step counter only if a subset of condition exists, check if the conditions
	// in scope are included in this subset or not.
	if mergeOpt.addStepCounterIfOnlyConditionTypes != nil {
		for _, c := range conditionsInScope {
			found := false
			for _, t := range mergeOpt.addStepCounterIfOnlyConditionTypes {
				if c.Type == t {
					found = true
					break
				}
			}
			if !found {
				mergeOpt.addStepCounter = false
				break
			}
		}
	}

	// If it is required to add a step counter, determine the total number of conditions defaulting
	// to the selected conditions or, if defined, to the total number of conditions type to be considered.
	if mergeOpt.addStepCounter {
		mergeOpt.stepCounter = len(conditionsInScope)
		if mergeOpt.conditionTypes != nil {
			mergeOpt.stepCounter = len(mergeOpt.conditionTypes)
		}
		if mergeOpt.addStepCounterIfOnlyConditionTypes != nil {
			mergeOpt.stepCounter = len(mergeOpt.addStepCounterIfOnlyConditionTypes)
		}
	}

	return merge(conditionsInScope, ReadyConditionType, mergeOpt)
}

// mirrorOptions allows to set options for the mirror operation.
type mirrorOptions struct {
	fallbackTo      *bool
	fallbackReason  string
	fallbackMessage string
}

// MirrorOptions defines an option for mirroring conditions.
type MirrorOptions func(*mirrorOptions)

// WithFallbackValue specify a fallback value to use in case the mirrored condition does not exists;
// in case the fallbackValue is false, given values for reason, severity and message will be used.
func WithFallbackValue(fallbackValue bool, reason string, message string) MirrorOptions {
	return func(c *mirrorOptions) {
		c.fallbackTo = &fallbackValue
		c.fallbackReason = reason
		c.fallbackMessage = message
	}
}

// mirror mirrors the Ready condition from a dependent object into the target condition;
// if the Ready condition does not exists in the source object, no target conditions is generated.
func mirror(from Getter, targetCondition string, options ...MirrorOptions) *metav1.Condition {
	mirrorOpt := &mirrorOptions{}
	for _, o := range options {
		o(mirrorOpt)
	}

	condition := Get(from, ReadyConditionType)

	if mirrorOpt.fallbackTo != nil && condition == nil {
		switch *mirrorOpt.fallbackTo {
		case true:
			condition = TrueCondition(targetCondition)
		case false:
			condition = FalseCondition(targetCondition, mirrorOpt.fallbackReason, "%s", mirrorOpt.fallbackMessage)
		}
	}

	if condition != nil {
		condition.Type = targetCondition
	}

	return condition
}

// Aggregates all the Ready condition from a list of dependent objects into the target object;
// if the Ready condition does not exists in one of the source object, the object is excluded from
// the aggregation; if none of the source object have ready condition, no target conditions is generated.
func aggregate(from []Getter, targetCondition string, options ...MergeOption) *metav1.Condition {
	conditionsInScope := make([]localizedCondition, 0, len(from))
	for i := range from {
		condition := Get(from[i], ReadyConditionType)

		conditionsInScope = append(conditionsInScope, localizedCondition{
			Condition: condition,
			Getter:    from[i],
		})
	}

	mergeOpt := &mergeOptions{
		addStepCounter: true,
		stepCounter:    len(from),
	}
	for _, o := range options {
		o(mergeOpt)
	}
	return merge(conditionsInScope, targetCondition, mergeOpt)
}
