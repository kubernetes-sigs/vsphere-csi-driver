package conditions

/*
Copyright 2025 The Kubernetes Authors.

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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetCondition retrieves a pointer to the condition of the given type if it exists, else returns nil.
func GetCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// SetCondition adds or updates a condition in the conditions slice.
// It updates the LastTransitionTime only if the condition's status, reason, or message has changed.
func SetCondition(conditions []metav1.Condition, newCondition metav1.Condition) []metav1.Condition {
	now := metav1.Now()
	existing := GetCondition(conditions, newCondition.Type)

	if existing != nil {
		if existing.Status != newCondition.Status ||
			existing.Reason != newCondition.Reason ||
			existing.Message != newCondition.Message {
			existing.Status = newCondition.Status
			existing.Reason = newCondition.Reason
			existing.Message = newCondition.Message
			existing.LastTransitionTime = now
		}
	} else {
		newCondition.LastTransitionTime = now
		conditions = append(conditions, newCondition)
	}

	return conditions
}

// IsConditionTrue returns true if the specified condition exists and is set to "True".
func IsConditionTrue(conditions []metav1.Condition, conditionType string) bool {
	cond := GetCondition(conditions, conditionType)
	return cond != nil && cond.Status == metav1.ConditionTrue
}

// IsConditionFalse returns true if the specified condition exists and is set to "False".
func IsConditionFalse(conditions []metav1.Condition, conditionType string) bool {
	cond := GetCondition(conditions, conditionType)
	return cond != nil && cond.Status == metav1.ConditionFalse
}

// RemoveCondition removes a condition by type if it exists.
func RemoveCondition(conditions *[]metav1.Condition, conditionType string) {
	filtered := make([]metav1.Condition, 0, len(*conditions))
	for _, cond := range *conditions {
		if cond.Type != conditionType {
			filtered = append(filtered, cond)
		}
	}
	*conditions = filtered
}

// NewCondition returns a new Condition object with the given parameters.
// It sets the LastTransitionTime to the current time.
func NewCondition(conditionType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}
