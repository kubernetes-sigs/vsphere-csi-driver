/*
Copyright 2019 The Kubernetes Authors.

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

package types

import "strings"

// CaseInsensitiveMap is a case-insensitive map for string keys.
// All keys are stored and compared in lowercase to handle DNS names and FQDNs.
type CaseInsensitiveMap[V any] map[string]V

// NewCaseInsensitiveMap creates a new case-insensitive map.
func NewCaseInsensitiveMap[V any]() CaseInsensitiveMap[V] {
	return make(CaseInsensitiveMap[V])
}

// Set stores a value with a case-insensitive key.
func (m CaseInsensitiveMap[V]) Set(key string, value V) {
	m[strings.ToLower(key)] = value
}

// Get retrieves a value by case-insensitive key.
func (m CaseInsensitiveMap[V]) Get(key string) (V, bool) {
	val, ok := m[strings.ToLower(key)]
	return val, ok
}

// Exists checks if a key exists with case-insensitive comparison.
func (m CaseInsensitiveMap[V]) Exists(key string) bool {
	_, ok := m[strings.ToLower(key)]
	return ok
}

// Delete removes the entry for a case-insensitive key.
func (m CaseInsensitiveMap[V]) Delete(key string) {
	delete(m, strings.ToLower(key))
}

// Normalize re-keys every entry to lowercase. Callers that populate the map
// through means other than Set - e.g. gcfg's reflection-based section
// parsing, which assigns section names as map keys directly and never goes
// through Set - must call Normalize afterwards so Get/Exists/Delete can find
// those entries.
func (m CaseInsensitiveMap[V]) Normalize() {
	updates := make(map[string]V)
	for k, v := range m {
		if lk := strings.ToLower(k); lk != k {
			updates[lk] = v
			delete(m, k)
		}
	}
	for k, v := range updates {
		m[k] = v
	}
}
