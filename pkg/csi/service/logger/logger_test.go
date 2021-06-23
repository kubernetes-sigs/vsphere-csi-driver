/*
Copyright 2021 The Kubernetes Authors.

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

package logger

import (
	"testing"

	"google.golang.org/grpc/codes"
)

func TestLogNewError(t *testing.T) {
	log := GetLoggerWithNoContext()
	e := LogNewError(log, "Error Test")
	if e == nil {
		t.Error("Failed to create an error")
	}
}

func TestLogNewErrorf(t *testing.T) {
	log := GetLoggerWithNoContext()
	e := LogNewErrorf(log, "%s", "Error Test")
	if e == nil {
		t.Error("Failed to create an error")
	}
}

func TestLogNewErrorCode(t *testing.T) {
	log := GetLoggerWithNoContext()
	e := LogNewErrorCode(log, codes.Unknown, "Error Test")
	if e == nil {
		t.Error("Failed to create an error")
	}
}

func TestLogNewErrorCodef(t *testing.T) {
	log := GetLoggerWithNoContext()
	e := LogNewErrorCodef(log, codes.Unknown, "%s", "Error Test")
	if e == nil {
		t.Error("Failed to create an error")
	}
}
