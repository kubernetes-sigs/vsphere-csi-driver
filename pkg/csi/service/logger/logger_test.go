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
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/container-storage-interface/spec/lib/go/csi"
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

func BenchmarkLogNewError(b *testing.B) {
	log := GetLoggerWithNoContext()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := LogNewError(log, "Error Benchmark")
		if e == nil {
			b.Error("Failed to create an error")
		}
	}
}

func BenchmarkLogNewErrorf(b *testing.B) {
	log := GetLoggerWithNoContext()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := LogNewErrorf(log, "%s", "Error Benchmark")
		if e == nil {
			b.Error("Failed to create an error")
		}
	}
}

func BenchmarkLogNewErrorCode(b *testing.B) {
	log := GetLoggerWithNoContext()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := LogNewErrorCode(log, codes.Unknown, "Error Benchmark")
		if e == nil {
			b.Error("Failed to create an error")
		}
	}
}

func BenchmarkLogNewErrorCodef(b *testing.B) {
	log := GetLoggerWithNoContext()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := LogNewErrorCodef(log, codes.Unknown, "%s", "Error Benchmark")
		if e == nil {
			b.Error("Failed to create an error")
		}
	}
}

func TestRedactCSIRequestStripsSecrets(t *testing.T) {
	req := &csi.CreateVolumeRequest{
		Name:    "test-volume",
		Secrets: map[string]string{"username": "admin", "password": "topsecret"},
	}

	redacted := RedactCSIRequest(req)

	out := fmt.Sprintf("%+v", redacted)
	if strings.Contains(out, "topsecret") || strings.Contains(out, "admin") {
		t.Errorf("secret leaked in redacted output: %s", out)
	}

	rv, ok := redacted.(*csi.CreateVolumeRequest)
	if !ok {
		t.Fatalf("expected *csi.CreateVolumeRequest, got %T", redacted)
	}
	if len(rv.Secrets) != 1 || rv.Secrets["***stripped***"] != "***stripped***" {
		t.Errorf("expected Secrets to be replaced with stripped marker, got %v", rv.Secrets)
	}
}

func TestRedactCSIRequestDoesNotMutateOriginal(t *testing.T) {
	req := &csi.CreateVolumeRequest{
		Name:    "test-volume",
		Secrets: map[string]string{"password": "topsecret"},
	}

	_ = RedactCSIRequest(req)

	if req.Secrets["password"] != "topsecret" {
		t.Errorf("original request was mutated, Secrets = %v", req.Secrets)
	}
}

func TestRedactCSIRequestEmptySecretsUnchanged(t *testing.T) {
	req := &csi.CreateVolumeRequest{Name: "test-volume"}

	redacted := RedactCSIRequest(req)

	rv, ok := redacted.(*csi.CreateVolumeRequest)
	if !ok {
		t.Fatalf("expected *csi.CreateVolumeRequest, got %T", redacted)
	}
	if rv != req {
		t.Errorf("expected the same request pointer to be returned unchanged when Secrets is empty")
	}
}

func TestRedactCSIRequestNonSecretBearingTypeUnchanged(t *testing.T) {
	req := &csi.GetCapacityRequest{}

	redacted := RedactCSIRequest(req)

	if redacted != req {
		t.Errorf("expected request without a Secrets field to be returned unchanged")
	}
}

func TestRedactCSIRequestNilPointerDoesNotPanic(t *testing.T) {
	var req *csi.CreateVolumeRequest

	redacted := RedactCSIRequest(req)

	if redacted != req {
		t.Errorf("expected nil pointer to be returned unchanged")
	}
}

func TestRedactCSIRequestNonPointerUnchanged(t *testing.T) {
	req := "not a csi request"

	redacted := RedactCSIRequest(req)

	if redacted != req {
		t.Errorf("expected non-secretsGetter value to be returned unchanged")
	}
}
