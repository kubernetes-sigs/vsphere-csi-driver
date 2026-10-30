/*
Copyright 2026 The Kubernetes Authors.

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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// loadCnsRegisterVolumeCRD parses the CRD the syncer actually embeds and applies at
// startup, so these assertions are about the shipped artifact rather than a copy.
func loadCnsRegisterVolumeCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	data, err := EmbedCnsRegisterVolumeCRFile.ReadFile(EmbedCnsRegisterVolumeCRFileName)
	require.NoError(t, err)

	crd := &apiextensionsv1.CustomResourceDefinition{}
	require.NoError(t, yaml.Unmarshal(data, crd))
	return crd
}

func cnsRegisterVolumeStatusSchema(t *testing.T) *apiextensionsv1.JSONSchemaProps {
	t.Helper()
	crd := loadCnsRegisterVolumeCRD(t)
	require.Len(t, crd.Spec.Versions, 1)
	status, ok := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"]
	require.True(t, ok, "the CRD must declare a status schema")
	return &status
}

// TestCRDDeclaresConditionsAndTaskID is the guard for the central hazard of this
// change. The schema is structural, so a status field the API server does not know
// about is silently pruned on write: the controller would appear to publish
// conditions, every write would succeed, and the stored object would never carry
// them. Only the shipped CRD can rule that out.
func TestCRDDeclaresConditionsAndTaskID(t *testing.T) {
	status := cnsRegisterVolumeStatusSchema(t)

	conditions, ok := status.Properties["conditions"]
	require.True(t, ok, "status.conditions must be declared in the CRD or the API server prunes it")
	assert.Equal(t, "array", conditions.Type)
	require.NotNil(t, conditions.Items, "conditions must declare its item schema")
	require.NotNil(t, conditions.Items.Schema)

	item := conditions.Items.Schema
	for _, field := range []string{"type", "status", "reason", "message", "lastTransitionTime"} {
		assert.Contains(t, item.Properties, field,
			"a condition must declare %s or the API server prunes it", field)
	}
	assert.ElementsMatch(t, []string{"lastTransitionTime", "message", "reason", "status", "type"},
		item.Required, "metav1.Condition requires all five of these keys")

	// reason is the diagnostic payload, so its constraint is part of the contract:
	// every reason the controller publishes has to satisfy it.
	reason := item.Properties["reason"]
	require.NotNil(t, reason.MinLength)
	assert.EqualValues(t, 1, *reason.MinLength, "reason may not be empty")
	assert.Equal(t, "^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$", reason.Pattern)

	// Declared as a map list keyed by type, so a condition is addressable by type
	// rather than by array index.
	require.NotNil(t, conditions.XListType)
	assert.Equal(t, "map", *conditions.XListType)
	assert.Equal(t, []string{"type"}, conditions.XListMapKeys)

	cnsTaskID, ok := status.Properties["cnsTaskID"]
	require.True(t, ok, "status.cnsTaskID must be declared in the CRD or the API server prunes it")
	assert.Equal(t, "string", cnsTaskID.Type)
}

// TestCRDDoesNotDeclareRemovedFields verifies the superseded fields are gone. They
// were replaced by conditions, which carry the same progress plus a reason and a
// per-step lastTransitionTime.
func TestCRDDoesNotDeclareRemovedFields(t *testing.T) {
	status := cnsRegisterVolumeStatusSchema(t)
	assert.NotContains(t, status.Properties, "phase",
		"phase was replaced by status.conditions")
	assert.NotContains(t, status.Properties, "lastUpdateTime",
		"lastUpdateTime was replaced by each condition's lastTransitionTime")
}

// TestCRDStatusRemainsBackwardsCompatible verifies the change is purely additive:
// the fields existing consumers read are untouched, and registered stays required so
// the merge-patch behaviour in patchCnsRegisterVolumeStatus is unchanged.
func TestCRDStatusRemainsBackwardsCompatible(t *testing.T) {
	status := cnsRegisterVolumeStatusSchema(t)

	registered, ok := status.Properties["registered"]
	require.True(t, ok)
	assert.Equal(t, "boolean", registered.Type)

	statusErr, ok := status.Properties["error"]
	require.True(t, ok)
	assert.Equal(t, "string", statusErr.Type)

	assert.Equal(t, []string{"registered"}, status.Required,
		"registered must stay required; patchCnsRegisterVolumeStatus always sends it for that reason")

	// The new fields must not be required, so CRs written by an older controller
	// stay valid.
	assert.NotContains(t, status.Required, "conditions")
	assert.NotContains(t, status.Required, "cnsTaskID")
}

// TestCRDSpecIsUnchanged verifies no spec field was disturbed; the spec is the half
// of the API that clients write, so a change there would be a breaking change.
func TestCRDSpecIsUnchanged(t *testing.T) {
	crd := loadCnsRegisterVolumeCRD(t)
	spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]

	for _, field := range []string{
		"pvcName", "volumeID", "accessMode", "diskURLPath",
		"volumeMode", "backingType", "storageClassName",
	} {
		assert.Contains(t, spec.Properties, field, "spec.%s must still be declared", field)
	}
	assert.Equal(t, []string{"pvcName"}, spec.Required)
}

// TestCRDPrinterColumns verifies the readiness and its reason are visible from
// kubectl, which is the main way an operator would use them.
func TestCRDPrinterColumns(t *testing.T) {
	crd := loadCnsRegisterVolumeCRD(t)
	columns := crd.Spec.Versions[0].AdditionalPrinterColumns

	byName := map[string]apiextensionsv1.CustomResourceColumnDefinition{}
	for _, c := range columns {
		byName[c.Name] = c
	}

	ready, ok := byName["Ready"]
	require.True(t, ok, "kubectl get cnsregistervolume should show readiness")
	assert.Equal(t, `.status.conditions[?(@.type=="Ready")].status`, ready.JSONPath)

	reasonCol, ok := byName["Reason"]
	require.True(t, ok, "kubectl get cnsregistervolume should show why it is not ready")
	assert.Equal(t, `.status.conditions[?(@.type=="Ready")].reason`, reasonCol.JSONPath)

	assert.NotContains(t, byName, "Phase", "the phase column was replaced by Ready and Reason")

	registered, ok := byName["Registered"]
	require.True(t, ok, "registered is the completion signal and must stay visible")
	assert.Equal(t, ".status.registered", registered.JSONPath)
}
