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

package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testCRDName = "widgets.example.com"

func testCRD(kind string, established bool) *apiextensionsv1.CustomResourceDefinition {
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: testCRDName},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "example.com",
			Names: apiextensionsv1.CustomResourceDefinitionNames{Kind: kind, Plural: "widgets"},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}
	if established {
		crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{{
			Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue,
		}}
	}
	return crd
}

func TestCreateOrUpdateCustomResourceDefinitionUpdatesEstablishedCRD(t *testing.T) {
	ctx := context.Background()
	clientSet := apiextensionsfake.NewClientset(testCRD("OldWidget", true))
	err := createOrUpdateCustomResourceDefinition(ctx, clientSet, testCRD("Widget", false),
		time.Millisecond, time.Second)
	require.NoError(t, err)
	crd, err := clientSet.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, testCRDName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "Widget", crd.Spec.Names.Kind)
	assert.NotEmpty(t, crd.Status.Conditions, "the status of the existing CRD is kept")
}

func TestCreateOrUpdateCustomResourceDefinitionNeverDeletes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing *apiextensionsv1.CustomResourceDefinition
	}{
		{name: "created CRD not established in time"},
		{name: "updated CRD not established in time", existing: testCRD("OldWidget", false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clientSet := apiextensionsfake.NewClientset()
			if tc.existing != nil {
				clientSet = apiextensionsfake.NewClientset(tc.existing)
			}
			err := createOrUpdateCustomResourceDefinition(ctx, clientSet, testCRD("Widget", false),
				time.Millisecond, 20*time.Millisecond)
			require.ErrorContains(t, err, "is not established")
			_, err = clientSet.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, testCRDName,
				metav1.GetOptions{})
			require.NoError(t, err, "the CRD must not be deleted")
			for _, action := range clientSet.Actions() {
				assert.NotEqual(t, "delete", action.GetVerb())
			}
		})
	}
}
