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

package syncer

import (
	"context"
	"testing"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestMigrateStorageClassExpansion(t *testing.T) {
	yes, no := true, false
	sc := func(name, provisioner string, expansion *bool) *storagev1.StorageClass {
		return &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: provisioner,
			AllowVolumeExpansion: expansion}
	}
	client := fake.NewSimpleClientset(
		sc("unset", vSphereCSIProvisioner, nil),
		sc("disabled", vSphereCSIProvisioner, &no),
		sc("enabled", vSphereCSIProvisioner, &yes),
		sc("other", "example.com/other", nil),
	)
	ctx := context.Background()
	if err := MigrateStorageClassExpansion(ctx, client); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]bool{"unset": true, "disabled": true, "enabled": true, "other": false}
	for name, expansion := range want {
		got, err := client.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if name == "other" {
			if got.AllowVolumeExpansion != nil {
				t.Errorf("%s: another provisioner's StorageClass was modified", name)
			}
			continue
		}
		if got.AllowVolumeExpansion == nil || *got.AllowVolumeExpansion != expansion {
			t.Errorf("%s: allowVolumeExpansion = %v, want %v", name, got.AllowVolumeExpansion, expansion)
		}
	}

	// A second run finds nothing to patch.
	patches := 0
	client.PrependReactor("patch", "storageclasses", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil
	})
	if err := MigrateStorageClassExpansion(ctx, client); err != nil || patches != 0 {
		t.Errorf("second run: err=%v patches=%d, want no error and no patches", err, patches)
	}
}
