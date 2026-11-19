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

package runtimeconfig

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const migrationKey = "sddc-1.fileVolumeActivated"

func legacySecret(ini string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: legacyConfigSecretName, Namespace: ns},
		Data:       map[string][]byte{legacyConfigSecretKey: []byte(ini)},
	}
}

func legacyINI(activated string) string {
	return fmt.Sprintf(`[Global]
cluster-id = "domain-c1"
[VirtualCenter "vc.example.com"]
user = "administrator@vsphere.local"
password = "hunter2-secret"
fileVolumeActivated = %s
`, activated)
}

func updatesOf(client *fake.Clientset) int {
	n := 0
	for _, a := range client.Actions() {
		if a.Matches("update", "configmaps") {
			n++
		}
	}
	return n
}

func migratedValue(t *testing.T, client *fake.Clientset) (string, bool) {
	t.Helper()
	cm, err := client.CoreV1().ConfigMaps(ns).Get(context.Background(), FileVolumeConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := cm.Data[migrationKey]
	return v, ok
}

func TestMigrateFileVolumeConfigMapsLegacyActivation(t *testing.T) {
	tests := []struct {
		name   string
		secret *corev1.Secret
		want   string
	}{
		{"legacy true", legacySecret(legacyINI("true")), "true"},
		{"legacy false", legacySecret(legacyINI("false")), "false"},
		{"legacy key absent", legacySecret(strings.ReplaceAll(legacyINI("true"), "fileVolumeActivated = true\n", "")),
			"false"},
		{"package-rendered INI", legacySecret("[Global]\nvsphere-service-account = \"csi\"\n" +
			"[VirtualCenter \"vc.example.com\"]\nsddc-name = \"sddc-1\"\nfileVolumeActivated = true\n"), "false"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(tc.secret, fileVolumeConfigMap(nil))
			if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
				t.Fatal(err)
			}
			if got, ok := migratedValue(t, client); !ok || got != tc.want {
				t.Fatalf("%s = %q (present %v), want %q", migrationKey, got, ok, tc.want)
			}
		})
	}
}

func TestMigrateFileVolumeConfigMissingSecretWritesFalse(t *testing.T) {
	client := fake.NewSimpleClientset(fileVolumeConfigMap(nil))
	if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
		t.Fatal(err)
	}
	if got, ok := migratedValue(t, client); !ok || got != "false" {
		t.Fatalf("%s = %q (present %v), want false", migrationKey, got, ok)
	}
}

func TestMigrateFileVolumeConfigNeverOverwritesExistingKey(t *testing.T) {
	// An administrator turned file volumes off after an earlier run; a rerun against a legacy "true" must not
	// bring them back.
	client := fake.NewSimpleClientset(legacySecret(legacyINI("true")),
		fileVolumeConfigMap(map[string]string{migrationKey: "false", "other.fileVolumeActivated": "true"}))
	if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := migratedValue(t, client); got != "false" {
		t.Fatalf("%s = %q, want the existing false to survive", migrationKey, got)
	}
	if n := updatesOf(client); n != 0 {
		t.Fatalf("got %d ConfigMap updates, want none when the key is present", n)
	}
}

func TestMigrateFileVolumeConfigExistingKeyIgnoresLegacySecret(t *testing.T) {
	// A rerun of the versioned Job must succeed once the key exists, whatever the Secret holds.
	for name, secret := range map[string]*corev1.Secret{
		"unparsable": legacySecret("[Global\npassword = \"hunter2-secret\"\n"),
		"empty":      legacySecret(""),
		"missing":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			objs := []runtime.Object{fileVolumeConfigMap(map[string]string{migrationKey: "true"})}
			if secret != nil {
				objs = append(objs, secret)
			}
			client := fake.NewSimpleClientset(objs...)
			if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
				t.Fatal(err)
			}
			if n := updatesOf(client); n != 0 {
				t.Fatalf("got %d ConfigMap updates, want none", n)
			}
		})
	}
}

func TestMigrateFileVolumeConfigIsIdempotent(t *testing.T) {
	client := fake.NewSimpleClientset(legacySecret(legacyINI("true")), fileVolumeConfigMap(nil))
	for i := 0; i < 3; i++ {
		if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	if got, _ := migratedValue(t, client); got != "true" {
		t.Fatalf("%s = %q, want true", migrationKey, got)
	}
	if n := updatesOf(client); n != 1 {
		t.Fatalf("got %d ConfigMap updates over three runs, want exactly one", n)
	}
}

func TestMigrateFileVolumeConfigMissingConfigMapFails(t *testing.T) {
	client := fake.NewSimpleClientset(legacySecret(legacyINI("true")))
	if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err == nil {
		t.Fatal("want an error when the runtime ConfigMap does not exist")
	}
}

func TestMigrateFileVolumeConfigRetriesConflictAndWritesOnce(t *testing.T) {
	client := fake.NewSimpleClientset(legacySecret(legacyINI("true")), fileVolumeConfigMap(nil))
	conflicts := 1
	client.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		if conflicts > 0 {
			conflicts--
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"},
				FileVolumeConfigMapName, fmt.Errorf("the object has been modified"))
		}
		return false, nil, nil
	})
	if err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1"); err != nil {
		t.Fatal(err)
	}
	if got, ok := migratedValue(t, client); !ok || got != "true" {
		t.Fatalf("%s = %q (present %v), want true after the retry", migrationKey, got, ok)
	}
}

func TestMigrateFileVolumeConfigBadLegacyINIFailsWithoutLeakingSecrets(t *testing.T) {
	tests := []struct {
		name string
		ini  string
	}{
		{"unparsable", "[Global\ncluster-id = domain-c1\npassword = \"hunter2-secret\"\n"},
		{"no VirtualCenter", "[Global]\ncluster-id = \"domain-c1\"\n"},
		{"two VirtualCenters", legacyINI("true") + "[VirtualCenter \"vc2.example.com\"]\n" +
			"user = \"u\"\npassword = \"hunter2-secret\"\n"},
		{"empty document", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewSimpleClientset(legacySecret(tc.ini), fileVolumeConfigMap(nil))
			err := MigrateFileVolumeConfig(context.Background(), client, ns, "sddc-1")
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), "hunter2-secret") {
				t.Fatalf("error leaks the password: %v", err)
			}
			if _, ok := migratedValue(t, client); ok {
				t.Fatalf("%s must not be written when the legacy INI is unusable", migrationKey)
			}
		})
	}
}

func TestMigrateFileVolumeConfigRejectsInvalidNames(t *testing.T) {
	for _, tc := range []struct{ namespace, sddc string }{
		{"", "sddc-1"}, {"Bad_NS", "sddc-1"}, {ns, ""}, {ns, "Not A Name"},
	} {
		client := fake.NewSimpleClientset(fileVolumeConfigMap(nil))
		if err := MigrateFileVolumeConfig(context.Background(), client, tc.namespace, tc.sddc); err == nil {
			t.Fatalf("namespace %q, sddc %q: want an error", tc.namespace, tc.sddc)
		}
		if n := len(client.Actions()); n != 0 {
			t.Fatalf("namespace %q, sddc %q: made %d API calls before validating", tc.namespace, tc.sddc, n)
		}
	}
}
