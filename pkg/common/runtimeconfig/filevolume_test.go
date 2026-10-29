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
	"errors"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
)

func TestFileVolumeFallbackPreservesCredentialRotation(t *testing.T) {
	for _, failure := range []string{"read error", "invalid boolean"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			source := newFileVolumeSource()
			cm := fileVolumeConfigMap(map[string]string{
				"sddc-1.fileVolumeActivated": "true", "sddc-1.targetvSANFileShareClusters": "cluster-1",
			})
			secret := credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "old")
			client := fake.NewSimpleClientset(secret, cm)
			if err := apply(ctx, client, ns, newConfig("csi", &config.VirtualCenterConfig{SDDCName: "sddc-1"}),
				source); err != nil {
				t.Fatal(err)
			}
			secret.Data["password"] = []byte("rotated")
			if _, err := client.CoreV1().Secrets(ns).Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if failure == "read error" {
				client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, context.DeadlineExceeded
				})
			} else {
				cm.Data["sddc-1.fileVolumeActivated"] = "invalid"
				if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			// Each reload starts with a fresh config Secret, which has no vSAN file-share settings.
			vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
			if err := apply(ctx, client, ns, newConfig("csi", vc), source); err != nil {
				t.Fatal(err)
			}
			if vc.Password != "rotated" || !vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "cluster-1" {
				t.Fatal("the reload must apply rotated credentials and retain the last successful vSAN settings")
			}
		})
	}
}

func TestFileVolumeFallbackTracksAuthoritativeChanges(t *testing.T) {
	for _, change := range []string{"disabled", "missing keys", "deleted"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			source := newFileVolumeSource()
			cm := fileVolumeConfigMap(map[string]string{
				"sddc-1.fileVolumeActivated": "true", "sddc-1.targetvSANFileShareClusters": "cluster-1",
			})
			client := fake.NewSimpleClientset(cm)
			cfg := func() *config.Config { return newConfig("", &config.VirtualCenterConfig{SDDCName: "sddc-1"}) }
			if err := source.apply(ctx, client, ns, cfg()); err != nil {
				t.Fatal(err)
			}
			if change == "deleted" {
				if err := client.CoreV1().ConfigMaps(ns).Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				cm.Data = map[string]string{}
				if change == "disabled" {
					cm.Data["sddc-1.fileVolumeActivated"] = "false"
				}
				if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := source.apply(ctx, client, ns, cfg()); err != nil {
				t.Fatal(err)
			}
			client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, context.DeadlineExceeded
			})
			vc := &config.VirtualCenterConfig{SDDCName: "sddc-1", FileVolumeActivated: true,
				TargetvSANFileShareClusters: "stale-ini"}
			if err := source.apply(ctx, client, ns, newConfig("", vc)); err == nil {
				t.Fatal("expected the injected read error")
			}
			if vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "" {
				t.Fatalf("fallback restored settings that were successfully removed: %+v", vc)
			}
		})
	}
}

func TestFileVolumeFallbackIsScopedToTheSource(t *testing.T) {
	for _, change := range []string{"namespace", "host", "SDDC"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			source := newFileVolumeSource()
			client := fake.NewSimpleClientset(fileVolumeConfigMap(map[string]string{
				"sddc-1.fileVolumeActivated": "true", "sddc-1.targetvSANFileShareClusters": "cluster-1",
			}))
			if err := source.apply(ctx, client, ns,
				newConfig("", &config.VirtualCenterConfig{SDDCName: "sddc-1"})); err != nil {
				t.Fatal(err)
			}
			client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, context.DeadlineExceeded
			})
			namespace := ns
			vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
			cfg := newConfig("", vc)
			switch change {
			case "namespace":
				namespace = "another-namespace"
			case "host":
				cfg.VirtualCenter = map[types.FQDN]*config.VirtualCenterConfig{types.NewFQDN("other.example.com"): vc}
			case "SDDC":
				vc.SDDCName = "sddc-2"
			}
			if err := source.apply(ctx, client, namespace, cfg); err == nil {
				t.Fatal("expected the injected read error")
			}
			if vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "" {
				t.Fatal("settings from a different source must not be reused")
			}
		})
	}
}

func TestFileVolumeRetriesWithoutAnObjectChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		source := newFileVolumeSource()
		client := fake.NewSimpleClientset(fileVolumeConfigMap(map[string]string{
			"sddc-1.fileVolumeActivated": "true", "sddc-1.targetvSANFileShareClusters": "cluster-1",
		}))
		// Do not let an initial Add event hide a missing retry signal from the failed read.
		client.PrependReactor("list", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, &corev1.ConfigMapList{}, nil
		})
		reads := 0
		client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			reads++
			if reads <= 3 {
				return true, nil, context.DeadlineExceeded
			}
			return false, nil, nil
		})
		cfg := newConfig("", &config.VirtualCenterConfig{SDDCName: "sddc-1"})
		// A failed first read must queue recovery even before the informer is started.
		if err := apply(ctx, client, ns, cfg, source); err != nil {
			t.Fatal(err)
		}
		recovered := make(chan struct{}, 10)
		if err := start(ctx, client, ns, cfg, func() error { return nil }, func() error {
			vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
			if err := apply(ctx, client, ns, newConfig("", vc), source); err != nil {
				return err
			}
			if vc.FileVolumeActivated && vc.TargetvSANFileShareClusters == "cluster-1" {
				recovered <- struct{}{}
			}
			return nil
		}, source); err != nil {
			t.Fatal(err)
		}
		select {
		case <-recovered:
		case <-time.After(5 * retryInterval):
			t.Fatal("a failed optional read was never retried without a ConfigMap change")
		}
	})
}

func TestFileVolumeReloadDoesNotLoseAFailureToAnotherRead(t *testing.T) {
	ctx := context.Background()
	source := newFileVolumeSource()
	client := fake.NewSimpleClientset()
	reads := 0
	client.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		reads++
		if reads == 1 {
			return true, nil, context.DeadlineExceeded
		}
		return false, nil, nil
	})
	read := func() error {
		return apply(ctx, client, ns, newConfig("", &config.VirtualCenterConfig{SDDCName: "sddc-1"}), source)
	}
	err := source.reload(func() error {
		if err := read(); err != nil {
			return err
		}
		// A successful read for another caller does not apply settings to this controller.
		return read()
	})()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reload error = %v, want the failed ConfigMap read", err)
	}
	if err := source.reload(read)(); err != nil {
		t.Fatalf("successful recovery still requests a retry: %v", err)
	}
}

func TestStartRetriesAFailedInitialCredentialsProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		secret := credSecret("s1", "csi", "", "u@vsphere.local", "rotated")
		client := fake.NewSimpleClientset(secret)
		lists := 0
		client.PrependReactor("list", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
			lists++
			// The initial informer list succeeds, then the drift probe and first reload fail.
			if lists == 2 || lists == 3 {
				return true, nil, context.DeadlineExceeded
			}
			return false, nil, nil
		})
		cfg := newConfig("csi", &config.VirtualCenterConfig{User: "u@vsphere.local", Password: "old"})
		recovered := make(chan struct{}, 10)
		if err := start(ctx, client, ns, cfg, func() error {
			vc := &config.VirtualCenterConfig{}
			if err := applyCredentials(ctx, client, ns, newConfig("csi", vc)); err != nil {
				return err
			}
			if vc.Password == "rotated" {
				recovered <- struct{}{}
			}
			return nil
		}, func() error { return nil }, newFileVolumeSource()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-recovered:
		case <-time.After(5 * retryInterval):
			t.Fatal("the failed initial probe never retried the rotated credentials")
		}
	})
}
