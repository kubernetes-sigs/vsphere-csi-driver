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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/unittestcommon"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"
)

const ns = "vmware-system-csi"

func TestBound(t *testing.T) {
	long := strings.Repeat("a", 64)
	for name, tc := range map[string]struct{ in, want string }{
		"short name is kept":       {"csi", "csi"},
		"mixed case is lowered":    {"CSI-Driver", "csi-driver"},
		"63 characters fit":        {strings.Repeat("a", 63), strings.Repeat("a", 63)},
		"64 characters are hashed": {long, "ffe054fe7ae0cb6d"},
		"hash is of the lowercase": {strings.ToUpper(long), "ffe054fe7ae0cb6d"},
	} {
		if got := Bound(tc.in); got != tc.want {
			t.Errorf("%s: Bound(%q) = %q, want %q", name, tc.in, got, tc.want)
		}
	}
}

func newConfig(account string, vcs ...*config.VirtualCenterConfig) *config.Config {
	cfg := &config.Config{VirtualCenter: map[types.FQDN]*config.VirtualCenterConfig{}}
	cfg.Global.VSphereServiceAccount = account
	hosts := []string{"vc1.example.com", "vc2.example.com"}
	for i, vc := range vcs {
		cfg.VirtualCenter[types.NewFQDN(hosts[i])] = vc
	}
	return cfg
}

func credSecret(name, account, sddc, user, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{
			LabelServiceAccount: Bound(account), LabelSDDC: Bound(sddc)}},
		Data: map[string][]byte{"username": []byte(user), "password": []byte(password)},
	}
}

func TestApplyCredentials(t *testing.T) {
	ctx := context.Background()
	good := credSecret("s1", "csi", "sddc-1", "csi-user@vsphere.local", "pw1")

	t.Run("fills user and password", func(t *testing.T) {
		vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
		if err := applyCredentials(ctx, fake.NewSimpleClientset(good), ns, newConfig("csi", vc)); err != nil {
			t.Fatal(err)
		}
		if vc.User != "csi-user@vsphere.local" || vc.Password != "pw1" {
			t.Errorf("got %q/%q", vc.User, vc.Password)
		}
	})
	t.Run("does nothing without a service account", func(t *testing.T) {
		vc := &config.VirtualCenterConfig{User: "kept@vsphere.local", Password: "kept"}
		if err := applyCredentials(ctx, fake.NewSimpleClientset(good), ns, newConfig("", vc)); err != nil {
			t.Fatal(err)
		}
		if vc.User != "kept@vsphere.local" || vc.Password != "kept" {
			t.Errorf("credentials changed: %q/%q", vc.User, vc.Password)
		}
	})
	t.Run("selects by service account and SDDC", func(t *testing.T) {
		other := credSecret("s2", "csi", "sddc-2", "other@vsphere.local", "pw2")
		otherAccount := credSecret("s3", "another", "sddc-1", "x@vsphere.local", "pw3")
		vc1 := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
		vc2 := &config.VirtualCenterConfig{SDDCName: "SDDC-2"}
		cfg := newConfig("csi", vc1, vc2)
		if err := applyCredentials(ctx, fake.NewSimpleClientset(good, other, otherAccount), ns, cfg); err != nil {
			t.Fatal(err)
		}
		if vc1.User != "csi-user@vsphere.local" || vc2.User != "other@vsphere.local" {
			t.Errorf("got %q and %q", vc1.User, vc2.User)
		}
	})
	t.Run("long names are matched by their hash", func(t *testing.T) {
		account := strings.Repeat("A", 70)
		vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
		secret := credSecret("s1", account, "sddc-1", "u@vsphere.local", "pw")
		if err := applyCredentials(ctx, fake.NewSimpleClientset(secret), ns, newConfig(account, vc)); err != nil {
			t.Fatal(err)
		}
		if vc.User != "u@vsphere.local" {
			t.Errorf("got %q", vc.User)
		}
	})
	for name, tc := range map[string]struct {
		secrets []*corev1.Secret
		want    string
	}{
		"missing":   {nil, "found 0"},
		"duplicate": {[]*corev1.Secret{good, credSecret("dup", "csi", "sddc-1", "d@vsphere.local", "p")}, "found 2"},
		"no password": {[]*corev1.Secret{credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "")},
			"has no"},
		"unqualified user": {[]*corev1.Secret{credSecret("s1", "csi", "sddc-1", "Administrator", "p")},
			"invalid"},
	} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			for _, s := range tc.secrets {
				if _, err := client.CoreV1().Secrets(ns).Create(ctx, s, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
			err := applyCredentials(ctx, client, ns, newConfig("csi", vc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want it to contain %q", err, tc.want)
			}
			if vc.User != "" || vc.Password != "" {
				t.Errorf("credentials set despite the error: %q/%q", vc.User, vc.Password)
			}
		})
	}
	t.Run("rotation is picked up on the next read", func(t *testing.T) {
		client := fake.NewSimpleClientset(good)
		vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
		cfg := newConfig("csi", vc)
		if err := applyCredentials(ctx, client, ns, cfg); err != nil {
			t.Fatal(err)
		}
		rotated := good.DeepCopy()
		rotated.Data["password"] = []byte("pw-rotated")
		if _, err := client.CoreV1().Secrets(ns).Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := applyCredentials(ctx, client, ns, cfg); err != nil {
			t.Fatal(err)
		}
		if vc.Password != "pw-rotated" {
			t.Errorf("password = %q", vc.Password)
		}
	})
}

func fileVolumeConfigMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: FileVolumeConfigMapName, Namespace: ns}, Data: data}
}

func TestApplyFileVolumeConfig(t *testing.T) {
	ctx := context.Background()
	newVC := func() *config.VirtualCenterConfig {
		return &config.VirtualCenterConfig{SDDCName: "sddc-1", FileVolumeActivated: true,
			TargetvSANFileShareClusters: "from-ini"}
	}

	t.Run("without the ConfigMap the ini values stay", func(t *testing.T) {
		vc := newVC()
		if err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(), ns, newConfig("csi", vc)); err != nil {
			t.Fatal(err)
		}
		if !vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "from-ini" {
			t.Errorf("ini values changed: %+v", vc)
		}
	})
	t.Run("present keys override the ini", func(t *testing.T) {
		vc := newVC()
		cm := fileVolumeConfigMap(map[string]string{
			"sddc-1.fileVolumeActivated":         "false",
			"sddc-1.targetvSANFileShareClusters": " domain-c1,domain-c2 ",
		})
		if err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("csi", vc)); err != nil {
			t.Fatal(err)
		}
		if vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "domain-c1,domain-c2" {
			t.Errorf("got %+v", vc)
		}
	})
	t.Run("missing keys mean off", func(t *testing.T) {
		vc := newVC()
		cm := fileVolumeConfigMap(map[string]string{"other-sddc.fileVolumeActivated": "true"})
		if err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("csi", vc)); err != nil {
			t.Fatal(err)
		}
		if vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "" {
			t.Errorf("got %+v", vc)
		}
	})
	t.Run("partial keys", func(t *testing.T) {
		vc := newVC()
		cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "true"})
		if err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("csi", vc)); err != nil {
			t.Fatal(err)
		}
		if !vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "" {
			t.Errorf("got %+v", vc)
		}
	})
	t.Run("a vCenter without sddc-name keeps the ini values", func(t *testing.T) {
		vc := newVC()
		vc.SDDCName = ""
		cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "false"})
		if err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("", vc)); err != nil {
			t.Fatal(err)
		}
		if !vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "from-ini" {
			t.Errorf("got %+v", vc)
		}
	})
	t.Run("updated ConfigMap is picked up on the next read", func(t *testing.T) {
		client := fake.NewSimpleClientset(fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "false"}))
		vc := newVC()
		cfg := newConfig("csi", vc)
		if err := applyFileVolumeConfig(ctx, client, ns, cfg); err != nil || vc.FileVolumeActivated {
			t.Fatalf("first read: err=%v vc=%+v", err, vc)
		}
		cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "true"})
		if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := applyFileVolumeConfig(ctx, client, ns, cfg); err != nil || !vc.FileVolumeActivated {
			t.Fatalf("second read: err=%v vc=%+v", err, vc)
		}
	})
	t.Run("a malformed boolean is an error", func(t *testing.T) {
		cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "maybe"})
		err := applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("csi", newVC()))
		if err == nil || !strings.Contains(err.Error(), "not a boolean") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("a malformed ConfigMap leaves the config untouched", func(t *testing.T) {
		vc := newVC()
		cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "maybe"})
		_ = applyFileVolumeConfig(ctx, fake.NewSimpleClientset(cm), ns, newConfig("csi", vc))
		if !vc.FileVolumeActivated || vc.TargetvSANFileShareClusters != "from-ini" {
			t.Errorf("got %+v", vc)
		}
	})
}

// useFSS makes Enabled report the given state for a Supervisor.
func useFSS(t *testing.T, enabled bool) {
	t.Helper()
	t.Setenv("CLUSTER_FLAVOR", "WORKLOAD")
	previous := commonco.ContainerOrchestratorUtility
	t.Cleanup(func() { commonco.ContainerOrchestratorUtility = previous })
	co, err := unittestcommon.GetFakeContainerOrchestratorInterface(common.Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	commonco.ContainerOrchestratorUtility = co
	latched.Store(stateUnset)
	if enabled {
		for _, name := range []string{common.VSphereCSIRuntimeConfig, common.SupportsSelfManagedMode} {
			if err := co.EnableFSS(context.Background(), name); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestEnabled(t *testing.T) {
	ctx := context.Background()
	useFSS(t, false)
	if Enabled(ctx) {
		t.Error("enabled with the FSS off")
	}
	useFSS(t, true)
	if !Enabled(ctx) {
		t.Error("disabled with the FSS on")
	}
	for _, only := range []string{common.VSphereCSIRuntimeConfig, common.SupportsSelfManagedMode} {
		useFSS(t, false)
		if err := commonco.ContainerOrchestratorUtility.EnableFSS(ctx, only); err != nil {
			t.Fatal(err)
		}
		if Enabled(ctx) {
			t.Errorf("enabled with only %s on", only)
		}
	}
	useFSS(t, true)
	t.Setenv("CLUSTER_FLAVOR", "VANILLA")
	if Enabled(ctx) {
		t.Error("enabled on a non-Supervisor flavor")
	}
	commonco.ContainerOrchestratorUtility = nil
	t.Setenv("CLUSTER_FLAVOR", "WORKLOAD")
	if Enabled(ctx) {
		t.Error("enabled before the container orchestrator utility exists")
	}
}

func TestProcess(t *testing.T) {
	ctx := context.Background()
	previousClient, previousNew := cachedClient, newClient
	t.Cleanup(func() { cachedClient, newClient = previousClient, previousNew })
	secret := credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "pw")
	var client kubernetes.Interface = fake.NewSimpleClientset(secret)
	cachedClient = client

	t.Run("FSS off leaves the config alone", func(t *testing.T) {
		useFSS(t, false)
		vc := &config.VirtualCenterConfig{User: "kept@vsphere.local", Password: "kept"}
		if err := process(ctx, newConfig("", vc)); err != nil || vc.User != "kept@vsphere.local" {
			t.Errorf("err=%v vc=%+v", err, vc)
		}
	})
	t.Run("FSS off with a service account is an error", func(t *testing.T) {
		useFSS(t, false)
		err := process(ctx, newConfig("csi", &config.VirtualCenterConfig{SDDCName: "sddc-1"}))
		if err == nil || !strings.Contains(err.Error(), common.VSphereCSIRuntimeConfig) {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("FSS on applies the runtime sources", func(t *testing.T) {
		useFSS(t, true)
		vc := &config.VirtualCenterConfig{SDDCName: "sddc-1"}
		if err := process(ctx, newConfig("csi", vc)); err != nil || vc.User != "u@vsphere.local" {
			t.Errorf("err=%v vc=%+v", err, vc)
		}
	})
}

func TestIsCAFileRotation(t *testing.T) {
	ctx := context.Background()
	dir := "/etc/vmware/wcp/tls/"
	cases := []struct {
		name    string
		file    string
		op      fsnotify.Op
		fssOn   bool
		rotated bool
	}{
		{"vmca.pem created, FSS off", dir + "vmca.pem", fsnotify.Create, false, true},
		{"vmca.pem created, FSS on", dir + "vmca.pem", fsnotify.Create, true, true},
		{"vmca.pem removed", dir + "vmca.pem", fsnotify.Remove, true, false},
		{"..data swapped, FSS on", dir + "..data", fsnotify.Create, true, true},
		{"..data swapped, FSS off", dir + "..data", fsnotify.Create, false, false},
		{"..data removed, FSS on", dir + "..data", fsnotify.Remove, true, false},
		{"other file, FSS on", dir + "other.pem", fsnotify.Create, true, false},
		{"..data elsewhere, FSS on", "/etc/cloud/..data", fsnotify.Create, true, false},
	}
	for _, tc := range cases {
		useFSS(t, tc.fssOn)
		if got := IsCAFileRotation(ctx, tc.file, tc.op); got != tc.rotated {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.rotated)
		}
	}
}

func waitFor(t *testing.T, ch <-chan struct{}, want bool, what string) {
	t.Helper()
	timeout := 3 * time.Second
	if !want {
		timeout = 300 * time.Millisecond
	}
	select {
	case <-ch:
		if !want {
			t.Fatalf("unexpected reload: %s", what)
		}
	case <-time.After(timeout):
		if want {
			t.Fatalf("no reload: %s", what)
		}
	}
}

func TestStartTriggersReloads(t *testing.T) {
	if testing.Short() {
		t.Skip("uses timers")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	previous := retryInterval
	retryInterval = 50 * time.Millisecond
	defer func() { retryInterval = previous }()

	secret := credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "pw")
	client := fake.NewSimpleClientset(secret, fileVolumeConfigMap(map[string]string{}))
	vc := &config.VirtualCenterConfig{SDDCName: "sddc-1", User: "u@vsphere.local", Password: "pw"}
	creds, fileVolume := make(chan struct{}, 10), make(chan struct{}, 10)
	failures := 1
	err := start(ctx, client, ns, newConfig("csi", vc),
		func() error {
			if failures > 0 {
				failures--
				return context.DeadlineExceeded
			}
			creds <- struct{}{}
			return nil
		},
		func() error { fileVolume <- struct{}{}; return nil }, newFileVolumeSource())
	if err != nil {
		t.Fatal(err)
	}

	// Objects that exist at start-up are not changes.
	waitFor(t, creds, false, "initial credentials Secret")
	waitFor(t, fileVolume, false, "initial file volume ConfigMap")

	// A credentials change reloads, and a failed reload is retried.
	rotated := secret.DeepCopy()
	rotated.Data["password"] = []byte("new")
	rotated.ResourceVersion = "2"
	if _, err := client.CoreV1().Secrets(ns).Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, creds, true, "rotated credentials Secret")

	// The fake clientset ignores selectors on watches, so check that the informers ask for the
	// right objects: only this service account's Secrets and only the file volume ConfigMap.
	var secretSelector, configMapSelector string
	for _, action := range client.Actions() {
		if action.GetVerb() != "watch" {
			continue
		}
		restrictions := action.(k8stesting.WatchAction).GetWatchRestrictions()
		switch action.GetResource().Resource {
		case "secrets":
			secretSelector = restrictions.Labels.String()
		case "configmaps":
			configMapSelector = restrictions.Fields.String()
		}
	}
	if want := LabelServiceAccount + "=csi"; secretSelector != want {
		t.Errorf("Secret watch selector = %q, want %q", secretSelector, want)
	}
	if want := "metadata.name=" + FileVolumeConfigMapName; configMapSelector != want {
		t.Errorf("ConfigMap watch selector = %q, want %q", configMapSelector, want)
	}

	// A ConfigMap change reloads.
	cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "true"})
	cm.ResourceVersion = "2"
	if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, fileVolume, true, "changed file volume ConfigMap")
}

func TestStartWithoutRuntimeSources(t *testing.T) {
	os.Unsetenv("CLUSTER_FLAVOR")
	useFSS(t, false)
	called := false
	fn := func() error { called = true; return nil }
	cfg := newConfig("csi", &config.VirtualCenterConfig{SDDCName: "s"})
	if err := Start(context.Background(), cfg, fn, fn); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("callbacks ran with the FSS off")
	}
}

func TestHandleCAFileEvent(t *testing.T) {
	ctx := context.Background()
	dir := "/etc/vmware/wcp/tls/"
	previous := retryInterval
	retryInterval = time.Millisecond
	defer func() { retryInterval = previous }()

	run := func(fssOn bool, file string, op fsnotify.Op, failures int) int {
		useFSS(t, fssOn)
		var calls atomic.Int32
		done := make(chan struct{})
		HandleCAFileEvent(ctx, fsnotify.Event{Name: file, Op: op}, func() error {
			if int(calls.Add(1)) <= failures {
				return context.DeadlineExceeded
			}
			close(done)
			return nil
		})
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond): // an ignored event never reconnects
		}
		waitCAIdle(t)
		return int(calls.Load())
	}
	for name, tc := range map[string]struct {
		fssOn    bool
		file     string
		op       fsnotify.Op
		failures int
		want     int
	}{
		"vmca.pem rotation reconnects":           {false, dir + "vmca.pem", fsnotify.Create, 0, 1},
		"..data swap reconnects with the FSS on": {true, dir + "..data", fsnotify.Create, 0, 1},
		"..data swap is ignored with FSS off":    {false, dir + "..data", fsnotify.Create, 0, 0},
		"unrelated file is ignored":              {true, dir + "other", fsnotify.Create, 0, 0},
		"removal is ignored":                     {true, dir + "vmca.pem", fsnotify.Remove, 0, 0},
		"failed reconnects are retried":          {true, dir + "..data", fsnotify.Create, 3, 4},
	} {
		if got := run(tc.fssOn, tc.file, tc.op, tc.failures); got != tc.want {
			t.Errorf("%s: reconnect called %d times, want %d", name, got, tc.want)
		}
	}
}

type fakeReloader struct{ calls chan bool }

func (f fakeReloader) ReloadConfiguration(reconnectToVC bool) error {
	f.calls <- reconnectToVC
	return nil
}

func TestStartReloadingMapsChangesToReloads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secret := credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "pw")
	client := fake.NewSimpleClientset(secret, fileVolumeConfigMap(map[string]string{}))
	r := fakeReloader{calls: make(chan bool, 10)}
	cfg := newConfig("csi", &config.VirtualCenterConfig{SDDCName: "sddc-1", User: "u@vsphere.local", Password: "pw"})
	if err := startReloading(ctx, client, ns, cfg, r); err != nil {
		t.Fatal(err)
	}
	// The fake clientset drops events that happen before a watch exists.
	deadline := time.Now().Add(3 * time.Second)
	for watches(client) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the informers never started watching")
		}
		time.Sleep(10 * time.Millisecond)
	}
	next := func(what string) bool {
		t.Helper()
		select {
		case reconnect := <-r.calls:
			return reconnect
		case <-time.After(3 * time.Second):
			t.Fatalf("no reload after %s", what)
		}
		return false
	}

	rotated := secret.DeepCopy()
	rotated.ResourceVersion = "2"
	if _, err := client.CoreV1().Secrets(ns).Update(ctx, rotated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !next("a credentials change") {
		t.Error("a credentials change must reconnect to vCenter")
	}

	cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "true"})
	cm.ResourceVersion = "2"
	if _, err := client.CoreV1().ConfigMaps(ns).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if next("a file volume change") {
		t.Error("a file volume change must not force a reconnect")
	}
}

func watches(client *fake.Clientset) int {
	n := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "watch" {
			n++
		}
	}
	return n
}

func TestLockReloadSerializes(t *testing.T) {
	var running, maxRunning int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer LockReload()()
			n := atomic.AddInt32(&running, 1)
			for {
				m := atomic.LoadInt32(&maxRunning)
				if n <= m || atomic.CompareAndSwapInt32(&maxRunning, m, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt32(&running, -1)
		}()
	}
	wg.Wait()
	if maxRunning != 1 {
		t.Fatalf("%d reloads ran at once, want 1", maxRunning)
	}
}

func TestApplyIgnoresABadFileVolumeConfigMap(t *testing.T) {
	ctx := context.Background()
	vc := &config.VirtualCenterConfig{SDDCName: "sddc-1", FileVolumeActivated: true}
	secret := credSecret("s", "csi", "sddc-1", "user@vsphere.local", "pw")
	cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "maybe"})
	if err := apply(ctx, fake.NewSimpleClientset(secret, cm), ns,
		newConfig("csi", vc), newFileVolumeSource()); err != nil {
		t.Fatal(err)
	}
	if vc.User != "user@vsphere.local" || vc.Password != "pw" || !vc.FileVolumeActivated {
		t.Errorf("got %+v", vc)
	}
}

func TestRunReloadsRetriesImmediatelyOnANewTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	previous := retryInterval
	retryInterval = time.Hour
	defer func() { retryInterval = previous }()

	trigger := make(chan struct{}, 1)
	calls := make(chan int, 4)
	n := 0
	go runReloads(ctx, "test", trigger, func() error {
		n++
		calls <- n
		if n == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	trigger <- struct{}{}
	if got := <-calls; got != 1 {
		t.Fatalf("first call = %d", got)
	}
	trigger <- struct{}{} // the first reload failed and is now waiting an hour
	select {
	case got := <-calls:
		if got != 2 {
			t.Fatalf("second call = %d", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a new trigger did not end the retry wait")
	}
}

// waitCAIdle waits for the background reconnect to finish.
func waitCAIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		caReload.Lock()
		running := caReload.running
		caReload.Unlock()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the CA reconnect did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandleCAFileEventDoesNotBlockAndCoalesces(t *testing.T) {
	useFSS(t, false)
	ctx := context.Background()
	event := fsnotify.Event{Name: config.SupervisorCAFilePath, Op: fsnotify.Create}
	var calls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	reconnect := func() error {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return nil
	}
	returned := make(chan struct{})
	go func() {
		HandleCAFileEvent(ctx, event, reconnect)
		HandleCAFileEvent(ctx, event, reconnect) // arrives while the first is running
		HandleCAFileEvent(ctx, event, reconnect) // coalesces with the second
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("HandleCAFileEvent blocked on a running reconnect")
	}
	<-started
	release <- struct{}{} // finishes the first run
	<-started
	close(release)
	waitCAIdle(t)
	if got := calls.Load(); got != 2 {
		t.Errorf("reconnect ran %d times, want 2 (one, plus one for the rotations that arrived meanwhile)", got)
	}
}

func TestStartReloadsWhenTheSourcesChangedBeforeTheWatchStarted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The driver read the configuration before the Secret was rotated and the ConfigMap changed.
	secret := credSecret("s1", "csi", "sddc-1", "u@vsphere.local", "rotated")
	cm := fileVolumeConfigMap(map[string]string{"sddc-1.fileVolumeActivated": "true"})
	vc := &config.VirtualCenterConfig{SDDCName: "sddc-1", User: "u@vsphere.local", Password: "old"}
	creds, fileVolume := make(chan struct{}, 10), make(chan struct{}, 10)
	err := start(ctx, fake.NewSimpleClientset(secret, cm), ns, newConfig("csi", vc),
		func() error { creds <- struct{}{}; return nil },
		func() error { fileVolume <- struct{}{}; return nil }, newFileVolumeSource())
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, creds, true, "credentials changed before the watch started")
	waitFor(t, fileVolume, true, "file volume settings changed before the watch started")
}

func TestRegisterLatchesTheFeatureState(t *testing.T) {
	ctx := context.Background()
	useFSS(t, true)
	t.Cleanup(func() { latched.Store(stateUnset) })
	Register()
	if !Enabled(ctx) {
		t.Fatal("not enabled after Register with the FSS on")
	}
	if err := commonco.ContainerOrchestratorUtility.DisableFSS(ctx, common.VSphereCSIRuntimeConfig); err != nil {
		t.Skipf("the fake cannot disable a feature state: %v", err)
	}
	if !Enabled(ctx) {
		t.Error("the decision changed after the feature state flipped")
	}
}

func TestStartIsIdempotent(t *testing.T) {
	useFSS(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	client := fake.NewSimpleClientset(fileVolumeConfigMap(map[string]string{}))
	previousClient, previousCached := newClient, cachedClient
	newClient = func(context.Context) (kubernetes.Interface, error) { return client, nil }
	cachedClient = nil
	t.Cleanup(func() { newClient, cachedClient = previousClient, previousCached })

	cfg := newConfig("", &config.VirtualCenterConfig{SDDCName: "sddc-1"})
	nop := func() error { return nil }
	for i := 0; i < 3; i++ {
		if err := Start(ctx, cfg, nop, nop); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for watches(client) < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := watches(client); got != 1 {
		t.Errorf("%d watches after three Start calls, want 1", got)
	}

	// Once the watches have ended, Start may watch again.
	cancel()
	deadline = time.Now().Add(3 * time.Second)
	for {
		startedMu.Lock()
		s := started
		startedMu.Unlock()
		if !s {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Start still considers the watches running after ctx is done")
		}
		time.Sleep(time.Millisecond)
	}
}
