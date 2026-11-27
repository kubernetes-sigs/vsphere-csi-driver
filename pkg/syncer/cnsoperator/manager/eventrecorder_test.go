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

package manager

import (
	"context"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/reference"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// eventTargetKinds are the custom resources the cnsoperator controllers record
// events on. An event recorder resolves the involved object's kind from the
// object's TypeMeta and, when that is empty, from the recorder's scheme. The
// controller-runtime client leaves TypeMeta empty on every typed object it
// decodes from an Update or Patch response, so the recorder's scheme must know
// each of these kinds, or the event is dropped with "no kind is registered".
var eventTargetKinds = []string{
	"CnsFileAccessConfig",
	"CnsNodeVmAttachment",
	"CnsNodeVMBatchAttachment",
	"CnsRegisterVolume",
	"CnsUnregisterVolume",
	"CnsVolumeMetadata",
	"CSINodeTopology",
	"ClusterStoragePolicyInfo",
	"InfraStoragePolicyInfo",
	"StoragePolicyInfo",
	"TriggerCsiFullSync",
	"VirtualMachineSnapshot",
	"VKSRegisterVolume",
}

// newWithoutTypeMeta returns an object of the given kind from the scheme, named
// but with an empty TypeMeta, as the controller-runtime client leaves it.
func newWithoutTypeMeta(t *testing.T, s *runtime.Scheme, kind string) runtime.Object {
	t.Helper()
	var gvks []schema.GroupVersionKind
	for gvk := range s.AllKnownTypes() {
		if gvk.Kind == kind {
			gvks = append(gvks, gvk)
		}
	}
	require.NotEmpty(t, gvks, "kind %s is not registered in the manager's scheme", kind)
	sort.Slice(gvks, func(i, j int) bool { return gvks[i].String() < gvks[j].String() })
	obj, err := s.New(gvks[0])
	require.NoError(t, err)
	accessor, err := meta.Accessor(obj)
	require.NoError(t, err)
	accessor.SetName("event-target")
	accessor.SetNamespace("event-ns")
	obj.GetObjectKind().SetGroupVersionKind(schema.GroupVersionKind{})
	return obj
}

// TestManagerSchemeReferencesEventTargets checks that the manager's scheme can
// build an event reference for every event target that has lost its TypeMeta,
// and that client-go's kubernetes scheme, which the controllers used to pass to
// their recorders, cannot.
func TestManagerSchemeReferencesEventTargets(t *testing.T) {
	defer resetGlobalScheme()
	resetGlobalScheme()
	ctx := logger.NewContextWithLogger(context.Background())
	s := getGlobalScheme(ctx)

	for _, kind := range eventTargetKinds {
		t.Run(kind, func(t *testing.T) {
			obj := newWithoutTypeMeta(t, s, kind)

			ref, err := reference.GetReference(s, obj)
			require.NoError(t, err)
			assert.Equal(t, kind, ref.Kind)
			assert.Equal(t, "event-target", ref.Name)
			assert.Equal(t, "event-ns", ref.Namespace)

			_, err = reference.GetReference(clientgoscheme.Scheme, obj)
			assert.Error(t, err, "client-go's scheme should not know %s", kind)
		})
	}
}

// clientGoSchemeAllowed lists controllers whose recorder is fixed in a separate
// change. cnsregistervolume moves to the manager's scheme in
// kubernetes-sigs/vsphere-csi-driver#4440; remove the entry once that merges.
var clientGoSchemeAllowed = map[string]bool{
	filepath.Join("cnsregistervolume", "cnsregistervolume_controller.go"): true,
}

// TestEventRecordersUseManagerScheme guards against a controller building its
// event recorder with client-go's kubernetes scheme, which does not register
// the cnsoperator types. It inspects every NewRecorder call in the cnsoperator
// controllers and rejects a scheme argument of the form <pkg>.Scheme, such as
// scheme.Scheme or clientgoscheme.Scheme.
func TestEventRecordersUseManagerScheme(t *testing.T) {
	root := filepath.Join("..", "controller")
	fset := token.NewFileSet()
	calls := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if clientGoSchemeAllowed[rel] {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "NewRecorder" {
				return true
			}
			calls++
			var arg strings.Builder
			require.NoError(t, printer.Fprint(&arg, fset, call.Args[0]))
			assert.False(t, strings.HasSuffix(arg.String(), ".Scheme"),
				"%s: NewRecorder(%s, ...): the event recorder must use the manager's scheme "+
					"(mgr.GetScheme()), which registers the cnsoperator types; client-go's scheme "+
					"drops events on objects without TypeMeta", fset.Position(call.Pos()), arg.String())
			return true
		})
		return nil
	})
	require.NoError(t, err)
	require.Positive(t, calls, "no NewRecorder calls found under %s", root)
}
