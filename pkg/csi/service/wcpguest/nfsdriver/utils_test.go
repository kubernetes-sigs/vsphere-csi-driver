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

package nfsdriver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateSubDir(t *testing.T) {
	for _, ok := range []string{"pvc-1", "tenant/data", "/tenant", "a/./b", "pvc..1", ".hidden"} {
		require.NoError(t, validateSubDir(ok), ok)
	}
	for _, bad := range []string{"", ".", "/", "./", "//", "/./", "..", "../x", "a/../b"} {
		require.Error(t, validateSubDir(bad), "%q", bad)
	}
}

func TestValidatePathStillOnlyRejectsTraversal(t *testing.T) {
	// baseDir may legitimately be empty (the share root); only traversal is rejected.
	require.NoError(t, validatePath(""))
	require.NoError(t, validatePath("export/sub"))
	require.Error(t, validatePath("export/../etc"))
}

func TestValidateIDComponent(t *testing.T) {
	require.NoError(t, validateIDComponent("x", ""))
	require.NoError(t, validateIDComponent("x", "tenant/data"))
	require.Error(t, validateIDComponent("x", "a#b"))
	require.Error(t, validateIDComponent("x", "#"))
}

func TestRemoveEmptyDirs(t *testing.T) {
	mkdirs := func(t *testing.T, root string, rel ...string) {
		t.Helper()
		for _, r := range rel {
			require.NoError(t, os.MkdirAll(filepath.Join(root, r), 0755))
		}
	}

	t.Run("removes empty parents up to but not including the root", func(t *testing.T) {
		root := t.TempDir()
		mkdirs(t, root, "a/b/c")
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "a/b/c")))
		require.NoDirExists(t, filepath.Join(root, "a"))
		require.DirExists(t, root)
	})

	t.Run("stops at the first non-empty directory", func(t *testing.T) {
		root := t.TempDir()
		mkdirs(t, root, "a/b/c", "a/keep")
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "a/b/c")))
		require.NoDirExists(t, filepath.Join(root, "a/b"))
		require.DirExists(t, filepath.Join(root, "a/keep"))
	})

	t.Run("an already removed directory is not an error", func(t *testing.T) {
		root := t.TempDir()
		mkdirs(t, root, "ns")
		// "ns/claim" was removed earlier, and so was whatever was below it.
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "ns/claim")))
		// The walk continued past the missing directory and cleaned the empty parent.
		require.NoDirExists(t, filepath.Join(root, "ns"))
	})

	t.Run("a missing directory and missing parents are not an error", func(t *testing.T) {
		root := t.TempDir()
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "ns/claim")))
		require.DirExists(t, root)
	})

	t.Run("a repeat after the whole chain is gone is not an error", func(t *testing.T) {
		root := t.TempDir()
		mkdirs(t, root, "ns/claim")
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "ns/claim")))
		require.NoError(t, removeEmptyDirs(root, filepath.Join(root, "ns/claim")))
	})

	t.Run("other errors are still returned", func(t *testing.T) {
		root := t.TempDir()
		// A file where a directory is expected: ReadDir fails with ENOTDIR, not ENOENT.
		require.NoError(t, os.WriteFile(filepath.Join(root, "file"), nil, 0644))
		require.Error(t, removeEmptyDirs(root, filepath.Join(root, "file")))
	})

	t.Run("refuses a directory outside the root", func(t *testing.T) {
		base := t.TempDir()
		root := filepath.Join(base, "vol-1")
		sibling := filepath.Join(base, "vol-10") // shares root as a string prefix
		mkdirs(t, base, "vol-1", "vol-10/x")
		require.Error(t, removeEmptyDirs(root, filepath.Join(sibling, "x")))
		require.DirExists(t, filepath.Join(sibling, "x"), "the sibling must be left alone")
	})

	t.Run("empty arguments are a no-op", func(t *testing.T) {
		require.NoError(t, removeEmptyDirs("", "x"))
		require.NoError(t, removeEmptyDirs("x", ""))
	})
}

func TestWaitUntilTimeout(t *testing.T) {
	timeoutErr := func() error { return fmt.Errorf("timed out") }

	require.NoError(t, WaitUntilTimeout(time.Second, func() error { return nil }, timeoutErr))
	require.EqualError(t,
		WaitUntilTimeout(time.Second, func() error { return fmt.Errorf("failed") }, timeoutErr), "failed")

	release := make(chan struct{})
	defer close(release)
	require.EqualError(t,
		WaitUntilTimeout(20*time.Millisecond, func() error { <-release; return nil }, timeoutErr), "timed out")
}

func TestWaitUntilTimeoutWithCleanup(t *testing.T) {
	timeoutErr := func() error { return fmt.Errorf("timed out") }

	t.Run("completes in time: no timeout, no callback", func(t *testing.T) {
		called := make(chan error, 1)
		timedOut, err := waitUntilTimeoutWithCleanup(time.Second, func() error { return nil }, timeoutErr,
			func(e error) { called <- e })
		require.False(t, timedOut)
		require.NoError(t, err)
		select {
		case <-called:
			t.Fatal("callback must not run when the operation finished in time")
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("fails in time: error returned, no callback", func(t *testing.T) {
		called := make(chan error, 1)
		timedOut, err := waitUntilTimeoutWithCleanup(time.Second, func() error { return fmt.Errorf("failed") },
			timeoutErr, func(e error) { called <- e })
		require.False(t, timedOut)
		require.EqualError(t, err, "failed")
		require.Empty(t, called)
	})

	t.Run("timeout: callback gets the eventual result", func(t *testing.T) {
		release := make(chan struct{})
		called := make(chan error, 1)
		timedOut, err := waitUntilTimeoutWithCleanup(20*time.Millisecond,
			func() error { <-release; return fmt.Errorf("late failure") }, timeoutErr,
			func(e error) { called <- e })
		require.True(t, timedOut)
		require.EqualError(t, err, "timed out")
		require.Empty(t, called, "the callback must wait for the operation to finish")

		close(release)
		select {
		case e := <-called:
			require.EqualError(t, e, "late failure")
		case <-time.After(5 * time.Second):
			t.Fatal("callback never ran")
		}
	})

	t.Run("timeout without a callback is fine", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		timedOut, err := waitUntilTimeoutWithCleanup(20*time.Millisecond,
			func() error { <-release; return nil }, timeoutErr, nil)
		require.True(t, timedOut)
		require.Error(t, err)
	})
}
