/*
Copyright 2025-2026 The Kubernetes Authors.

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

package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

const (
	// Timeout constants for StatefulSet operations
	STATEFULSET_TIMEOUT = 15 * time.Minute
	PVC_DELETION_POLL   = 2 * time.Second
)

// CreateStatefulSet creates a StatefulSet from the manifest at manifestPath in the given namespace.
func CreateStatefulSet(ns string, ss *apps.StatefulSet, c clientset.Interface) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	framework.Logf("Creating statefulset %v/%v with %d replicas and selector %+v",
		ss.Namespace, ss.Name, *(ss.Spec.Replicas), ss.Spec.Selector)
	_, err := c.AppsV1().StatefulSets(ns).Create(ctx, ss, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	fss.WaitForRunningAndReady(ctx, c, *ss.Spec.Replicas, ss)
}

// deleteStatefulSetAndClaimPVCs deletes a specific StatefulSet and its associated PVCs only.
// This prevents issues where fss.DeleteAllStatefulSets deletes unrelated PVCs in the same namespace.
func deleteStatefulSetAndClaimPVCs(ctx context.Context, c clientset.Interface, ss *apps.StatefulSet) error {
	namespace := ss.Namespace
	statefulsetName := ss.Name

	framework.Logf("Deleting StatefulSet %s/%s and its associated PVCs", namespace, statefulsetName)

	// Scale down to 0 replicas first
	framework.Logf("Scaling down StatefulSet %s/%s to 0 replicas", namespace, statefulsetName)
	ss, err := fss.Scale(ctx, c, ss, 0)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to scale down StatefulSet %s/%s: %v", namespace, statefulsetName, err)
	}

	// Wait for StatefulSet to be scaled down
	if ss != nil {
		fss.WaitForStatusReplicas(ctx, c, ss, 0)
	}

	// Delete the StatefulSet
	framework.Logf("Deleting StatefulSet %s/%s", namespace, statefulsetName)
	err = c.AppsV1().StatefulSets(namespace).Delete(ctx, statefulsetName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete StatefulSet %s/%s: %v", namespace, statefulsetName, err)
	}

	// Get and delete PVCs that match the StatefulSet naming convention
	framework.Logf("Identifying PVCs associated with StatefulSet %s/%s", namespace, statefulsetName)
	pvcList, err := c.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list PVCs in namespace %s: %v", namespace, err)
	}

	var statefulSetPVCs []string
	for _, pvc := range pvcList.Items {
		if isStatefulSetPVC(pvc.Name, statefulsetName, ss) {
			statefulSetPVCs = append(statefulSetPVCs, pvc.Name)
		}
	}

	framework.Logf("Found %d PVCs associated with StatefulSet %s/%s: %v", len(statefulSetPVCs), namespace, statefulsetName, statefulSetPVCs)

	// Delete the identified PVCs
	for _, pvcName := range statefulSetPVCs {
		framework.Logf("Deleting PVC %s/%s", namespace, pvcName)
		err = c.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			framework.Logf("Warning: failed to delete PVC %s/%s: %v", namespace, pvcName, err)
		}
	}

	// Wait for all associated PVCs and PVs to be deleted
	framework.Logf("Waiting for PVCs associated with StatefulSet %s/%s to be deleted", namespace, statefulsetName)
	err = wait.PollUntilContextTimeout(ctx, PVC_DELETION_POLL, STATEFULSET_TIMEOUT, false, func(ctx context.Context) (bool, error) {
		remainingPVCs := 0
		for _, pvcName := range statefulSetPVCs {
			_, err := c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
			if err == nil {
				remainingPVCs++
			} else if !errors.IsNotFound(err) {
				return false, fmt.Errorf("error checking PVC %s/%s: %v", namespace, pvcName, err)
			}
		}

		if remainingPVCs > 0 {
			framework.Logf("Still waiting for %d PVCs of StatefulSet %s/%s to disappear", remainingPVCs, namespace, statefulsetName)
			return false, nil
		}

		framework.Logf("All PVCs associated with StatefulSet %s/%s have been deleted", namespace, statefulsetName)
		return true, nil
	})

	if err != nil {
		return fmt.Errorf("timeout waiting for PVCs of StatefulSet %s/%s to be deleted: %v", namespace, statefulsetName, err)
	}

	return nil
}

// isStatefulSetPVC checks if a PVC name matches the StatefulSet volumeClaimTemplate naming convention.
// StatefulSet PVCs follow the pattern: <claimName>-<statefulsetName>-<ordinal>
// Example: www-web-0, www-web-1, etc.
func isStatefulSetPVC(pvcName, statefulsetName string, statefulSet *apps.StatefulSet) bool {
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		claimName := template.Name
		// StatefulSet PVC naming convention: <claimName>-<statefulsetName>-<ordinal>
		prefix := fmt.Sprintf("%s-%s-", claimName, statefulsetName)
		if strings.HasPrefix(pvcName, prefix) {
			// Check if the suffix after the prefix is a valid ordinal (numeric)
			suffix := strings.TrimPrefix(pvcName, prefix)
			if len(suffix) > 0 && isNumeric(suffix) {
				return true
			}
		}
	}
	return false
}

// isNumeric checks if a string contains only numeric characters
func isNumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
