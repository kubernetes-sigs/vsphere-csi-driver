/*
Copyright 2025 The Kubernetes Authors.

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
	"os"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
)

var (
	// certGVR defines the GroupVersionResource for cert-manager Certificate CRD
	certGVR = schema.GroupVersionResource{
		Group:    "cert-manager.io",
		Version:  "v1",
		Resource: "certificates",
	}
)

var _ = ginkgo.Describe("Storage-Quota-Cert-Manager-Update", func() {
	var (
		dynamicClient dynamic.Interface
		ctx           context.Context
	)

	ginkgo.BeforeEach(func() {
		ctx = context.Background()

		// Get Kubernetes config
		config, err := getKubeConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Create dynamic client
		dynamicClient, err = dynamic.NewForConfig(config)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	ginkgo.It("should update certificate duration and renewBefore", func() {
		updateCertificate(ctx, dynamicClient, "kube-system", "storage-quota-webhook-internal-serving-cert",
			"1h", "10m")
	})

	ginkgo.It("should update cns-storage-quota-extension-cert duration and renewBefore", func() {
		updateCertificate(ctx, dynamicClient, "kube-system", "cns-storage-quota-extension-cert", "1h", "10m")
	})

	ginkgo.It("should update storage-quota-selfsigned-issuer-cert duration and renewBefore", func() {
		updateCertificate(ctx, dynamicClient, "vmware-system-cert-manager", "storage-quota-selfsigned-issuer-cert",
			"2h", "1h")
	})
})

// updateCertificate updates the duration and renewBefore fields of a cert-manager Certificate resource
func updateCertificate(ctx context.Context, dynamicClient dynamic.Interface, namespace, certName,
	duration, renewBefore string) {
	// Get the certificate
	framework.Logf("Getting certificate %s in namespace %s", certName, namespace)
	cert, err := dynamicClient.Resource(certGVR).Namespace(namespace).Get(ctx, certName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		framework.Failf("Certificate %s not found in namespace %s", certName, namespace)
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Log current values
	spec, found, err := unstructured.NestedMap(cert.Object, "spec")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	if found {
		currentDuration, _, _ := unstructured.NestedString(spec, "duration")
		currentRenewBefore, _, _ := unstructured.NestedString(spec, "renewBefore")
		framework.Logf("Current duration: %s, renewBefore: %s", currentDuration, currentRenewBefore)
	}

	// Update spec.duration
	err = unstructured.SetNestedField(cert.Object, duration, "spec", "duration")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Setting spec.duration to %s", duration)

	// Update spec.renewBefore
	err = unstructured.SetNestedField(cert.Object, renewBefore, "spec", "renewBefore")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	framework.Logf("Setting spec.renewBefore to %s", renewBefore)

	// Update the certificate
	framework.Logf("Updating certificate %s", certName)
	updatedCert, err := dynamicClient.Resource(certGVR).Namespace(namespace).Update(ctx, cert, metav1.UpdateOptions{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	// Verify the update
	updatedSpec, found, err := unstructured.NestedMap(updatedCert.Object, "spec")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(found).To(gomega.BeTrue(), "spec should be present in updated certificate")

	durationValue, found, err := unstructured.NestedString(updatedSpec, "duration")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(found).To(gomega.BeTrue(), "duration should be present in spec")
	gomega.Expect(durationValue).To(gomega.Equal(duration), "duration should match expected value")
	framework.Logf("Verified spec.duration is set to: %s", durationValue)

	renewBeforeValue, found, err := unstructured.NestedString(updatedSpec, "renewBefore")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(found).To(gomega.BeTrue(), "renewBefore should be present in spec")
	gomega.Expect(renewBeforeValue).To(gomega.Equal(renewBefore), "renewBefore should match expected value")
	framework.Logf("Verified spec.renewBefore is set to: %s", renewBeforeValue)

	framework.Logf("Successfully updated certificate %s with duration=%s and renewBefore=%s",
		certName, duration, renewBefore)
}

// getKubeConfig retrieves the Kubernetes configuration
func getKubeConfig() (*rest.Config, error) {
	// Try to get config from in-cluster service account first
	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	// Fall back to kubeconfig file
	kubeconfigPath := clientcmd.RecommendedHomeFile
	if envPath := clientcmd.RecommendedConfigPathEnvVar; os.Getenv(envPath) != "" {
		kubeconfigPath = os.Getenv(envPath)
	}

	config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	return config, nil
}
