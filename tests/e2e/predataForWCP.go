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
	"os"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	vmopv2 "github.com/vmware-tanzu/vm-operator/api/v1alpha2"
	vmopv3 "github.com/vmware-tanzu/vm-operator/api/v1alpha3"
	vmopv4 "github.com/vmware-tanzu/vm-operator/api/v1alpha4"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsoperatorv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
)

var _ = ginkgo.Describe("WCP-predata",
	ginkgo.Label(p0, wcp, core), func() {

		f := framework.NewDefaultFramework("supervisor-predata")
		f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

		var (
			client            clientset.Interface
			namespace         string
			storagePolicyName string
			scParameters      map[string]string
			vmClass           string
			statuscode        int
		)

		ginkgo.BeforeEach(func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			client = f.ClientSet
			bootstrap()

			// Skip if not supervisor cluster
			if !supervisorCluster {
				ginkgo.Skip("Test is only for Supervisor Cluster")
			}

			scParameters = make(map[string]string)
			storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID

			//datastoreURL is required to get dsRef ID which is used to get contentLibId
			datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
			dsRef := getDsMoRefFromURL(ctx, datastoreURL)
			framework.Logf("dsmoId: %v", dsRef.Value)

			vcRestSessionId := createVcSession4RestApis(ctx)
			contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId, GetAndExpectStringEnvVar(envContentLibraryUrl),
				dsRef.Value, &e2eVSphere)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("Create a WCP namespace for the test")
			vmClass := os.Getenv(envVMClass)
			if vmClass == "" {
				vmClass = vmClassBestEffortSmall
			}

			// Create SVC namespace and assign storage policy and vmContent Library
			namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
				[]string{profileID}, getSvcId(vcRestSessionId, &e2eVSphere),
				nil, vmClass, contentLibId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		})

		/*
			Supervisor Cluster Pre-Data Setup

			Creates resources WITHOUT cleanup:
			- 5 standalone PVCs with Pods
			- 1 StatefulSet with 5 replicas
			- 3 VolumeSnapshots
			- 2 VMService VMs

			Total: 12 PVCs (5 standalone + 5 StatefulSet + 2 VM PVCs)
		*/
		ginkgo.It("Create 5 PVCs, 1 StatefulSet (5 replicas), 3 Snapshots, and 2 VMs without cleanup",
			func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				framework.Logf("=== Supervisor Pre-Data Setup Started ===")
				framework.Logf("Namespace: %s", namespace)
				framework.Logf("Storage Policy: %s", storagePolicyName)

				// Get storage class
				storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// Initialize clients
				var snapc *snapclient.Clientset
				restConfig = getRestConfigClient()
				snapc, err = snapclient.NewForConfig(restConfig)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				cnsopC, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorv1alpha1.GroupName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())

				// ==============================================
				// Section 1: Create 5 Standalone PVCs with Pods
				// ==============================================
				ginkgo.By("Step 1: Creating 5 standalone PVCs with Pods")

				standalonePVCs := make([]*v1.PersistentVolumeClaim, 5)
				standalonePods := make([]*v1.Pod, 5)

				for i := 0; i < 5; i++ {
					pvc, err := createPVC(ctx, client, namespace, nil, diskSize, storageclass, v1.ReadWriteOnce)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					standalonePVCs[i] = pvc
					framework.Logf("Created PVC: %s", pvc.Name)
				}

				// Wait for PVCs to be bound
				standalonePVs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, standalonePVCs,
					framework.ClaimProvisionTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("All 5 standalone PVCs are bound")

				// Verify CNS metadata
				for i := 0; i < 5; i++ {
					volHandle := standalonePVs[i].Spec.CSI.VolumeHandle
					err = waitAndVerifyCnsVolumeMetadata(ctx, volHandle, standalonePVCs[i], standalonePVs[i], nil)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create pods
				for i := 0; i < 5; i++ {
					pod, err := createPod(ctx, client, namespace, nil,
						[]*v1.PersistentVolumeClaim{standalonePVCs[i]}, false, "")
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					standalonePods[i] = pod
					framework.Logf("Created Pod: %s", pod.Name)
				}

				// Wait for pods to be running
				for i := 0; i < 5; i++ {
					err = fpod.WaitForPodRunningInNamespace(ctx, client, standalonePods[i])
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
				framework.Logf("All 5 standalone pods are running")

				// ==============================================
				// Section 2: Create StatefulSet with 5 replicas
				// ==============================================
				ginkgo.By("Step 2: Creating StatefulSet with 5 replicas")

				// Check if nginx service already exists
				var service *v1.Service
				existingService, err := client.CoreV1().Services(namespace).Get(ctx, servicename, metav1.GetOptions{})
				if err != nil {
					if apierrors.IsNotFound(err) {
						// Service doesn't exist, create it
						service = CreateService(namespace, client)
						framework.Logf("Service created: %s", service.Name)
					} else {
						// Some other error occurred
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				} else {
					// Service already exists
					framework.Logf("Service '%s' already exists in namespace '%s', reusing it", existingService.Name, namespace)
					service = existingService
				}

				statefulset := GetStatefulSetFromManifest(namespace)
				replicas := int32(3)
				statefulset.Spec.Replicas = &replicas
				statefulset.Spec.VolumeClaimTemplates[0].Spec.StorageClassName = &storagePolicyName
				statefulset.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[v1.ResourceStorage] =
					resource.MustParse(diskSize)

				CreateStatefulSet(namespace, statefulset, client)
				framework.Logf("StatefulSet created: %s with 3 replicas", statefulset.Name)

				// Wait for StatefulSet to be ready
				fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
				gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())

				ssPodsAfterCreation, err := fss.GetPodList(ctx, client, statefulset)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(len(ssPodsAfterCreation.Items)).To(gomega.Equal(int(replicas)))
				framework.Logf("StatefulSet is ready with 5 replicas")

				// ==============================================
				// Section 3: Create 3 VolumeSnapshots
				// ==============================================

				vcVersion = getVCversion(ctx, vcAddress)
				isVC90 := isVersionGreaterOrEqual(vcVersion, quotaSupportedVCVersion)

				if isVC90 {
					ginkgo.By("Step 3: Creating 3 VolumeSnapshots")

					volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.Logf("VolumeSnapshotClass created: %s", volumeSnapshotClass.Name)

					volumeSnapshots := make([]*snapV1.VolumeSnapshot, 3)
					for i := 0; i < 3; i++ {
						snapshot, err := snapc.SnapshotV1().VolumeSnapshots(namespace).Create(ctx,
							getVolumeSnapshotSpec(namespace, volumeSnapshotClass.Name, standalonePVCs[i].Name),
							metav1.CreateOptions{})
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						volumeSnapshots[i] = snapshot
						framework.Logf("Created VolumeSnapshot: %s from PVC: %s",
							snapshot.Name, standalonePVCs[i].Name)
					}

					// Wait for snapshots to be ready
					for i := 0; i < 3; i++ {
						snapshot, err := waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshots[i].Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						gomega.Expect(snapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize))).To(gomega.BeZero())
						framework.Logf("VolumeSnapshot %s is ready", volumeSnapshots[i].Name)
					}
				}

				// ==============================================
				// Section 4: Create 2 VMService VMs
				// ==============================================
				ginkgo.By("Step 4: Creating 2 VMService VMs")

				vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
				vmopScheme := runtime.NewScheme()
				gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
				gomega.Expect(vmopv2.AddToScheme(vmopScheme)).Should(gomega.Succeed())
				gomega.Expect(vmopv3.AddToScheme(vmopScheme)).Should(gomega.Succeed())
				gomega.Expect(vmopv4.AddToScheme(vmopScheme)).Should(gomega.Succeed())
				vmopC, err := ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				framework.Logf("Found VirtualMachineImage: %s", vmi)

				secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)
				framework.Logf("Created bootstrap secret: %s", secretName)

				vmPVCs := make([]*v1.PersistentVolumeClaim, 2)
				vms := make([]*vmopv1.VirtualMachine, 2)

				for i := 0; i < 2; i++ {
					// Create PVC for VM
					vmPvc, err := createPVC(ctx, client, namespace, nil, "2Gi", storageclass, v1.ReadWriteOnce)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					vmPVCs[i] = vmPvc
					framework.Logf("Created VM PVC: %s", vmPvc.Name)

					// Wait for PVC to be bound
					_, err = fpv.WaitForPVClaimBoundPhase(ctx, client,
						[]*v1.PersistentVolumeClaim{vmPVCs[i]}, framework.ClaimProvisionTimeout)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())

					// Create VM
					vm := createVmServiceVmWithPvcs(ctx, vmopC, namespace, vmClass,
						[]*v1.PersistentVolumeClaim{vmPVCs[i]}, vmi, storagePolicyName, secretName)
					vms[i] = vm
					framework.Logf("Created VMService VM: %s", vm.Name)

					// Verify PVC attached to VM
					err = waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vms[i],
						[]*v1.PersistentVolumeClaim{vmPVCs[i]})
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					framework.Logf("Verified PVC attached to VM: %s", vms[i].Name)
				}

				// ==============================================
				// Final Summary
				// ==============================================
				ginkgo.By("Step 5: Verifying all resources")

				framework.Logf("=== Supervisor Pre-Data Setup Complete ===")
				framework.Logf("Namespace: %s", namespace)
				framework.Logf("Storage Policy: %s", storagePolicyName)
				framework.Logf("")
				framework.Logf("Resources Created:")
				framework.Logf("  - Standalone PVCs: 3 (3 x 2Gi = 6Gi)")
				framework.Logf("  - Standalone Pods: 3")
				framework.Logf("  - StatefulSet: 1 with 3 replicas (3 x 2Gi = 6Gi)")
				framework.Logf("  - VolumeSnapshots: 3 (~6Gi)")
				framework.Logf("  - VMService VMs: 2 (2 x 10Gi = 20Gi)")
				framework.Logf("  - VM PVCs: 2")
				framework.Logf("")
				framework.Logf("Total Resources:")
				framework.Logf("  - PVCs: 12 (5 standalone + 5 StatefulSet + 2 VM)")
				framework.Logf("  - Pods: 10 (5 standalone + 5 StatefulSet)")
				framework.Logf("  - VMs: 2")
				framework.Logf("  - Snapshots: 3")
				framework.Logf("  - Total Storage: ~46Gi")
				framework.Logf("==========================================")

				ginkgo.By("*** RESOURCES NOT CLEANED UP - Will persist after test ***")
			})
	})
