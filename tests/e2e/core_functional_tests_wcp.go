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
	"strconv"
	"strings"
	"time"

	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	vmopv1 "github.com/vmware-tanzu/vm-operator/api/v1alpha1"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	fss "k8s.io/kubernetes/test/e2e/framework/statefulset"
	admissionapi "k8s.io/pod-security-admission/api"
	ctlrclient "sigs.k8s.io/controller-runtime/pkg/client"

	cnsop "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
)

/*
	Test performs following operations

	Steps
	1. Create a storage class.
	2. Create nginx service.
	3. Create nginx statefulsets with 3 replicas.
	4. Wait until all Pods are ready and PVCs are bounded with PV.
	5. Scale down statefulsets to 2 replicas.
	6. Scale up statefulsets to 3 replicas.
	7. Scale down statefulsets to 0 replicas and delete all pods.
	8. Delete all PVCs from the tests namespace.
	9. Delete the storage class.
*/

var _ = ginkgo.Describe("core-functional-tests-wcp", func() {
	f := framework.NewDefaultFramework("e2e-cf")
	f.SkipNamespaceCreation = true // tests will create their own namespaces
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		namespace          string
		client             clientset.Interface
		storagePolicyName  string
		storagePolicyName2 string
		scParameters       map[string]string

		//zonalPolicy       string

		// allowedTopologyHAMap       map[string][]string
		// nodeList                   *v1.NodeList
		// stsReplicas                int32
		// allowedTopologies          []v1.TopologySelectorLabelRequirement
		isQuotaValidationSupported bool
		vcRestSessionId            string
		vmClass                    string
		storageProfileId           string
		storageProfileId2          string
		quota                      map[string]*resource.Quantity
		isLateBinding              bool
		storageclass               *storagev1.StorageClass
		statuscode                 int
		labelsMap                  map[string]string
		vmopC                      ctlrclient.Client
		cnsopC                     ctlrclient.Client
		defaultDatastore           *object.Datastore
	)

	ginkgo.BeforeEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		client = f.ClientSet
		bootstrap()
		sc, err := client.StorageV1().StorageClasses().Get(ctx, defaultNginxStorageClassName, metav1.GetOptions{})
		if err == nil && sc != nil {
			gomega.Expect(client.StorageV1().StorageClasses().Delete(ctx, sc.Name,
				*metav1.NewDeleteOptions(0))).NotTo(gomega.HaveOccurred())
		}

		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		storagePolicyName2 = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores2)

		storageProfileId = e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		framework.Logf("storageProfileId: %s", storageProfileId)
		storageProfileId2 = e2eVSphere.GetSpbmPolicyID(storagePolicyName2)
		framework.Logf("storageProfileId2: %s", storageProfileId2)

		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		//datastoreURL is required to get dsRef ID which is used to get contentLibId
		datastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		dsRef := getDsMoRefFromURL(ctx, datastoreURL)
		framework.Logf("dsmoId: %v", dsRef.Value)

		if supervisorCluster || stretchedSVC {
			//if isQuotaValidationSupported is true then quotaValidation is considered in tests
			vcVersion = getVCversion(ctx, vcAddress)
			isQuotaValidationSupported = isVersionGreaterOrEqual(vcVersion, quotaSupportedVCVersion)
		}

		vcRestSessionId = createVcSession4RestApis(ctx)
		contentLibId, err := createAndOrGetContentlibId4Url(vcRestSessionId,
			GetAndExpectStringEnvVar(envContentLibraryUrl), dsRef.Value, &e2eVSphere)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("contentLibId: %s", contentLibId)

		framework.Logf("Create a WCP namespace for the test")
		vmClass = os.Getenv(envVMClass)
		if vmClass == "" {
			vmClass = vmClassBestEffortSmall
		}

		// Create SVC namespace and assign storage policy and vmContent Library
		framework.Logf("Creating Namespace")
		namespace, statuscode, err = createtWcpNsWithZonesAndPolicies(vcRestSessionId,
			[]string{storageProfileId, storageProfileId2}, getSvcId(vcRestSessionId, &e2eVSphere),
			nil, vmClass, contentLibId)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(statuscode).To(gomega.Equal(status_code_success))
		framework.Logf("Namespace Created : %s", namespace)

		vmopScheme := runtime.NewScheme()
		gomega.Expect(vmopv1.AddToScheme(vmopScheme)).Should(gomega.Succeed())
		vmopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: vmopScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		cnsOpScheme := runtime.NewScheme()
		gomega.Expect(cnsop.AddToScheme(cnsOpScheme)).Should(gomega.Succeed())
		cnsopC, err = ctlrclient.New(f.ClientConfig(), ctlrclient.Options{Scheme: cnsOpScheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		time.Sleep(sleepTimeOut)
		isLateBinding = false
		restConfig := getRestConfigClient()
		ginkgo.By("Get Immediate binding stotage class")
		storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// ginkgo.By("Set Storage Quota on namespace")
		// setStoragePolicyQuota(ctx, restConfig, storagePolicyName, namespace, rqLimit)

		ginkgo.By("Read QuotaDetails Before creating workload")
		quota = make(map[string]*resource.Quantity)
		//PVCQuota Details Before creating workload
		quota["totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
			quota["pvc_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx, restConfig,
			storageclass.Name, namespace, pvcUsage, volExtensionName, isLateBinding)

		framework.Logf("quota[totalQuotaUsedBefore] : %s", quota["totalQuotaUsedBefore"])

		var datacenters []string
		datastoreURL = GetAndExpectStringEnvVar(envSharedDatastoreURL)

		finder := find.NewFinder(e2eVSphere.Client.Client, false)
		cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}
		for _, dc := range datacenters {
			defaultDatacenter, err := finder.Datacenter(ctx, dc)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			finder.SetDatacenter(defaultDatacenter)
			defaultDatastore, err = getDatastoreByURL(ctx, datastoreURL, defaultDatacenter)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		delTestWcpNs(vcRestSessionId, namespace)
		gomega.Expect(waitForNamespaceToGetDeleted(ctx, client, namespace, poll, pollTimeout)).To(gomega.Succeed())

		dumpSvcNsEventsOnTestFailure(client, namespace)
	})

	/**
	Statefulset testing with default podManagementPolicy
	1. create appropriate storage class
	2. create statefull set with 3 replica's with above created SC
	3. scale down statefill set
	4. scale up statefull set
	5. Validate POD's are coming up on appropriate nodes
	6. in case of stretched svc - validate PV node affinity
	7. clean up the statefulset
	*/
	ginkgo.It("[csi-supervisor-cf] Statefulset-default-podManagementPolicy",
		ginkgo.Label(p0, block, wcp, core, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())

			defer cancel()

			if isQuotaValidationSupported && supervisorCluster {
				//PVCQuota Details Before creating workload
				quota["pvc_totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
					quota["pvc_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx,
					restConfig, storagePolicyName, namespace, pvcUsage, volExtensionName, isLateBinding)
				framework.Logf("quota[vm_storagePolicyQuotaBefore] : %s", quota["vm_storagePolicyQuotaBefore"])
			}

			ginkgo.By("Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID

			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Creating statefulset")
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storagePolicyName

			CreateStatefulSet(namespace, statefulset, client)
			replicas := *(statefulset.Spec.Replicas)
			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// Get the list of Volumes attached to Pods before scale down
			var volumesBeforeScaleDown []string
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace,
							volumespec.PersistentVolumeClaim.ClaimName)
						volumesBeforeScaleDown = append(volumesBeforeScaleDown, pv.Spec.CSI.VolumeHandle)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			diskInGb := (diskSize1Gi / 1024) * 3
			diskInGbStr := convertInt64ToStrGbFormat(diskInGb)

			//if quotaValidation is supported in VC then this block will be executed
			if isQuotaValidationSupported && supervisorCluster {
				var expectedTotalStorage []string
				expectedTotalStorage = append(expectedTotalStorage, diskInGbStr)
				//Validate TotalQuota
				_, quotavalidationStatus := validateTotalQuota(ctx, restConfig, storagePolicyName, namespace,
					expectedTotalStorage, quota["totalQuotaUsedBefore"], isLateBinding)
				gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())

				//Validates PVC quota in both StoragePolicyQuota and StoragePolicyUsage CR
				sp_quota_status_pvc, sp_usage_status_pvc := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
					storagePolicyName, namespace, pvcUsage, volExtensionName, []string{diskInGbStr},
					quota["pvc_totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"],
					quota["pvc_storagePolicyUsageBefore"], isLateBinding)
				gomega.Expect(sp_quota_status_pvc && sp_usage_status_pvc).NotTo(gomega.BeFalse())

			}

			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas-1))
			_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas-1)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas-1)
			ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas-1)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale down, verify vSphere volumes are detached from deleted pods
			ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				if err != nil {
					gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
					for _, volumespec := range sspod.Spec.Volumes {
						if volumespec.PersistentVolumeClaim != nil {
							pv := getPvFromClaim(client, statefulset.Namespace,
								volumespec.PersistentVolumeClaim.ClaimName)

							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from "+
									"the PodVM", vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}

			// After scale down, verify the attached volumes match those in CNS Cache
			for _, sspod := range ssPodsAfterScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
			_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
			gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

			ssPodsAfterScaleUp, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale up, verify all vSphere volumes are attached to node VMs.
			ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
			for _, sspod := range ssPodsAfterScaleUp.Items {
				err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range pod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						ginkgo.By("Verify scale up operation should not introduced new volume")
						gomega.Expect(isValuePresentInTheList(volumesBeforeScaleDown,
							pv.Spec.CSI.VolumeHandle)).To(gomega.BeTrue())
						ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
							pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						var vmUUID string
						var exists bool
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation",
							vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle,
							vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
						ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
						err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

					}
				}
			}
			replicas = 0
			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
			_, scaledownErr = fss.Scale(ctx, client, statefulset, replicas)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
			ssPodsAfterScaleDown, err = fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		})

	/*
		Test performs following operations

		Steps
		1. Create a storage class.
		2. Create nginx service.
		3. Create nginx statefulsets with 8 replicas.
		4. Wait until all Pods are ready and PVCs are bounded with PV.
		5. Scale down statefulsets to 5 replicas.
		6. Scale up statefulsets to 12 replicas.
		7. Scale down statefulsets to 0 replicas and delete all pods.
		8. Delete all PVCs from the tests namespace.
		9. Delete the storage class.
	*/
	ginkgo.It("[csi-supervisor-cf] Statefulset-parallel-podManagementPolicy",
		ginkgo.Label(p0, vanilla, block, wcp, core, vc70), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ginkgo.By("Creating StorageClass for Statefulset")
			// decide which test setup is available to run

			ginkgo.By("Running for WCP setup")
			profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
			scParameters[scParamStoragePolicyID] = profileID

			ginkgo.By("Creating service")
			service := CreateService(namespace, client)
			defer func() {
				deleteService(namespace, client, service)
			}()
			statefulset := GetStatefulSetFromManifest(namespace)
			ginkgo.By("Updating replicas to 8 and podManagement Policy as Parallel")
			*(statefulset.Spec.Replicas) = 8
			statefulset.Spec.PodManagementPolicy = apps.ParallelPodManagement
			statefulset.Spec.VolumeClaimTemplates[len(statefulset.Spec.VolumeClaimTemplates)-1].
				Spec.StorageClassName = &storagePolicyName
			ginkgo.By("Creating statefulset")
			CreateStatefulSet(namespace, statefulset, client)
			replicas := *(statefulset.Spec.Replicas)
			// Waiting for pods status to be Ready
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
			gomega.Expect(fss.CheckMount(ctx, client, statefulset, mountPath)).NotTo(gomega.HaveOccurred())
			ssPodsBeforeScaleDown, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsBeforeScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsBeforeScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace,
							volumespec.PersistentVolumeClaim.ClaimName)
						// Verify the attached volume match the one in CNS cache
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
			replicas -= 3
			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
			_, scaledownErr := fss.Scale(ctx, client, statefulset, replicas)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)
			ssPodsAfterScaleDown, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsAfterScaleDown.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale down, verify vSphere volumes are detached from deleted pods
			ginkgo.By("Verify Volumes are detached from Nodes after Statefulsets is scaled down")
			for _, sspod := range ssPodsBeforeScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				if err != nil {
					gomega.Expect(apierrors.IsNotFound(err), gomega.BeTrue())
					for _, volumespec := range sspod.Spec.Volumes {
						if volumespec.PersistentVolumeClaim != nil {
							pv := getPvFromClaim(client, statefulset.Namespace,
								volumespec.PersistentVolumeClaim.ClaimName)

							annotations := sspod.Annotations
							vmUUID, exists := annotations[vmUUIDLabel]
							gomega.Expect(exists).To(gomega.BeTrue(),
								fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))

							ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
								pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
							gomega.Expect(err).To(gomega.HaveOccurred(),
								fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from "+
									"the PodVM", vmUUID, sspod.Spec.NodeName))
						}
					}
				}
			}

			// After scale down, verify the attached volumes match those in CNS Cache
			for _, sspod := range ssPodsAfterScaleDown.Items {
				_, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range sspod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace, volumespec.PersistentVolumeClaim.ClaimName)
						err := verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}

			replicas += 7
			ginkgo.By(fmt.Sprintf("Scaling up statefulsets to number of Replica: %v", replicas))
			_, scaleupErr := fss.Scale(ctx, client, statefulset, replicas)
			gomega.Expect(scaleupErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
			fss.WaitForStatusReadyReplicas(ctx, client, statefulset, replicas)

			ssPodsAfterScaleUp, err := fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(ssPodsAfterScaleUp.Items).NotTo(gomega.BeEmpty(),
				fmt.Sprintf("Unable to get list of Pods from the Statefulset: %v", statefulset.Name))
			gomega.Expect(len(ssPodsAfterScaleUp.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")

			// After scale up, verify all vSphere volumes are attached to node VMs.
			ginkgo.By("Verify all volumes are attached to Nodes after Statefulsets is scaled up")
			for _, sspod := range ssPodsAfterScaleUp.Items {
				err := fpod.WaitTimeoutForPodReadyInNamespace(ctx, client, sspod.Name, statefulset.Namespace,
					pollTimeout)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				pod, err := client.CoreV1().Pods(namespace).Get(ctx, sspod.Name, metav1.GetOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				for _, volumespec := range pod.Spec.Volumes {
					if volumespec.PersistentVolumeClaim != nil {
						pv := getPvFromClaim(client, statefulset.Namespace,
							volumespec.PersistentVolumeClaim.ClaimName)
						ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
							pv.Spec.CSI.VolumeHandle, sspod.Spec.NodeName))
						var vmUUID string
						var exists bool
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						annotations := pod.Annotations
						vmUUID, exists = annotations[vmUUIDLabel]
						gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation",
							vmUUIDLabel))
						_, err := e2eVSphere.getVMByUUID(ctx, vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())

						isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, pv.Spec.CSI.VolumeHandle,
							vmUUID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Disk is not attached to the node")
						gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Disk is not attached")
						ginkgo.By("After scale up, verify the attached volumes match those in CNS Cache")
						err = verifyVolumeMetadataInCNS(&e2eVSphere, pv.Spec.CSI.VolumeHandle,
							volumespec.PersistentVolumeClaim.ClaimName, pv.ObjectMeta.Name, sspod.Name)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
				}
			}
			replicas = 0
			ginkgo.By(fmt.Sprintf("Scaling down statefulsets to number of Replica: %v", replicas))
			_, scaledownErr = fss.Scale(ctx, client, statefulset, replicas)
			gomega.Expect(scaledownErr).NotTo(gomega.HaveOccurred())
			fss.WaitForStatusReplicas(ctx, client, statefulset, replicas)
			ssPodsAfterScaleDown, err = fss.GetPodList(ctx, client, statefulset)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(len(ssPodsAfterScaleDown.Items) == int(replicas)).To(gomega.BeTrue(),
				"Number of Pods in the statefulset should match with number of replicas")
		})

	/*
		Create/Delete snapshot via k8s API using PVC (Dynamic Provisioning)

		1. Create a storage class (eg: vsan default) and create a pvc using this sc
		2. Create a VolumeSnapshot class with snapshotter as vsphere-csi-driver and set deletionPolicy to Delete
		3. Create a volume-snapshot with labels, using the above snapshot-class and pvc (from step-1) as source
		4. Ensure the snapshot is created, verify using get VolumeSnapshot
		5. Also verify that VolumeSnapshotContent is auto-created
		6. Verify the references to pvc and volume-snapshot on this object
		7. Verify that the VolumeSnapshot has ready-to-use set to True
		8. Verify that the Restore Size set on the snapshot is same as that of the source volume size
		9. Query the snapshot from CNS side using volume id - should pass and return the snapshot entry
		10. Delete the above snapshot from k8s side using kubectl delete, run a get and ensure it is removed
		11. Also ensure that the VolumeSnapshotContent is deleted along with the
			volume snapshot as the policy is delete
		12. Query the snapshot from CNS side - should return 0 entries
		13. Cleanup: Delete PVC, SC (validate they are removed)
	*/
	ginkgo.It("[supervisor-snapshot] snapshot-dynamic-provisioning-workflow",
		ginkgo.Label(p0, block, wcp, snapshot, stable, vc90), func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			//setting map values
			labelsMap = make(map[string]string)
			labelsMap["app"] = "test"

			ginkgo.By("Get storage class")
			storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, storagePolicyName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if isQuotaValidationSupported && supervisorCluster {
				//PVCQuota Details Before creating workload
				quota["pvc_totalQuotaUsedBefore"], _, quota["pvc_storagePolicyQuotaBefore"], _,
					quota["pvc_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx,
					restConfig, storagePolicyName, namespace, pvcUsage, volExtensionName, isLateBinding)
				framework.Logf("quota[vm_storagePolicyQuotaBefore] : %s", quota["vm_storagePolicyQuotaBefore"])

				//SnapshotQuota Details Before creating workload
				quota["snp_totalQuotaUsedBefore"], _, quota["snp_storagePolicyQuotaBefore"], _,
					quota["snp_storagePolicyUsageBefore"], _ = getStoragePolicyUsedAndReservedQuotaDetails(ctx,
					restConfig, storagePolicyName, namespace, snapshotUsage, snapshotExtensionName, isLateBinding)
				framework.Logf("quota[snp_storagePolicyQuotaBefore] : %s", quota["snp_storagePolicyQuotaBefore"])

			}

			ginkgo.By("Create PVC")
			pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
				diskSize, storageclass, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
			if guestCluster {
				volHandle = getVolumeIDFromSupervisorCluster(volHandle)
			}
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

			diskInGb := (diskSize1Gi / 1024) * 2
			diskInGbStr := convertInt64ToStrGbFormat(diskInGb)

			ginkgo.By("Create Pod to attach to Pvc")
			pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
				execRWXCommandPod1)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
				err = fpod.DeletePodWithWait(ctx, client, pod)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()

			var vmUUID string
			var exists bool

			snapc, err := snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
			_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create volume snapshot class")
			volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a dynamic volume snapshot")
			volumeSnapshot, _, _,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			snapshotSize1 := getAggregatedSnapshotCapacityInMb(e2eVSphere, volHandle)
			snapshotSizeStr := convertInt64ToStrMbFormat(snapshotSize1)

			if isQuotaValidationSupported && supervisorCluster {
				var expectedTotalStorage []string
				expectedTotalStorage = append(expectedTotalStorage, diskInGbStr, snapshotSizeStr)

				//Validate TotalQuota
				_, quotavalidationStatus := validateTotalQuota(ctx, restConfig, storagePolicyName, namespace,
					expectedTotalStorage, quota["totalQuotaUsedBefore"], isLateBinding)
				gomega.Expect(quotavalidationStatus).NotTo(gomega.BeFalse())

				//Validates PVC quota in both StoragePolicyQuota and StoragePolicyUsage CR
				sp_quota_status_pvc, sp_usage_status_pvc := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
					storagePolicyName, namespace, pvcUsage, volExtensionName, []string{diskInGbStr},
					quota["pvc_totalQuotaUsedBefore"], quota["pvc_storagePolicyQuotaBefore"],
					quota["pvc_storagePolicyUsageBefore"], isLateBinding)
				gomega.Expect(sp_quota_status_pvc && sp_usage_status_pvc).NotTo(gomega.BeFalse())

				//Validates PVC quota in both StoragePolicyQuota and StoragePolicyUsage CR
				sp_quota_status_snap, sp_usage_status_snap := validateQuotaUsageAfterResourceCreation(ctx, restConfig,
					storagePolicyName, namespace, snapshotUsage, snapshotExtensionName, []string{snapshotSizeStr},
					quota["snp_totalQuotaUsedBefore"], quota["snp_storagePolicyQuotaBefore"],
					quota["snp_storagePolicyUsageBefore"], isLateBinding)
				gomega.Expect(sp_quota_status_snap && sp_usage_status_snap).NotTo(gomega.BeFalse())
			}

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, defaultPandoraSyncWaitTime, volHandle, snapshotId, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
		Volume restore using snapshot on a different storageclass
		1. Create a sc with thin-provisioned spbm policy, create a pvc and attach the pvc to a pod
		2. Create a dynamically provisioned snapshots using this pvc
		3. create another sc pointing to a different spbm policy (say thick)
		4. Run a restore workflow by giving a different storageclass in the pvc spec
		5. the new storageclass would point to a thick provisioned spbm plocy,
		while the source pvc was created usig thin provisioned psp-operatorlicy
		6. cleanup spbm policies, sc's, pvc's
	*/
	ginkgo.It("[supervisor-snapshot] Restore-using-snapshot-on-a-different-storageclass",
		ginkgo.Label(p0, block, wcp, snapshot, stable, vc90), func() {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			snapc, err := snapclient.NewForConfig(restConfig)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Get storage class")
			storageclass, err := createStorageClass(client, scParameters, nil, "", "", false, storagePolicyName)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create PVC")
			pvclaim, persistentVolumes, err := createPVCAndQueryVolumeInCNS(ctx, client, namespace, labelsMap, "",
				diskSize, storageclass, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

			ginkgo.By("Create volume snapshot class")
			volumeSnapshotClass, err := createVolumeSnapshotClass(ctx, snapc, deletionPolicy)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Create a dynamic volume snapshot")
			volumeSnapshot, _, _,
				_, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var storageclass1 *storagev1.StorageClass
			storageclass1, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName2, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Restore a pvc using a dynamic volume snapshot created above but with a different storage class")
			_, pvs2, _ := verifyVolumeRestoreOperation(ctx, client, namespace, storageclass1,
				volumeSnapshot, diskSize, false)
			volHandle2 := pvs2[0].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle2).NotTo(gomega.BeEmpty())

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, defaultPandoraSyncWaitTime, volHandle, snapshotId, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		})

	/*
	   Basic test
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create a PVC say pvc1
	   3   Create a VMservice VM say vm1, pvc1
	   4   verify pvc1 CNS metadata.
	   5   Once the vm1 is up verify that the volume is accessible inside vm1
	   6   Delete vm1
	   7   delete pvc1
	   8   Remove spbm policy attached to test namespace

	   statically provisioned CSI volumes
	   Steps:
	   1   Assign a spbm policy to test namespace with sufficient quota
	   2   Create two FCDs
	   3   Create a static PV/PVC using cns register volume API
	   4   Create a VMservice VM and with the pvcs created in step 3
	   5   Verify CNS metadata for pvcs.
	   6   Write some IO to the CSI volumes and read it back from them and verify the data integrity
	   7   Delete VM service VM
	   8   delete pvcs
	   9   Remove spbm policy attached to test namespace
	*/
	ginkgo.It("vmservice-vm-create-Delete-WithPVC", ginkgo.Label(p0,
		vmServiceVm, block, wcp, vc80), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pandoraSyncWaitTime int
		var err error
		curtime := time.Now().Unix()
		curtimestring := strconv.FormatInt(curtime, 10)
		pvcName := "cns-pvc-" + curtimestring
		framework.Logf("pvc name :%s", pvcName)

		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}

		ginkgo.By("Creating FCD Disk")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, storageProfileId, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			pandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
			namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		staticPvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		staticPv := getPvFromClaim(client, namespace, pvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, staticPvc, staticPv, fcdID)

		ginkgo.By("Create a storageclass")
		storageclass, err := client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", storageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		ginkgo.By("Creating VM")
		vm := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc, staticPvc}, vmi, storagePolicyName,
			secretName)

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		_ = createService4Vm(ctx, vmopC, namespace, vm.Name)

		ginkgo.By("Wait for VM to come up and get an IP")
		vmIp, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Wait and verify PVCs are attached to the VM")
		gomega.Expect(waitNverifyPvcsAreAttachedToVmsvcVm(ctx, vmopC, cnsopC, vm,
			[]*v1.PersistentVolumeClaim{pvc, staticPvc})).NotTo(gomega.HaveOccurred())

		isPrivateNetwork := GetBoolEnvVarOrDefault("IS_PRIVATE_NETWORK", false)
		if !isPrivateNetwork {
			ginkgo.By("Verify PVCs are accessible to the VM")
			ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
			vm, err = getVmsvcVM(ctx, vmopC, vm.Namespace, vm.Name) // refresh vm info
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for i, vol := range vm.Status.Volumes {
				volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp)
				verifyDataIntegrityOnVmDisk(vmIp, volFolder)
			}
		}

		ginkgo.By("Deleting VM")
		err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
			Name:      vm.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	/**
	1. Create a PVC using WFFC / Late binding storage class
	2. PVC's with WFFC will be in pending state
	3. Use the above  PVC and create a VmService VM
	4. Once the VM is on Verify that the PVC should go to bound state
	5. TODO : verify PVC with the below annotations cns.vmware.com/selected-node-is-zone is set to true
	   volume.kubernetes.io/selected-node is set to zone - This should have the zone name where the VM gets provisioned
	6. Verify CNS metadata for PVC
	7. Verify PVC's attached to VM
	8. Validate TotalQuota, StoragePolicyQuota, storageQuotaUsage of VmserviceVm's and PVc's

	*/
	ginkgo.It("vmserviceVM-Using-latebinding-storageclass", ginkgo.Label(p0, block, wcp, vmsvc, vc901), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wffsStoragePolicyName := storagePolicyName + "-latebinding"
		isLateBinding = true

		ginkgo.By("Get WFFC stotage class")
		wffcStorageclass, err := client.StorageV1().StorageClasses().Get(ctx, wffsStoragePolicyName,
			metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("wffsStoragePolicyName: %s wffcStorageclass: %s", wffsStoragePolicyName, wffcStorageclass)

		ginkgo.By("Create a PVC")
		pvc, err := createPVC(ctx, client, namespace, nil, "", wffcStorageclass, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		vmImageName := GetAndExpectStringEnvVar(envVmsvcVmImageName)
		framework.Logf("Waiting for virtual machine image list to be available in namespace '%s' for image '%s'",
			namespace, vmImageName)
		vmi := waitNGetVmiForImageName(ctx, vmopC, vmImageName)
		gomega.Expect(vmi).NotTo(gomega.BeEmpty())

		ginkgo.By("Wait for VM images to get listed under namespace and create VM")
		err = pollWaitForVMImageToSync(ctx, namespace, vmImageName, poll, supervisorClusterOperationsTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Creating VM bootstrap data")
		secretName := createBootstrapSecretForVmsvcVms(ctx, client, namespace)

		ginkgo.By("Create vm service vm")
		vm1 := createVmServiceVmWithPvcs(
			ctx, vmopC, namespace, vmClass, []*v1.PersistentVolumeClaim{pvc}, vmi, wffsStoragePolicyName, secretName)

		ginkgo.By("Creating loadbalancing service for ssh with the VM")
		_ = createService4Vm(ctx, vmopC, namespace, vm1.Name)

		ginkgo.By("Wait for VMs to come up and get an IP")
		vmIp1, err := waitNgetVmsvcVmIp(ctx, vmopC, namespace, vm1.Name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting PVC to be in bound state")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, []*v1.PersistentVolumeClaim{pvc}, pollTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := pvs[0]
		volHandle := pv.Spec.CSI.VolumeHandle
		gomega.Expect(volHandle).NotTo(gomega.BeEmpty())

		ginkgo.By("get VM storage")
		vmQuotaUsed := getVMStorageData(ctx, vmopC, namespace, vm1.Name)
		framework.Logf("vmQuotaUsed : %s", vmQuotaUsed)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify PVCs are accessible to the VM")
		ginkgo.By("Write some IO to the CSI volumes and read it back from them and verify the data integrity")
		vm1, err = getVmsvcVM(ctx, vmopC, vm1.Namespace, vm1.Name) // refresh vm info
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		for i, vol := range vm1.Status.Volumes {
			volFolder := formatNVerifyPvcIsAccessible(vol.DiskUuid, i+1, vmIp1)
			verifyDataIntegrityOnVmDisk(vmIp1, volFolder)
		}

		ginkgo.By("Deleting VM")
		err = vmopC.Delete(ctx, &vmopv1.VirtualMachine{ObjectMeta: metav1.ObjectMeta{
			Name:      vm1.Name,
			Namespace: namespace,
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

	})

	// This test verifies the static provisioning workflow on supervisor cluster.
	//
	// Test Steps:
	// 1. Create CNS volume note the volumeID.
	// 2. Create Resource quota.
	// 3. create CNS register volume with above created VolumeID.
	// 4. verify created PV, PVC and check the bidirectional reference.
	// 5. Create Pod , with above created PVC.
	// 6. Verify volume is attached to the node and volume is accessible in the pod.
	// 7. Delete POD.
	// 8. Delete PVC.
	// 9. Verify PV is deleted automatically.
	// 10. Verify Volume id deleted automatically.
	// 11. Verify CRD deleted automatically.
	ginkgo.It("[csi-supervisor] static-provisioning-import-CNS-volume", ginkgo.Label(p0, block, wcp, vc70), func() {
		var err error
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		curtime := time.Now().Unix()
		curtimestring := strconv.FormatInt(curtime, 10)
		pvcName := "cns-pvc-" + curtimestring
		framework.Logf("pvc name :%s", pvcName)

		restConfig, storageclass, profileID := staticProvisioningPreSetUpUtil(ctx, f, client, storagePolicyName)
		framework.Logf("Storage class : %s", storageclass.Name)

		ginkgo.By("Creating FCD (CNS Volume)")
		fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
			"staticfcd"+curtimestring, profileID, diskSizeInMb, defaultDatastore.Reference())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
			defaultPandoraSyncWaitTime, fcdID))
		time.Sleep(time.Duration(defaultPandoraSyncWaitTime) * time.Second)

		ginkgo.By("Create CNS register volume with above created FCD ")
		cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
		err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx, restConfig,
			namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
		cnsRegisterVolumeName := cnsRegisterVolume.GetName()
		framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

		ginkgo.By("verify created PV, PVC and check the bidirectional reference")
		pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pv := getPvFromClaim(client, namespace, pvcName)
		verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvc, pv, fcdID)

		ginkgo.By("Creating pod")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvc}, false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		podName := pod.GetName()
		framework.Logf("podName : %s", podName)

		ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s",
			pv.Spec.CSI.VolumeHandle, pod.Spec.NodeName))
		var vmUUID string
		var exists bool
		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting the pod")
		err = fpod.DeletePodWithWait(ctx, client, pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
			pv.Spec.CSI.VolumeHandle, vmUUID))
		_, err = e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
		gomega.Expect(err).To(gomega.HaveOccurred(),
			fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from the PodVM",
				vmUUID, pv.Spec.CSI.VolumeHandle))

	})

	/*
		Verify online volume expansion on dynamic volume

		1. Create StorageClass with allowVolumeExpansion set to true.
		2. Create PVC which uses the StorageClass created in step 1.
		3. Wait for PV to be provisioned.
		4. Wait for PVC's status to become Bound and note down the size
		5. Create a Pod using the above created PVC
		6. Modify PVC's size to trigger online volume expansion
		7. verify the PVC status will change to "FilesystemResizePending". Wait till the status is removed
		8. Verify the resized PVC by doing CNS query
		9. Make sure data is intact on the PV mounted on the pod
		10.  Make sure file system has increased

	*/
	ginkgo.It("[csi-supervisor] Online-volume-expansion-on-dynamic-volume", ginkgo.Label(p0, block, wcp, core, vc70), func() {
		ginkgo.By("Invoking Test for Volume Expansion")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		//var storageclass *storagev1.StorageClass
		var pvclaim *v1.PersistentVolumeClaim
		var pv *v1.PersistentVolume
		var volHandle string

		//precreatednamespace := getNamespaceToRunTests(f)

		//setStoragePolicyQuota(ctx, restConfig, storagePolicyName, precreatednamespace, rqLimit)

		ginkgo.By("Create StorageClass with allowVolumeExpansion set to true, Create PVC")
		sharedVSANDatastoreURL := GetAndExpectStringEnvVar(envSharedDatastoreURL)
		volHandle, pvclaim, pv, storageclass = createSCwithVolumeExpansionTrueAndDynamicPVC(
			ctx, f, client, sharedVSANDatastoreURL, storagePolicyName, namespace, ext4FSType)

		ginkgo.By("Create Pod to attach to Pvc")
		pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim}, false,
			execRWXCommandPod1)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer func() {
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err = fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		var vmUUID string
		var exists bool

		annotations := pod.Annotations
		vmUUID, exists = annotations[vmUUIDLabel]
		gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation", vmUUIDLabel))
		_, err = e2eVSphere.getVMByUUID(ctx, vmUUID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		time.Sleep(sleepTimeOut)

		// ginkgo.By("Create Pod using the above PVC")
		// pod, vmUUID = createPODandVerifyVolumeMount(ctx, f, client, namespace, pvclaim, volHandle, "")

		defer func() {
			// Delete Pod.
			ginkgo.By(fmt.Sprintf("Deleting the pod %s in namespace %s", pod.Name, namespace))
			err := fpod.DeletePodWithWait(ctx, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if supervisorCluster {
				ginkgo.By(fmt.Sprintf("Verify volume: %s is detached from PodVM with vmUUID: %s",
					pv.Spec.CSI.VolumeHandle, vmUUID))
				_, err := e2eVSphere.getVMByUUIDWithWait(ctx, vmUUID, supervisorClusterOperationsTimeout)
				gomega.Expect(err).To(gomega.HaveOccurred(),
					fmt.Sprintf("PodVM with vmUUID: %s still exists. So volume: %s is not detached from "+
						"the PodVM", vmUUID, pv.Spec.CSI.VolumeHandle))
			} else {
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client,
					volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName))
			}
		}()

		ginkgo.By("Increase PVC size and verify online volume resize")
		//increaseSizeOfPvcAttachedToPod(f, client, namespace, pvclaim, pod)
		// resize PVC
		// Modify PVC spec to trigger volume expansion

		var originalSizeInMb int64

		// Fetch original FileSystemSize if not raw block volume
		if *pvclaim.Spec.VolumeMode != v1.PersistentVolumeBlock {
			ginkgo.By("Verify filesystem size for mount point /mnt/volume1 before expansion")
			originalSizeInMb, err = getFileSystemSizeForOsType(f, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Expanding current pvc")
		currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
		newSize := currentPvcSize.DeepCopy()
		newSize.Add(resource.MustParse("1Gi"))
		framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
		pvclaim, err = expandPVCSize(pvclaim, newSize, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvclaim).NotTo(gomega.BeNil())

		ginkgo.By("Waiting for file system resize to finish")
		pvclaim, err = waitForFSResize(pvclaim, client)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcConditions := pvclaim.Status.Conditions
		expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

		if *pvclaim.Spec.VolumeMode != v1.PersistentVolumeBlock {
			var fsSize int64
			ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
			fsSize, err = getFileSystemSizeForOsType(f, client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %v, fsSize, after expansion : %v",
				originalSizeInMb, fsSize)
			// Filesystem size may be smaller than the size of the block volume
			// so here we are checking if the new filesystem size is greater than
			// the original volume size as the filesystem is formatted for the
			// first time
			gomega.Expect(fsSize).Should(gomega.BeNumerically(">", originalSizeInMb),
				fmt.Sprintf("error updating filesystem size for %q. Resulting filesystem size is %d",
					pvclaim.Name, fsSize))
			ginkgo.By("File system resize finished successfully")
		} else {
			ginkgo.By("Volume resize finished successfully")
		}
	})

	/*
		This test verifies offline and online volume expansion on statically created PVC.

		Test Steps:
			1. Create FCD with valid storage policy .
			2. Call CnsRegisterVolume API by specifying VolumeID, AccessMode set to "ReadWriteOnce” and PVC Name
			3. Verify PV and PVC’s should be created and they are bound and note the size
			4. Trigger Offline volume expansion
			5. Verify that PVC size shouldn't have increased
			6. Verify PV size is updated
			7. Create POD
		    8. Verify PVC size is increased and Offline volume expansion comepleted after creating POD
		    9. Resize PVC to trigger online volume expansion on the same PVC
			10. wait for some time for resize to complete and verify that "FilesystemResizePending" is removed from PVC
			11. query CNS volume and make sure new size is updated
			12. Verify data is intact on the PV mounted on the pod
			13. Verify File system has increased
			14. Delete POD, PVC, PV, CNSregisterVolume and SC
	*/
	ginkgo.It("[csi-supervisor] Offline-volume-resize-on-staticVolume ",
		ginkgo.Label(p0, block, wcp, vc70), func() {
			var err error
			var fsSize int64
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			curtime := time.Now().Unix()
			curtimestring := strconv.FormatInt(curtime, 10)
			pvcName := "cns-pvc-" + curtimestring
			framework.Logf("pvc name :%s", pvcName)

			storageclass, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Creating FCD (CNS Volume)")
			fcdID, err := e2eVSphere.createFCDwithValidProfileID(ctx,
				"staticfcd"+curtimestring, storageProfileId, diskSizeInMb, defaultDatastore.Reference())
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow newly created FCD:%s to sync with pandora",
				defaultPandoraSyncWaitTime, fcdID))
			time.Sleep(time.Duration(defaultPandoraSyncWaitTime) * time.Second)

			ginkgo.By("Create CNS register volume with above created FCD ")
			cnsRegisterVolume := getCNSRegisterVolumeSpec(ctx, namespace, fcdID, "", pvcName, v1.ReadWriteOnce)
			err = createCNSRegisterVolume(ctx, restConfig, cnsRegisterVolume)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.ExpectNoError(waitForCNSRegisterVolumeToGetCreated(ctx,
				restConfig, namespace, cnsRegisterVolume, poll, supervisorClusterOperationsTimeout))
			cnsRegisterVolumeName := cnsRegisterVolume.GetName()
			framework.Logf("CNS register volume name : %s", cnsRegisterVolumeName)

			ginkgo.By(" verify created PV, PVC and check the bidirectional reference")
			pvclaim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			pv := getPvFromClaim(client, namespace, pvcName)
			verifyBidirectionalReferenceOfPVandPVC(ctx, client, pvclaim, pv, fcdID)
			volHandle := pv.Spec.CSI.VolumeHandle

			// Modify PVC spec to trigger volume expansion
			// We expand the PVC while no pod is using it to ensure offline expansion
			ginkgo.By("Expanding current pvc")
			currentPvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %v, newSize %v", currentPvcSize, newSize)
			pvclaim, err = expandPVCSize(pvclaim, newSize, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(pvclaim).NotTo(gomega.BeNil())

			pvcSize := pvclaim.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc size %q", pvclaim.Name)
			}

			ginkgo.By("Waiting for controller volume resize to finish")
			err = waitForPvResizeForGivenPvc(pvclaim, client, totalResizeWaitPeriod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Checking for conditions on pvc")
			pvclaim, err = waitForPVCToReachFileSystemResizePendingCondition(client, namespace,
				pvclaim.Name, pollTimeout)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By(fmt.Sprintf("Invoking QueryCNSVolumeWithResult with VolumeID: %s", volHandle))
			queryResult, err := e2eVSphere.queryCNSVolumeWithResult(volHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if len(queryResult.Volumes) == 0 {
				err = fmt.Errorf("queryCNSVolumeWithResult returned no volume")
			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By("Verifying disk size requested in volume expansion is honored")
			newSizeInMb := int64(3072)
			if queryResult.Volumes[0].BackingObjectDetails.(*cnstypes.CnsBlockBackingDetails).
				CapacityInMb != newSizeInMb {
				err = fmt.Errorf("got wrong disk size after volume expansion")

			}
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			framework.Logf("namespace inside method :%s", namespace)

			// Create a Pod to use this PVC, and verify volume has been attached
			ginkgo.By("Creating pod to attach PV to the node")
			pod, err := createPod(ctx, client, namespace, nil, []*v1.PersistentVolumeClaim{pvclaim},
				false, execCommand)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			var vmUUID string
			var exists bool
			ginkgo.By(fmt.Sprintf("Verify volume: %s is attached to the node: %s", volHandle,
				pod.Spec.NodeName))
			annotations := pod.Annotations
			vmUUID, exists = annotations[vmUUIDLabel]
			gomega.Expect(exists).To(gomega.BeTrue(), fmt.Sprintf("Pod doesn't have %s annotation",
				vmUUIDLabel))

			framework.Logf("VMUUID : %s", vmUUID)
			isDiskAttached, err := e2eVSphere.isVolumeAttachedToVM(client, volHandle, vmUUID)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			gomega.Expect(isDiskAttached).To(gomega.BeTrue(), "Volume is not attached to the node")

			ginkgo.By("Verify the volume is accessible and filesystem type is as expected")
			_, err = e2eoutput.LookForStringInPodExec(namespace, pod.Name,
				[]string{"/bin/cat", "/mnt/volume1/fstype"}, "", time.Minute)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Waiting for file system resize to finish")
			pvclaim, err = waitForFSResize(pvclaim, client)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			pvcConditions := pvclaim.Status.Conditions
			expectEqual(len(pvcConditions), 0, "pvc should not have conditions")

			ginkgo.By("Verify filesystem size for mount point /mnt/volume1")
			fsSize, err = getFSSizeMb(f, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			framework.Logf("File system size after expansion : %d", fsSize)

			// Filesystem size may be smaller than the size of the block volume
			// so here we are checking if the new filesystem size is greater than
			// the original volume size as the filesystem is formatted for the
			// first time after pod creation
			if fsSize < diskSizeInMb {
				framework.Failf("error updating filesystem size for %q. Resulting filesystem size is %d",
					pvclaim.Name, fsSize)
			}
			ginkgo.By("File system resize finished successfully")
		})

})
