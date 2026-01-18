package restore_snapshot

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/bootstrap"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/constants"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/csisnapshot"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/k8testutil"
	"sigs.k8s.io/vsphere-csi-driver/v3/tests/e2e/vcutil"
)

var (
	ctx                 context.Context
	cancel              context.CancelFunc
	e2eTestConfig       *config.E2eTestConfig
	client              clientset.Interface
	volumeSnapshot      *snapV1.VolumeSnapshot
	namespace           string
	snapc               *snapclient.Clientset
	pandoraSyncWaitTime int
)

type PreSetupTest struct {
	Namespace      string
	StorageClass   *storagev1.StorageClass
	PVC            *v1.PersistentVolumeClaim
	VolumeSnapshot *snapV1.VolumeSnapshot
	VolHandle      string
	SnapC          *snapclient.Clientset
}

func PreSetup(f *framework.Framework, storagePolicyName string) *PreSetupTest {
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	client = f.ClientSet
	e2eTestConfig = bootstrap.Bootstrap()
	namespace = vcutil.GetNamespaceToRunTests(f, e2eTestConfig)

	scParameters := make(map[string]string)
	storagePolicyName = strings.ToLower(strings.ReplaceAll(storagePolicyName, " ", "-"))
	profileID := vcutil.GetSpbmPolicyID(storagePolicyName, e2eTestConfig)
	scParameters[constants.ScParamStoragePolicyID] = profileID

	labelsMap := map[string]string{"app": "test"}

	ginkgo.By("Create storage class")
	storageclass, err := k8testutil.CreateStorageClass(client, e2eTestConfig, scParameters, nil, "", "", false, storagePolicyName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		ginkgo.By("Delete Storage Class")
		err = client.StorageV1().StorageClasses().Delete(ctx, storageclass.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	}()

	ginkgo.By("Create PVC")
	pvclaim, persistentVolumes, err := k8testutil.CreatePVCAndQueryVolumeInCNS(ctx, client, e2eTestConfig, namespace, labelsMap, "", constants.DiskSize, storageclass, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	volHandle := persistentVolumes[0].Spec.CSI.VolumeHandle
	if e2eTestConfig.TestInput.ClusterFlavor.GuestCluster {
		volHandle = k8testutil.GetVolumeIDFromSupervisorCluster(volHandle)
	}
	gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
	ginkgo.DeferCleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		err := fpv.DeletePersistentVolumeClaim(cleanupCtx, client, pvclaim.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		err = vcutil.WaitForCNSVolumeToBeDeleted(e2eTestConfig, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
	// reading fullsync wait time
	if os.Getenv(constants.EnvPandoraSyncWaitTime) != "" {
		pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(constants.EnvPandoraSyncWaitTime))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	} else {
		pandoraSyncWaitTime = constants.DefaultPandoraSyncWaitTime
	}
	var snapc *snapclient.Clientset
	restConfig := k8testutil.GetRestConfigClient(e2eTestConfig)
	snapc, err = snapclient.NewForConfig(restConfig)

	ginkgo.By("Create volume snapshot class")
	volumeSnapshotClass, err := csisnapshot.CreateVolumeSnapshotClass(ctx, e2eTestConfig, snapc, constants.DeletionPolicy)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Create a dynamic volume snapshot")
	volumeSnapshot, _, _, _, _, _, err = csisnapshot.CreateDynamicVolumeSnapshot(
		ctx, e2eTestConfig, namespace, snapc, volumeSnapshotClass, pvclaim, volHandle, constants.DiskSize, true)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	return &PreSetupTest{
		Namespace:      namespace,
		StorageClass:   storageclass,
		PVC:            pvclaim,
		VolumeSnapshot: volumeSnapshot,
		VolHandle:      volHandle,
		SnapC:          snapc,
	}
}

type RestoreMatrixEntry struct {
	SourceSC  string   `json:"sourceSC"`
	TargetSCs []string `json:"targetSCs"`
}

func LoadRestoreMatrix(sharedDatastoreType string) ([]RestoreMatrixEntry, error) {
	data, err := os.ReadFile("restoreMatrix-OSA.json")
	if sharedDatastoreType == "VSAN2" {
		data, err = os.ReadFile("restoreMatrix-ESA.json")
		if err != nil {
			return nil, err
		}
	}

	var matrix []RestoreMatrixEntry
	err = json.Unmarshal(data, &matrix)
	return matrix, err
}
