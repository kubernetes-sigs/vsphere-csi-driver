package e2e

import (
	"context"
	"fmt"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
)

var _ bool = ginkgo.Describe("[csi-supervisor] config-change-test", func() {
	f := framework.NewDefaultFramework("e2e-config-change-test")
	var (
		client               clientset.Interface
		namespace            string
		scParameters         map[string]string
		storagePolicyName    string
		ctx                  context.Context
		nimbusGeneratedVcPwd string
	)
	const (
		configSecret = "vsphere-config-secret"
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		nodeList, err := fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		nimbusGeneratedVcPwd = GetAndExpectStringEnvVar(nimbusVcPwd)
	})

	ginkgo.AfterEach(func() {
		if supervisorCluster {
			deleteResourceQuota(client, namespace)
		}
	})
	/*
		Perform Password change and check if k8s resources can be modified after the password change
		Steps:
			1. Create StorageClass and PVC
			2. Wait for PVC to be Bound
			3. Change VC password
			4. Modify the k8s secret file to reflect the new password
			5. Delete PVC
			6. Revert the secret file change
			7. Revert the password change
			8. Delete Storage class
	*/
	ginkgo.It("verify PVC deletion after VC password change", func() {
		ginkgo.By("Invoking password change test")
		profileID := e2eVSphere.GetSpbmPolicyID(storagePolicyName)
		scParameters[scParamStoragePolicyID] = profileID
		// create resource quota
		createResourceQuota(client, namespace, rqLimit, storagePolicyName)
		// Create Storage class and PVC
		ginkgo.By("Creating Storage Class and PVC")
		_, pvc, err := createPVCAndStorageClass(client, namespace, nil,
			scParameters, "", nil, "", false, "", storagePolicyName)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Waiting for claim %s to be in bound phase", pvc.Name))
		pvs, err := fpv.WaitForPVClaimBoundPhase(client,
			[]*v1.PersistentVolumeClaim{pvc}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(pvs).NotTo(gomega.BeEmpty())
		pv := pvs[0]
		defer func() {
			err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		ginkgo.By("fetching the username and password of the current vcenter session from secret")
		secret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		originalConf := string(secret.Data[vsphereCloudProviderConfiguration])
		vsphereCfg, err := readConfigFromSecretString(originalConf)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintln("Changing password on the vCenter host"))
		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		username := vsphereCfg.Global.User
		currentPassword := vsphereCfg.Global.Password
		newPassword := e2eTestPassword
		err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, newPassword, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Modifying the password in the secret")
		vsphereCfg.Global.Password = newPassword
		modifiedConf, err := writeConfigToSecretString(vsphereCfg)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Updating the secret to reflect the new password")
		secret.Data[vsphereCloudProviderConfiguration] = []byte(modifiedConf)
		_, err = client.CoreV1().Secrets(csiSystemNamespace).Update(ctx, secret, metav1.UpdateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			ginkgo.By("Reverting the password change")
			err = invokeVCenterChangePassword(username, nimbusGeneratedVcPwd, currentPassword, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Reverting the secret change back to reflect the original password")
			currentSecret, err := client.CoreV1().Secrets(csiSystemNamespace).Get(ctx, configSecret, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			currentSecret.Data[vsphereCloudProviderConfiguration] = []byte(originalConf)
			_, err = client.CoreV1().Secrets(csiSystemNamespace).Update(ctx, currentSecret, metav1.UpdateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}()

		// As we are in the same vCenter session, deletion of PVC should go through
		ginkgo.By("Deleting PVC")
		err = fpv.DeletePersistentVolumeClaim(client, pvc.Name, namespace)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})

	/*
		TODO: e2e test to perform username change and check if k8s resources can be accessed after the change

		Steps:
			1. Create StorageClass and PVC
			2. Wait for PVC to be Bound
			3. Change VC username
				This will evict the user from the current vCenter session
			4. Delete PVC should fail
			5. Modify the secret with the new username
			6. Delete PVC -> This time it should succeed
			7. Delete Storage class
	*/

	/*
		TODO: Add negative test case where the test is performing create/delete with invalid credentials

		Steps:
			1. Create StorageClass and PVC
			2. Wait for PVC to be Bound
			3. Change VC password
				This will evict the user from the current vCenter session
			4. Delete PVC should fail
			5. Modify the secret with the wrong password
			6. Delete PVC -> This should result in failure
			7. Update the secret with the correct password
			8. Delete PVC -> This should succeed
			9. Delete Storage class
	*/
})
