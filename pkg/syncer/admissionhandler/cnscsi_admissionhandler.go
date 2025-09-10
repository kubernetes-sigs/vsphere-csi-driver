package admissionhandler

import (
	"context"
	"crypto/tls"
	_ "crypto/tls/fipsonly"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco/k8sorchestrator"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common/commonco"

	cnsfileaccessconfigv1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsfileaccessconfig/v1alpha1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	ValidationWebhookPath            = "/validate"
	MutationWebhookPath              = "/mutate"
	DefaultWebhookPort               = 9883
	DefaultWebhookMetricsBindAddress = "0"
	devopsUserLabelKey               = "cns.vmware.com/user-created"
	vmNameLabelKey                   = "cns.vmware.com/vm-name"
	pvcNameLabelKey                  = "cns.vmware.com/pvc-name"
)

var (
	// This client is generated in the kubeadm bootstrap stages
	// using the kubernetes CA
	webhookClientCAFile        = "client-ca/ca.crt"
	webhookClientCertificateCN = "apiserver-webhook-client"
)

func getWebhookPort() int {
	portStr, ok := os.LookupEnv("CNSCSI_WEBHOOK_SERVICE_CONTAINER_PORT")
	if !ok {
		return DefaultWebhookPort
	}

	result, err := strconv.ParseInt(portStr, 0, 0)
	if err != nil {
		panic(fmt.Sprintf("malformed configuration: CNSCSI_WEBHOOK_SERVICE_CONTAINER_PORT, expected int: %v", err))
	}

	return int(result)
}

func getMetricsBindAddress() string {
	metricsAddr, ok := os.LookupEnv("CNSCSI_WEBHOOK_SERVICE_METRICS_BIND_ADDR")
	if !ok {
		return DefaultWebhookMetricsBindAddress
	}

	return metricsAddr
}

// startCNSCSIWebhookManager starts the webhook server in supervisor cluster
func startCNSCSIWebhookManager(ctx context.Context, enableWebhookClientCertVerification bool,
	commonInterface commonco.COCommonInterface) error {
	log := logger.GetLogger(ctx)

	var clientCAName string
	webhookPort := getWebhookPort()
	metricsBindAddress := getMetricsBindAddress()
	log.Infof("setting up webhook manager with webhookPort %v and metricsBindAddress %v",
		webhookPort, metricsBindAddress)

	tlsConfigOpts := []func(*tls.Config){
		func(t *tls.Config) {
			// CipherSuites allows us to specify TLS 1.2 cipher suites that have been recommended by the Security team
			t.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_AES_128_GCM_SHA256,
				tls.TLS_AES_256_GCM_SHA384}
			t.MinVersion = tls.VersionTLS12
		},
	}
	// This client CA is used to verify the client connections being made to the webhook server and
	// authenticate whether a valid cert is used to contact the server.
	// Ref: https://github.com/kubernetes-sigs/controller-runtime/blob/main/pkg/webhook/server.go#L220-L235
	if enableWebhookClientCertVerification {
		// set the copied file as the client CA
		clientCAName = webhookClientCAFile
		tlsConfigOpts = append(tlsConfigOpts, setVerifyPeerCertificate)
	}
	mgrOpts := manager.Options{
		Metrics: metricsserver.Options{
			BindAddress: metricsBindAddress,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port:         webhookPort,
			TLSOpts:      tlsConfigOpts,
			ClientCAName: clientCAName,
		})}

	if featureGateByokEnabled {
		var err error
		if mgrOpts.Scheme, err = crypto.NewK8sScheme(); err != nil {
			return err
		}

		mgrOpts.Client = client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{
					&corev1.ConfigMap{},
					&corev1.Secret{},
				},
			},
		}
	}

	mgr, err := manager.New(crConfig.GetConfigOrDie(), mgrOpts)
	if err != nil {
		log.Fatal(err, "unable to set up overall controller manager")
	}

	k8sClient := mgr.GetClient()
	cryptoClient := crypto.NewClient(ctx, k8sClient)

	log.Infof("registering validating webhook with the endpoint %v", ValidationWebhookPath)

	webhookServer := mgr.GetWebhookServer()

	webhookServer.Register(ValidationWebhookPath, &webhook.Admission{Handler: &CSISupervisorWebhook{
		Client:            k8sClient,
		CryptoClient:      cryptoClient,
		clientConfig:      mgr.GetConfig(),
		coCommonInterface: commonInterface,
	}})

	log.Infof("registering mutation webhook with the endpoint %v", MutationWebhookPath)
	webhookServer.Register(MutationWebhookPath, &webhook.Admission{Handler: &CSISupervisorMutationWebhook{
		Client:            k8sClient,
		CryptoClient:      cryptoClient,
		coCommonInterface: commonInterface,
	}})

	if err = mgr.Start(signals.SetupSignalHandler()); err != nil {
		return fmt.Errorf("unable to run the webhook manager: %w", err)
	}

	return nil
}

var _ admission.Handler = &CSISupervisorWebhook{}

type CSISupervisorWebhook struct {
	client.Client
	CryptoClient      crypto.Client
	clientConfig      *rest.Config
	coCommonInterface commonco.COCommonInterface
}

func (h *CSISupervisorWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("CNS-CSI validation webhook handler called with request: %+v", req)
	defer log.Debugf("CNS-CSI validation webhook handler completed for the request: %+v", req)

	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		if featureGateByokEnabled {
			resp = validatePVCRequestForCrypto(ctx, h.CryptoClient, req)
			if !resp.Allowed {
				return
			}
		}
		if featureGateTKGSHaEnabled {
			resp = validatePVCAnnotationForTKGSHA(ctx, req)
			if !resp.Allowed {
				return
			}
		}
		resp = validatePVCAnnotationForVolumeHealth(ctx, req)
		if !resp.Allowed {
			return
		}
		if featureGateBlockVolumeSnapshotEnabled {
			admissionResp := validatePVC(ctx, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()
		}
	} else if req.Kind.Kind == "CnsFileAccessConfig" {
		if featureFileVolumesWithVmServiceEnabled {
			switch req.Operation {
			case admissionv1.Create:
				admissionResp := validateCreateCnsFileAccessConfig(ctx, h.clientConfig, &req.AdmissionRequest)
				resp.AdmissionResponse = *admissionResp.DeepCopy()
			case admissionv1.Delete:
				admissionResp := validateDeleteCnsFileAccessConfig(ctx, h.clientConfig, &req.AdmissionRequest)
				resp.AdmissionResponse = *admissionResp.DeepCopy()
			}
		}
	} else if req.Kind.Kind == "VolumeSnapshot" {
		if featureIsLinkedCloneSupportEnabled {
			admissionResp := validateSnapshotOperationSupervisorRequest(ctx, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()
		}
	}
	return
}

var _ admission.Handler = &CSISupervisorMutationWebhook{}

type CSISupervisorMutationWebhook struct {
	client.Client
	CryptoClient      crypto.Client
	coCommonInterface commonco.COCommonInterface
}

func (h *CSISupervisorMutationWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	log := logger.GetLogger(ctx)
	log.Debugf("CNS-CSI mutation webhook handler called with request: %+v", req)
	defer log.Debugf("CNS-CSI mutation webhook handler completed for the request: %+v", req)

	if req.Kind.Kind == "PersistentVolumeClaim" {
		switch req.Operation {
		case admissionv1.Create:
			return h.mutateNewPVC(ctx, req)
		}
	} else if req.Kind.Kind == "CnsFileAccessConfig" {
		if featureFileVolumesWithVmServiceEnabled {
			switch req.Operation {
			case admissionv1.Create:
				return h.mutateNewCnsFileAccessConfig(ctx, req)
			}
		}
	}

	return admission.Allowed("")
}

// mutateNewCnsFileAccessConfig adds devops label on a CnsFileAccessConfig CR
// if it is being created by a devops user.
func (h *CSISupervisorMutationWebhook) mutateNewCnsFileAccessConfig(ctx context.Context,
	req admission.Request) admission.Response {
	log := logger.GetLogger(ctx)

	newCnsFileAccessConfig := cnsfileaccessconfigv1alpha1.CnsFileAccessConfig{}
	if err := json.Unmarshal(req.Object.Raw, &newCnsFileAccessConfig); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// If CR is created by CSI service account, do not add devops label.
	isPvCSIServiceAccount, err := validatePvCSIServiceAccount(req.UserInfo.Username)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if isPvCSIServiceAccount {
		log.Infof("Skip mutating CnsFileAccessConfig instance %s for PVCSI service account user",
			newCnsFileAccessConfig.Name)
		return admission.Allowed("")
	}

	if newCnsFileAccessConfig.Labels == nil {
		newCnsFileAccessConfig.Labels = make(map[string]string)
	}

	// Add VM name and PVC name label.
	// If someone created this CR with these labels already present, CSI will overrite on them
	// with the correct values.
	newCnsFileAccessConfig.Labels[devopsUserLabelKey] = "true"
	newCnsFileAccessConfig.Labels[vmNameLabelKey] = newCnsFileAccessConfig.Spec.VMName
	newCnsFileAccessConfig.Labels[pvcNameLabelKey] = newCnsFileAccessConfig.Spec.PvcName

	newRawCnsFileAccessConfig, err := json.Marshal(newCnsFileAccessConfig)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, newRawCnsFileAccessConfig)
}

func (h *CSISupervisorMutationWebhook) mutateNewPVC(ctx context.Context, req admission.Request) admission.Response {
	newPVC := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(req.Object.Raw, newPVC); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	var wasMutated bool

	if featureGateByokEnabled {
		if ok, err := setDefaultEncryptionClass(ctx, h.CryptoClient, newPVC); err != nil {
			return admission.Denied(err.Error())
		} else if ok {
			wasMutated = true
		}
	}

	if featureIsLinkedCloneSupportEnabled &&
		v1.HasAnnotation(newPVC.ObjectMeta, common.AnnKeyLinkedClone) &&
		newPVC.Annotations[common.AnnKeyLinkedClone] == "true" {
		// Set the same label
		if newPVC.Labels == nil {
			newPVC.Labels = make(map[string]string)
		}
		if _, ok := newPVC.Labels[common.AnnKeyLinkedClone]; !ok {
			newPVC.Labels[common.LinkedClonePVCLabel] = newPVC.Annotations[common.AnnKeyLinkedClone]
			wasMutated = true
		}
		// Retrieve the datasource
		dataSource, err := k8sorchestrator.GetPVCDataSource(ctx, newPVC)
		if err != nil {
			return admission.Denied("failed to retrieve the linked clone source " +
				"volumesnapshot. err:" + err.Error())
		}
		volumeSnapshotNamespace, volumeSnapshotName := dataSource.Namespace, dataSource.Name
		// Retrieve the source PVC
		sourcePVC, err := h.coCommonInterface.GetVolumeSnapshotPVCSource(ctx, volumeSnapshotNamespace,
			volumeSnapshotName)
		if err != nil {
			return admission.Denied("failed to retrieve the linked clone source PVC. err:" + err.Error())
		}
		sourcePVCAccessibility, ok := sourcePVC.Annotations[common.AnnVolumeAccessibleTopology]
		if !ok {
			errMsg := fmt.Sprintf("source PVC %s/%s does not have volume accessiblity annotation %s"+
				" set, cannot determine accessibility requrirement for linked clone PVC %s/%s",
				sourcePVC.Namespace, sourcePVC.Name, common.AnnVolumeAccessibleTopology,
				newPVC.Namespace, newPVC.Name)
			return admission.Denied(errMsg)
		}
		// Case-1: If the linked clone PVC has "csi.vsphere.volume-requested-topology" annotation:
		// - Determine the source PVC accessibility from "csi.vsphere.volume-accessible-topology" annotation
		// - Validate that it is same the linked clone PVC requested topology, fail the request if not.
		// Case-2: If the linked clone PVC does NOT have "csi.vsphere.volume-requested-topology" annotation:
		// - Determine the source PVC accessibility from "csi.vsphere.volume-accessible-topology" annotation
		// - Add it as the "csi.vsphere.volume-requested-topology" annotation on the linked clone PVC
		hasTopologyRequirement := v1.HasAnnotation(newPVC.ObjectMeta, common.AnnGuestClusterRequestedTopology)
		if hasTopologyRequirement {
			// determined the source accessibility requirement
			newPVCAccessibility := newPVC.Annotations[common.AnnGuestClusterRequestedTopology]
			if strings.Compare(newPVCAccessibility, sourcePVCAccessibility) != 0 {
				// accessibility requirement mismatch, deny the request and suggest the correct annotation
				errMsg := fmt.Sprintf("expected accessibility requirement: %s but got %s, "+
					"linked clone volumes must have the same accessibility as the source volume, if unset, it "+
					"will be automatically chosen", sourcePVCAccessibility, newPVCAccessibility)
				return admission.Denied(errMsg)
			}
		} else {
			// If not present, set it as the same as the source PVC
			newPVC.Annotations[common.AnnGuestClusterRequestedTopology] = sourcePVCAccessibility
			wasMutated = true
		}
	}

	if !wasMutated {
		return admission.Allowed("")
	}

	newRawPVC, err := json.Marshal(newPVC)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, newRawPVC)
}

// setVerifyPeerCertificate func sets VerifyPeerCertificate function to be used to
// verify webhook client i.e. APIserver certificate during connection to CSI webhook server
func setVerifyPeerCertificate(cfg *tls.Config) {
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
			return fmt.Errorf("no verified chains")
		}
		cert := verifiedChains[0][0]
		if cert.Subject.CommonName != webhookClientCertificateCN {
			return fmt.Errorf("unauthorized client CN: %s", cert.Subject.CommonName)
		}
		return nil
	}
}
