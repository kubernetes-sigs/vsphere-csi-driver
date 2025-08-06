package admissionhandler

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	PVCSIValidationWebhookPath            = "/validate"
	PVCSIMutationWebhookPath              = "/mutate"
	PVCSIDefaultWebhookPort               = 9883
	PVCSIDefaultWebhookMetricsBindAddress = "0"
	PVCSIWebhookTlsMinVersion             = "1.2"
)

func getPVCSIWebhookPort() int {
	portStr, ok := os.LookupEnv("PVCSI_WEBHOOK_SERVICE_CONTAINER_PORT")
	if !ok {
		return DefaultWebhookPort
	}

	result, err := strconv.ParseInt(portStr, 0, 0)
	if err != nil {
		panic(fmt.Sprintf("malformed configuration: PVCSI_WEBHOOK_SERVICE_CONTAINER_PORT, expected int: %v", err))
	}

	return int(result)
}

func getPVCSIMetricsBindAddress() string {
	metricsAddr, ok := os.LookupEnv("PVCSI_WEBHOOK_SERVICE_METRICS_BIND_ADDR")
	if !ok {
		return DefaultWebhookMetricsBindAddress
	}

	return metricsAddr
}

// startPVCSIWebhookManager starts the webhook server in guest cluster
func startPVCSIWebhookManager(ctx context.Context) {
	log := logger.GetLogger(ctx)

	webhookPort := getPVCSIWebhookPort()
	metricsBindAddress := getPVCSIMetricsBindAddress()
	log.Infof("setting up webhook manager with webhookPort %v and metricsBindAddress %v",
		webhookPort, metricsBindAddress)
	mgr, err := manager.New(crConfig.GetConfigOrDie(), manager.Options{
		Metrics: metricsserver.Options{
			BindAddress: metricsBindAddress,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: webhookPort,
			TLSOpts: []func(*tls.Config){
				func(t *tls.Config) {
					// CipherSuites allows us to specify TLS 1.2 cipher suites that have been recommended by the Security team
					t.CipherSuites = []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
						tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384}
					t.MinVersion = tls.VersionTLS12
				},
			},
		})})
	if err != nil {
		log.Fatal(err, "unable to set up overall controller manager")
	}

	log.Infof("registering validating webhook with the endpoint %v", PVCSIValidationWebhookPath)

	mgr.GetWebhookServer().Register(PVCSIValidationWebhookPath, &webhook.Admission{Handler: &CSIGuestWebhook{
		Client:       mgr.GetClient(),
		clientConfig: mgr.GetConfig(),
	}})

	log.Infof("registering mutating webhook with the endpoint %v", PVCSIMutationWebhookPath)

	mgr.GetWebhookServer().Register(PVCSIMutationWebhookPath, &webhook.Admission{Handler: &CSIGuestMutationWebhook{
		Client:       mgr.GetClient(),
		clientConfig: mgr.GetConfig(),
	}})

	log.Info("registering webhooks complete")

	if err = mgr.Start(signals.SetupSignalHandler()); err != nil {
		log.Fatal(err, "unable to run the webhook manager")
	}
}

var _ admission.Handler = &CSIGuestWebhook{}
var _ admission.Handler = &CSIGuestMutationWebhook{}

type CSIGuestWebhook struct {
	client.Client
	clientConfig *rest.Config
}

type CSIGuestMutationWebhook struct {
	client.Client
	clientConfig *rest.Config
}

func (h *CSIGuestWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("PV-CSI validation webhook handler called with request: %s/%s", req.Name, req.Namespace)
	defer log.Debugf("PV-CSI validation webhook handler completed for the request: %s/%s",
		req.Name, req.Namespace)

	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		// Do additional checks only if the previous checks were successful
		if resp.Allowed && featureIsLinkedCloneSupportEnabled {
			admissionResp := validateGuestPVCOperation(ctx, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()
		}
	} else if req.Kind.Kind == "VolumeSnapshotClass" || req.Kind.Kind == "VolumeSnapshot" ||
		req.Kind.Kind == "VolumeSnapshotContent" {
		admissionResp := validateSnapshotOperationGuestRequest(ctx, &req.AdmissionRequest)
		resp.AdmissionResponse = *admissionResp.DeepCopy()
	}
	return
}

func (g *CSIGuestMutationWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("PV-CSI mutation webhook handler called with request: %s/%s", req.Name, req.Namespace)
	defer log.Debugf("PV-CSI mutation webhook handler completed for the request:%s/%s",
		req.Name, req.Namespace)
	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		switch req.Operation {
		case admissionv1.Create:
			return g.mutateNewPVC(ctx, req)
		}
	}
	return
}

func (g *CSIGuestMutationWebhook) mutateNewPVC(ctx context.Context, req admission.Request) admission.Response {
	newPVC := &corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal(req.Object.Raw, newPVC); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	var wasMutated bool
	if featureIsLinkedCloneSupportEnabled {
		if v1.HasAnnotation(newPVC.ObjectMeta, common.AttributeIsLinkedClone) {
			// Set the same label
			if newPVC.Labels == nil {
				newPVC.Labels = make(map[string]string)
			}
			if _, ok := newPVC.Labels[common.AnnKeyLinkedClone]; !ok {
				newPVC.Labels[common.LinkedClonePVCLabel] = newPVC.Annotations[common.AttributeIsLinkedClone]
				wasMutated = true
			}
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
