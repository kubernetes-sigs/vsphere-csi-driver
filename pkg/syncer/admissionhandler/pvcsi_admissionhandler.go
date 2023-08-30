package admissionhandler

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crConfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	PVCSIValidationWebhookPath            = "/validate"
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
		MetricsBindAddress: metricsBindAddress,
		Port:               webhookPort})
	if err != nil {
		log.Fatal(err, "unable to set up overall controller manager")
	}

	log.Infof("registering validating webhook with the endpoint %v", PVCSIValidationWebhookPath)
	// we should not allow TLS < 1.2
	mgr.GetWebhookServer().TLSMinVersion = PVCSIWebhookTlsMinVersion
	mgr.GetWebhookServer().Register(PVCSIValidationWebhookPath, &webhook.Admission{Handler: &CSIGuestWebhook{
		Client:       mgr.GetClient(),
		clientConfig: mgr.GetConfig(),
	}})

	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		log.Fatal(err, "unable to run the webhook manager")
	}
}

var _ admission.Handler = &CSIGuestWebhook{}

type CSIGuestWebhook struct {
	client.Client
	clientConfig *rest.Config
}

func (h *CSIGuestWebhook) Handle(ctx context.Context, req admission.Request) (resp admission.Response) {
	log := logger.GetLogger(ctx)
	log.Debugf("PV-CSI validation webhook handler called with request: %+v", req)
	defer log.Debugf("PV-CSI validation webhook handler completed for the request: %+v", req)

	resp = admission.Allowed("")
	if req.Kind.Kind == "PersistentVolumeClaim" {
		if featureGateBlockVolumeSnapshotEnabled {
			admissionResp := validatePVC(ctx, &req.AdmissionRequest)
			resp.AdmissionResponse = *admissionResp.DeepCopy()
		}
	} else if req.Kind.Kind == "VolumeSnapshotClass" || req.Kind.Kind == "VolumeSnapshot" ||
		req.Kind.Kind == "VolumeSnapshotContent" {
		admissionResp := validateSnapshotOperationGuestRequest(ctx, &req.AdmissionRequest)
		resp.AdmissionResponse = *admissionResp.DeepCopy()
	}
	return
}
