package admissionhandler

import (
	"context"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func validatePVCAnnotation(ctx context.Context, request admission.Request) admission.Response {
	//log := logger.GetLogger(ctx)

	//if request.Operation == admissionv1.Create {
	//
	//} else if request.Operation == admissionv1.Update {
	//
	//} else if request.Operation == admissionv1.Delete {
	//
	//}

	return admission.Allowed("dummy webhook handler always allow any request")
}
