package persistentvolumeclaim

import (
	"context"
	"fmt"

	byokv1 "github.com/vmware-tanzu/vm-operator/external/byok/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/crypto"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

// EncryptionClassToPersistentVolumeClaimMapper returns a mapper function used to
// enqueue reconcile requests for PVCs in response to an event on the
// EncryptionClass resource.
func EncryptionClassToPersistentVolumeClaimMapper(
	ctx context.Context,
	k8sClient client.Client) handler.MapFunc {

	if ctx == nil {
		panic("context is nil")
	}
	if k8sClient == nil {
		panic("k8sClient is nil")
	}

	// For a given EncryptionClass, return reconcile requests for PVCs that
	// specify the same EncryptionClass.
	return func(ctx context.Context, o client.Object) []reconcile.Request {
		if ctx == nil {
			panic("context is nil")
		}
		if o == nil {
			panic("object is nil")
		}
		obj, ok := o.(*byokv1.EncryptionClass)
		if !ok {
			panic(fmt.Sprintf("object is %T", o))
		}

		log := logger.GetLogger(ctx).
			With("name", o.GetName(), "namespace", o.GetNamespace())

		log.Info("Reconciling all PVCs referencing an EncryptionClass")

		// Find all PVC resources that reference this EncryptionClass.
		pvcList := &corev1.PersistentVolumeClaimList{}
		if err := k8sClient.List(
			ctx,
			pvcList,
			client.InNamespace(obj.Namespace)); err != nil {

			if !apierrors.IsNotFound(err) {
				log.Error(
					err,
					"Failed to list PersistentVolumeClaims for "+
						"reconciliation due to EncryptionClass watch")
			}
			return nil
		}

		// Populate reconcile requests for PVCs that reference this
		// EncryptionClass.
		var requests []reconcile.Request
		for i := range pvcList.Items {
			pvc := &pvcList.Items[i]
			encClassName := crypto.GetEncryptionClassNameForPVC(pvc)
			if encClassName == obj.Name {
				requests = append(
					requests,
					reconcile.Request{
						NamespacedName: client.ObjectKey{
							Namespace: pvc.Namespace,
							Name:      pvc.Name,
						},
					})
			}
		}

		if len(requests) > 0 {
			log.Info(
				"Reconciling PVCs due to EncryptionClass watch",
				"requests", requests)
		}

		return requests
	}
}
