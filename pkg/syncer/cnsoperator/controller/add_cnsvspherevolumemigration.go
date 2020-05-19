package controller

import (
	"sigs.k8s.io/vsphere-csi-driver/pkg/syncer/cnsoperator/controller/cnsvspherevolumemigration"
)

func init() {
	// AddToManagerFuncsForVanillaFlavor is a list of functions to add all Controllers to the Manager
	// for vanilla flavor cluster
	AddToManagerFuncsForVanillaFlavor = append(AddToManagerFuncsForVanillaFlavor, cnsvspherevolumemigration.Add)
}
