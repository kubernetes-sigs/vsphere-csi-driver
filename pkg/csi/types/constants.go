package types

const (
	// Name is the name of this CSI SP
	Name = "csi.vsphere.vmware.com"
	// For more information please see
	// https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domain-beta-kubernetes-io-region.
	// LabelRegionFailureDomain is label placed on nodes and PV containing region detail
	LabelRegionFailureDomain = "failure-domain.beta.kubernetes.io/region"
	// LabelZoneFailureDomain is label placed on nodes and PV containing zone detail
	LabelZoneFailureDomain = "failure-domain.beta.kubernetes.io/zone"
)
