package types

const (
	// Name is the name of this CSI SP
	Name = "csi.vsphere.vmware.com"

	// VanillaK8SControllerType indicated Vanilla K8S CSI Controller
	VanillaK8SControllerType = "VANILLA"

	// WcpControllerType indicated WCP CSI Controller
	WcpControllerType = "WCP"

	//WcpGuestControllerType indicated WCPGC CSI Controller
	WcpGuestControllerType = "WCPGC"

	// For more information please see
	// https://kubernetes.io/docs/reference/kubernetes-api/labels-annotations-taints/#failure-domain-beta-kubernetes-io-region.
	// LabelRegionFailureDomain is label placed on nodes and PV containing region detail
	LabelRegionFailureDomain = "failure-domain.beta.kubernetes.io/region"
	// LabelZoneFailureDomain is label placed on nodes and PV containing zone detail
	LabelZoneFailureDomain = "failure-domain.beta.kubernetes.io/zone"
)
