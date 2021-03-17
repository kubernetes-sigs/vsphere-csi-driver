package vsphere

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"sigs.k8s.io/vsphere-csi-driver/pkg/csi/service/logger"

	"github.com/davecgh/go-spew/spew"

	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/sts"
	"github.com/vmware/govmomi/vapi/rest"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"

	"sigs.k8s.io/vsphere-csi-driver/pkg/common/config"
)

const vsanDType = "vsanD"
const defaultVCClientTimeoutInMinutes = 5

// IsInvalidCredentialsError returns true if error is of type InvalidLogin
func IsInvalidCredentialsError(err error) bool {
	isInvalidCredentialsError := false
	if soap.IsSoapFault(err) {
		_, isInvalidCredentialsError = soap.ToSoapFault(err).VimFault().(types.InvalidLogin)
	}
	return isInvalidCredentialsError
}

// IsNotFoundError checks if err is the NotFound fault, if yes then returns true else return false
func IsNotFoundError(err error) bool {
	isNotFoundError := false
	if soap.IsSoapFault(err) {
		_, isNotFoundError = soap.ToSoapFault(err).VimFault().(types.NotFound)
	}
	return isNotFoundError
}

// IsManagedObjectNotFound checks if err is the ManagedObjectNotFound fault, if yes then returns true else return false
func IsManagedObjectNotFound(err error) bool {
	isNotFoundError := false
	if soap.IsSoapFault(err) {
		_, isNotFoundError = soap.ToSoapFault(err).VimFault().(types.ManagedObjectNotFound)
	}
	return isNotFoundError
}

// GetCnsKubernetesEntityMetaData creates a CnsKubernetesEntityMetadataObject object from given parameters
func GetCnsKubernetesEntityMetaData(entityName string, labels map[string]string, deleteFlag bool, entityType string, namespace string, clusterID string, referredEntity []cnstypes.CnsKubernetesEntityReference) *cnstypes.CnsKubernetesEntityMetadata {
	// Create new metadata spec
	var newLabels []types.KeyValue
	for labelKey, labelVal := range labels {
		newLabels = append(newLabels, types.KeyValue{
			Key:   labelKey,
			Value: labelVal,
		})
	}

	entityMetadata := &cnstypes.CnsKubernetesEntityMetadata{}
	entityMetadata.EntityName = entityName
	entityMetadata.Delete = deleteFlag
	if labels != nil {
		entityMetadata.Labels = newLabels
	}
	entityMetadata.EntityType = entityType
	entityMetadata.Namespace = namespace
	entityMetadata.ClusterID = clusterID
	entityMetadata.ReferredEntity = referredEntity
	return entityMetadata
}

// GetContainerCluster creates ContainerCluster object from given parameters
func GetContainerCluster(clusterid string, username string, clusterflavor cnstypes.CnsClusterFlavor, clusterdistribution string) cnstypes.CnsContainerCluster {
	return cnstypes.CnsContainerCluster{
		ClusterType:         string(cnstypes.CnsClusterTypeKubernetes),
		ClusterId:           clusterid,
		VSphereUser:         username,
		ClusterFlavor:       string(clusterflavor),
		ClusterDistribution: clusterdistribution,
	}
}

// CreateCnsKuberenetesEntityReference returns an  EntityReference object to which the given entity refers to.
func CreateCnsKuberenetesEntityReference(entityType string, entityName string, namespace string, clusterid string) cnstypes.CnsKubernetesEntityReference {
	return cnstypes.CnsKubernetesEntityReference{
		EntityType: entityType,
		EntityName: entityName,
		Namespace:  namespace,
		ClusterID:  clusterid,
	}
}

// GetVirtualCenterConfig returns VirtualCenterConfig Object created using vSphere Configuration
// specified in the argurment.
func GetVirtualCenterConfig(ctx context.Context, cfg *config.Config) (*VirtualCenterConfig, error) {
	log := logger.GetLogger(ctx)
	var err error
	vCenterIPs, err := GetVcenterIPs(cfg) //  make([]string, 0)
	if err != nil {
		return nil, err
	}
	host := vCenterIPs[0]
	port, err := strconv.Atoi(cfg.VirtualCenter[host].VCenterPort)
	if err != nil {
		return nil, err
	}
	var targetDatastoreUrlsForFile []string

	if strings.TrimSpace(cfg.VirtualCenter[host].TargetvSANFileShareDatastoreURLs) != "" {
		targetDatastoreUrlsForFile = strings.Split(cfg.VirtualCenter[host].TargetvSANFileShareDatastoreURLs, ",")
	}

	var vcClientTimeout int
	if cfg.Global.VCClientTimeout == 0 {
		log.Info("Defaulting timeout for vCenter Client to 5 minutes")
		cfg.Global.VCClientTimeout = defaultVCClientTimeoutInMinutes
	}
	if cfg.Global.VCClientTimeout < 0 {
		log.Warnf("Invalid value %v for timeout is specified as vc-client-timeout. Defaulting to %v minutes.",
			cfg.Global.VCClientTimeout, defaultVCClientTimeoutInMinutes)
		cfg.Global.VCClientTimeout = defaultVCClientTimeoutInMinutes
	}
	vcClientTimeout = cfg.Global.VCClientTimeout

	vcCAFile := cfg.Global.CAFile
	vcThumbprint := cfg.Global.Thumbprint

	vcConfig := &VirtualCenterConfig{
		Host:                             host,
		Port:                             port,
		CAFile:                           vcCAFile,
		Thumbprint:                       vcThumbprint,
		Username:                         cfg.VirtualCenter[host].User,
		Password:                         cfg.VirtualCenter[host].Password,
		Insecure:                         cfg.VirtualCenter[host].InsecureFlag,
		TargetvSANFileShareDatastoreURLs: targetDatastoreUrlsForFile,
		VCClientTimeout:                  vcClientTimeout,
	}

	if strings.TrimSpace(cfg.VirtualCenter[host].Datacenters) != "" {
		vcConfig.DatacenterPaths = strings.Split(cfg.VirtualCenter[host].Datacenters, ",")
		for idx := range vcConfig.DatacenterPaths {
			vcConfig.DatacenterPaths[idx] = strings.TrimSpace(vcConfig.DatacenterPaths[idx])
		}
	}

	// validate if target file volume datastores present are vsan datastores
	for idx := range vcConfig.TargetvSANFileShareDatastoreURLs {
		vcConfig.TargetvSANFileShareDatastoreURLs[idx] = strings.TrimSpace(vcConfig.TargetvSANFileShareDatastoreURLs[idx])
		if vcConfig.TargetvSANFileShareDatastoreURLs[idx] == "" {
			return nil, errors.New("Invalid datastore URL specified in targetvSANFileShareDatastoreURLs")
		}
		if !strings.HasPrefix(vcConfig.TargetvSANFileShareDatastoreURLs[idx], "ds:///vmfs/volumes/vsan:") {
			err = errors.New("Non vSAN datastore specified for targetvSANFileShareDatastoreURLs")
			return nil, err
		}
	}
	return vcConfig, nil
}

// GetVcenterIPs returns list of vCenter IPs from VSphereConfig
func GetVcenterIPs(cfg *config.Config) ([]string, error) {
	var err error
	vCenterIPs := make([]string, 0)
	for key := range cfg.VirtualCenter {
		vCenterIPs = append(vCenterIPs, key)
	}
	if len(vCenterIPs) == 0 {
		err = errors.New("Unable get vCenter Hosts from VSphereConfig")
	}
	return vCenterIPs, err
}

// GetLabelsMapFromKeyValue creates a  map object from given parameter
func GetLabelsMapFromKeyValue(labels []types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// CompareKubernetesMetadata compares the whole CnsKubernetesEntityMetadata from two given parameters
func CompareKubernetesMetadata(ctx context.Context, k8sMetaData *cnstypes.CnsKubernetesEntityMetadata, cnsMetaData *cnstypes.CnsKubernetesEntityMetadata) bool {
	log := logger.GetLogger(ctx)
	log.Debugf("CompareKubernetesMetadata called with k8spvMetaData: %+v \n and cnsMetaData: %+v \n", spew.Sdump(k8sMetaData), spew.Sdump(cnsMetaData))
	if (k8sMetaData.EntityName != cnsMetaData.EntityName) || (k8sMetaData.Delete != cnsMetaData.Delete) || (k8sMetaData.Namespace != cnsMetaData.Namespace) {
		return false
	}
	labelsMatch := reflect.DeepEqual(GetLabelsMapFromKeyValue(k8sMetaData.Labels), GetLabelsMapFromKeyValue(cnsMetaData.Labels))
	log.Debugf("CompareKubernetesMetadata - labelsMatch returned: %v for k8spvMetaData: %+v \n and cnsMetaData: %+v \n", labelsMatch, spew.Sdump(GetLabelsMapFromKeyValue(k8sMetaData.Labels)), spew.Sdump(GetLabelsMapFromKeyValue(cnsMetaData.Labels)))
	return labelsMatch
}

// Signer decodes the certificate and private key and returns SAML token needed for authentication
func signer(ctx context.Context, client *vim25.Client, username string, password string) (*sts.Signer, error) {
	pemBlock, _ := pem.Decode([]byte(username))
	if pemBlock == nil {
		return nil, nil
	}
	certificate, err := tls.X509KeyPair([]byte(username), []byte(password))
	if err != nil {
		return nil, fmt.Errorf("failed to load X509 key pair. Error: %+v", err)
	}
	tokens, err := sts.NewClient(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create STS client. err: %+v", err)
	}
	req := sts.TokenRequest{
		Certificate: &certificate,
		Delegatable: true,
	}
	signer, err := tokens.Issue(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to issue SAML token. err: %+v", err)
	}
	return signer, nil
}

// GetTagManager returns tagManager connected to given VirtualCenter
func GetTagManager(ctx context.Context, vc *VirtualCenter) (*tags.Manager, error) {
	// validate input
	if vc == nil || vc.Client == nil || vc.Client.Client == nil {
		return nil, fmt.Errorf("vCenter not initialized")
	}
	restClient := rest.NewClient(vc.Client.Client)
	signer, err := signer(ctx, vc.Client.Client, vc.Config.Username, vc.Config.Password)
	if err != nil {
		return nil, fmt.Errorf("Failed to create the Signer. Error: %v", err)
	}
	if signer == nil {
		user := url.UserPassword(vc.Config.Username, vc.Config.Password)
		err = restClient.Login(ctx, user)
	} else {
		err = restClient.LoginByToken(restClient.WithSigner(ctx, signer))
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to login for the rest client. Error: %v", err)
	}
	tagManager := tags.NewManager(restClient)
	if tagManager == nil {
		return nil, fmt.Errorf("failed to create a tagManager")
	}
	return tagManager, nil
}

// GetCandidateDatastoresInCluster gets the shared datastores and vSAN-direct managed datastores of given VC cluster
// The 1st output parameter will be shared datastores
// The 2nd output parameter will be vSAN-direct managed datastores
func GetCandidateDatastoresInCluster(ctx context.Context, vc *VirtualCenter, clusterID string) ([]*DatastoreInfo, []*DatastoreInfo, error) {
	// get all the vsan direct datastore urls in this VC; and later filter in this cluster
	allVsanDirectUrls, err := getVsanDirectDatastores(ctx, vc, clusterID)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get vSAN Direct VMFS datastores. Err: %+v", err)
	}

	// find datastores shared across all hosts in given cluster
	hosts, err := vc.GetHostsByCluster(ctx, clusterID)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to get hosts from VC. Err: %+v", err)
	}
	if len(hosts) == 0 {
		return nil, nil, fmt.Errorf("Empty List of hosts returned from VC")
	}
	sharedDatastores := make([]*DatastoreInfo, 0)
	vsanDirectDatastores := make([]*DatastoreInfo, 0)
	for index, host := range hosts {
		accessibleDatastores, err := host.GetAllAccessibleDatastores(ctx)
		if err != nil {
			return nil, nil, err
		}
		if index == 0 {
			for _, accessibleDs := range accessibleDatastores {
				if allVsanDirectUrls[accessibleDs.Info.Url] {
					vsanDirectDatastores = append(vsanDirectDatastores, accessibleDs)
				} else {
					sharedDatastores = append(sharedDatastores, accessibleDs)
				}
			}
		} else {
			var sharedAccessibleDatastores []*DatastoreInfo
			for _, accessibleDs := range accessibleDatastores {
				if allVsanDirectUrls[accessibleDs.Info.Url] {
					vsanDirectDatastores = append(vsanDirectDatastores, accessibleDs)
					continue
				}
				// Intersect sharedDatastores with accessibleDatastores
				for _, sharedDs := range sharedDatastores {
					// Intersection is performed based on the datastoreUrl as this uniquely identifies the datastore.
					if sharedDs.Info.Url == accessibleDs.Info.Url {
						sharedAccessibleDatastores = append(sharedAccessibleDatastores, sharedDs)
						break
					}
				}
			}
			sharedDatastores = sharedAccessibleDatastores
		}
	}
	if len(sharedDatastores) == 0 && len(vsanDirectDatastores) == 0 {
		return nil, nil, fmt.Errorf("No candidates datastores found in the Kubernetes cluster")
	}
	return sharedDatastores, vsanDirectDatastores, nil
}

// getVsanDirectDatastores returns the datastore URLs of all the vSAN-Direct managed datatores in the
// given VirtualCenter
func getVsanDirectDatastores(ctx context.Context, vc *VirtualCenter, clusterID string) (map[string]bool, error) {
	log := logger.GetLogger(ctx)
	var datastores = make(map[string]bool)

	// get all datastores in this cluster
	datastoreInfos, err := vc.GetDatastoresByCluster(ctx, clusterID)
	if err != nil {
		log.Warnf("Not able to fetch datastores in cluster %q. Err: %v", clusterID, err)
		return nil, err
	}

	// filter them by datastore type of vsanD
	for _, dsInfo := range datastoreInfos {
		dsURL, dsType, err := dsInfo.GetDatastoreURLAndType(ctx)
		if err != nil {
			log.Errorf("Not able to find datastore type and url for %s. Err: %v", dsInfo.Reference().Value, err)
			return nil, err
		}
		if dsType == vsanDType {
			datastores[dsURL] = true
		}
	}
	return datastores, nil
}

// GetDatastoreInfoByURL returns info of a datastore found in given cluster whose URL matches the specified datastore URL
func GetDatastoreInfoByURL(ctx context.Context, vc *VirtualCenter, clusterID, dsURL string) (*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	// get all datastores in this cluster
	datastoreInfos, err := vc.GetDatastoresByCluster(ctx, clusterID)
	if err != nil {
		log.Warnf("Not able to fetch datastores in cluster %q. Err: %v", clusterID, err)
		return nil, err
	}
	for _, dsInfo := range datastoreInfos {
		if dsInfo.Info.Url == dsURL {
			return dsInfo, nil
		}
	}
	return nil, fmt.Errorf("datastore corresponding to URL %v not found in cluster %v", dsURL, clusterID)
}

// isVsan67u3Release returns true if it is vSAN 67u3 Release of vCenter.
func isVsan67u3Release(ctx context.Context, m *defaultVirtualCenterManager, host string) (bool, error) {
	log := logger.GetLogger(ctx)
	log.Debug("Checking if vCenter version is of vsan 67u3 release")
	vc, err := m.GetVirtualCenter(ctx, host)
	if err != nil || vc == nil {
		log.Errorf("failed to get vcenter version. Err: %v", err)
		return false, err
	}
	log.Debugf("vCenter version is :%q", vc.Client.Version)
	return vc.Client.Version == cns.ReleaseVSAN67u3, nil
}
