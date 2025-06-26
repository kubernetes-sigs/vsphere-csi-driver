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

	"github.com/davecgh/go-spew/spew"
	"github.com/vmware/govmomi/cns"
	cnstypes "github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/sts"
	"github.com/vmware/govmomi/vapi/rest"
	"github.com/vmware/govmomi/vapi/tags"
	"github.com/vmware/govmomi/vim25"
	"github.com/vmware/govmomi/vim25/soap"
	"github.com/vmware/govmomi/vim25/types"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
)

const (
	vsanDType                = "vsanD"
	cnsMgrDatastoreSuspended = "cns.vmware.com/datastoreSuspended"
	// VSphere70u3Version is a 3 digit value to indicate the minimum vSphere
	// version to use query volume async API.
	VSphere70u3Version int = 703
	// VSphere80u3Version is a 3 digit value to indicate the minimum vSphere
	// version to ensure calling supported 8.0u3 APIs
	VSphere80u3Version int = 803
)

var (
	// ErrNotSupported represents not supported error.
	ErrNotSupported = errors.New("not supported")
)

// IsNotFoundError checks if err is the NotFound fault.
func IsNotFoundError(err error) bool {
	isNotFoundError := false
	if soap.IsSoapFault(err) {
		_, isNotFoundError = soap.ToSoapFault(err).VimFault().(types.NotFound)
	}
	return isNotFoundError
}

// IsAlreadyExists checks if err is the AlreadyExists fault.
// If the error is AlreadyExists fault, the method returns true along with the
// name of the managed object. Otherwise, returns false.
func IsAlreadyExists(err error) (bool, string) {
	isAlreadyExistsError := false
	objectName := ""
	if soap.IsSoapFault(err) {
		_, isAlreadyExistsError = soap.ToSoapFault(err).VimFault().(types.AlreadyExists)
		if isAlreadyExistsError {
			objectName = soap.ToSoapFault(err).VimFault().(types.AlreadyExists).Name
		}
	}
	return isAlreadyExistsError, objectName
}

// IsManagedObjectNotFound checks if err is the ManagedObjectNotFound fault.
// Returns true, if 'err' is a MnagedObjectNotFound fault for the intended
// 'moRef' object. Otherwise, return false.
func IsManagedObjectNotFound(err error, moRef types.ManagedObjectReference) bool {
	if soap.IsSoapFault(err) {
		fault, isNotFoundError := soap.ToSoapFault(err).VimFault().(types.ManagedObjectNotFound)
		return isNotFoundError && fault.Obj.Type == moRef.Type && fault.Obj.Value == moRef.Value
	}
	return false
}

func IsInvalidArgumentError(err error) bool {
	isInvalidArgumentError := false
	if soap.IsVimFault(err) {
		_, isInvalidArgumentError = soap.ToVimFault(err).(*types.InvalidArgument)
	}
	return isInvalidArgumentError
}

func IsVimFaultNotFoundError(err error) bool {
	isNotFoundError := false
	if soap.IsVimFault(err) {
		_, isNotFoundError = soap.ToVimFault(err).(*types.NotFound)
	}
	return isNotFoundError
}

// IsCnsSnapshotCreatedFaultError checks if err is the CnsSnapshotCreatedFault fault returned by
// CNS CreateSnapshots API. This fault is returned by CNS in case snapshot creation is successful,
// but post-processing failed (like update db failed).
func IsCnsSnapshotCreatedFaultError(err error) bool {
	isCnsSnapshotCreatedFaultError := false
	if soap.IsVimFault(err) {
		_, isCnsSnapshotCreatedFaultError = soap.ToVimFault(err).(*cnstypes.CnsSnapshotCreatedFault)
	}
	return isCnsSnapshotCreatedFaultError
}

// IsCnsSnapshotNotFoundError checks if err is the CnsSnapshotNotFoundFault fault returned by CNS QuerySnapshots API
func IsCnsSnapshotNotFoundError(err error) bool {
	isCnsSnapshotNotFoundError := false
	if soap.IsVimFault(err) {
		_, isCnsSnapshotNotFoundError = soap.ToVimFault(err).(*cnstypes.CnsSnapshotNotFoundFault)
	}
	return isCnsSnapshotNotFoundError
}

// GetCnsKubernetesEntityMetaData creates a CnsKubernetesEntityMetadataObject
// object from given parameters.
func GetCnsKubernetesEntityMetaData(entityName string, labels map[string]string,
	deleteFlag bool, entityType string, namespace string, clusterID string,
	referredEntity []cnstypes.CnsKubernetesEntityReference) *cnstypes.CnsKubernetesEntityMetadata {
	// Create new metadata spec.
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

// GetContainerCluster creates ContainerCluster object from given parameters.
func GetContainerCluster(clusterid string, username string, clusterflavor cnstypes.CnsClusterFlavor,
	clusterdistribution string) cnstypes.CnsContainerCluster {
	return cnstypes.CnsContainerCluster{
		ClusterType:         string(cnstypes.CnsClusterTypeKubernetes),
		ClusterId:           clusterid,
		VSphereUser:         username,
		ClusterFlavor:       string(clusterflavor),
		ClusterDistribution: clusterdistribution,
	}
}

// CreateCnsKuberenetesEntityReference returns an EntityReference object to
// which the given entity refers to.
func CreateCnsKuberenetesEntityReference(entityType string, entityName string,
	namespace string, clusterid string) cnstypes.CnsKubernetesEntityReference {
	return cnstypes.CnsKubernetesEntityReference{
		EntityType: entityType,
		EntityName: entityName,
		Namespace:  namespace,
		ClusterID:  clusterid,
	}
}

// GetVirtualCenterConfig returns VirtualCenterConfig Object created using
// vSphere Configuration specified in the argument.
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

	var targetvSANClustersForFile []string
	if strings.TrimSpace(cfg.VirtualCenter[host].TargetvSANFileShareClusters) != "" {
		targetvSANClustersForFile = strings.Split(cfg.VirtualCenter[host].TargetvSANFileShareClusters, ",")
	}

	vcCAFile := cfg.Global.CAFile
	vcThumbprint := cfg.Global.Thumbprint

	vcConfig := &VirtualCenterConfig{
		Host:                        host,
		Port:                        port,
		CAFile:                      vcCAFile,
		Thumbprint:                  vcThumbprint,
		Username:                    cfg.VirtualCenter[host].User,
		Password:                    cfg.VirtualCenter[host].Password,
		Insecure:                    cfg.VirtualCenter[host].InsecureFlag,
		TargetvSANFileShareClusters: targetvSANClustersForFile,
		QueryLimit:                  cfg.Global.QueryLimit,
		ListVolumeThreshold:         cfg.Global.ListVolumeThreshold,
		MigrationDataStoreURL:       cfg.VirtualCenter[host].MigrationDataStoreURL,
		FileVolumeActivated:         cfg.VirtualCenter[host].FileVolumeActivated,
	}

	log.Debugf("Setting the queryLimit = %v, ListVolumeThreshold = %v", vcConfig.QueryLimit, vcConfig.ListVolumeThreshold)
	if strings.TrimSpace(cfg.VirtualCenter[host].Datacenters) != "" {
		vcConfig.DatacenterPaths = strings.Split(cfg.VirtualCenter[host].Datacenters, ",")
		for idx := range vcConfig.DatacenterPaths {
			vcConfig.DatacenterPaths[idx] = strings.TrimSpace(vcConfig.DatacenterPaths[idx])
		}
	}

	return vcConfig, nil
}

// GetVirtualCenterConfigs returns VirtualCenterConfig Objects created using
// vSphere Configuration specified in the argument.
func GetVirtualCenterConfigs(ctx context.Context, cfg *config.Config) ([]*VirtualCenterConfig, error) {
	log := logger.GetLogger(ctx)
	var err error
	VirtualCenterConfigs := make([]*VirtualCenterConfig, 0)
	vCenterIPs, err := GetVcenterIPs(cfg)
	if err != nil {
		return nil, err
	}
	for _, vCenterIP := range vCenterIPs {
		port, err := strconv.Atoi(cfg.VirtualCenter[vCenterIP].VCenterPort)
		if err != nil {
			return nil, err
		}

		var targetvSANClustersForFile []string
		if strings.TrimSpace(cfg.VirtualCenter[vCenterIP].TargetvSANFileShareClusters) != "" {
			targetvSANClustersForFile = strings.Split(cfg.VirtualCenter[vCenterIP].TargetvSANFileShareClusters, ",")
		}

		vcConfig := &VirtualCenterConfig{
			Host:                        vCenterIP,
			Port:                        port,
			CAFile:                      cfg.VirtualCenter[vCenterIP].CAFile,
			Thumbprint:                  cfg.VirtualCenter[vCenterIP].Thumbprint,
			Username:                    cfg.VirtualCenter[vCenterIP].User,
			Password:                    cfg.VirtualCenter[vCenterIP].Password,
			Insecure:                    cfg.VirtualCenter[vCenterIP].InsecureFlag,
			TargetvSANFileShareClusters: targetvSANClustersForFile,
			QueryLimit:                  cfg.Global.QueryLimit,
			ListVolumeThreshold:         cfg.Global.ListVolumeThreshold,
			FileVolumeActivated:         cfg.VirtualCenter[vCenterIP].FileVolumeActivated,
		}
		if vcConfig.CAFile == "" {
			vcConfig.CAFile = cfg.Global.CAFile
		}
		if vcConfig.Thumbprint == "" {
			vcConfig.Thumbprint = cfg.Global.Thumbprint
		}
		log.Debugf("Setting the queryLimit = %v, ListVolumeThreshold = %v", vcConfig.QueryLimit, vcConfig.ListVolumeThreshold)
		if strings.TrimSpace(cfg.VirtualCenter[vCenterIP].Datacenters) != "" {
			vcConfig.DatacenterPaths = strings.Split(cfg.VirtualCenter[vCenterIP].Datacenters, ",")
			for idx := range vcConfig.DatacenterPaths {
				vcConfig.DatacenterPaths[idx] = strings.TrimSpace(vcConfig.DatacenterPaths[idx])
			}
		}
		VirtualCenterConfigs = append(VirtualCenterConfigs, vcConfig)
	}
	return VirtualCenterConfigs, nil
}

// GetVcenterIPs returns list of vCenter IPs from VSphereConfig.
func GetVcenterIPs(cfg *config.Config) ([]string, error) {
	var err error
	vCenterIPs := make([]string, 0)
	for key := range cfg.VirtualCenter {
		vCenterIPs = append(vCenterIPs, key)
	}
	if len(vCenterIPs) == 0 {
		err = errors.New("unable get vCenter Hosts from VSphereConfig")
	}
	return vCenterIPs, err
}

// GetLabelsMapFromKeyValue creates a  map object from given parameter.
func GetLabelsMapFromKeyValue(labels []types.KeyValue) map[string]string {
	labelsMap := make(map[string]string)
	for _, label := range labels {
		labelsMap[label.Key] = label.Value
	}
	return labelsMap
}

// CompareKubernetesMetadata compares the whole CnsKubernetesEntityMetadata
// from two given parameters.
func CompareKubernetesMetadata(ctx context.Context, k8sMetaData *cnstypes.CnsKubernetesEntityMetadata,
	cnsMetaData *cnstypes.CnsKubernetesEntityMetadata) bool {
	log := logger.GetLogger(ctx)
	log.Debugf("CompareKubernetesMetadata called with k8spvMetaData: %+v\n and cnsMetaData: %+v\n",
		spew.Sdump(k8sMetaData), spew.Sdump(cnsMetaData))
	if (k8sMetaData.EntityName != cnsMetaData.EntityName) || (k8sMetaData.Delete != cnsMetaData.Delete) ||
		(k8sMetaData.Namespace != cnsMetaData.Namespace) {
		return false
	}
	labelsMatch := reflect.DeepEqual(GetLabelsMapFromKeyValue(k8sMetaData.Labels),
		GetLabelsMapFromKeyValue(cnsMetaData.Labels))
	log.Debugf("CompareKubernetesMetadata - labelsMatch returned: %v for k8spvMetaData: %+v\n and cnsMetaData: %+v\n",
		labelsMatch, spew.Sdump(GetLabelsMapFromKeyValue(k8sMetaData.Labels)),
		spew.Sdump(GetLabelsMapFromKeyValue(cnsMetaData.Labels)))
	return labelsMatch
}

// Signer decodes the certificate and private key and returns SAML token needed
// for authentication.
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

// GetTagManager returns tagManager connected to given VirtualCenter.
func GetTagManager(ctx context.Context, vc *VirtualCenter) (*tags.Manager, error) {
	log := logger.GetLogger(ctx)
	// Validate input.
	if vc == nil || vc.Client == nil || vc.Client.Client == nil {
		return nil, fmt.Errorf("vCenter not initialized")
	}

	restClient := rest.NewClient(vc.Client.Client)
	signer, err := signer(ctx, vc.Client.Client, vc.Config.Username, vc.Config.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to create the Signer. Error: %v", err)
	}
	if signer == nil {
		user := url.UserPassword(vc.Config.Username, vc.Config.Password)
		err = restClient.Login(ctx, user)
	} else {
		err = restClient.LoginByToken(restClient.WithSigner(ctx, signer))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to login for the rest client. Error: %v", err)
	}
	tagManager := tags.NewManager(restClient)
	if tagManager == nil {
		return nil, fmt.Errorf("failed to create a tagManager")
	}
	log.Infof("New tag manager with useragent '%s'", tagManager.UserAgent)
	return tagManager, nil
}

// GetCandidateDatastoresInClusters gets the shared datastores and vSAN-direct
// managed datastores of given VC clusters from GetCandidateDatastoresInCluster and
// returns a map of clusterID -> array of datastores
func GetCandidateDatastoresInClusters(ctx context.Context, vc *VirtualCenter, clusterIDs []string,
	includevSANDirectDatastores bool) map[string][]*DatastoreInfo {
	log := logger.GetLogger(ctx)

	clusterIDToDSs := make(map[string][]*DatastoreInfo)
	for _, clusterID := range clusterIDs {
		sharedDSs, vsanDirectDSs, err := GetCandidateDatastoresInCluster(ctx, vc, clusterID, includevSANDirectDatastores)
		if err != nil {
			log.Warnf("Getting datastores for the cluster %s failed - err: %s", clusterID, err)
			continue
		}

		clusterIDToDSs[clusterID] = append(sharedDSs, vsanDirectDSs...)
	}

	return clusterIDToDSs
}

// GetCandidateDatastoresInCluster gets the shared datastores and vSAN-direct
// managed datastores of given VC cluster.
// The 1st output parameter will be shared datastores.
// The 2nd output parameter will be vSAN-direct managed datastores.
// NOTE: The second output will be an empty list if `includevSANDirectDatastores` is set to false.
func GetCandidateDatastoresInCluster(ctx context.Context, vc *VirtualCenter, clusterID string,
	includevSANDirectDatastores bool) ([]*DatastoreInfo, []*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)

	// Find datastores shared across all hosts in given cluster.
	hosts, err := vc.GetHostsByCluster(ctx, clusterID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get hosts from VC. Err: %+v", err)
	}
	if len(hosts) == 0 {
		return nil, nil, fmt.Errorf("empty List of hosts returned from VC")
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
				var dsType string
				if includevSANDirectDatastores {
					_, dsType, err = accessibleDs.GetDatastoreURLAndType(ctx)
					if err != nil {
						return nil, nil, logger.LogNewErrorf(log,
							"Unable to find datastore type and URL for %q. Error: %+v",
							accessibleDs.Reference().Value, err)
					}
				}
				if dsType == vsanDType {
					vsanDirectDatastores = append(vsanDirectDatastores, accessibleDs)
				} else {
					sharedDatastores = append(sharedDatastores, accessibleDs)
				}
			}
		} else {
			var sharedAccessibleDatastores []*DatastoreInfo
			for _, accessibleDs := range accessibleDatastores {
				var dsType string
				if includevSANDirectDatastores {
					_, dsType, err = accessibleDs.GetDatastoreURLAndType(ctx)
					if err != nil {
						return nil, nil, logger.LogNewErrorf(log,
							"Unable to find datastore type and URL for %q. Error: %+v",
							accessibleDs.Reference().Value, err)
					}
				}
				if dsType == vsanDType {
					vsanDirectDatastores = append(vsanDirectDatastores, accessibleDs)
					continue
				}
				// Intersect sharedDatastores with accessibleDatastores.
				for _, sharedDs := range sharedDatastores {
					// Intersection is performed based on the datastoreUrl as this
					// uniquely identifies the datastore.
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
		return nil, nil, fmt.Errorf("no candidates datastores found in the Kubernetes cluster")
	}
	log.Debugf("Found shared datastores: %+v and vSAN Direct datastores: %+v", sharedDatastores,
		vsanDirectDatastores)
	return sharedDatastores, vsanDirectDatastores, nil
}

// GetDatastoreInfoByURL returns info of a datastore found in given cluster
// whose URL matches the specified datastore URL.
// TODO: optimise this by caching the datastore URL to clusterID mapping or
// adding the clusterID as part of the CRD.
func GetDatastoreInfoByURL(ctx context.Context, vc *VirtualCenter,
	clusterIDs []string, dsURL string) (*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	for _, clusterID := range clusterIDs {
		// Get all datastores in this cluster.
		datastoreInfos, err := vc.GetDatastoresByCluster(ctx, clusterID)
		if err != nil {
			log.Warnf("Not able to fetch datastores in cluster %q. Err: %v", clusterID, err)
			continue
		}

		for _, dsInfo := range datastoreInfos {
			if dsInfo.Info.Url == dsURL {
				return dsInfo, nil
			}
		}

		log.Debugf("datastore corresponding to URL %s not found in cluster %s", dsURL, clusterID)
	}

	return nil, fmt.Errorf("datastore corresponding to URL %v not found in any cluster", dsURL)
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

// IsvSphereVersion70U3orAbove checks if specified version is 7.0 Update 3 or
// higher. The method takes aboutInfo as input which contains details about
// VC version, build number and so on. If the version is 7.0 Update 3 or higher,
// returns true, else returns false along with appropriate errors for the failure.
func IsvSphereVersion70U3orAbove(ctx context.Context, aboutInfo types.AboutInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	items := strings.Split(aboutInfo.Version, ".")
	version := strings.Join(items[:], "")
	// Convert version string to int: e.g. "7.0.3" to 703, "7.0.3.1" to 703.
	if len(version) >= 3 {
		vSphereVersionInt, err := strconv.Atoi(version[0:3])
		if err != nil {
			return false, logger.LogNewErrorf(log, "error while converting version %q to integer, err %+v", version, err)
		}
		// Check if the current vSphere version is 7.0.3 or higher.
		if vSphereVersionInt >= VSphere70u3Version {
			return true, nil
		}
	}
	// For all other versions.
	return false, nil
}

// IsvSphereVersion80U3orAbove checks if specified version is 8.0 Update 3 or
// higher. The method takes aboutInfo as input which contains details about
// VC version, build number and so on. If the version is 8.0 Update 3 or higher,
// returns true, else returns false along with appropriate errors for the failure.
func IsvSphereVersion80U3orAbove(ctx context.Context, aboutInfo types.AboutInfo) (bool, error) {
	log := logger.GetLogger(ctx)
	items := strings.Split(aboutInfo.Version, ".")
	version := strings.Join(items[:], "")
	// Convert version string to int: e.g. "8.0.3" to 803, "8.0.3.1" to 803.
	if len(version) >= 3 {
		vSphereVersionInt, err := strconv.Atoi(version[0:3])
		if err != nil {
			return false, logger.LogNewErrorf(log, "error while converting version %q to integer, err %+v", version, err)
		}
		// Check if the current vSphere version is 8.0.3 or higher.
		if vSphereVersionInt >= VSphere80u3Version {
			return true, nil
		}
	}
	// For all other versions.
	return false, nil
}

// IsVolumeCreationSuspended checks whether a given Datastore has cns.vmware.com/datastoreSuspended customValue
func IsVolumeCreationSuspended(ctx context.Context, datastoreInfo *DatastoreInfo) bool {
	log := logger.GetLogger(ctx)
	for _, customValField := range datastoreInfo.CustomValues {
		customVal := customValField.(*types.CustomFieldStringValue)
		if customVal.Value == cnsMgrDatastoreSuspended {
			log.Infof("Ignoring datastore %v as it is suspended. Datastore moref: %v", datastoreInfo.Info.Name,
				datastoreInfo.Datastore.Reference())
			return true
		}
	}
	return false
}

// FilterSuspendedDatastores filters out datastores which cns.vmware.com/datastoreSuspended customValue
func FilterSuspendedDatastores(ctx context.Context, datastoreInfoList []*DatastoreInfo) ([]*DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var filteredList []*DatastoreInfo
	for _, ds := range datastoreInfoList {
		if !IsVolumeCreationSuspended(ctx, ds) {
			filteredList = append(filteredList, ds)
		}
	}
	if len(filteredList) == 0 {
		return filteredList, logger.LogNewErrorf(log,
			"No datastores are available after filtering suspended datastores")
	}
	log.Infof("Filtered list of datastores after removing suspended ones are: %+v", filteredList)
	return filteredList, nil
}
