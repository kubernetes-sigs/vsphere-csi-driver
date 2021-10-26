package config

import "embed"

//go:embed cns.vmware.com_cnsnodevmattachments.yaml
var EmbedCnsNodeVmAttachmentCRFile embed.FS

const EmbedCnsNodeVmAttachmentCRFileName = "cns.vmware.com_cnsnodevmattachments.yaml"

//go:embed cns.vmware.com_cnsvolumemetadata.yaml
var EmbedCnsVolumeMetadataCRFile embed.FS

const EmbedCnsVolumeMetadataCRFileName = "cns.vmware.com_cnsvolumemetadata.yaml"

//go:embed cnsfileaccessconfig_crd.yaml
var EmbedCnsFileAccessConfigCRFile embed.FS

const EmbedCnsFileAccessConfigCRFileName = "cnsfileaccessconfig_crd.yaml"

//go:embed cnsregistervolume_crd.yaml
var EmbedCnsRegisterVolumeCRFile embed.FS

const EmbedCnsRegisterVolumeCRFileName = "cnsregistervolume_crd.yaml"
