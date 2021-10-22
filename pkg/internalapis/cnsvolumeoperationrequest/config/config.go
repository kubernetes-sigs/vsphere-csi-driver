package config

import "embed"

//go:embed cns.vmware.com_cnsvolumeoperationrequests.yaml
var EmbedCnsVolumeOperationRequestFile embed.FS

const EmbedCnsVolumeOperationRequestFileName = "cns.vmware.com_cnsvolumeoperationrequests.yaml"
