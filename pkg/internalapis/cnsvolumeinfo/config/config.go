package config

import "embed"

//go:embed cns.vmware.com_cnsvolumeinfoes.yaml
var EmbedCnsVolumeInfoFile embed.FS

const EmbedCnsVolumeInfoFileName = "cns.vmware.com_cnsvolumeinfoes.yaml"
