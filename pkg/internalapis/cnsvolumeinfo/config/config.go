package config

import "embed"

//go:embed cns.vmware.com_cnsvolumeinfoes.yaml
var EmbedCnsVolumeInfoFile embed.FS

const EmbedCnsVolumeInfoFileName = "cns.vmware.com_cnsvolumeinfoes.yaml"

// EmbedCnsDeletedVolumeInfoFile holds the CnsDeletedVolumeInfo CRD manifest.
//
//go:embed cns.vmware.com_cnsdeletedvolumeinfoes.yaml
var EmbedCnsDeletedVolumeInfoFile embed.FS

// EmbedCnsDeletedVolumeInfoFileName is the file name of the CnsDeletedVolumeInfo CRD manifest.
const EmbedCnsDeletedVolumeInfoFileName = "cns.vmware.com_cnsdeletedvolumeinfoes.yaml"
