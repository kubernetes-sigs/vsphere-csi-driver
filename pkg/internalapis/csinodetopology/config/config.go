package config

import "embed"

//go:embed cns.vmware.com_csinodetopologies.yaml
var EmbedCSINodeTopologyFile embed.FS

const EmbedCSINodeTopologyFileName = "cns.vmware.com_csinodetopologies.yaml"
