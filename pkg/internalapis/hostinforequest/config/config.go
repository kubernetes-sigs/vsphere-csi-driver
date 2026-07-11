package config

import "embed"

//go:embed cns.vmware.com_hostinforequests.yaml
var EmbedHostInfoRequestFile embed.FS

const EmbedHostInfoRequestFileName = "cns.vmware.com_hostinforequests.yaml"
