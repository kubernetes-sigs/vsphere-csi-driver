package config

import "embed"

//go:embed cns.vmware.com_cnscsisvfeaturestates.yaml
var EmbedCnsCsiSvFeatureStatesCRFile embed.FS

const EmbedCnsCsiSvFeatureStatesCRFileName = "cns.vmware.com_cnscsisvfeaturestates.yaml"
